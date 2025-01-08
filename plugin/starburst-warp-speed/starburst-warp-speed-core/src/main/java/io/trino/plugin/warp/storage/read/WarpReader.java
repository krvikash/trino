/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.storage.read;

import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;

import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static java.util.Objects.requireNonNull;

public class WarpReader
{
    private static final Logger logger = Logger.get(WarpReader.class);

    // parameters
    private final ShapingLogger shapingLogger;
    private final WorkerMemoryManager workerMemoryManager;
    private final QueryArgs queryArgs;
    private final WarpQueryState queryState;
    private final long rowsLimit;
    private final ChunksQueue chunksQueue;
    private ThreadArena pageArena;

    private final AggregatorArgs aggregatorArgs;
    private final BlocksAggregator blocksAggregator;
    private AggregatorPageArgs aggregatorPageArgs;

    private final Matcher matcher;
    private final MatcherArgs matcherArgs;
    private MatcherPageArgs matcherPageArgs;

    WarpReader(QueryParams queryParams,
            CustomStatsContext customStatsContext,
            BlocksAggregator blocksAggregator,
            Matcher matcher,
            WorkerMemoryManager workerMemoryManager,
            GlobalConfig globalConfig,
            int pageSize,
            long rowsLimit)
    {
        this.workerMemoryManager = requireNonNull(workerMemoryManager);
        this.blocksAggregator = requireNonNull(blocksAggregator);
        this.matcher = requireNonNull(matcher);
        this.rowsLimit = rowsLimit;
        this.queryArgs = blocksAggregator.getQueryArgs(queryParams, customStatsContext);
        this.aggregatorArgs = blocksAggregator.open(queryArgs);
        this.matcherArgs = matcher.open(queryArgs, customStatsContext);

        queryState = new WarpQueryState();
        chunksQueue = new ChunksQueue(queryArgs.maxMatchedChunks(), queryArgs.chunkSize(), pageSize);

        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    public boolean isRowsLimitReached()
    {
        return rowsLimit <= queryState.getTotalNumReadRecords();
    }

    void close()
    {
        blocksAggregator.close(queryArgs);
    }

    /**
     * prepare buffers for filling
     */
    @NativeInterrupt
    private void openPage()
    {
        pageArena = workerMemoryManager.getThreadArena();

        // each API call will throw exception if failed
        aggregatorPageArgs = blocksAggregator.openPage(queryArgs,
                pageArena,
                aggregatorArgs,
                queryState,
                (int) Math.min(rowsLimit - queryState.getTotalNumReadRecords(), Integer.MAX_VALUE));

        matcherPageArgs = matcher.openPage(chunksQueue,
                pageArena,
                queryArgs,
                matcherArgs,
                aggregatorPageArgs);
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    private boolean prepareBlocks()
    {
        if (aggregatorPageArgs == null) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }

        // we continue as long as we didn't reach the rowsLimit nor a limit from the matcher or aggregator
        while (queryState.getNumRecordsInCurPage() < rowsLimit - queryState.getTotalNumReadRecords()) {
            if (chunksQueue.isChunkRangeCompleted() && !matcher.match(chunksQueue, queryArgs, matcherArgs, matcherPageArgs)) {
                break;
            }

            if (!blocksAggregator.prepareBlocks(chunksQueue,
                    queryArgs,
                    aggregatorPageArgs,
                    queryState)) {
                break;
            }
        }

        return queryState.getNumRecordsInCurPage() > 0;
    }

    ReadResult getPage()
    {
        try {
            Block[] blocks = new Block[0];
            WarpStoragePageSource.RowRanges ranges = WarpStoragePageSource.RowRanges.EMPTY;

            openPage();
            if (prepareBlocks()) {
                if (queryState.getNumRecordsInCurPage() > rowsLimit - queryState.getTotalNumReadRecords()) {
                    shapingLogger.warn("numRecordsInCurPage is exceeding the limit. numRecordsInCurPage %d totalNumReadRecords %d rowsLimit %d page limit %d",
                            queryState.getNumRecordsInCurPage(),
                            queryState.getTotalNumReadRecords(),
                            rowsLimit,
                            rowsLimit - queryState.getTotalNumReadRecords());
                }

                blocks = blocksAggregator.aggregateBlocks(queryArgs,
                        aggregatorArgs,
                        aggregatorPageArgs,
                        queryState);
                if (queryArgs.queryParams().isRangesRequired()) {
                    ranges = blocksAggregator.getRanges(aggregatorPageArgs);
                }
            }
            long numReadPages = closePage();

            return new ReadResult(blocks, queryState.getNumRecordsInCurPage(), ranges, numReadPages);
        }
        catch (Exception e) {
            abortPage(e);
            throw e;
        }
    }

    @NativeInterrupt
    private long closePage()
    {
        long readPages = 0;

        try {
            chunksQueue.storeMatchBitmaps(queryArgs); // only the bitmaps that were added during this round and not processed are stored

            if (matcherPageArgs != null) {
                matcher.closePage(queryArgs, matcherPageArgs);
            }

            if (aggregatorPageArgs != null) {
                queryState.addTotalNumReadRecords(queryState.getNumRecordsInCurPage());
                readPages = blocksAggregator.closePage(queryArgs,
                        aggregatorArgs,
                        aggregatorPageArgs,
                        queryState,
                        chunksQueue);
            }

            if (pageArena != null) {
                pageArena.close();
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to close page");
        }
        finally {
            pageArena = null;
            matcherPageArgs = null;
            aggregatorPageArgs = null;
        }

        return readPages;
    }

    private void abortPage(Exception e)
    {
        try {
            if (matcherPageArgs != null) {
                matcher.abortPage(queryArgs, matcherPageArgs, e);
            }
            if (aggregatorPageArgs != null) {
                blocksAggregator.abortPage(queryArgs, aggregatorPageArgs, e);
            }
            if (pageArena != null) {
                pageArena.close();
            }
        }
        catch (Exception e2) {
            shapingLogger.error(e2, "failed to abort page");
        }
        finally {
            pageArena = null;
            matcherPageArgs = null;
            aggregatorPageArgs = null;
        }
    }
}
