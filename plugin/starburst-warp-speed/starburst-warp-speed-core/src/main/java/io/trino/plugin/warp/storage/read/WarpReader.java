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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static java.util.Objects.requireNonNull;

public class WarpReader
{
    private static final Logger logger = Logger.get(WarpReader.class);
    // The heap size of a worker node on galaxy is 80GB. The total off heap memory limit we take here is 512KB * 64 threads equals 32MB.
    // 1 percentage of the heap size is 800MB, so these 32MB is much less than 1 percentage. It means we are guaranteed the GC will
    // not be blocked by this small off heap memory.
    // The limit check is to make sure we do not accidentally enlarge the off heap allocation
    private static final long LIMIT_OFF_HEAP_MEMORY = 512 * 1024;

    // parameters
    private final ReadTimeMeasurement readTimeMeasurement;
    private final ShapingLogger shapingLogger;

    private final QueryArgs queryArgs;
    private final WarpQueryState queryState;
    private final long rowsLimit;

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
            GlobalConfig globalConfig,
            long rowsLimit)
    {
        this.blocksAggregator = requireNonNull(blocksAggregator);
        this.matcher = requireNonNull(matcher);
        this.rowsLimit = rowsLimit;

        this.queryArgs = blocksAggregator.getQueryArgs(queryParams, customStatsContext);
        this.aggregatorArgs = blocksAggregator.open(queryArgs);
        this.matcherArgs = matcher.open(queryArgs, customStatsContext);

        queryState = new WarpQueryState();

        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());

        checkOffHeapMemoryUsage();
        readTimeMeasurement = new ReadTimeMeasurement();
    }

    public boolean isRowsLimitReached()
    {
        return rowsLimit <= queryState.getTotalNumReadRecords();
    }

    void close()
    {
        blocksAggregator.close(queryArgs);
    }

    // verify total amount of off heap memory allocated does not exceed a limit
    private void checkOffHeapMemoryUsage()
    {
        long totalOffHeapSize = blocksAggregator.getOffHeapMemoryUsage(aggregatorArgs) + matcher.getOffHeapMemoryUsage(queryArgs, matcherArgs);
        if (totalOffHeapSize > LIMIT_OFF_HEAP_MEMORY) {
            shapingLogger.warn("off heap memory exceeded threshold " + totalOffHeapSize);
        }
    }

    /**
     * prepare buffers for filling
     */
    @NativeInterrupt
    private void openPage()
    {
        int pageLimit = (int) Math.min(rowsLimit - queryState.getTotalNumReadRecords(), Integer.MAX_VALUE);

        aggregatorPageArgs = blocksAggregator.openPage(queryArgs, aggregatorArgs, queryState, pageLimit);
        try {
            matcherPageArgs = matcher.openPage(queryArgs, matcherArgs, aggregatorPageArgs);
        }
        catch (Exception e) {
            throw new TrinoException(WARP_MATCH_FAILED, "failed to open match");
        }
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    private boolean prepareBlocks()
    {
        long startTime = readTimeMeasurement.getStartTime();

        if (aggregatorPageArgs == null) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }

        // we continue as long as we didn't reach a limit from the match nor prepare
        while (matcher.match(queryArgs, matcherArgs, matcherPageArgs)) {
            if (!blocksAggregator.prepareBlocks(queryArgs,
                    aggregatorArgs,
                    aggregatorPageArgs,
                    queryState)) {
                break;
            }
        }

        readTimeMeasurement.updateRuntimeMeasurements(startTime, queryArgs);
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
        if (matcherPageArgs != null) {
            matcher.closePage(queryArgs, matcherPageArgs);
            matcherPageArgs = null;
        }

        long readPages = 0;
        if (aggregatorPageArgs != null) {
            queryState.addTotalNumReadRecords(queryState.getNumRecordsInCurPage());
            readPages = blocksAggregator.closePage(queryArgs,
                    aggregatorArgs,
                    aggregatorPageArgs,
                    queryState);
            aggregatorPageArgs = null;
        }

        resetMemory();
        return readPages;
    }

    private void abortPage(Exception e)
    {
        if (matcherPageArgs != null) {
            matcher.abortPage(queryArgs, matcherPageArgs, e);
            matcherPageArgs = null;
        }
        if (aggregatorPageArgs != null) {
            blocksAggregator.abortPage(queryArgs, aggregatorPageArgs, e);
            aggregatorPageArgs = null;
        }
        resetMemory();
    }

    private void resetMemory()
    {
        aggregatorArgs.recordIndexes().resetMemory();
        matcherArgs.matchState().resetMemory();
    }
}
