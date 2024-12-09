package io.trino.plugin.warp.storage.read;
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

import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Provide a PageSource API implemented with the helper class WarpStorageReader.
 */
public class WarpPageSource
        implements WarpStoragePageSource
{
    private static final Logger logger = Logger.get(WarpPageSource.class);
    public static final int INVALID_COL_IX = -1;
    private final ShapingLogger shapingLogger;

    private final StorageEngineConstants storageEngineConstants;
    private final PredicatesCacheService predicatesCacheService;
    private final QueryParams queryParams;
    private final WarpReader reader;
    private boolean finished;
    private boolean closed; // set explicitly by someone calling {@link #close()}
    private RowRanges sortedRowRanges;
    private long completedBytes;
    private long completedPositions;

    public WarpPageSource(StorageEngineConstants storageEngineConstants,
            long rowsLimit,
            QueryParams queryParams,
            PredicatesCacheService predicatesCacheService,
            CustomStatsContext customStatsContext,
            GlobalConfig globalConfig,
            StorageCollectorService storageCollectorService,
            LazyCollectorService lazyCollectorService,
            MatchService matchService)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.predicatesCacheService = predicatesCacheService;
        this.sortedRowRanges = RowRanges.EMPTY;
        this.queryParams = queryParams;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
        boolean useLazyCollect = lazyCollectorService.useLazyCollect(queryParams);
        StorageCollectorService collectorService = useLazyCollect ? lazyCollectorService : storageCollectorService;
        reader = new WarpReader(
                queryParams,
                customStatsContext,
                collectorService,
                matchService,
                globalConfig,
                storageEngineConstants.getPageSize(),
                rowsLimit);
    }

    @Override
    public long getMemoryUsage()
    {
        // TODO: https://expandb.atlassian.net/browse/VDB-4020
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            if (reader != null) {
                reader.close();
            }
            predicatesCacheService.markFinished(queryParams.getPredicateCacheData());
        }
    }

    @Override
    public boolean isFinished()
    {
        return finished || closed;
    }

    @Override
    public Page getNextPage()
    {
        Optional<Block[]> blocks = Optional.empty();
        int currentPositionsCount = 0;
        if (!isFinished()) {
            try {
                ReadResult readResult = reader.getPage();
                if (readResult.numCollectedRows() == 0) {
                    finished = true;
                }
                updateRowsLimit();
                blocks = Optional.of(readResult.blocks());
                sortedRowRanges = readResult.ranges();
                completedBytes += (readResult.numReadPages() << storageEngineConstants.getPageSizeShift());
                completedPositions += readResult.numCollectedRows();
                currentPositionsCount = readResult.numCollectedRows();
            }
            catch (Exception e) {
                close();
                if (!Thread.currentThread().isInterrupted()) {
                    shapingLogger.error(e, "query failure filePath %s matchList %s collectList %s", queryParams.getFilePath(), queryParams.getMatchElementsParamsList(), queryParams.getCollectElementsParamsList());
                }
                throw e;
            }
        }

        // blocks.length can be 0 in case we just match in Warp when collect is done in external/prefilled
        return (blocks.isPresent() && blocks.get().length > 0) ? new Page(currentPositionsCount, blocks.get()) : new Page(currentPositionsCount);
    }

    private void updateRowsLimit()
    {
        if (isRowsLimitReached()) {
            finished = true;
        }
    }

    @Override
    public RowRanges getSortedRowRanges()
    {
        return sortedRowRanges;
    }

    @Override
    public boolean isRowsLimitReached()
    {
        return reader.isRowsLimitReached();
    }
}
