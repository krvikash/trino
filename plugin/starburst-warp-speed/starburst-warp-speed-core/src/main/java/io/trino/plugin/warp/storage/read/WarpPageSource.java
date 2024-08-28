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
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

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

    private final DictionaryCacheService dictionaryCacheService;
    private final CollectTxService collectTxService;
    private final ChunksQueueService chunksQueueService;
    private final StorageCollectorService storageCollectorService;
    private final LazyCollectorService lazyCollectorService;
    private final RangeFillerService rangeFillerService;
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final boolean isMatchGetNumRanges;
    private final PredicatesCacheService predicatesCacheService;
    private final BufferAllocator bufferAllocator;
    private final GlobalConfig globalConfig;
    private final QueryParams queryParams;
    private final CustomStatsContext customStatsContext;
    private StorageReader reader;
    private long rowsLimit;
    private boolean finished;
    private boolean closed; // May be set explicitly by someone calling {@link #close()} or if we finished reading all available data from the table
    private RowRanges sortedRowRanges;
    private long completedBytes;
    private long completedPositions;

    public WarpPageSource(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            long rowsLimit,
            BufferAllocator bufferAllocator,
            QueryParams queryParams,
            boolean isMatchGetNumRanges,
            PredicatesCacheService predicatesCacheService,
            DictionaryCacheService dictionaryCacheService,
            CustomStatsContext customStatsContext,
            GlobalConfig globalConfig,
            CollectTxService collectTxService,
            ChunksQueueService chunksQueueService,
            StorageCollectorService storageCollectorService,
            LazyCollectorService lazyCollectorService,
            RangeFillerService rangeFillerService)
    {
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.isMatchGetNumRanges = isMatchGetNumRanges;
        this.predicatesCacheService = predicatesCacheService;
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.globalConfig = requireNonNull(globalConfig);
        this.customStatsContext = customStatsContext;
        this.collectTxService = collectTxService;
        this.chunksQueueService = chunksQueueService;
        this.storageCollectorService = storageCollectorService;
        this.lazyCollectorService = lazyCollectorService;
        this.rangeFillerService = rangeFillerService;
        this.sortedRowRanges = RowRanges.EMPTY;
        this.rowsLimit = rowsLimit;
        this.queryParams = queryParams;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
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
        Block[] blocks = new Block[queryParams.getCollectElementsParamsList().size()];
        int currentPositionsCount = fillPage(blocks);

        markFinishedIfDone();

        // blocks.length can be 0 in case we just match in Warp when collect is done in external/prefilled
        return ((blocks.length > 0) && blocks[0] != null) ? new Page(currentPositionsCount, blocks) : new Page(currentPositionsCount);
    }

    private int fillPage(Block[] blocks)
    {
        if (!isFinished()) {
            try {
                if (reader == null) { // first time
                    StorageCollectorArgs storageCollectorArgs = storageCollectorService.getStorageCollectorArgs(queryParams);
                    boolean useLazyCollect = lazyCollectorService.useLazyCollect(queryParams);
                    StorageCollectorService collectorService = useLazyCollect ? lazyCollectorService : storageCollectorService;
                    reader = new StorageReader(storageEngine,
                            storageEngineConstants,
                            bufferAllocator,
                            dictionaryCacheService,
                            queryParams,
                            customStatsContext,
                            storageCollectorArgs,
                            collectTxService,
                            chunksQueueService,
                            collectorService,
                            globalConfig);
                }
                return pipe(blocks);
            }
            catch (Exception e) {
                close();
                if (!Thread.currentThread().isInterrupted()) {
                    shapingLogger.error(e, "query failure filePath %s matchList %s collectList %s", queryParams.getFilePath(), queryParams.getMatchElementsParamsList(), queryParams.getCollectElementsParamsList());
                }
                throw e;
            }
        }
        return 0;
    }

    private void markFinishedIfDone()
    {
        if (isRowsLimitReached()) {
            finished = true;
        }
    }

    private int pipe(Block[] blocks)
    {
        int limit = (int) Math.min(rowsLimit, Integer.MAX_VALUE);
        int collectedRows = 0;
        sortedRowRanges = RowRanges.EMPTY; // Reset the row ranges before reading another page.
        CollectOpenResult collectOpenResult = null;
        try {
            collectOpenResult = reader.queryOpen(limit);
            if (!reader.matchAndCollect(collectOpenResult, isMatchGetNumRanges)) {
                finished = true;
            }
            else {
                // Get the current available rows - cannot be zero at this point since the reader has something
                collectedRows = reader.fillBlocks(blocks, collectOpenResult);
                if (isMatchGetNumRanges) {
                    sortedRowRanges = rangeFillerService.collectRanges(collectOpenResult.rangeData(), collectOpenResult.rowsLimit());
                }
                rowsLimit -= collectedRows;
            }
            long readPagesResult = reader.queryClose(collectOpenResult);
            completedBytes += (readPagesResult << storageEngineConstants.getPageSizeShift());
            completedPositions += collectedRows;
        }
        catch (Exception e) {
            reader.queryAbort(e, collectOpenResult);
            throw e;
        }

        return collectedRows;
    }

    @Override
    public RowRanges getSortedRowRanges()
    {
        return sortedRowRanges;
    }

    @Override
    public boolean isRowsLimitReached()
    {
        return rowsLimit <= 0;
    }
}
