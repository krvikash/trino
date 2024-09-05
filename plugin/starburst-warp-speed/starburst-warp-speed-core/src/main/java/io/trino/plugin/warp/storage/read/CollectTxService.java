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

import com.google.inject.Inject;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.gen.constants.CollectStats;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.RecordBufferState;
import io.trino.plugin.warp.gen.stats.TestStats;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.QueryMemory;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;

import java.lang.foreign.SegmentAllocator;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;

public class CollectTxService
        extends BaseCollectTxService
{
    private final ChunksQueueService chunksQueueService;
    private final RangeFillerService rangeFillerService;
    private final StorageEngineConstants storageEngineConstants;

    @Inject
    public CollectTxService(StorageEngine storageEngine,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            ConnectorSync connectorSync,
            StorageEngineConstants storageEngineConstants,
            GlobalConfig globalConfig)
    {
        super(storageEngine, globalConfig, connectorSync);
        this.chunksQueueService = chunksQueueService;
        this.rangeFillerService = rangeFillerService;
        this.storageEngineConstants = storageEngineConstants;
    }

    @PreDestroy
    public void shutdown()
    {
    }

    /**
     * prepare buffers for filling
     */
    CollectOpenResult collectOpenAndRestore(QueryArgs queryArgs,
            int rowsLimit,
            int numCollectedInPrevRounds,
            StorageCollectorArgs storageCollectorArgs,
            Optional<StoreRowListResult> storeRowListResult)
    {
        QueryParams queryParams = queryArgs.queryParams();
        List<WarmupElementCollectParams> collectParamsList = queryParams.getCollectElementsParamsList();
        int numCollectElements = collectParamsList.size();
        long[] metadataBuffIds = new long[2];

        QueryMemory queryMemory = allocQueryMemory();
        SegmentAllocator queryMemoryAllocator = getQueryMemoryAllocator(queryMemory);
        int queryMemoryId = queryMemory.id();
        long matchBmAddr = 0;
        if (queryParams.getNumMatchElements() > 0) {
            final long alignment = 32; // this is the alignment required for intel optimized bitmap operations
            final long allocSize = (long) storageEngineConstants.getPageSize() * (long) storageEngineConstants.getMaxChunksInRange();
            matchBmAddr = queryMemoryAllocator.allocate(allocSize, alignment).address();
        }
        collectOpen(queryArgs.queryParams(),
                queryArgs.txArgs(),
                queryMemoryId,
                numCollectElements,
                queryArgs.numChunksInRange(),
                matchBmAddr,
                metadataBuffIds);

        int collectIx = 0;
        for (WarmupElementCollectParams collectParams : collectParamsList) {
            storageCollectorArgs.collectJuffersWE().get(collectIx).createBuffers(
                    collectParams.mappedMatchCollect() ? RecTypeCode.REC_TYPE_TINYINT : collectParams.getRecTypeCode(),
                    collectParams.mappedMatchCollect() ? 1 : collectParams.getRecTypeLength(),
                    collectParams.hasDictionary(),
                    queryArgs.txArgs().collectBuffIds()[collectIx]);
            collectIx++;
        }

        RangeData rangeData = new RangeData(metadataBuffIds[0]);
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = Collections.emptyList();
        if (numCollectElements > 0) {
            warmupElementRecordBufferStates = IntStream.range(0, numCollectElements)
                    .mapToObj(weIx -> new WarmupElementRecordBufferState(weIx * RecordBufferState.RECORD_BUFFER_STATE_NUM_OF.ordinal(), metadataBuffIds))
                    .toList();
        }

        int restoredChunkIndex = -1;
        if (chunksQueueService.storeRestoreRequired(queryArgs.chunksQueue())) {
            checkState(storeRowListResult.isPresent(), "Restore needed but store data doesn't exists");
            rangeFillerService.restoreRowList(rangeData.getRowsBuffId(), storeRowListResult.get(), storageCollectorArgs.storeRowListBuff());
            restoredChunkIndex = queryArgs.chunksQueue().getCurrent();
            if (storageEngine.collectRestoreState(queryMemoryId, restoredChunkIndex, storageCollectorArgs.storageCollectorCallBack()) < 0) {
                throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED,
                        String.format("failed to restore collect state restoredChunkIndex %d numChunks %d",
                        restoredChunkIndex,
                        queryArgs.numChunks()));
            }
        }

        logger.debug("collectOpen queryMemoryId %d rowsLimit %d numChunks %d numCollectElements %d restoredChunkIndex %d",
                queryMemoryId, rowsLimit, queryArgs.numChunks(), queryParams.getNumCollectElements(), restoredChunkIndex);
        return new CollectOpenResult(queryMemoryId,
                matchBmAddr,
                rowsLimit,
                numCollectedInPrevRounds,
                queryMemoryAllocator,
                rangeData,
                warmupElementRecordBufferStates);
    }

    CollectCloseResult collectStoreAndClose(QueryArgs queryArgs,
            CollectOpenResult collectOpenResult,
            StorageCollectorArgs storageCollectorArgs,
            int numCollectedRows)
    {
        Optional<StoreRowListResult> storeRowListResult = Optional.empty();
        // idiom potent case
        if (collectOpenResult == null) {
            return new CollectCloseResult(storeRowListResult, 0);
        }

        Optional<int[]> chunksWithBitmapsToStoreOpt = Optional.empty();
        if (chunksQueueService.storeRestoreRequired(queryArgs.chunksQueue())) {
            if (chunksQueueService.isChunkPreparationNeeded(queryArgs.chunksQueue())) {
                prepareChunk(collectOpenResult.queryMemoryId(),
                        queryArgs.chunksQueue().getCurrent(),
                        collectOpenResult.rowsLimit() - numCollectedRows,
                        queryArgs.chunksQueue().getCurrentResetPoint());
            }

            chunksWithBitmapsToStoreOpt = queryArgs.chunksQueue().getChunkIndexesWithBitmap();
            storeRowListResult = Optional.of(rangeFillerService.storeRowList(queryArgs, storageCollectorArgs, collectOpenResult.rangeData()));
        }

        int[] chunksWithBitmaps = chunksWithBitmapsToStoreOpt.orElse(null);
        int numChunksWithBitmap = (chunksWithBitmaps != null) ? chunksWithBitmaps.length : 0;
        long[] collectStats = new long[CollectStats.COLLECT_STATS_NUM_OF.ordinal()];
        storageEngine.collectClose(collectOpenResult.queryMemoryId(),
                chunksWithBitmaps,
                numChunksWithBitmap,
                storageCollectorArgs.storageCollectorCallBack(),
                collectStats);

        int totalReadPages = 0;
        TestStats testStats = queryArgs.testStats();
        testStats.addread_cache_md_chunk_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_HITS.ordinal()];
        testStats.addread_cache_md_basic_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_HITS.ordinal()];
        testStats.addread_cache_md_data_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_HITS.ordinal()];
        testStats.addread_cache_md_nulls_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_HITS.ordinal()];
        testStats.addread_cache_md_chunk_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_MISSES.ordinal()];
        testStats.addread_cache_md_basic_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_MISSES.ordinal()];
        testStats.addread_cache_md_data_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_MISSES.ordinal()];
        testStats.addread_cache_md_nulls_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_MISSES.ordinal()];
        testStats.addread_uncache_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_UNCACHE_MISSES.ordinal()];
        testStats.addread_uncache_data_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_DATA_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_UNCACHE_DATA_MISSES.ordinal()];
        testStats.addread_uncache_ext_data_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_EXT_DATA_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_UNCACHE_EXT_DATA_MISSES.ordinal()];
        testStats.addread_time_wait_nanos(collectStats[CollectStats.COLLECT_STATS_READ_TIME_WAIT_NANOS.ordinal()]);

        freeQueryMemory(collectOpenResult.queryMemoryId());
        return new CollectCloseResult(storeRowListResult, totalReadPages);
    }

    void collectAbort(CollectOpenResult collectOpenResult, Exception e)
    {
        collectAbort(e, collectOpenResult.queryMemoryId());
        freeQueryMemory(collectOpenResult.queryMemoryId());
    }
}
