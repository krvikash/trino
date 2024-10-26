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
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.CollectStats;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.TestStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.QueryMemory;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;

public class CollectTxService
        extends BaseCollectTxService
{
    private final ChunksQueueService chunksQueueService;
    private final RangeFillerService rangeFillerService;

    @Inject
    public CollectTxService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ConnectorSync connectorSync,
            BufferAllocator bufferAllocator,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            GlobalConfig globalConfig)
    {
        super(storageEngine, storageEngineConstants, connectorSync, bufferAllocator, globalConfig);
        this.chunksQueueService = chunksQueueService;
        this.rangeFillerService = rangeFillerService;
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

        QueryMemory queryMemory = allocQueryMemory();
        int queryMemoryId = queryMemory.id();
        SegmentAllocator queryMemoryAllocator = getQueryMemoryAllocator(queryMemory);

        long matchBmAddr = 0;
        if (queryParams.getNumMatchElements() > 0) {
            final long alignment = 32; // this is the alignment required for intel optimized bitmap operations
            final long allocSize = (long) storageEngineConstants.getPageSize() * (long) storageEngineConstants.getMaxChunksInRange();
            matchBmAddr = queryMemoryAllocator.allocate(allocSize, alignment).address();

            if (numCollectElements > 0) {
                allocCollectBuffers(collectParamsList,
                        queryMemoryAllocator,
                        queryArgs.txArgs().collectBuffers(),
                        storageCollectorArgs.collectJuffersWE());
            }
        }

        RangeData rangeData = new RangeData(storageCollectorArgs.recordIndexes());
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = Collections.emptyList();
        if (numCollectElements > 0) {
            warmupElementRecordBufferStates = storageCollectorArgs.recordBufferStates()
                    .elements(WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT)
                    .map(recordBufferState -> new WarmupElementRecordBufferState(recordBufferState))
                    .toList();

            Iterator<WarmupElementCollectParams> collectParamsListItr = collectParamsList.iterator();
            storageCollectorArgs.warmUpElementAtts()
                    .elements(WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT)
                    .forEach(warmupElementAtt -> {
                        WarmupElementCollectParams collectParams = collectParamsListItr.next();
                        WarmUpElement.setRecTypeCode(warmupElementAtt, collectParams.getRecTypeCode());
                        WarmUpElement.setRecTypeLength(warmupElementAtt, collectParams.getRecTypeLength());
                        WarmUpElement.setWarmUpType(warmupElementAtt, collectParams.getWarmUpType());
                    });
        }

        collectOpen(queryArgs.queryParams(),
                queryArgs.txArgs(),
                queryMemoryId,
                numCollectElements,
                queryArgs.numChunksInRange(),
                storageCollectorArgs.warmUpElementAtts().address(),
                matchBmAddr,
                storageCollectorArgs.recordBufferStates().address(),
                storageCollectorArgs.recordIndexes().getAddress());

        int restoredChunkIndex = -1;
        if (chunksQueueService.storeRestoreRequired(queryArgs.chunksQueue())) {
            checkState(storeRowListResult.isPresent(), "Restore needed but store data doesn't exists");
            rangeFillerService.restoreRowList(rangeData.getRecordIndexes(), storeRowListResult.get(), storageCollectorArgs.storeRowListBuff());
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

    private void allocCollectBuffers(List<WarmupElementCollectParams> collectParamsList,
            SegmentAllocator queryMemoryAllocator,
            long[][] outCollectBuffers,
            List<ReadJuffersWarmUpElement> outCollectJuffersWE)
    {
        final long queryMemorySize = globalConfig.getCollectMemorySize();
        final int numCollectElements = collectParamsList.size();
        int collectIx = 0;

        // fill allocation parameters and count total buffer sizes and how much of it is optional record buffer
        CollectAllocPararms[] allocParams = new CollectAllocPararms[numCollectElements];
        long totalRecordBufferSizeMust = 0;
        long totalRecordBufferSizeOptional = 0;
        for (WarmupElementCollectParams collectParams : collectParamsList) {
            // consider mapped match collect when calculating rec type code and length
            RecTypeCode recTypeCode = collectParams.mappedMatchCollect() ? RecTypeCode.REC_TYPE_TINYINT : collectParams.getRecTypeCode();
            final int recTypeLength = collectParams.mappedMatchCollect() ? 1 : collectParams.getRecTypeLength();
            // get maximal record buffer size for this element
            final int recordBufferSize = bufferAllocator.getCollectRecordBufferSize(recTypeCode, recTypeLength);
            // get how much of this size can be optional
            final int recordBufferSizeOptional = bufferAllocator.getCollectRecordBufferSizeOptional(recTypeCode, recordBufferSize);
            final int recordBufferSizeMust = recordBufferSize - recordBufferSizeOptional;
            // save all parameters in a record array
            allocParams[collectIx] = new CollectAllocPararms(recTypeCode,
                    recTypeLength,
                    recordBufferSizeMust,
                    recordBufferSizeOptional,
                    bufferAllocator.getQueryNullBufferSize(collectParams.getRecTypeCode()),
                    collectParams.hasDictionary());
            // update total counts
            totalRecordBufferSizeMust += (recordBufferSizeMust + allocParams[collectIx].nullBufferSize()); // including extras inside and nulls
            totalRecordBufferSizeOptional += recordBufferSizeOptional;
            // advance
            collectIx++;
        }

        // size left for optional record buffer is total memory minus the must to allocate without optional
        // we reduce 1 byte for each element to avoid over allocation due to floating point roundings
        long queryMemoryOptional = queryMemorySize - totalRecordBufferSizeMust - numCollectElements;
        // make sure each juffer that needs extra will get same fair
        double satisfyPrecentage = (queryMemoryOptional >= totalRecordBufferSizeOptional) ? 1.0 : ((double) queryMemoryOptional / (double) totalRecordBufferSizeOptional);

        // perform actual allocation of record and null buffers and create the juffers
        for (collectIx = 0; collectIx < numCollectElements; collectIx++) {
            long[] collectBuffers = outCollectBuffers[collectIx]; // save the addresses here for native
            MemorySegment[] collectSegments = new MemorySegment[collectBuffers.length]; // used to create the juffers below
            // record buffer size including the optional part which is calculated using the precentage
            final int recordBufferSizeOptional = (int) (satisfyPrecentage * allocParams[collectIx].recordBufferSizeOptional());
            allocCollectBuffer(queryMemoryAllocator,
                    JbufType.JBUF_TYPE_REC,
                    allocParams[collectIx].recordBufferSizeMust() + recordBufferSizeOptional,
                    collectSegments,
                    collectBuffers);
            // null buffer
            allocCollectBuffer(queryMemoryAllocator,
                    JbufType.JBUF_TYPE_NULL,
                    allocParams[collectIx].nullBufferSize(),
                    collectSegments,
                    collectBuffers);
            // create the juffers from the segments
            outCollectJuffersWE.get(collectIx).createBuffers(allocParams[collectIx].recTypeCode(),
                    allocParams[collectIx].recTypeLength(),
                    allocParams[collectIx].hasDictionary(),
                    collectSegments);
        }
    }

    private record CollectAllocPararms(RecTypeCode recTypeCode,
            int recTypeLength,
            int recordBufferSizeMust,
            int recordBufferSizeOptional,
            int nullBufferSize,
            boolean hasDictionary)
    {
    }
}
