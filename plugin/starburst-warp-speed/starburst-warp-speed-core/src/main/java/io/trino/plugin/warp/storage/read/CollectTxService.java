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
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import jakarta.annotation.PreDestroy;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.dispatcher.query.classifier.NativeCollectClassifier.COLLECT_BUFFER_MAX_MEMORY;

public class CollectTxService
        extends BaseCollectTxService
{
    private final RangeFillerService rangeFillerService;

    @Inject
    public CollectTxService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ConnectorSync connectorSync,
            BufferAllocator bufferAllocator,
            RangeFillerService rangeFillerService,
            GlobalConfig globalConfig)
    {
        super(storageEngine, storageEngineConstants, connectorSync, bufferAllocator, globalConfig);
        this.rangeFillerService = rangeFillerService;
    }

    @PreDestroy
    public void shutdown()
    {
    }

    /**
     * prepare buffers for filling
     */
    AggregatorPageArgs collectOpenAndRestore(QueryArgs queryArgs,
            ThreadArena pageArena,
            int rowsLimit,
            int numCollectedInPrevRounds,
            AggregatorArgs aggregatorArgs,
            Optional<StoreRowListResult> storeRowListResult)
    {
        QueryParams queryParams = queryArgs.queryParams();
        List<WarmupElementCollectParams> collectParamsList = queryParams.getCollectElementsParamsList();
        int numCollectElements = collectParamsList.size();

        int readerId = allocReaderId();
        // if there are no match elements we are lazy collecting and do not need to allocate all the buffers per element
        if ((queryParams.getNumMatchElements() > 0) && (numCollectElements > 0)) {
            allocCollectBuffers(collectParamsList, pageArena, queryArgs.txArgs().collectBuffers(), aggregatorArgs.collectJuffersWE());
        }

        RecordIndexes recordIndexes = new RecordIndexes(pageArena, queryArgs.chunkSize());
        RangeData rangeData = new RangeData(recordIndexes);
        Optional<MemorySegment> recordBufferStatesOpt = Optional.empty();
        Optional<MemorySegment> warmUpElementAttsOpt = Optional.empty();
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = Collections.emptyList();
        if (numCollectElements > 0) {
            final long recordBufferStatesSize =
                    MemoryLayout.sequenceLayout(queryParams.getNumCollectElements(), WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT).byteSize();
            MemorySegment recordBufferStates = pageArena.allocate(recordBufferStatesSize, ValueLayout.JAVA_INT.byteSize());
            warmupElementRecordBufferStates = recordBufferStates.elements(WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT)
                    .map(recordBufferState -> new WarmupElementRecordBufferState(recordBufferState))
                    .toList();
            recordBufferStatesOpt = Optional.of(recordBufferStates);

            final long warmUpElementAttsSize =
                    MemoryLayout.sequenceLayout(queryParams.getNumCollectElements(), WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT).byteSize();
            MemorySegment warmUpElementAtts = pageArena.allocate(warmUpElementAttsSize, ValueLayout.JAVA_BYTE.byteSize());
            Iterator<WarmupElementCollectParams> collectParamsListItr = collectParamsList.iterator();
            warmUpElementAtts.elements(WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT)
                    .forEach(warmupElementAtt -> {
                        WarmupElementCollectParams collectParams = collectParamsListItr.next();
                        WarmUpElement.setRecTypeCode(warmupElementAtt, collectParams.getRecTypeCode());
                        WarmUpElement.setRecTypeLength(warmupElementAtt, collectParams.getRecTypeLength());
                        WarmUpElement.setWarmUpType(warmupElementAtt, collectParams.getWarmUpType());
                    });
            warmUpElementAttsOpt = Optional.of(warmUpElementAtts);
        }

        if (storeRowListResult.isPresent()) {
            // restore row list
            rangeFillerService.restoreRowList(rangeData.getRecordIndexes(), storeRowListResult.get(), aggregatorArgs.storeRowListBuff());

            // restore match collect metadata
            queryArgs.storeMatchCollectMetadataBuff().ifPresent(s ->
                    MemorySegment.copy(MemorySegment.ofArray(s), 0, queryArgs.matchCollectMetadata().get(), 0, s.length));
        }

        collectOpen(queryArgs.queryParams(),
                queryArgs.txArgs(),
                readerId,
                numCollectElements,
                queryArgs.numChunksInRange(),
                storeRowListResult.map(StoreRowListResult::storedChunkIx).orElse(-1),
                warmUpElementAttsOpt.map(m -> m.address()).orElse(0L),
                recordBufferStatesOpt.map(m -> m.address()).orElse(0L),
                recordIndexes.getAddress(),
                queryArgs.matchCollectMetadata().map(m -> m.address()).orElse(0L),
                queryArgs.dispatcherPageSourceStats());

        final long queryResultTypesSize = MemoryLayout.sequenceLayout(queryParams.getNumCollectElements(), ValueLayout.JAVA_INT).byteSize();
        return new AggregatorPageArgs(readerId,
                rowsLimit,
                numCollectedInPrevRounds,
                rangeData,
                warmupElementRecordBufferStates,
                pageArena.allocate(queryResultTypesSize, ValueLayout.JAVA_INT.byteSize()),
                pageArena.allocate(queryResultTypesSize, ValueLayout.JAVA_INT.byteSize()),
                warmUpElementAttsOpt);
    }

    CollectCloseResult collectStoreAndClose(QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs,
            AggregatorArgs aggregatorArgs,
            ChunksQueue chunksQueue)
    {
        Optional<StoreRowListResult> storeRowListResult = Optional.empty();
        if (!chunksQueue.isChunkRangeCompleted()) {
            // store row list
            storeRowListResult = Optional.of(rangeFillerService.storeRowList(chunksQueue, queryArgs, aggregatorArgs, aggregatorPageArgs.rangeData()));

            // store match collect metadata
            queryArgs.storeMatchCollectMetadataBuff().ifPresent(s ->
                    MemorySegment.copy(queryArgs.matchCollectMetadata().get(), 0, MemorySegment.ofArray(s), 0, s.length));
        }

        long[] collectStats = new long[CollectStats.COLLECT_STATS_NUM_OF.ordinal()];
        long startTime = System.nanoTime();
        storageEngine.collectClose(aggregatorPageArgs.readerId(), collectStats);
        queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);

        int totalReadPages = 0;
        NativeStats nativeStats = queryArgs.nativeStats();
        nativeStats.addread_cache_md_chunk_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_HITS.ordinal()];
        nativeStats.addread_cache_md_basic_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_HITS.ordinal()];
        nativeStats.addread_cache_md_data_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_HITS.ordinal()];
        nativeStats.addread_cache_md_nulls_hits(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_HITS.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_HITS.ordinal()];
        nativeStats.addread_cache_md_chunk_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_CHUNK_MISSES.ordinal()];
        nativeStats.addread_cache_md_basic_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_BASIC_MISSES.ordinal()];
        nativeStats.addread_cache_md_data_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_DATA_MISSES.ordinal()];
        nativeStats.addread_cache_md_nulls_misses(collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_CACHE_MD_NULLS_MISSES.ordinal()];
        nativeStats.addread_uncache_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_UNCACHE_MISSES.ordinal()];
        nativeStats.addread_uncache_data_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_DATA_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_UNCACHE_DATA_MISSES.ordinal()];
        nativeStats.addread_uncache_ext_data_misses(collectStats[CollectStats.COLLECT_STATS_UNCACHE_EXT_DATA_MISSES.ordinal()]);
        totalReadPages += (int) collectStats[CollectStats.COLLECT_STATS_UNCACHE_EXT_DATA_MISSES.ordinal()];
        nativeStats.addread_time_wait_nanos(collectStats[CollectStats.COLLECT_STATS_READ_TIME_WAIT_NANOS.ordinal()]);

        freeQueryMemory(aggregatorPageArgs.readerId());
        return new CollectCloseResult(storeRowListResult, totalReadPages);
    }

    void collectAbort(AggregatorPageArgs aggregatorPageArgs, Exception e, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        collectAbort(e, aggregatorPageArgs.readerId(), dispatcherPageSourceStats);
        freeQueryMemory(aggregatorPageArgs.readerId());
    }

    private void allocCollectBuffers(List<WarmupElementCollectParams> collectParamsList,
            ThreadArena pageArena,
            long[][] outCollectBuffers,
            List<ReadJuffersWarmUpElement> outCollectJuffersWE)
    {
        // fill allocation parameters and count total buffer sizes and how much of it is optional record buffer
        ArrayList<CollectAllocPararms> allocParamsList = new ArrayList<>(collectParamsList.size());
        long totalBufferSizeMust = 0;
        long totalRecordBufferSizeOptional = 0;
        for (WarmupElementCollectParams collectParams : collectParamsList) {
            // consider mapped match collect when calculating rec type code and length
            RecTypeCode recTypeCode = collectParams.mappedMatchCollect() ? RecTypeCode.REC_TYPE_TINYINT : collectParams.getRecTypeCode();
            final int recTypeLength = collectParams.mappedMatchCollect() ? 1 : collectParams.getRecTypeLength();
            final int recordBufferSizeMust = bufferAllocator.getCollectRecordBufferSizeMust(recTypeCode, recTypeLength);
            final int recordBufferSizeOptional = bufferAllocator.getCollectRecordBufferSizeOptional(recTypeCode, recTypeLength);
            final int nullBufferSize = bufferAllocator.getQueryNullBufferSize(collectParams.getRecTypeCode());
            // save all parameters in a record array
            allocParamsList.add(new CollectAllocPararms(recTypeCode,
                    recTypeLength,
                    recordBufferSizeMust,
                    recordBufferSizeOptional,
                    nullBufferSize,
                    collectParams.hasDictionary()));
            // update total counts
            totalBufferSizeMust += (recordBufferSizeMust + nullBufferSize); // including extras inside and nulls
            totalRecordBufferSizeOptional += recordBufferSizeOptional;
        }

        final int pageSize = storageEngineConstants.getPageSize();
        final int pageSizeMask = storageEngineConstants.getPageSizeMask();
        // size left for optional record buffer is total memory minus the must to allocate without optional
        long queryMemoryOptional = COLLECT_BUFFER_MAX_MEMORY - totalBufferSizeMust;
        // make sure each juffer that needs extra will get same fair
        double satisfyPrecentage = (queryMemoryOptional >= totalRecordBufferSizeOptional) ? 1.0 : ((double) queryMemoryOptional / (double) totalRecordBufferSizeOptional);

        // allocate total memory for records and nulls
        MemorySegment collectMemory;
        try {
            collectMemory = pageArena.allocate(totalBufferSizeMust + Math.min(queryMemoryOptional, totalRecordBufferSizeOptional), pageSize);
        }
        catch (Throwable t) {
            throw new RuntimeException("no memory available for lazy collect size totalBufferSizeMust " + totalBufferSizeMust + " totalRecordBufferSizeOptional " + totalRecordBufferSizeOptional);
        }
        SegmentAllocator queryMemoryAllocator = SegmentAllocator.slicingAllocator(collectMemory);

        int collectIx = 0;
        ArrayList<Integer> optionalSizes = new ArrayList<>();
        try {
            // perform actual allocation of record and null buffers and create the juffers
            for (CollectAllocPararms allocParams : allocParamsList) {
                long[] collectBuffers = outCollectBuffers[collectIx]; // save the addresses here for native
                MemorySegment[] collectSegments = new MemorySegment[collectBuffers.length]; // used to create the juffers below
                // record buffer size including the optional part which is calculated using the precentage and masked to page size
                int recordBufferSizeOptional = (int) (satisfyPrecentage * allocParams.recordBufferSizeOptional());
                recordBufferSizeOptional &= pageSizeMask;
                optionalSizes.add(recordBufferSizeOptional);
                allocCollectBuffer(queryMemoryAllocator,
                        JbufType.JBUF_TYPE_REC,
                        allocParams.recordBufferSizeMust() + recordBufferSizeOptional,
                        collectSegments,
                        collectBuffers);
                // null buffer
                allocCollectBuffer(queryMemoryAllocator,
                        JbufType.JBUF_TYPE_NULL,
                        allocParams.nullBufferSize(),
                        collectSegments,
                        collectBuffers);
                // create the juffers from the segments
                outCollectJuffersWE.get(collectIx).createBuffers(allocParams.recTypeCode(),
                        allocParams.recTypeLength(),
                        allocParams.hasDictionary(),
                        collectSegments);
                // advance
                collectIx++;
            }
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to allocate record and null buffers totalBufferSizeMust %d totalRecordBufferSizeOptional %d",
                    totalBufferSizeMust, totalRecordBufferSizeOptional);
            throw new RuntimeException("failed to allocate record and null buffers" +
                    " totalBufferSizeMust " + totalBufferSizeMust +
                    " totalRecordBufferSizeOptional " + totalRecordBufferSizeOptional +
                    " queryMemoryOptional " + queryMemoryOptional +
                    " satisfyPrecentage " + satisfyPrecentage +
                    " collectIx " + collectIx +
                    " numCollectElements " + allocParamsList.size() +
                    " allocParamsList " + allocParamsList +
                    " optionalSizes " + optionalSizes +
                    " collectMemory " + collectMemory);
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
