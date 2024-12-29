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
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
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

        Optional<MemorySegment> collectMemory = Optional.empty();
        CollectMetadataMemory collectMetadataMemory = new CollectMetadataMemory(new RecordIndexes(pageArena, queryArgs.chunkSize()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList());
        if (numCollectElements > 0) {
            collectMetadataMemory = allocCollectMetadataMemory(collectMetadataMemory,
                    queryParams,
                    queryArgs.storeMatchCollectMetadataBuff(),
                    pageArena);
            // if there are no match elements we are lazy collecting and do not need to allocate all the buffers per element
            if (queryParams.getNumMatchElements() > 0) {
                collectMemory = allocCollectBuffers(collectParamsList,
                        pageArena,
                        collectMetadataMemory.collectBuffersOpt().get(),
                        aggregatorArgs.collectJuffersWE());
            }
        }

        if (storeRowListResult.isPresent()) {
            // restore row list
            rangeFillerService.restoreRowList(collectMetadataMemory.recordIndexes(), storeRowListResult.get(), aggregatorArgs.storeRowListBuff());

            // restore match collect metadata
            Optional<MemorySegment> matchCollectMetadataOpt = collectMetadataMemory.matchCollectMetadataOpt();
            queryArgs.storeMatchCollectMetadataBuff().ifPresent(s -> MemorySegment.copy(MemorySegment.ofArray(s), 0, matchCollectMetadataOpt.get(), 0, s.length));
        }

        CollectState collectState = new CollectState(pageArena, storageEngineConstants.getCollectStatePayload());
        AggregatorPageArgs aggregatorPageArgs = new AggregatorPageArgs(collectState,
                allocReaderId(),
                rowsLimit,
                numCollectedInPrevRounds,
                collectMemory,
                new RangeData(collectMetadataMemory.recordIndexes()),
                collectMetadataMemory.collectBuffersOpt(),
                collectMetadataMemory.warmupElementRecordBufferStates(),
                collectMetadataMemory.recordBufferStatesOpt(),
                collectMetadataMemory.queryResultTypesOpt(),
                collectMetadataMemory.prepareQueryResultTypesOpt(),
                collectMetadataMemory.matchCollectMetadataOpt(),
                new ReadStats(pageArena));
        collectState.setState(queryArgs, aggregatorPageArgs, storeRowListResult);
        collectOpen(collectState, queryArgs.dispatcherPageSourceStats());

        return aggregatorPageArgs;
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
                    MemorySegment.copy(aggregatorPageArgs.matchCollectMetadata().get(), 0, MemorySegment.ofArray(s), 0, s.length));
        }

        long startTime = System.nanoTime();
        storageEngine.collectClose(aggregatorPageArgs.collectState().getStateMemory(), aggregatorPageArgs.readStats().getMemory());
        queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);

        int totalReadPages = aggregatorPageArgs.readStats().fillStats(queryArgs.nativeStats());
        freeQueryMemory(aggregatorPageArgs.readerId());
        return new CollectCloseResult(storeRowListResult, totalReadPages);
    }

    void collectAbort(AggregatorPageArgs aggregatorPageArgs, Exception e, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        collectAbort(e, aggregatorPageArgs.collectState(), dispatcherPageSourceStats);
        freeQueryMemory(aggregatorPageArgs.readerId());
    }

    private Optional<MemorySegment> allocCollectBuffers(List<WarmupElementCollectParams> collectParamsList,
            ThreadArena pageArena,
            MemorySegment collectBuffers,
            List<ReadJuffersWarmUpElement> outCollectJuffersWE)
    {
        // fill allocation parameters and count total buffer sizes and how much of it is optional record buffer
        ArrayList<CollectAllocPararms> allocParamsList = new ArrayList<>(collectParamsList.size());
        long totalBufferSizeMust = 0;
        long totalRecordBufferSizeOptional = 0;
        for (WarmupElementCollectParams collectParams : collectParamsList) {
            // consider mapped match collect when calculating rec type code and length
            MemorySegment warmupElementAtt = collectParams.getWarmupElementAtt();
            RecTypeCode recTypeCode = collectParams.mappedMatchCollect() ? RecTypeCode.REC_TYPE_TINYINT : WarmUpElement.getRecTypeCode(warmupElementAtt);
            final int recTypeLength = collectParams.mappedMatchCollect() ? 1 : WarmUpElement.getRecTypeLength(warmupElementAtt);
            final int recordBufferSizeMust = bufferAllocator.getCollectRecordBufferSizeMust(recTypeCode, recTypeLength);
            final int recordBufferSizeOptional = bufferAllocator.getCollectRecordBufferSizeOptional(recTypeCode, recTypeLength);
            final int nullBufferSize = bufferAllocator.getQueryNullBufferSize(WarmUpElement.getRecTypeCode(warmupElementAtt));
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
            throw new RuntimeException("no memory available for collect size totalBufferSizeMust " + totalBufferSizeMust + " totalRecordBufferSizeOptional " + totalRecordBufferSizeOptional);
        }
        SegmentAllocator allocator = SegmentAllocator.slicingAllocator(collectMemory);

        int collectIx = 0;
        int collectBufIx = 0;
        ArrayList<Integer> optionalSizes = new ArrayList<>();
        try {
            // perform actual allocation of record and null buffers and create the juffers
            for (CollectAllocPararms allocParams : allocParamsList) {
                MemorySegment[] collectSegments = new MemorySegment[JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal()]; // used to create the juffers below
                // record buffer size including the optional part which is calculated using the precentage and masked to page size
                int recordBufferSizeOptional = (int) (satisfyPrecentage * allocParams.recordBufferSizeOptional());
                recordBufferSizeOptional &= pageSizeMask;
                optionalSizes.add(recordBufferSizeOptional);
                allocCollectBuffer(allocator,
                        JbufType.JBUF_TYPE_REC,
                        allocParams.recordBufferSizeMust() + recordBufferSizeOptional,
                        collectSegments);
                // null buffer
                allocCollectBuffer(allocator,
                        JbufType.JBUF_TYPE_NULL,
                        allocParams.nullBufferSize(),
                        collectSegments);
                // create the juffers from the segments
                outCollectJuffersWE.get(collectIx).createBuffers(allocParams.recTypeCode(),
                        allocParams.recTypeLength(),
                        allocParams.hasDictionary(),
                        collectSegments);
                // set the addresses in the output segment
                collectBuffers.setAtIndex(ValueLayout.JAVA_LONG, collectBufIx + JbufType.JBUF_TYPE_REC.ordinal(), collectSegments[JbufType.JBUF_TYPE_REC.ordinal()].address());
                collectBuffers.setAtIndex(ValueLayout.JAVA_LONG, collectBufIx + JbufType.JBUF_TYPE_NULL.ordinal(), collectSegments[JbufType.JBUF_TYPE_NULL.ordinal()].address());
                // advance
                collectIx++;
                collectBufIx += collectSegments.length;
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
        return Optional.of(collectMemory);
    }

    private CollectMetadataMemory allocCollectMetadataMemory(CollectMetadataMemory collectMetadataMemory,
            QueryParams queryParams,
            Optional<byte[]> storeMatchCollectMetadataBuff,
            ThreadArena pageArena)
    {
        final int numElements = queryParams.getNumCollectElements();
        final long collectBuffersSize = (long) numElements * JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal() * ValueLayout.JAVA_LONG.byteSize();
        final long recordBufferStatesSize = MemoryLayout.sequenceLayout(numElements, WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT).byteSize();
        final long queryResultTypesSize = MemoryLayout.sequenceLayout(numElements, ValueLayout.JAVA_INT).byteSize();
        final long matchCollectMetadataSize = storeMatchCollectMetadataBuff.map(s -> s.length).orElse(0);
        final long totalSize = collectBuffersSize +
                recordBufferStatesSize +
                queryResultTypesSize * 2 +
                matchCollectMetadataSize +
                ValueLayout.JAVA_LONG.byteSize();

        MemorySegment collectMemory;
        try {
            collectMemory = pageArena.allocate(totalSize, ValueLayout.JAVA_LONG.byteSize());
        }
        catch (Throwable t) {
            throw new RuntimeException("no memory available for collect metadata size totalSize " + totalSize);
        }
        SegmentAllocator allocator = SegmentAllocator.slicingAllocator(collectMemory);

        MemorySegment collectBuffers = allocator.allocate(collectBuffersSize, ValueLayout.JAVA_LONG.byteSize());
        MemorySegment recordBufferStates = allocator.allocate(recordBufferStatesSize, ValueLayout.JAVA_INT.byteSize());
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates =
                recordBufferStates.elements(WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT)
                .map(recordBufferState -> new WarmupElementRecordBufferState(recordBufferState))
                .toList();

        return new CollectMetadataMemory(collectMetadataMemory.recordIndexes(),
                Optional.of(collectBuffers),
                Optional.of(recordBufferStates),
                Optional.of(allocator.allocate(queryResultTypesSize, ValueLayout.JAVA_INT.byteSize())),
                Optional.of(allocator.allocate(queryResultTypesSize, ValueLayout.JAVA_INT.byteSize())),
                (matchCollectMetadataSize > 0) ? Optional.of(allocator.allocate(matchCollectMetadataSize, ValueLayout.JAVA_INT.byteSize())) : Optional.empty(),
                warmupElementRecordBufferStates);
    }

    private record CollectAllocPararms(RecTypeCode recTypeCode,
            int recTypeLength,
            int recordBufferSizeMust,
            int recordBufferSizeOptional,
            int nullBufferSize,
            boolean hasDictionary)
    {
    }

    private record CollectMetadataMemory(RecordIndexes recordIndexes,
            Optional<MemorySegment> collectBuffersOpt,
            Optional<MemorySegment> recordBufferStatesOpt,
            Optional<MemorySegment> queryResultTypesOpt,
            Optional<MemorySegment> prepareQueryResultTypesOpt,
            Optional<MemorySegment> matchCollectMetadataOpt,
            List<WarmupElementRecordBufferState> warmupElementRecordBufferStates)
    {
    }
}
