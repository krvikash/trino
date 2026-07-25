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
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
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
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.query.classifier.NativeCollectClassifier.COLLECT_BUFFER_MAX_MEMORY;
import static java.lang.Math.min;

public class CollectTxService
        extends BaseCollectTxService
{
    @Inject
    public CollectTxService(
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            ShapingLoggerFactory shapingLoggerFactory,
            NativeConfig nativeConfig)
    {
        super(storageEngine, storageEngineConstants, bufferAllocator, shapingLoggerFactory, nativeConfig);
    }

    @PreDestroy
    public void shutdown() {}

    /**
     * prepare buffers for filling
     */
    AggregatorPageArgs collectOpenAndRestore(
            RecordIndexes recordIndexes,
            QueryArgs queryArgs,
            ThreadArena pageArena,
            AggregatorArgs aggregatorArgs,
            List<Integer> blocksToLoad)
    {
        int numCollectElements = blocksToLoad.size();

        Optional<MemorySegment> collectMemory = Optional.empty();
        recordIndexes.allocateRecordIndexesSegment(pageArena);

        CollectMetadataMemory collectMetadataMemory;
        if (numCollectElements > 0) {
            collectMetadataMemory = allocCollectMetadataMemory(
                    numCollectElements,
                    queryArgs.storeMatchCollectMetadataBuff(),
                    pageArena);
            collectMemory = allocCollectBuffers(
                    aggregatorArgs.collectBuffersParams(),
                    pageArena,
                    collectMetadataMemory.collectBuffersOpt().get(),
                    blocksToLoad);

            MemorySegment collectParamsListMemory = collectMetadataMemory.collectParamsOpt().get();
            MemorySegment elementCollectParamsMemory;
            int collectIx = 0;
            for (Integer blockIx : blocksToLoad) {
                elementCollectParamsMemory = queryArgs.queryParams().getCollectElementsParamsList().get(blockIx).getMemory();
                collectParamsListMemory.setAtIndex(ValueLayout.ADDRESS, collectIx, elementCollectParamsMemory);
                collectIx++;
            }
        }
        else {
            collectMetadataMemory = new CollectMetadataMemory(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Collections.emptyList());
        }

        // restore match collect metadata
        Optional<MemorySegment> matchCollectMetadataOpt = collectMetadataMemory.matchCollectMetadataOpt();
        queryArgs.storeMatchCollectMetadataBuff().ifPresent(s -> MemorySegment.copy(MemorySegment.ofArray(s), 0, matchCollectMetadataOpt.get(), 0, s.length));

        CollectState collectState = new CollectState(
                pageArena,
                storageEngineConstants.getCollectStatePayload(),
                nativeConfig.getLimitNumIosInParallel() * nativeConfig.getMaxIOMetadataSize());
        AggregatorPageArgs aggregatorPageArgs = new AggregatorPageArgs(
                collectState,
                collectMetadataMemory.collectParamsOpt(),
                collectMemory,
                collectMetadataMemory.collectBuffersOpt(),
                collectMetadataMemory.warmupElementRecordBufferStates(),
                collectMetadataMemory.recordBufferStatesOpt(),
                collectMetadataMemory.queryResultTypesOpt(),
                collectMetadataMemory.matchCollectMetadataOpt(),
                new ReadStats(pageArena));

        collectState.setState(queryArgs, aggregatorPageArgs, recordIndexes, numCollectElements);
        collectOpen(collectState, queryArgs.dispatcherPageSourceStats());

        return aggregatorPageArgs;
    }

    long collectStoreAndClose(
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs)
    {
        // store match collect metadata
        queryArgs.storeMatchCollectMetadataBuff().ifPresent(s ->
                MemorySegment.copy(aggregatorPageArgs.matchCollectMetadata().get(), 0, MemorySegment.ofArray(s), 0, s.length));

        long startTime = System.nanoTime();
        storageEngine.collectClose(aggregatorPageArgs.collectState().getStateMemory(), aggregatorPageArgs.readStats().getMemory());
        queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);

        return aggregatorPageArgs.readStats().fillStats(queryArgs.nativeStats());
    }

    void collectAbort(AggregatorPageArgs aggregatorPageArgs, Exception e, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        collectAbort(e, aggregatorPageArgs.collectState(), dispatcherPageSourceStats);
    }

    public CollectBuffersParams getCollectBuffersAllocationParams(QueryParams queryParams)
    {
        List<ReadJuffersWarmUpElement> collectJuffersWE = queryParams.getCollectElementsParamsList()
                .stream()
                .map(_ -> new ReadJuffersWarmUpElement(bufferAllocator, true))
                .collect(Collectors.toList());

        // fill allocation parameters and count total buffer sizes
        long totalNullBuffs = 0;
        long totalRequestedRecordBufferSize = 0;
        ArrayList<CollectBuffersParams.CollectBufAllocParams> allocParamsList = new ArrayList<>(queryParams.getCollectElementsParamsList().size());

        for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
            MemorySegment warmupElementAtt = collectParams.getWarmupElementAtt();
            RecTypeCode recTypeCode = collectParams.mappedMatchCollect() ? RecTypeCode.REC_TYPE_TINYINT : WarmUpElement.getRecTypeCode(warmupElementAtt);
            int recTypeLength = collectParams.mappedMatchCollect() ? 1 : WarmUpElement.getRecTypeLength(warmupElementAtt);
            int requestedRecordBufferSize = bufferAllocator.getCollectRecordBufferSize(recTypeCode, recTypeLength);
            int nullBufferSize = bufferAllocator.getQueryNullBufferSize(WarmUpElement.getRecTypeCode(warmupElementAtt));
            // save all parameters in a record array
            allocParamsList.add(new CollectBuffersParams.CollectBufAllocParams(
                    recTypeCode,
                    recTypeLength,
                    requestedRecordBufferSize,
                    nullBufferSize));
            totalNullBuffs += nullBufferSize;
            totalRequestedRecordBufferSize += requestedRecordBufferSize;
        }

        // we split the buffer in a way that will maximize number of records in a page, so give to each we the same percentage of the requested size.
        // In NativeCollectClassifier.getCollectBufferSize we want to get the minimal buffer size needed to collect each we, so the calculation is different.
        double satisfyPercentage = bufferAllocator.calculateSatisfyPercentage(totalRequestedRecordBufferSize, totalNullBuffs, queryParams);
        int maxRecordsInJuffer = min(1 << storageEngineConstants.getChunkSizeShift(),
                allocParamsList.stream()
                        .map(allocParams -> bufferAllocator.calcWeRecordBufferSize(allocParams.requestedRecordBufferSize(), satisfyPercentage) / allocParams.recTypeLength())
                        .min(Integer::compare)
                        .orElse(1 << storageEngineConstants.getChunkSizeShift()));
        long totalAllocation = satisfyPercentage < 1 ? COLLECT_BUFFER_MAX_MEMORY : totalRequestedRecordBufferSize + totalNullBuffs;

        return new CollectBuffersParams(
                collectJuffersWE,
                allocParamsList,
                satisfyPercentage,
                totalAllocation,
                maxRecordsInJuffer);
    }

    private Optional<MemorySegment> allocCollectBuffers(
            CollectBuffersParams collectBuffersParams,
            ThreadArena pageArena,
            MemorySegment collectBuffers,
            List<Integer> blocksToLoad)
    {
        List<CollectBuffersParams.CollectBufAllocParams> allocParamsList = collectBuffersParams.collectAllocParams();
        List<CollectBuffersParams.CollectBufAllocParams> preLoadedAllocParamsList = blocksToLoad.stream()
                .map(allocParamsList::get)
                .toList();
        final int pageSize = storageEngineConstants.getPageSize();

        // allocate total memory for records and nulls
        MemorySegment collectMemory;
        try {
            collectMemory = pageArena.allocate(collectBuffersParams.totalAllocationSize(), pageSize);
        }
        catch (Throwable t) {
            throw new RuntimeException("no memory available for collect size totalBufferSize " + collectBuffersParams.totalAllocationSize());
        }
        SegmentAllocator allocator = SegmentAllocator.slicingAllocator(collectMemory);

        int collectIx = 0;
        int collectBufIx = 0;
        try {
            // perform actual allocation of record and null buffers and create the juffers
            for (CollectBuffersParams.CollectBufAllocParams allocParams : preLoadedAllocParamsList) {
                MemorySegment[] collectSegments = new MemorySegment[JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal()]; // used to create the juffers below
                // record buffer size including the optional part which is calculated using the precentage and masked to page size
                int recordBufferSize = bufferAllocator.calcWeRecordBufferSize(allocParams.requestedRecordBufferSize(), collectBuffersParams.satisfyPercentage());
                allocCollectBuffer(
                        allocator,
                        JbufType.JBUF_TYPE_REC,
                        recordBufferSize,
                        collectSegments);
                // null buffer
                allocCollectBuffer(
                        allocator,
                        JbufType.JBUF_TYPE_NULL,
                        allocParams.nullBufferSize(),
                        collectSegments);
                // create the juffers from the segments
                collectBuffersParams.collectJuffersWE().get(collectIx).createBuffers(
                        allocParams.recTypeCode(),
                        allocParams.recTypeLength(),
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
            shapingLogger.error(t, "failed to allocate record and null buffers totalAllocationSize %d", collectBuffersParams.totalAllocationSize());
            throw new RuntimeException("failed to allocate record and null buffers" +
                    " totalAllocationSize " + collectBuffersParams.totalAllocationSize() +
                    " satisfyPrecentage " + collectBuffersParams.satisfyPercentage() +
                    " collectIx " + collectIx +
                    " numCollectElements " + allocParamsList.size() +
                    " allocParamsList " + allocParamsList +
                    " collectMemory " + collectMemory);
        }
        return Optional.of(collectMemory);
    }

    private CollectMetadataMemory allocCollectMetadataMemory(
            int numPreLoadedElements,
            Optional<byte[]> storeMatchCollectMetadataBuff,
            ThreadArena pageArena)
    {
        final long collectParamsListSize = (long) numPreLoadedElements * ValueLayout.ADDRESS.byteSize();
        final long collectBuffersSize = (long) numPreLoadedElements * JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal() * ValueLayout.JAVA_LONG.byteSize();
        final long recordBufferStatesSize = MemoryLayout.sequenceLayout(numPreLoadedElements, WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT).byteSize();
        final long queryResultTypesSize = MemoryLayout.sequenceLayout(numPreLoadedElements, ValueLayout.JAVA_INT).byteSize();
        final long matchCollectMetadataSize = storeMatchCollectMetadataBuff.map(s -> s.length).orElse(0);
        final long totalSize = collectParamsListSize +
                collectBuffersSize +
                recordBufferStatesSize +
                queryResultTypesSize +
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

        MemorySegment collectParams = allocator.allocate(collectParamsListSize, ValueLayout.ADDRESS.byteSize());
        MemorySegment collectBuffers = allocator.allocate(collectBuffersSize, ValueLayout.JAVA_LONG.byteSize());
        MemorySegment recordBufferStates = allocator.allocate(recordBufferStatesSize, ValueLayout.JAVA_INT.byteSize());
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates =
                recordBufferStates.elements(WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT)
                        .map(recordBufferState -> new WarmupElementRecordBufferState(recordBufferState))
                        .toList();

        return new CollectMetadataMemory(
                Optional.of(collectParams),
                Optional.of(collectBuffers),
                Optional.of(recordBufferStates),
                Optional.of(allocator.allocate(queryResultTypesSize, ValueLayout.JAVA_INT.byteSize())),
                (matchCollectMetadataSize > 0) ? Optional.of(allocator.allocate(matchCollectMetadataSize, ValueLayout.JAVA_INT.byteSize())) : Optional.empty(),
                warmupElementRecordBufferStates);
    }

    private record CollectMetadataMemory(
            Optional<MemorySegment> collectParamsOpt,
            Optional<MemorySegment> collectBuffersOpt,
            Optional<MemorySegment> recordBufferStatesOpt,
            Optional<MemorySegment> queryResultTypesOpt,
            Optional<MemorySegment> matchCollectMetadataOpt,
            List<WarmupElementRecordBufferState> warmupElementRecordBufferStates) {}
}
