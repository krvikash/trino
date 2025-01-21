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
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

public class LazyCollectTxService
        extends BaseCollectTxService
{
    private final WorkerMemoryManager workerMemoryManager;

    @Inject
    public LazyCollectTxService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            WorkerMemoryManager workerMemoryManager,
            ShapingLoggerFactory shapingLoggerFactory,
            NativeConfig nativeConfig)
    {
        super(storageEngine, storageEngineConstants, bufferAllocator, shapingLoggerFactory, nativeConfig);
        this.workerMemoryManager = workerMemoryManager;
    }

    LazyCollectOpenResult collectOpen(LazyCollectorLoaderArgs lazyCollectorLoaderArgs, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        // initialize warm up element properties
        WarmupElementCollectParams collectParams = lazyCollectorLoaderArgs.collectParams();
        MemorySegment warmupElementAtt = collectParams.getWarmupElementAtt();
        final RecTypeCode recTypeCode = WarmUpElement.getRecTypeCode(warmupElementAtt);
        final int recTypeLength = WarmUpElement.getRecTypeLength(warmupElementAtt);

        // initialize memory parameters
        final int recordBufferSize = bufferAllocator.calcWeRecordBufferSize(bufferAllocator.getCollectRecordBufferSize(recTypeCode, recTypeLength), 1);
        final int nullBufferSize = bufferAllocator.getQueryNullBufferSize(recTypeCode);

        // allocate memory
        ThreadArena pageArena = workerMemoryManager.getThreadArena();
        MemorySegment collectMemory;
        MemorySegment metadataMemory;
        try {
            final int pageSize = storageEngineConstants.getPageSize();
            collectMemory = pageArena.allocate(recordBufferSize + nullBufferSize + pageSize, pageSize);
            metadataMemory = pageArena.allocate(RecordIndexes.RECORD_INDEXES_LAYOUT.byteSize() +
                    JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal() * ValueLayout.JAVA_LONG.byteSize() +
                    WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT.byteSize() +
                    ValueLayout.JAVA_LONG.byteSize(), ValueLayout.JAVA_LONG.byteSize());
        }
        catch (Throwable t) {
            throw new RuntimeException("no memory available for lazy collect size requestedRecordBufferSize " + recordBufferSize + " nullBufferSize " + nullBufferSize);
        }
        SegmentAllocator queryMemoryAllocator = SegmentAllocator.slicingAllocator(collectMemory);
        SegmentAllocator metadataAllocator = SegmentAllocator.slicingAllocator(metadataMemory);

        // allocate metadata
        RecordIndexes recordIndexes = new RecordIndexes(metadataAllocator);
        MemorySegment collectBuffers = metadataAllocator.allocate(JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal() * ValueLayout.JAVA_LONG.byteSize(), ValueLayout.JAVA_LONG.byteSize());
        MemorySegment recordBufferStates = metadataAllocator.allocate(WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize());

        // allcoate buffers
        MemorySegment[] collectSegments = new MemorySegment[JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal()];
        allocCollectBuffer(queryMemoryAllocator, JbufType.JBUF_TYPE_REC, recordBufferSize, collectSegments);
        allocCollectBuffer(queryMemoryAllocator, JbufType.JBUF_TYPE_NULL, nullBufferSize, collectSegments);
        lazyCollectorLoaderArgs.collectJufferWE().createBuffers(recTypeCode, recTypeLength, collectParams.hasDictionary(), collectSegments);
        collectBuffers.setAtIndex(ValueLayout.JAVA_LONG, JbufType.JBUF_TYPE_REC.ordinal(), collectSegments[JbufType.JBUF_TYPE_REC.ordinal()].address());
        collectBuffers.setAtIndex(ValueLayout.JAVA_LONG, JbufType.JBUF_TYPE_NULL.ordinal(), collectSegments[JbufType.JBUF_TYPE_NULL.ordinal()].address());

        CollectState collectState = new CollectState(pageArena,
                storageEngineConstants.getCollectStatePayload(),
                nativeConfig.getLimitNumIosInParallel() * nativeConfig.getMaxIOMetadataSize());
        collectState.setLazyState(lazyCollectorLoaderArgs.queryParams(),
                lazyCollectorLoaderArgs.fileCookie(),
                lazyCollectorLoaderArgs.numChunksInRange(),
                recordBufferStates,
                recordIndexes,
                collectBuffers,
                collectParams.getMemory());
        collectOpen(collectState, dispatcherPageSourceStats);
        return new LazyCollectOpenResult(collectState,
                pageArena,
                recordIndexes,
                collectBuffers,
                recordBufferStates,
                new ReadStats(pageArena));
    }

    // Lazy collect doesn't use store/restore mechanism, so store/restore params are not initialized
    void collectClose(LazyCollectOpenResult collectOpenResult, NativeStats nativeStats, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        long startTime = System.nanoTime();
        storageEngine.collectClose(collectOpenResult.collectState().getStateMemory(), collectOpenResult.readStats().getMemory());
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);

        collectOpenResult.readStats().fillStats(nativeStats);
        collectOpenResult.pageArena().close();
    }
}
