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
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.spi.TrinoException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.Locale;

import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;

public abstract class BaseCollectTxService
{
    protected static final Logger logger = Logger.get(BaseCollectTxService.class);

    protected final StorageEngine storageEngine;
    protected final StorageEngineConstants storageEngineConstants;
    protected final BufferAllocator bufferAllocator;
    protected final NativeConfig nativeConfig;
    protected final ShapingLogger shapingLogger;

    public BaseCollectTxService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            ShapingLoggerFactory shapingLoggerFactory,
            NativeConfig nativeConfig)
    {
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.bufferAllocator = bufferAllocator;
        this.nativeConfig = nativeConfig;
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    void collectOpen(CollectState collectState, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        long startTime = System.nanoTime();
        storageEngine.collectOpen(collectState.getStateMemory());
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
    }

    // open chunk before collect
    void openChunk(CollectState collectState,
            int chunkIndex,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        logger.debug("openChunk chunkIndex %d", chunkIndex);
        long startTime = System.nanoTime();
        boolean success = storageEngine.openChunk(collectState.getStateMemory(),
                chunkIndex);
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
        if (!success) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED,
                    String.format(Locale.US, "openChunk failed unexpectedly chunkIndex %d", chunkIndex));
        }
    }

    void collectChunk(CollectState collectState,
            MemorySegment outQueryResultTypes,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        long startTime = System.nanoTime();
        storageEngine.collectChunk(collectState.getStateMemory(), outQueryResultTypes);
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
    }

    void collectAbort(Exception e, CollectState collectState, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        boolean nativeThrowed = false;
        if (e instanceof TrinoException te) {
            nativeThrowed = ExceptionThrower.isNativeException(te);
        }
        if (!nativeThrowed) {
            long startTime = System.nanoTime();
            if (collectState != null) {
                storageEngine.collectClose(collectState.getStateMemory(), MemorySegment.NULL);
            }
            dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
        }
    }

    void allocCollectBuffer(SegmentAllocator allocator,
            JbufType bufType,
            int bufferSize,
            MemorySegment[] outCollectSegments)
    {
        final int alignment = storageEngineConstants.getPageSize();
        outCollectSegments[bufType.ordinal()] = allocator.allocate(bufferSize, alignment);
    }
}
