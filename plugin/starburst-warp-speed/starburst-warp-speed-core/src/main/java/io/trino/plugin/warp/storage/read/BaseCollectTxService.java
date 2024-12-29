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
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.spi.TrinoException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;

public abstract class BaseCollectTxService
{
    protected static final Logger logger = Logger.get(BaseCollectTxService.class);

    protected final StorageEngine storageEngine;
    protected final StorageEngineConstants storageEngineConstants;
    protected final ConnectorSync connectorSync;
    protected final BufferAllocator bufferAllocator;
    protected final GlobalConfig globalConfig;
    protected final ShapingLogger shapingLogger;

    public BaseCollectTxService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ConnectorSync connectorSync,
            BufferAllocator bufferAllocator,
            GlobalConfig globalConfig)
    {
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.connectorSync = connectorSync;
        this.bufferAllocator = bufferAllocator;
        this.globalConfig = globalConfig;
        this.shapingLogger = ShapingLogger.getInstance(logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    protected int allocReaderId()
    {
        return connectorSync.allocReaderId();
    }

    protected void freeQueryMemory(int readerId)
    {
        connectorSync.freeReaderId(readerId);
    }

    // LazyCollect collects 1 WE at a time, therefore not using queryParams.getCollectElementsParamsList()
    void collectOpen(CollectState collectState, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        long startTime = System.nanoTime();
        storageEngine.collectOpen(collectState.getStateMemory());
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
    }

    // prepare chunk with match result, error throws and exception
    void prepareChunk(CollectState collectState,
            int chunkIndex,
            int numRowsToCollect,
            int bitmapResetPoint,
            MemorySegment outQueryResultTypes,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        logger.debug("prepareChunk chunkIndex %d numRowsToCollect %d", chunkIndex, numRowsToCollect);
        long startTime = System.nanoTime();
        boolean success = storageEngine.processMatchResult(collectState.getStateMemory(),
                chunkIndex,
                bitmapResetPoint,
                numRowsToCollect,
                outQueryResultTypes);
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
        if (!success) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, String.format("prepareChunk failed unexpectedly chunkIndex %d numRowsToCollect %d",
                    chunkIndex, numRowsToCollect));
        }
    }

    // prepare chunk for full scan case, also used by lazy collect, throws exception if error
    void prepareChunkFullScan(CollectState collectState, int chunkIndex, int numRowsToCollect, int startRowIndex, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        logger.debug("prepareChunk chunkIndex %d numRowsToCollect %d startRowIndex %d", chunkIndex, numRowsToCollect, startRowIndex);
        long startTime = System.nanoTime();
        boolean success = storageEngine.processFullScanChunk(collectState.getStateMemory(), chunkIndex, startRowIndex, numRowsToCollect);
        dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
        if (!success) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, String.format("prepareChunk failed unexpectedly chunkIndex %d startRowIndex %d numRowsToCollect %d",
                    chunkIndex, startRowIndex, numRowsToCollect));
        }
    }

    void collectChunk(CollectState collectState, int chunkIndex, int numToCollect, MemorySegment outQueryResultTypes, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        try {
            long startTime = System.nanoTime();
            storageEngine.collectChunk(collectState.getStateMemory(), chunkIndex, numToCollect, outQueryResultTypes);
            dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
        }
        catch (Exception e) {
            shapingLogger.error(e, "collect failed chunkIndex %d rowsLimit %d numToCollect %d", chunkIndex, numToCollect, numToCollect);
            throw e;
        }
    }

    void collectAbort(Exception e, CollectState collectState, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        boolean nativeThrowed = false;
        if (e instanceof TrinoException) {
            nativeThrowed = ExceptionThrower.isNativeException((TrinoException) e);
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
