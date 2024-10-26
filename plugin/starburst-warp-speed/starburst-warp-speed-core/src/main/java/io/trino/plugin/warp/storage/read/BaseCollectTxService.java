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
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.QueryMemory;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.spi.TrinoException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;

public abstract class BaseCollectTxService
{
    protected static final Logger logger = Logger.get(BaseCollectTxService.class);
    protected static final int INVALID_TX_ID = -1;

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

    protected QueryMemory allocQueryMemory()
    {
        return connectorSync.allocQueryMemory();
    }

    protected SegmentAllocator getQueryMemoryAllocator(QueryMemory queryMemory)
    {
        return SegmentAllocator.slicingAllocator(queryMemory.memory());
    }

    protected void freeQueryMemory(int queryMemoryId)
    {
        if (queryMemoryId != INVALID_TX_ID) {
            connectorSync.freeQueryMemory(queryMemoryId);
        }
    }

    // LazyCollect collects 1 WE at a time, therefore not using queryParams.getCollectElementsParamsList()
    void collectOpen(QueryParams queryParams,
            TxArgs txArgs,
            int collectTxId,
            int numCollectElements,
            int numChunksInRange,
            long warmUpElementAttsAddr,
            long matchBmAddr,
            long recordBufferStatesAddr,
            long recordIndexesAddr)
    {
        txArgs.matchCollectMetadataAddress()[0] = storageEngine.collectOpen(queryParams.getTotalNumRecords(),
                txArgs.fileCookie(),
                collectTxId,
                txArgs.collectStoreBuff(),
                numCollectElements,
                numChunksInRange,
                txArgs.weCollectParams(),
                warmUpElementAttsAddr,
                queryParams.getCatalogContext(),
                queryParams.getMinCollectOffset(),
                matchBmAddr,
                recordBufferStatesAddr,
                recordIndexesAddr,
                txArgs.collectStateBuff().address(),
                txArgs.collectBuffers());
    }

    // prepare chunk with match result, error throws and exception
    // returns true if we should stop before this collect since query result type is now single, false otherwise
    boolean prepareChunk(int collectTxId, int chunkIndex, int numRowsToCollect, int matchBitmapResetPoint)
    {
        logger.debug("prepareChunk chunkIndex %d numRowsToCollect %d matchBitmapResetPoint %d", chunkIndex, numRowsToCollect, matchBitmapResetPoint);
        int ret = storageEngine.processMatchResult(collectTxId,
                chunkIndex,
                matchBitmapResetPoint,
                numRowsToCollect);
        if (ret == -1) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED,
                    String.format("prepareChunk failed unexpectedly collectTxId %d chunkIndex %d matchBitmapResetPoint %d numRowsToCollect %d",
                            collectTxId, chunkIndex, matchBitmapResetPoint, numRowsToCollect));
        }
        return (ret > 0); // if storage engine returned a positive number it means at least one element has a single chunk
    }

    // prepare chunk for full scan case, also used by lazy collect, throws exception if error
    void prepareChunkFullScan(int collectTxId, int chunkIndex, int numRowsToCollect, int startRowIndex)
    {
        logger.debug("prepareChunk chunkIndex %d numRowsToCollect %d startRowIndex %d", chunkIndex, numRowsToCollect, startRowIndex);
        if (storageEngine.processFullScanChunk(collectTxId, chunkIndex, startRowIndex, numRowsToCollect) < 0) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED,
                    String.format("prepareChunk failed unexpectedly collectTxId %d chunkIndex %d startRowIndex %d numRowsToCollect %d",
                            collectTxId, chunkIndex, startRowIndex, numRowsToCollect));
        }
    }

    // returns true if we should stop after this collect since query result type is different than raw, false otherwise
    boolean collectChunk(int txId, int numWes, int chunkIndex, int numToCollect, MemorySegment outQueryResultTypes)
    {
        try {
            storageEngine.collectChunk(txId, numWes, chunkIndex, numToCollect, outQueryResultTypes);
            for (int weIx = 0; weIx < numWes; weIx++) {
                int queryResultType = outQueryResultTypes.getAtIndex(ValueLayout.JAVA_INT, weIx);
                if ((queryResultType == QueryResultType.QUERY_RESULT_TYPE_SINGLE.ordinal()) ||
                        (queryResultType == QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL.ordinal()) ||
                        (queryResultType == QueryResultType.QUERY_RESULT_TYPE_ALL_NULL.ordinal())) {
                    return true;
                }
            }
            return false;
        }
        catch (Exception e) {
            shapingLogger.error(e, "collect failed chunkIndex %d rowsLimit %d numToCollect %d", chunkIndex, numToCollect, numToCollect);
            throw e;
        }
    }

    void collectAbort(Exception e, int collectTxId)
    {
        if (collectTxId != BaseCollectTxService.INVALID_TX_ID) {
            boolean nativeThrowed = false;
            if (e instanceof TrinoException) {
                nativeThrowed = ExceptionThrower.isNativeException((TrinoException) e);
            }
            if (!nativeThrowed) {
                storageEngine.collectClose(collectTxId, null, 0, null, null);
            }
        }
    }

    void allocCollectBuffer(SegmentAllocator queryMemoryAllocator,
            JbufType bufType,
            int bufferSize,
            MemorySegment[] outCollectSegments,
            long[] outCollectBuffers)
    {
        final int alignment = storageEngineConstants.getPageSize();
        outCollectSegments[bufType.ordinal()] = queryMemoryAllocator.allocate(bufferSize, alignment);
        outCollectBuffers[bufType.ordinal()] = outCollectSegments[bufType.ordinal()].address();
    }
}
