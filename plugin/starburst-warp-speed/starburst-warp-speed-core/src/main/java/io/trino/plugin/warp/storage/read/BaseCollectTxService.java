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
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.spi.TrinoException;

import static io.trino.plugin.warp.WarpErrorCode.WARP_TX_ALLOCATION_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;

public abstract class BaseCollectTxService
{
    protected static final Logger logger = Logger.get(BaseCollectTxService.class);
    protected static final int INVALID_TX_ID = -1;

    protected final StorageEngine storageEngine;
    protected final ShapingLogger shapingLogger;

    public BaseCollectTxService(StorageEngine storageEngine, GlobalConfig globalConfig)
    {
        this.storageEngine = storageEngine;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    // LazyCollect collects 1 WE at a time, therefore not using queryParams.getCollectElementsParamsList()
    int collectOpen(CollectTxArgs collectTxArgs, int numCollectElements, int numChunksInRange, long matchBmAddr, long[] metadataBuffIds)
    {
        metadataBuffIds[0] = -1;
        metadataBuffIds[1] = -1;
        QueryParams queryParams = collectTxArgs.queryParams();

        int collectTxId = (int) storageEngine.collectOpen(queryParams.getTotalNumRecords(),
                collectTxArgs.fileCookie(),
                collectTxArgs.collectStoreBuff(),
                collectTxArgs.collect2MatchParams(),
                numCollectElements,
                numChunksInRange,
                collectTxArgs.weCollectParams(),
                queryParams.getCatalogSequence(),
                matchBmAddr,
                queryParams.getMinCollectOffset(),
                collectTxArgs.collectBuffIds(),
                metadataBuffIds);
        if (collectTxId < 0) {
            throw new TrinoException(WARP_TX_ALLOCATION_FAILED, "failed to allocate tx for collect");
        }
        return collectTxId;
    }

    // prepare chunk with match result, error throws and exception
    void prepareChunk(int collectTxId, int chunkIndex, int numRowsToCollect, int matchBitmapResetPoint)
    {
        logger.debug("prepareChunk chunkIndex %d numRowsToCollect %d matchBitmapResetPoint %d", chunkIndex, numRowsToCollect, matchBitmapResetPoint);
        int ret = (int) storageEngine.processMatchResult(collectTxId,
                chunkIndex,
                matchBitmapResetPoint,
                numRowsToCollect);
        if (ret == -1) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED,
                    String.format("prepareChunk failed unexpectedly collectTxId %d chunkIndex %d matchBitmapResetPoint %d numRowsToCollect %d",
                            collectTxId, chunkIndex, matchBitmapResetPoint, numRowsToCollect));
        }
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

    void collect(int txId, int numWes, int chunkIndex, int numToCollect, int[] outQueryResultType)
    {
        try {
            storageEngine.collect(txId, numWes, chunkIndex, numToCollect, outQueryResultType);
            for (int weIx = 0; weIx < outQueryResultType.length; weIx++) {
                if (outQueryResultType[weIx] == QueryResultType.QUERY_RESULT_TYPE_SINGLE.ordinal()) {
                    outQueryResultType[weIx] = QueryResultType.QUERY_RESULT_TYPE_RAW.ordinal();
                }
                if (outQueryResultType[weIx] == QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL.ordinal()) {
                    outQueryResultType[weIx] = QueryResultType.QUERY_RESULT_TYPE_RAW_NO_NULL.ordinal();
                }
            }
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
                storageEngine.collectClose(collectTxId, null, 0, null);
            }
        }
    }
}
