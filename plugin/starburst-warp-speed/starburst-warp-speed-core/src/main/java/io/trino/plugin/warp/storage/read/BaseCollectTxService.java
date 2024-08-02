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
import io.trino.plugin.warp.log.ShapingLogger;
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
    int collectOpen(CollectTxArgs collectTxArgs, int numChunksInRange, long matchBmAddr, long[] metadataBuffIds, int[] outResultType)
    {
        int numCollectElements = outResultType.length;
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
                metadataBuffIds,
                outResultType);
        if (collectTxId < 0) {
            throw new TrinoException(WARP_TX_ALLOCATION_FAILED, "failed to allocate tx for collect");
        }
        return collectTxId;
    }

    // returns > 0 if buffer is exhausted and we need to stop collecting, 0 if not, -1 for error
    int prepareNextChunk(int chunkIx,
            int curResetPoint,
            boolean chunkPrepared,
            int collectTxId,
            int rowsLimit,
            int numCollectedRows,
            int[] outResultType)
    {
        int ret = 0;
        if (!chunkPrepared) {
            logger.debug("collectFromStorage process match chunkIndex %d bitmapResetPoint %d rowsLimit %d numCollectedRows %d",
                    chunkIx, curResetPoint, rowsLimit, numCollectedRows);
            ret = (int) storageEngine.processMatchResult(collectTxId,
                    chunkIx,
                    curResetPoint,
                    rowsLimit - numCollectedRows,
                    outResultType);
            if (ret == -1) {
                throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED,
                        String.format("prepareNextChunk failed unexpectedly collectTxId %d chunkIx %d resetPoint %d numCollectedRows %d rowsLimit %d",
                                collectTxId,
                                chunkIx,
                                curResetPoint,
                                numCollectedRows,
                                rowsLimit));
            }
        }
        return ret;
    }

    void collect(int txId, int[] outResultType, int numWes, int chunkIndex, int numToCollect)
    {
        try {
            storageEngine.collect(txId, numWes, chunkIndex, numToCollect, outResultType);
        }
        catch (Exception e) {
            shapingLogger.error(e, "collect failed chunkIndex %d rowsLimit %d numToCollect %d", chunkIndex, numToCollect, numToCollect);
            throw e;
        }
    }
}
