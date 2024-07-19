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
package io.trino.plugin.warp.storage.engine.nativeimpl;

import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.di.WarpNativeStorageEngineModule;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil;
import io.trino.plugin.warp.gen.stats.WarpStatsMgr;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.lucene.LuceneMatcher;
import io.trino.plugin.warp.storage.read.StorageCollectorCallBack;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageEngine
        implements StorageEngine
{
    private static final Logger logger = Logger.get(NativeStorageEngine.class);
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ExceptionThrower exceptionThrower; // we keep a reference to hold this object for native layer ref

    public NativeStorageEngine(
            NativeConfig nativeConfig,
            MetricsManager metricsManager,
            ExceptionThrower exceptionThrower,
            GlobalConfig globalConfig)
    {
        this.exceptionThrower = requireNonNull(exceptionThrower);

        final int taskMaxWorkerThreads = nativeConfig.getTaskMaxWorkerThreads();
        final int panicHaltPolicy = nativeConfig.getDebugPanicHaltPolicy();
        logger.info("load storage engine taskMaxWorkerThreads %d panicHaltPolicy %d bundleSize %d",
                taskMaxWorkerThreads,
                panicHaltPolicy,
                nativeConfig.getBundleSize());
        try {
            nativeInit(taskMaxWorkerThreads,
                    Runtime.getRuntime().maxMemory(),
                    nativeConfig.getGeneralReservedMemory(),
                    nativeConfig.getBundleSize(),
                    nativeConfig.getMaxRecJufferSize(),
                    nativeConfig.getCompressionLevel(),
                    panicHaltPolicy,
                    nativeConfig.getCollectTxSize(),
                    nativeConfig.getStorageCacheSizeInPages(),
                    PredicateUtil.PREDICATE_HEADER_SIZE,
                    nativeConfig.getSkipIndexPercent(),
                    WarpNativeStorageEngineModule.getNativeLibrariesDirectory().toString(),
                    nativeConfig.getEnableSingleChunk(),
                    nativeConfig.getEnableQueryResultType(),
                    nativeConfig.getEnablePackedChunk(),
                    nativeConfig.getEnableWarmingExtraLogs(),
                    nativeConfig.getEnableCompression(),
                    nativeConfig.getExceptionalListCompression(),
                    globalConfig.getDebugWarming());
        }
        catch (Throwable t) {
            logger.error(t, "failed loading storage engine");
            throw t;
        }
        new WarpStatsMgr(metricsManager);
        logger.debug("finish initializing storage engine");
    }

    private native void nativeInit(int maxWorkerThreads,
            long jvmMemory,
            long generalReservedMemory,
            int bundleSizeInBytes,
            int maxRecJufferSize,
            int lz4HcPercent,
            int panicHaltPolicy,
            int collectTxSize,
            int storageCacheSizeInPages,
            int predicateHeaderSize,
            int skipIndexPercentage,
            String libraryPath,
            boolean enableSingleChunk,
            boolean enableQueryResultType,
            boolean enablePackedChunk,
            boolean enableWarmingExtraLogs,
            boolean enableCompression,
            int exceptionalListCompression,
            boolean validateWarmId);

    @Override
    public native void initRecordBufferSizes(int[] fixedRecordBufferSizes, int[] varlenRecordBufferSizes);

    @Override
    public native void initCollectTxSizes(int[] fixedCollectTxSizes, int[] varlenCollectTxSizes);

    @Override
    public native long initWarmupTxSizes(int[] fixedWarmupDataTxSizes, int[] varlenWarmupDataTxSizes);

    @Override
    public native long fileOpen(String fileName);

    @Override
    public native void fileClose(long fileFd);

    @Override
    public native void fileTruncate(long[] fileCookie, int offset);

    @Override
    public native void filePunchHole(String fileName, int startOffset, int endOffset);

    @Override
    public native void fileIsAboutToBeDeleted(long fileHash, long fileModTime, int fileSizeInPages);

    @Override
    public native long warmupElementOpen(long context, int recTypeCode, int recTypeLength, int warmUpType);

    @Override
    public native long warmupElementClose(long weCookie, int recTypeCode, int recTypeLength, int warmUpType, long[] fileCookie, long[] buffAddresses, int[] outQueryFileParams);

    @Override
    public native void warmupVerifyQueryOffset(int queryOffset, long[] fileCookie);

    @Override
    public native long warmupChunk(long weCookie, int addedNumRows, int addedNV, int addedBytes, long valueMin, long valueMax, int singleValOffset,
            boolean close, int recTypeCode, int recTypeLength, int warmUpType, long[] fileCookieParams, long[] buffAddresses,
            byte[] inOutCompressionStats, byte[] outChunkCookies, int[] outWarmEvents);

    @Override
    public native long warmupChunkExtRec(long weCookie, int extRecordFirstOffset, int addedExtBytes, int recTypeCode, int recTypeLength, int warmUpType, long[] fileCookieParams, long[] buffAddresses);

    @Override
    public native long warmupLucene(long weCookie, int fileId, int offset, int len, int recTypeCode, int recTypeLength, int warmUpType, long[] fileCookieParams, long[] buffAddresses);

    @Override
    public native long warmupLuceneChunk(long weCookie, boolean singleVal, int[] fileLengths, int recTypeCode, int recTypeLength, int warmUpType, long[] fileCookieParams, long[] buffAddresses);

    @Override
    public native int queryGetCollect2MatchSize();

    @Override
    public native long queryGetCollectStateSize(int numMatchCollect);

    @Override
    public native long collectOpen(int totalNumRecords, long[] fileCookie, byte[] parsingBuff, byte[] collect2MatchParams, int numCollectWes,
            int[] weCollectParams, int connectorId, long matchBitmapAddress, int minOffset,
            long[][] outCollectColBuffIds, long[] outMetadataBuffIds, int[] outResultType);

    @Override
    public native long matchOpen(int totalNumRecords, long[] fileCookie, int collectTxId, byte[] parsingBuff, byte[] collect2MatchParams,
            int numMatchWes, int[] weMatchTree, int numLucenes, LuceneMatcher[] luceneMatchers, long matchBitmapAddress, int minOffset, long[][] outMatchColBuffIds);

    @Override
    public native long collectRestoreState(int txId, int chunkIndex, StorageCollectorCallBack collectStateObj);

    @Override
    public native long match(int txId, int nextState, short[] outMatchedChunksIndexes, int[] outMatchBitmapResetPoints);

    @Override
    public native long processMatchResult(int txId, int chunkIndex, int bitmapResetPoint, int rowsLimit, int[] resultTypes);

    @Override
    public native void collect(int txId, int startWeIx, int endWeIx, int chunkIndex, int numToCollect, int[] outResultTypes);

    @Override
    public native long collectClose(int txId, int[] chunksWithBitmapsToStore, int numChunksWithBitmaps, StorageCollectorCallBack obj);

    @Override
    public native void matchClose(int txId);

    @Override
    public native ByteBuffer getBundleFromPool(int bufIx);

    @Override
    public native void setDebugThrowPolicy(int numElements, int[] panicID, int[] repetitionMode, int[] ratio);

    @Override
    public native String executeDebugCommand(String commandName, int numParams, String[] paramNames, String[] paramValues);

    @Override
    public native int luceneReadBuffer(long nativeCookie, int fileId, int offset, int length);
}
