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
import io.trino.plugin.warp.storage.read.StorageCollectorCallBack;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageEngine
        implements StorageEngine
{
    private static final Logger logger = Logger.get(NativeStorageEngine.class);
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ExceptionThrower exceptionThrower; // we keep a reference to hold this object for native layer ref
    // file API
    private final MethodHandle mFileOpen;
    private final MethodHandle mFileClose;
    private final MethodHandle mFileTruncate;
    private final MethodHandle mFilePunchHole;
    private final MethodHandle mFileAboutToBeDeleted;
    // initialization API
    private final MethodHandle mInitGetFixedRecordBufferSize;
    private final MethodHandle mInitGetVarlenRecordBufferSize;
    private final MethodHandle mInitGetFixedCollectTxSize;
    private final MethodHandle mInitGetVarlenCollectTxSize;
    private final MethodHandle mInitGetFixedWarmupDataTxSize;
    private final MethodHandle mInitGetVarlenWarmupDataTxSize;
    private final MethodHandle mInitGetWarmupBasicTxSize;
    private final MethodHandle mInitGetWarmupLuceneTxSize;

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
            SymbolLookup libraryHandle = SymbolLookup.loaderLookup();
            Linker linker = Linker.nativeLinker();

            // file API
            mFileOpen = linker.downcallHandle(libraryHandle.find("storage_file_open").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
            mFileClose = linker.downcallHandle(libraryHandle.find("storage_file_close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT));
            mFileTruncate = linker.downcallHandle(libraryHandle.find("storage_file_truncate").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mFilePunchHole = linker.downcallHandle(libraryHandle.find("storage_file_punch_hole").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mFileAboutToBeDeleted = linker.downcallHandle(libraryHandle.find("warp_speed_file_is_about_to_be_deleted").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));

            // init API
            mInitGetFixedRecordBufferSize = linker.downcallHandle(libraryHandle.find("we_get_fixed_record_buffer_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetVarlenRecordBufferSize = linker.downcallHandle(libraryHandle.find("we_get_varlen_record_buffer_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetFixedCollectTxSize = linker.downcallHandle(libraryHandle.find("data_chunk_get_fixed_query_tx_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetVarlenCollectTxSize = linker.downcallHandle(libraryHandle.find("data_chunk_get_varlen_query_tx_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetFixedWarmupDataTxSize = linker.downcallHandle(libraryHandle.find("data_chunk_get_fixed_warmup_tx_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetVarlenWarmupDataTxSize = linker.downcallHandle(libraryHandle.find("data_chunk_get_varlen_warmup_tx_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetWarmupBasicTxSize = linker.downcallHandle(libraryHandle.find("index_chunk_tx_warmup_alloc_get_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            mInitGetWarmupLuceneTxSize = linker.downcallHandle(libraryHandle.find("lucene_chunk_tx_warmup_alloc_get_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));

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
    public int getFixedRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            logger.error(t, "failed to init record buffer size");
            throw new RuntimeException("failed to init record buffer size");
        }
    }

    @Override
    public int getVarlenRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetVarlenRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            logger.error(t, "failed to init record buffer size");
            throw new RuntimeException("failed to init record buffer size");
        }
    }

    @Override
    public int getFixedCollectTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedCollectTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            logger.error(t, "failed to init collect tx size");
            throw new RuntimeException("failed to init collect tx size");
        }
    }

    @Override
    public int getVarlenCollectTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetVarlenCollectTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            logger.error(t, "failed to init collect tx size");
            throw new RuntimeException("failed to init collect tx size");
        }
    }

    @Override
    public int getFixedWarmupDataTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedWarmupDataTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            logger.error(t, "failed to init warmup tx size");
            throw new RuntimeException("failed to init warmup tx size");
        }
    }

    @Override
    public int getVarlenWarmupDataTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetVarlenWarmupDataTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            logger.error(t, "failed to init warmup tx size");
            throw new RuntimeException("failed to init warmup tx size");
        }
    }

    @Override
    public int getWarmupBasicTxSize()
    {
        try {
            return (int) mInitGetWarmupBasicTxSize.invokeExact();
        }
        catch (Throwable t) {
            logger.error(t, "failed to init warmup tx size");
            throw new RuntimeException("failed to init warmup tx size");
        }
    }

    @Override
    public int getWarmupLuceneTxSize()
    {
        try {
            return (int) mInitGetWarmupLuceneTxSize.invokeExact();
        }
        catch (Throwable t) {
            logger.error(t, "failed to init warmup tx size");
            throw new RuntimeException("failed to init warmup tx size");
        }
    }

    @Override
    public int fileOpen(String fileName)
    {
        int fileDescriptor;
        try (Arena arena = Arena.ofConfined()) {
            fileDescriptor = (int) mFileOpen.invokeExact(arena.allocateFrom(fileName));
            if (fileDescriptor >= 0) {
                return fileDescriptor;
            }
        }
        catch (Throwable t) {
            logger.error(t, "failed to open file");
        }
        throw new RuntimeException("failed to open file " + fileName);
    }

    @Override
    public void fileClose(int fileDescriptor)
    {
        try {
            mFileClose.invokeExact(fileDescriptor);
        }
        catch (Throwable t) {
            logger.error(t, "failed to close file");
            throw new RuntimeException("failed to close file");
        }
    }

    @Override
    public void fileTruncate(int fileDescriptor, int offset)
    {
        boolean success;
        try {
            success = (boolean) mFileTruncate.invokeExact(fileDescriptor, offset);
            if (success) {
                return;
            }
        }
        catch (Throwable t) {
            logger.error(t, "failed to truncate file");
        }
        throw new RuntimeException("failed to truncate file");
    }

    @Override
    public void filePunchHole(String fileName, int startOffset, int endOffset)
    {
        try (Arena arena = Arena.ofConfined()) {
            mFilePunchHole.invokeExact(arena.allocateFrom(fileName), startOffset, endOffset);
        }
        catch (Throwable t) {
            logger.error(t, "failed to punch hole file");
            throw new RuntimeException("failed to punch hole file" + fileName);
        }
    }

    @Override
    public void fileIsAboutToBeDeleted(long fileHash, long fileModTime, int fileSizeInPages)
    {
        try {
            mFileAboutToBeDeleted.invokeExact(fileHash, fileModTime, fileSizeInPages);
        }
        catch (Throwable t) {
            logger.error(t, "failed to clear native cache");
            throw new RuntimeException("failed to clear native cache");
        }
    }

    @Override
    public native long warmupElementOpen(long context, int recTypeCode, int recTypeLength, int warmUpType);

    @Override
    public native long warmupElementClose(int recTypeCode,
            int recTypeLength,
            int warmUpType,
            int numChunks,
            long[] fileCookie,
            long[] buffAddresses,
            int[] outQueryFileParams);

    @Override
    public native void warmupVerifyQueryOffset(int queryOffset, long[] fileCookie);

    @Override
    public native long warmupChunk(long weCookie, int addedNumRows, int addedNV, int addedBytes, long valueMin, long valueMax, int singleValOffset,
            boolean close, int recTypeCode, int recTypeLength, int warmUpType, long[] fileCookieParams, long[] buffAddresses,
            byte[] inOutCompressionStats, byte[] inOutChunkHeader, int[] outWarmEvents);

    @Override
    public native long warmupChunkExtRec(long weCookie, int extRecordFirstOffset, int addedExtBytes, int recTypeCode, int recTypeLength, int warmUpType,
            long[] fileCookieParams, long[] buffAddresses, byte[] inOutChunkHeader);

    @Override
    public native long warmupLucene(long weCookie, int fileId, int offset, int len, int recTypeCode, int recTypeLength, int warmUpType,
            long[] fileCookieParams, long[] buffAddresses, byte[] inOutChunkHeader);

    @Override
    public native long warmupLuceneChunk(long weCookie, boolean singleVal, int[] fileLengths, int recTypeCode, int recTypeLength, int warmUpType,
            long[] fileCookieParams, long[] buffAddresses, byte[] inOutChunkHeader);

    @Override
    public native int queryGetCollect2MatchSize();

    @Override
    public native long queryGetCollectStateSize(int numMatchCollect);

    @Override
    public native long collectOpen(int totalNumRecords, long[] fileCookie, byte[] parsingBuff, byte[] collect2MatchParams, int numCollectWes,
            int numChunksInRange, int[] weCollectParams, int connectorId, long matchBitmapAddress, int minOffset,
            long[][] outCollectColBuffIds, long[] outMetadataBuffIds);

    @Override
    public native long matchOpen(int totalNumRecords, long[] fileCookie, int collectTxId, byte[] parsingBuff, byte[] collect2MatchParams, int numMatchWes,
            int numChunksInRange, int[] weMatchTree, long matchBitmapAddress, int minOffset, long[][] outMatchColBuffIds);

    @Override
    public native long collectRestoreState(int txId, int chunkIndex, StorageCollectorCallBack collectStateObj);

    @Override
    public native long matchAgg(int txId, int startChunkIndex);

    @Override
    public native long matchLucenePrepare(int matchTxId, int matchWeIx, int startChunkIndex, int numChunks, long[] outParams);

    @Override
    public native void matchLuceneCompleted(int matchTxId, int matchWeIx, int startChunkIndex, int numChunks, int[] matchResult);

    @Override
    public native long match(int txId, int startChunkIndex, int numChunks, short[] outMatchedChunksIndexes, int[] outMatchBitmapResetPoints);

    @Override
    public native long processMatchResult(int txId, int chunkIndex, int bitmapResetPoint, int rowsLimit);

    @Override
    public native long processFullScanChunk(int txId, int chunkIndex, int startRowIx, int rowsLimit);

    @Override
    public native void collect(int txId, int numWes, int chunkIndex, int numToCollect, int[] outResultTypes);

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
    public native int luceneReadBuffer(int txId, long nativeCookie, int fileId, int offset, int length);
}
