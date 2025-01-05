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
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageEngine
        implements StorageEngine
{
    private static final Logger logger = Logger.get(NativeStorageEngine.class);

    private static final StructLayout ENV_PROPERTIES_LAYOUT;
    private static final long ENV_PROPERTIES_OFFSET_LIBRARY_PATH;
    private static final long ENV_PROPERTIES_OFFSET_JVM_MEMORY;
    private static final long ENV_PROPERTIES_OFFSET_MAX_WORKER_THREADS;
    private static final long ENV_PROPERTIES_OFFSET_MAX_REC_BUF_SIZE;
    private static final long ENV_PROPERTIES_OFFSET_PANIC_HALT_POLICY;
    private static final long ENV_PROPERTIES_OFFSET_LZ4_HC_PERCENT;
    private static final long ENV_PROPERTIES_OFFSET_COLLECT_TX_SIZE;
    private static final long ENV_PROPERTIES_OFFSET_STORAGE_CACHE_SIZE_IN_PAGES;
    private static final long ENV_PROPERTIES_OFFSET_PREDICATE_HEADER_SIZE;
    private static final long ENV_PROPERTIES_OFFSET_SKIP_INDEX_PERCENT;

    private static final StructLayout ENV_ENABLE_CONFIG_LAYOUT;
    private static final long ENV_ENABLE_CONFIG_OFFSET_COMPRESSION_EXCEPTION_LIST;
    private static final long ENV_ENABLE_CONFIG_OFFSET_SINGLE_CHUNK;
    private static final long ENV_ENABLE_CONFIG_OFFSET_PACKED_CHUNK;
    private static final long ENV_ENABLE_CONFIG_OFFSET_COMPRESSION;
    private static final long ENV_ENABLE_CONFIG_OFFSET_VALIDATE_WARM_ID;

    private final ShapingLogger shapingLogger;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ExceptionThrower exceptionThrower; // we keep a reference to hold this object for native layer ref
    private final boolean loaded;

    // file API
    private final MethodHandle mFileOpen;
    private final MethodHandle mFileClose;
    private final MethodHandle mFileTruncate;
    private final MethodHandle mFilePunchHole;
    private final MethodHandle mFileAboutToBeDeleted;
    // initialization API
    private final MethodHandle mInitEnv;
    private final MethodHandle mInitGetWarmupRecordBufferSize;
    private final MethodHandle mInitGetFixedCollectRecordBufferSize;
    private final MethodHandle mInitGetVarlenCollectRecordBufferSize;
    private final MethodHandle mInitGetFixedCollectTxSize;
    private final MethodHandle mInitGetVarlenCollectTxSize;
    private final MethodHandle mInitGetFixedWarmupDataTxSize;
    private final MethodHandle mInitGetVarlenWarmupDataTxSize;
    private final MethodHandle mInitGetWarmupBasicTxSize;
    private final MethodHandle mInitGetWarmupLuceneTxSize;
    // warmup API
    private final MethodHandle mWarmupElementOpen;
    private final MethodHandle mWarmupElementClose;
    private final MethodHandle mWarmupVerifyQueryOffset;
    private final MethodHandle mWarmupChunk;
    private final MethodHandle mWarmupChunkExtRec;
    // match API
    private final MethodHandle mMatchOpen;
    private final MethodHandle mMatchAgg;
    private final MethodHandle mMatchLucenePrepare;
    private final MethodHandle mMatchLuceneCompleted;
    private final MethodHandle mMatch;
    private final MethodHandle mMatchClose;
    // collect API
    private final MethodHandle mCollectOpen;
    private final MethodHandle mCollectProcessMatchResult;
    private final MethodHandle mCollectProcessFullScanChunk;
    private final MethodHandle mCollectCollectChunk;
    private final MethodHandle mCollectClose;

    static {
        ENV_PROPERTIES_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.ADDRESS.withName("plibrary_path"),
                ValueLayout.JAVA_LONG.withName("jvm_memory"),
                ValueLayout.JAVA_INT.withName("max_worker_threads"),
                ValueLayout.JAVA_INT.withName("max_rec_buf_size_in_bytes"),
                ValueLayout.JAVA_INT.withName("panic_halt_policy"),
                ValueLayout.JAVA_INT.withName("lz4_hc_percent"),
                ValueLayout.JAVA_INT.withName("collect_tx_size"),
                ValueLayout.JAVA_INT.withName("storage_cache_size_in_pages"),
                ValueLayout.JAVA_INT.withName("predicate_header_size"),
                ValueLayout.JAVA_INT.withName("skip_index_percent")).withName("env_properties_t");

        ENV_PROPERTIES_OFFSET_LIBRARY_PATH = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("plibrary_path"));
        ENV_PROPERTIES_OFFSET_JVM_MEMORY = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("jvm_memory"));
        ENV_PROPERTIES_OFFSET_MAX_WORKER_THREADS = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("max_worker_threads"));
        ENV_PROPERTIES_OFFSET_MAX_REC_BUF_SIZE = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("max_rec_buf_size_in_bytes"));
        ENV_PROPERTIES_OFFSET_PANIC_HALT_POLICY = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("panic_halt_policy"));
        ENV_PROPERTIES_OFFSET_LZ4_HC_PERCENT = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("lz4_hc_percent"));
        ENV_PROPERTIES_OFFSET_COLLECT_TX_SIZE = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("collect_tx_size"));
        ENV_PROPERTIES_OFFSET_STORAGE_CACHE_SIZE_IN_PAGES = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("storage_cache_size_in_pages"));
        ENV_PROPERTIES_OFFSET_PREDICATE_HEADER_SIZE = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("predicate_header_size"));
        ENV_PROPERTIES_OFFSET_SKIP_INDEX_PERCENT = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("skip_index_percent"));

        ENV_ENABLE_CONFIG_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("compression_exceptional_list"),
                ValueLayout.JAVA_BYTE.withName("single_chunk"),
                ValueLayout.JAVA_BYTE.withName("packed_chunk"),
                ValueLayout.JAVA_BYTE.withName("compression"),
                ValueLayout.JAVA_BYTE.withName("validate_warm_id")).withName("env_enable_config_t");

        ENV_ENABLE_CONFIG_OFFSET_COMPRESSION_EXCEPTION_LIST = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("compression_exceptional_list"));
        ENV_ENABLE_CONFIG_OFFSET_SINGLE_CHUNK = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("single_chunk"));
        ENV_ENABLE_CONFIG_OFFSET_PACKED_CHUNK = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("packed_chunk"));
        ENV_ENABLE_CONFIG_OFFSET_COMPRESSION = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("compression"));
        ENV_ENABLE_CONFIG_OFFSET_VALIDATE_WARM_ID = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("validate_warm_id"));
    }

    public NativeStorageEngine(
            NativeConfig nativeConfig,
            MetricsManager metricsManager,
            ExceptionThrower exceptionThrower,
            GlobalConfig globalConfig,
            ConnectorSync connectorSync)
    {
        this.exceptionThrower = requireNonNull(exceptionThrower);

        final int taskMaxWorkerThreads = nativeConfig.getTaskMaxWorkerThreads();
        final int panicHaltPolicy = nativeConfig.getDebugPanicHaltPolicy();
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());

        logger.info("load storage engine taskMaxWorkerThreads %d panicHaltPolicy %d", taskMaxWorkerThreads, panicHaltPolicy);
        try (Arena arena = Arena.ofConfined()) {
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
            mInitEnv = linker.downcallHandle(libraryHandle.find("env_init").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            mInitGetWarmupRecordBufferSize = linker.downcallHandle(libraryHandle.find("we_get_fixed_warmup_record_buffer_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetFixedCollectRecordBufferSize = linker.downcallHandle(libraryHandle.find("we_get_fixed_collect_record_buffer_size").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mInitGetVarlenCollectRecordBufferSize = linker.downcallHandle(libraryHandle.find("we_get_varlen_collect_record_buffer_size").orElseThrow(),
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

            // warmup API
            mWarmupElementOpen = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_element_open").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            mWarmupElementClose = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_element_close").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
            mWarmupVerifyQueryOffset = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_verify_query_offset").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
            mWarmupChunk = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_chunk").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            mWarmupChunkExtRec = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_chunk_ext_rec").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

            // match API
            mMatchOpen = linker.downcallHandle(libraryHandle.find("warp_speed_match_open").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
            mMatchAgg = linker.downcallHandle(libraryHandle.find("warp_speed_match_agg").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT));
            mMatchLucenePrepare = linker.downcallHandle(libraryHandle.find("warp_speed_match_lucene_prepare").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_SHORT));
            mMatchLuceneCompleted = linker.downcallHandle(libraryHandle.find("warp_speed_match_lucene_completed").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT));
            mMatch = linker.downcallHandle(libraryHandle.find("warp_speed_match").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_SHORT));
            mMatchClose = linker.downcallHandle(libraryHandle.find("warp_speed_match_close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

            // collect API
            mCollectOpen = linker.downcallHandle(libraryHandle.find("warp_speed_collect_open").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
            mCollectProcessMatchResult = linker.downcallHandle(libraryHandle.find("warp_speed_collect_process_match_result").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
            mCollectProcessFullScanChunk = linker.downcallHandle(libraryHandle.find("warp_speed_collect_process_full_scan_chunk").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT));
            mCollectCollectChunk = linker.downcallHandle(libraryHandle.find("warp_speed_collect_collect_chunk").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
            mCollectClose = linker.downcallHandle(libraryHandle.find("warp_speed_collect_close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

            MemorySegment envProperties = arena.allocate(ENV_PROPERTIES_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize());
            envProperties.set(ValueLayout.ADDRESS, ENV_PROPERTIES_OFFSET_LIBRARY_PATH, arena.allocateFrom(WarpNativeStorageEngineModule.getNativeLibrariesDirectory().toString()));
            envProperties.set(ValueLayout.JAVA_LONG, ENV_PROPERTIES_OFFSET_JVM_MEMORY, Runtime.getRuntime().maxMemory());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_MAX_WORKER_THREADS, taskMaxWorkerThreads);
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_MAX_REC_BUF_SIZE, nativeConfig.getMaxRecJufferSize());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_PANIC_HALT_POLICY, panicHaltPolicy);
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_LZ4_HC_PERCENT, nativeConfig.getCompressionLevel());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_COLLECT_TX_SIZE, nativeConfig.getCollectTxSize());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_STORAGE_CACHE_SIZE_IN_PAGES, nativeConfig.getStorageCacheSizeInPages());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_PREDICATE_HEADER_SIZE, PredicateUtil.PREDICATE_HEADER_SIZE);
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_SKIP_INDEX_PERCENT, nativeConfig.getSkipIndexPercent());

            MemorySegment envEnableConfig = arena.allocate(ENV_ENABLE_CONFIG_LAYOUT.byteSize(), ValueLayout.JAVA_BYTE.byteSize());
            envEnableConfig.set(ValueLayout.JAVA_INT, ENV_ENABLE_CONFIG_OFFSET_COMPRESSION_EXCEPTION_LIST, nativeConfig.getExceptionalListCompression());
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_SINGLE_CHUNK, nativeConfig.getEnableSingleChunk() ? (byte) 1 : (byte) 0);
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_PACKED_CHUNK, nativeConfig.getEnablePackedChunk() ? (byte) 1 : (byte) 0);
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_COMPRESSION, nativeConfig.getEnableCompression() ? (byte) 1 : (byte) 0);
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_VALIDATE_WARM_ID, globalConfig.getDebugWarming() ? (byte) 1 : (byte) 0);

            mInitEnv.invokeExact(envProperties, envEnableConfig);
            loaded = true;
        }
        catch (Throwable t) {
            logger.error(t, "failed loading native storage engine");
            throw new RuntimeException("failed loading native storage engine");
        }
        new WarpStatsMgr(metricsManager);
        logger.debug("finish initializing storage engine");

        ((NativeConnectorSync) connectorSync).init();
    }

    @Override
    public int getWarmupRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetWarmupRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to init warmup record buffer size");
            throw new RuntimeException("failed to init warmup record buffer size");
        }
    }

    @Override
    public int getFixedCollectRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedCollectRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to init collect record buffer size");
            throw new RuntimeException("failed to init collect record buffer size");
        }
    }

    @Override
    public int getVarlenCollectRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetVarlenCollectRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to init collect record buffer size");
            throw new RuntimeException("failed to init collect record buffer size");
        }
    }

    @Override
    public int getFixedCollectTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedCollectTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to init collect tx size");
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
            shapingLogger.error(t, "failed to init collect tx size");
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
            shapingLogger.error(t, "failed to init warmup tx size");
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
            shapingLogger.error(t, "failed to init warmup tx size");
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
            shapingLogger.error(t, "failed to init warmup tx size");
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
            shapingLogger.error(t, "failed to init warmup tx size");
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
            if (fileDescriptor == -2) {
                shapingLogger.error("open file file not found %s", fileName);
            }
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to open file");
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
            shapingLogger.error(t, "failed to close file");
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
            shapingLogger.error(t, "failed to truncate file");
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
            shapingLogger.error(t, "failed to punch hole file");
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
            shapingLogger.error(t, "failed to clear native cache");
            throw new RuntimeException("failed to clear native cache");
        }
    }

    @Override
    public boolean isLoaded()
    {
        return loaded;
    }

    @Override
    public void warmupElementOpen(MemorySegment warmUpState, MemorySegment context)
    {
        try {
            mWarmupElementOpen.invokeExact(warmUpState, context);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to warmupElementOpen");
            throw new RuntimeException("failed to open warm up element");
        }
    }

    @Override
    public int warmupElementClose(MemorySegment warmUpState)
    {
        try {
            return (int) mWarmupElementClose.invokeExact(warmUpState);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to warmupElementClose");
            throw new RuntimeException("failed to close warm up element");
        }
    }

    @Override
    public void warmupVerifyQueryOffset(MemorySegment warmUpState)
    {
        try {
            mWarmupVerifyQueryOffset.invokeExact(warmUpState);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to warmupVerifyQueryOffset");
            throw new RuntimeException("failed to warmup verify query offset");
        }
    }

    @Override
    public void warmupChunk(MemorySegment warmUpState, MemorySegment recordBufferParams, MemorySegment compressionState)
    {
        try {
            mWarmupChunk.invokeExact(warmUpState, recordBufferParams, compressionState);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to warmupChunk");
            throw new RuntimeException("failed to warmup chunk");
        }
    }

    @Override
    public void warmupChunkExtRec(MemorySegment warmUpState, MemorySegment recordBufferParams)
    {
        try {
            mWarmupChunkExtRec.invokeExact(warmUpState, recordBufferParams);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to warmupChunkExtRec");
            throw new RuntimeException("failed to warmup extended records");
        }
    }

    @Override
    public void matchOpen(MemorySegment matchState)
    {
        try {
            mMatchOpen.invokeExact(matchState);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to matchOpen");
            throw new RuntimeException("failed to match open");
        }
    }

    @Override
    public int matchAgg(MemorySegment matchState, int startChunkIndex)
    {
        try {
            return (int) mMatchAgg.invokeExact(matchState, (short) startChunkIndex);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to matchAgg");
            throw new RuntimeException("failed to match aggregates");
        }
    }

    @Override
    public boolean matchLucenePrepare(MemorySegment matchState, int weIx, int chunkIndex)
    {
        try {
            return (boolean) mMatchLucenePrepare.invokeExact(matchState, (short) weIx, (short) chunkIndex);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to matchLucenePrepare");
            throw new RuntimeException("failed to match lucene prepare");
        }
    }

    @Override
    public void matchLuceneCompleted(MemorySegment matchState, int weIx, int chunkIndex, int numMatchedRecords)
    {
        try {
            mMatchLuceneCompleted.invokeExact(matchState, (short) weIx, (short) chunkIndex, numMatchedRecords);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to matchLuceneCompleted");
            throw new RuntimeException("failed to match lucene completed");
        }
    }

    @Override
    public boolean match(MemorySegment matchState, int startChunkIndex, int numChunks)
    {
        try {
            return (boolean) mMatch.invokeExact(matchState, (short) startChunkIndex, (short) numChunks);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to match");
            throw new RuntimeException("failed to match");
        }
    }

    @Override
    public void matchClose(MemorySegment matchState)
    {
        try {
            mMatchClose.invokeExact(matchState);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to matchOpen");
            throw new RuntimeException("failed to match open");
        }
    }

    @Override
    public void collectOpen(MemorySegment collectState)
    {
        try {
            mCollectOpen.invokeExact(collectState);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to collectOpen");
            throw new RuntimeException("failed to collect open");
        }
    }

    @Override
    public boolean processMatchResult(MemorySegment collectState, int chunkIndex, int bmResetPoint, int rowsLimit, MemorySegment outQueryResultTypes)
    {
        try {
            return (boolean) mCollectProcessMatchResult.invokeExact(collectState, (short) chunkIndex, bmResetPoint, rowsLimit, outQueryResultTypes);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to processMatchResult");
            throw new RuntimeException("failed to process match result");
        }
    }

    @Override
    public boolean processFullScanChunk(MemorySegment collectState, int chunkIndex, int startRowIx, int rowsLimit)
    {
        try {
            return (boolean) mCollectProcessFullScanChunk.invokeExact(collectState, (short) chunkIndex, (short) startRowIx, rowsLimit);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to processFullScanChunk");
            throw new RuntimeException("failed to process full scan chunk");
        }
    }

    @Override
    public void collectChunk(MemorySegment collectState, int chunkIndex, int numToCollect, MemorySegment outQueryResultTypes)
    {
        try {
            mCollectCollectChunk.invokeExact(collectState, (short) chunkIndex, numToCollect, outQueryResultTypes);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to collectChunk");
            throw new RuntimeException("failed to collectChunk chunkIndex " + chunkIndex);
        }
    }

    @Override
    public void collectClose(MemorySegment collectState, MemorySegment readStats)
    {
        try {
            mCollectClose.invokeExact(collectState, readStats);
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to collectClose");
            throw new RuntimeException("failed to collect close");
        }
    }
}
