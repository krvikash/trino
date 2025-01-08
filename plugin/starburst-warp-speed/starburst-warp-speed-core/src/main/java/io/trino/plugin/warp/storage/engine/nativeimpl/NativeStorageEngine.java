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
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.spi.catalog.CatalogName;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
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
    private static final int MAX_LOG_STRING_LENGTH = 1500;

    private static final StructLayout ENV_PROPERTIES_LAYOUT;
    private static final long ENV_PROPERTIES_OFFSET_LIBRARY_PATH;
    private static final long ENV_PROPERTIES_OFFSET_LOG;
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

    private static final SequenceLayout LOGGER_LOG_STRING_LAYOUT;
    private static final StructLayout LOGGER_LOG_LAYOUT;
    private static final long LOGGER_LOG_OFFSET_STATE;
    private static final long LOGGER_LOG_OFFSET_MAX_LENGTH;
    private static final long LOGGER_LOG_OFFSET_STRING;

    private final ShapingLogger shapingLogger;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ExceptionThrower exceptionThrower; // we keep a reference to hold this object for native layer ref
    private final CatalogName catalogName;
    private MemorySegment logMem;
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
    private final MethodHandle mCollectOpenChunk;
    private final MethodHandle mCollectCollectChunk;
    private final MethodHandle mCollectClose;

    static {
        ENV_PROPERTIES_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.ADDRESS.withName("plibrary_path"),
                ValueLayout.ADDRESS.withName("plog"),
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
        ENV_PROPERTIES_OFFSET_LOG = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("plog"));
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

        LOGGER_LOG_STRING_LAYOUT = MemoryLayout.sequenceLayout(MAX_LOG_STRING_LENGTH, ValueLayout.JAVA_BYTE);
        LOGGER_LOG_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("state"),
                ValueLayout.JAVA_INT.withName("length"),
                ValueLayout.JAVA_INT.withName("max_length"),
                LOGGER_LOG_STRING_LAYOUT.withName("string")).withName("logger_log_t");
        LOGGER_LOG_OFFSET_STATE = LOGGER_LOG_LAYOUT.byteOffset(PathElement.groupElement("state"));
        //LOGGER_LOG_OFFSET_LENGTH = LOGGER_LOG_LAYOUT.byteOffset(PathElement.groupElement("length"));
        LOGGER_LOG_OFFSET_MAX_LENGTH = LOGGER_LOG_LAYOUT.byteOffset(PathElement.groupElement("max_length"));
        LOGGER_LOG_OFFSET_STRING = LOGGER_LOG_LAYOUT.byteOffset(PathElement.groupElement("string"));
    }

    public NativeStorageEngine(
            NativeConfig nativeConfig,
            ExceptionThrower exceptionThrower,
            GlobalConfig globalConfig,
            ConnectorSync connectorSync,
            CatalogName catalogName)
    {
        this.exceptionThrower = requireNonNull(exceptionThrower);
        this.catalogName = requireNonNull(catalogName);

        final int taskMaxWorkerThreads = nativeConfig.getTaskMaxWorkerThreads();
        final int panicHaltPolicy = nativeConfig.getDebugPanicHaltPolicy();
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());

        logger.info("load storage engine taskMaxWorkerThreads %d panicHaltPolicy %d logSize %d",
                taskMaxWorkerThreads, panicHaltPolicy, LOGGER_LOG_LAYOUT.byteSize());
        logMem = Arena.global().allocate(LOGGER_LOG_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize()); // its zeroed by default
        logMem.set(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_MAX_LENGTH, MAX_LOG_STRING_LENGTH);
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
                    FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
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
            mCollectOpenChunk = linker.downcallHandle(libraryHandle.find("warp_speed_collect_open_chunk").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.ADDRESS));
            mCollectCollectChunk = linker.downcallHandle(libraryHandle.find("warp_speed_collect_collect_chunk").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
            mCollectClose = linker.downcallHandle(libraryHandle.find("warp_speed_collect_close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

            MemorySegment envProperties = arena.allocate(ENV_PROPERTIES_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize());
            envProperties.set(ValueLayout.ADDRESS, ENV_PROPERTIES_OFFSET_LIBRARY_PATH, arena.allocateFrom(WarpNativeStorageEngineModule.getNativeLibrariesDirectory().toString()));
            envProperties.set(ValueLayout.ADDRESS, ENV_PROPERTIES_OFFSET_LOG, logMem);
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

            long logMemAddress = (long) mInitEnv.invokeExact(envProperties, envEnableConfig);
            if (logMemAddress != 0) {
                // we throw away our allocated log buffer and take the one storage engine gave us
                logMem = MemorySegment.ofAddress(logMemAddress).reinterpret(LOGGER_LOG_LAYOUT.byteSize());
            }
            checkForLogs();
            loaded = true;
        }
        catch (Throwable t) {
            logger.error(t, "failed loading native storage engine");
            throw new RuntimeException("failed loading native storage engine");
        }
        logger.debug("finish initializing storage engine");

        ((NativeConnectorSync) connectorSync).init();
    }

    @Override
    public boolean isLoaded()
    {
        return loaded;
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

    private void checkForLogs()
    {
        int logLevel = logMem.get(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_STATE);
        if (logLevel == 0) {
            return;
        }
        logMem.set(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_STATE, 0);

        String logString = logMem.getString(LOGGER_LOG_OFFSET_STRING);
        if (logLevel < 0) {
            final int expectionId = -1 * logLevel;
            shapingLogger.error("catalog %s throwed native excetpion id %d", catalogName, expectionId);
            exceptionThrower.throwException(expectionId, logString);
        }
        else if ((logString.length() > 0) && (logString.length() <= MAX_LOG_STRING_LENGTH)) {
            switch (logLevel) {
                case 1:
                    shapingLogger.error("catalog %s: %s", catalogName, logString);
                    break;
                case 2:
                    shapingLogger.info("catalog %s: %s", catalogName, logString);
                    break;
                case 3:
                    logger.debug("catalog %s: %s", catalogName, logString);
                    break;
                default: break;
            }
        }
    }

    @Override
    public int fileOpen(String fileName)
    {
        int fileDescriptor;
        try (Arena arena = Arena.ofConfined()) {
            fileDescriptor = (int) mFileOpen.invokeExact(arena.allocateFrom(fileName));
            checkForLogs();
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
            checkForLogs();
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
            checkForLogs();
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
            checkForLogs();
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
            checkForLogs();
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to clear native cache");
            throw new RuntimeException("failed to clear native cache");
        }
    }

    @Override
    public void warmupElementOpen(MemorySegment warmUpState, MemorySegment context)
    {
        try {
            mWarmupElementOpen.invokeExact(warmUpState, context);
            checkForLogs();
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
            int res = (int) mWarmupElementClose.invokeExact(warmUpState);
            checkForLogs();
            return res;
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
            checkForLogs();
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
            checkForLogs();
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
            checkForLogs();
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
            checkForLogs();
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
            int res = (int) mMatchAgg.invokeExact(matchState, (short) startChunkIndex);
            checkForLogs();
            return res;
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
            boolean res = (boolean) mMatchLucenePrepare.invokeExact(matchState, (short) weIx, (short) chunkIndex);
            checkForLogs();
            return res;
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
            checkForLogs();
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
            boolean res = (boolean) mMatch.invokeExact(matchState, (short) startChunkIndex, (short) numChunks);
            checkForLogs();
            return res;
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
            checkForLogs();
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
            checkForLogs();
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to collectOpen");
            throw new RuntimeException("failed to collect open");
        }
    }

    @Override
    public boolean openChunk(MemorySegment collectState, int chunkIndex, MemorySegment outQueryResultTypes)
    {
        try {
            boolean res = (boolean) mCollectOpenChunk.invokeExact(collectState, (short) chunkIndex, outQueryResultTypes);
            checkForLogs();
            return res;
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to openChunk");
            throw new RuntimeException("failed to open chunk");
        }
    }

    @Override
    public void collectChunk(MemorySegment collectState, boolean isFullScan, int startRecIx, int numToCollect, MemorySegment outQueryResultTypes)
    {
        try {
            mCollectCollectChunk.invokeExact(collectState, isFullScan, (short) startRecIx, numToCollect, outQueryResultTypes);
            checkForLogs();
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to collectChunk");
            throw new RuntimeException("failed to collectChunk");
        }
    }

    @Override
    public void collectClose(MemorySegment collectState, MemorySegment readStats)
    {
        try {
            mCollectClose.invokeExact(collectState, readStats);
            checkForLogs();
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to collectClose");
            throw new RuntimeException("failed to collect close");
        }
    }
}
