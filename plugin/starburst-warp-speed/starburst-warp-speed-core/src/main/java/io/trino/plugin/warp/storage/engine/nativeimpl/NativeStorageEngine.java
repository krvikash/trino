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
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.di.WarpNativeStorageEngineModule;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.spi.TrinoException;

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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageEngine
        implements StorageEngine
{
    private static final Logger logger = Logger.get(NativeStorageEngine.class);

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
    private static final long ENV_PROPERTIES_OFFSET_LIMIT_NUM_IOS_IN_PARALLEL;
    private static final long ENV_PROPERTIES_OFFSET_IO_MD_MAX_SIZE;

    private static final StructLayout ENV_ENABLE_CONFIG_LAYOUT;
    private static final long ENV_ENABLE_CONFIG_OFFSET_COMPRESSION_EXCEPTION_LIST;
    private static final long ENV_ENABLE_CONFIG_OFFSET_SINGLE_CHUNK;
    private static final long ENV_ENABLE_CONFIG_OFFSET_PACKED_CHUNK;
    private static final long ENV_ENABLE_CONFIG_OFFSET_COMPRESSION;
    private static final long ENV_ENABLE_CONFIG_OFFSET_WARM_DATA_DISABLED;

    private final ShapingLogger shapingLogger;
    private final NativeLogger nativeLogger;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final Optional<ExceptionThrower> exceptionThrower; // we keep a reference to hold this object for native layer ref
    private final boolean isFirstLoaded;
    private boolean isLoaded;

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
                ValueLayout.JAVA_INT.withName("skip_index_percent"),
                ValueLayout.JAVA_INT.withName("limit_num_ios_in_parallel"),
                ValueLayout.JAVA_INT.withName("io_md_max_size")).withName("env_properties_t");

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
        ENV_PROPERTIES_OFFSET_LIMIT_NUM_IOS_IN_PARALLEL = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("limit_num_ios_in_parallel"));
        ENV_PROPERTIES_OFFSET_IO_MD_MAX_SIZE = ENV_PROPERTIES_LAYOUT.byteOffset(PathElement.groupElement("io_md_max_size"));

        ENV_ENABLE_CONFIG_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("compression_exceptional_list"),
                ValueLayout.JAVA_BYTE.withName("single_chunk"),
                ValueLayout.JAVA_BYTE.withName("packed_chunk"),
                ValueLayout.JAVA_BYTE.withName("compression"),
                ValueLayout.JAVA_BYTE.withName("warm_data_disabled")).withName("env_enable_config_t");

        ENV_ENABLE_CONFIG_OFFSET_COMPRESSION_EXCEPTION_LIST = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("compression_exceptional_list"));
        ENV_ENABLE_CONFIG_OFFSET_SINGLE_CHUNK = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("single_chunk"));
        ENV_ENABLE_CONFIG_OFFSET_PACKED_CHUNK = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("packed_chunk"));
        ENV_ENABLE_CONFIG_OFFSET_COMPRESSION = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("compression"));
        ENV_ENABLE_CONFIG_OFFSET_WARM_DATA_DISABLED = ENV_ENABLE_CONFIG_LAYOUT.byteOffset(PathElement.groupElement("warm_data_disabled"));
    }

    public NativeStorageEngine(
            SharedConfig sharedConfig,
            NativeConfig nativeConfig,
            ExceptionThrower exceptionThrower,
            NativeLogger nativeLogger,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        final int taskMaxWorkerThreads = nativeConfig.getTaskMaxWorkerThreads();
        final int panicHaltPolicy = nativeConfig.getDebugPanicHaltPolicy();
        this.nativeLogger = requireNonNull(nativeLogger);
        this.exceptionThrower = (panicHaltPolicy == 0) ? Optional.of(exceptionThrower) : Optional.empty();
        shapingLogger = shapingLoggerFactory.getInstance(this.getClass());

        logger.info("load storage engine taskMaxWorkerThreads %d panicHaltPolicy %d",
                taskMaxWorkerThreads, panicHaltPolicy);
        try (Arena arena = Arena.ofConfined()) {
            SymbolLookup libraryHandle = SymbolLookup.loaderLookup();
            Linker linker = Linker.nativeLinker();

            // file API
            mFileOpen = linker.downcallHandle(libraryHandle.find("storage_file_open").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mFileClose = linker.downcallHandle(libraryHandle.find("storage_file_close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mFileTruncate = linker.downcallHandle(libraryHandle.find("storage_file_truncate").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mFilePunchHole = linker.downcallHandle(libraryHandle.find("storage_file_punch_hole").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mFileAboutToBeDeleted = linker.downcallHandle(libraryHandle.find("warp_speed_file_is_about_to_be_deleted").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));

            // init API
            mInitEnv = linker.downcallHandle(libraryHandle.find("env_init").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
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
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mWarmupElementClose = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_element_close").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mWarmupVerifyQueryOffset = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_verify_query_offset").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mWarmupChunk = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_chunk").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mWarmupChunkExtRec = linker.downcallHandle(libraryHandle.find("warp_speed_warmup_chunk_ext_rec").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

            // match API
            mMatchOpen = linker.downcallHandle(libraryHandle.find("warp_speed_match_open").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mMatchAgg = linker.downcallHandle(libraryHandle.find("warp_speed_match_agg").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT));
            mMatchLucenePrepare = linker.downcallHandle(libraryHandle.find("warp_speed_match_lucene_prepare").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT));
            mMatchLuceneCompleted = linker.downcallHandle(libraryHandle.find("warp_speed_match_lucene_completed").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            mMatch = linker.downcallHandle(libraryHandle.find("warp_speed_match").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT));
            mMatchClose = linker.downcallHandle(libraryHandle.find("warp_speed_match_close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

            // collect API
            mCollectOpen = linker.downcallHandle(libraryHandle.find("warp_speed_collect_open").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mCollectOpenChunk = linker.downcallHandle(libraryHandle.find("warp_speed_collect_open_chunk").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_INT));
            mCollectCollectChunk = linker.downcallHandle(libraryHandle.find("warp_speed_collect_collect_chunk").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            mCollectClose = linker.downcallHandle(libraryHandle.find("warp_speed_collect_close").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

            MemorySegment envProperties = arena.allocate(ENV_PROPERTIES_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize());
            envProperties.set(ValueLayout.ADDRESS, ENV_PROPERTIES_OFFSET_LIBRARY_PATH, arena.allocateFrom(WarpNativeStorageEngineModule.getNativeLibrariesDirectory().toString()));
            envProperties.set(ValueLayout.ADDRESS, ENV_PROPERTIES_OFFSET_LOG, nativeLogger.getLogMem());
            envProperties.set(ValueLayout.JAVA_LONG, ENV_PROPERTIES_OFFSET_JVM_MEMORY, Runtime.getRuntime().maxMemory());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_MAX_WORKER_THREADS, taskMaxWorkerThreads);
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_MAX_REC_BUF_SIZE, nativeConfig.getMaxRecJufferSize());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_PANIC_HALT_POLICY, panicHaltPolicy);
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_LZ4_HC_PERCENT, nativeConfig.getCompressionLevel());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_COLLECT_TX_SIZE, nativeConfig.getCollectTxSize());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_STORAGE_CACHE_SIZE_IN_PAGES, nativeConfig.getStorageCacheSizeInPages());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_PREDICATE_HEADER_SIZE, PredicateUtil.PREDICATE_HEADER_SIZE);
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_SKIP_INDEX_PERCENT, nativeConfig.getSkipIndexPercent());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_LIMIT_NUM_IOS_IN_PARALLEL, nativeConfig.getLimitNumIosInParallel());
            envProperties.set(ValueLayout.JAVA_INT, ENV_PROPERTIES_OFFSET_IO_MD_MAX_SIZE, nativeConfig.getMaxIOMetadataSize());

            MemorySegment envEnableConfig = arena.allocate(ENV_ENABLE_CONFIG_LAYOUT.byteSize(), ValueLayout.JAVA_BYTE.byteSize());
            envEnableConfig.set(ValueLayout.JAVA_INT, ENV_ENABLE_CONFIG_OFFSET_COMPRESSION_EXCEPTION_LIST, nativeConfig.getExceptionalListCompression());
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_SINGLE_CHUNK, nativeConfig.getEnableSingleChunk() ? (byte) 1 : (byte) 0);
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_PACKED_CHUNK, nativeConfig.getEnablePackedChunk() ? (byte) 1 : (byte) 0);
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_COMPRESSION, nativeConfig.getEnableCompression() ? (byte) 1 : (byte) 0);
            envEnableConfig.set(ValueLayout.JAVA_BYTE, ENV_ENABLE_CONFIG_OFFSET_WARM_DATA_DISABLED, sharedConfig.getEnableFSCacheMode() ? (byte) 1 : (byte) 0);

            int initRes = -1;
            try (NativeLogger.LogId logId = nativeLogger.getLogId(this.exceptionThrower)) {
                // 0 - first loaded
                // >0 - already loaded
                // <0 - error
                initRes = (int) mInitEnv.invokeExact(envProperties, envEnableConfig, logId.id());
                this.isFirstLoaded = initRes == 0;
            }
            this.isLoaded = (initRes >= 0);
        }
        catch (Throwable t) {
            logger.error(t, "failed loading native storage engine");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed loading native storage engine", t);
        }
    }

    @Override
    public boolean isLoaded()
    {
        return isLoaded;
    }

    @Override
    public boolean isFirstLoaded()
    {
        return isFirstLoaded;
    }

    @Override
    public void shutdown()
    {
        isLoaded = false;
    }

    @Override
    public int getWarmupRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetWarmupRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init warmup record buffer size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init warmup record buffer size", t);
        }
    }

    @Override
    public int getFixedCollectRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedCollectRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init collect record buffer size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init collect record buffer size", t);
        }
    }

    @Override
    public int getVarlenCollectRecordBufferSize(int recTypeLength)
    {
        try {
            return (int) mInitGetVarlenCollectRecordBufferSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init collect record buffer size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init collect record buffer size", t);
        }
    }

    @Override
    public int getFixedCollectTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedCollectTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init collect tx size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init collect tx size", t);
        }
    }

    @Override
    public int getVarlenCollectTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetVarlenCollectTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init collect tx size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init collect tx size", t);
        }
    }

    @Override
    public int getFixedWarmupDataTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetFixedWarmupDataTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init warmup tx size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init warmup tx size", t);
        }
    }

    @Override
    public int getVarlenWarmupDataTxSize(int recTypeLength)
    {
        try {
            return (int) mInitGetVarlenWarmupDataTxSize.invokeExact(recTypeLength);
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init warmup tx size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init warmup tx size", t);
        }
    }

    @Override
    public int getWarmupBasicTxSize()
    {
        try {
            return (int) mInitGetWarmupBasicTxSize.invokeExact();
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init warmup tx size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init warmup tx size", t);
        }
    }

    @Override
    public int getWarmupLuceneTxSize()
    {
        try {
            return (int) mInitGetWarmupLuceneTxSize.invokeExact();
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to init warmup tx size");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to init warmup tx size", t);
        }
    }

    @Override
    public int fileOpen(String fileName)
    {
        int fileDescriptor;
        try (Arena arena = Arena.ofConfined();
             NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            fileDescriptor = (int) mFileOpen.invokeExact(arena.allocateFrom(fileName), logId.id());
            if (fileDescriptor >= 0) {
                return fileDescriptor;
            }
            if (fileDescriptor == -2) {
                shapingLogger.error("open file file not found %s", fileName);
            }
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to open file");
            if (t instanceof TrinoException te) {
                throw te;
            }
        }
        throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to open file " + fileName);
    }

    @Override
    public void fileClose(int fileDescriptor)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mFileClose.invokeExact(fileDescriptor, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to close file");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to close file", t);
        }
    }

    @Override
    public void fileTruncate(int fileDescriptor, int offset)
    {
        boolean success;
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            success = (boolean) mFileTruncate.invokeExact(fileDescriptor, offset, logId.id());
            if (success) {
                return;
            }
        }
        catch (Throwable t) {
            shapingLogger.error(t, "failed to truncate file");
            if (t instanceof TrinoException te) {
                throw te;
            }
        }
        throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to truncate file");
    }

    @Override
    public void filePunchHole(String fileName, int startOffset, int endOffset)
    {
        try (Arena arena = Arena.ofConfined();
             NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mFilePunchHole.invokeExact(arena.allocateFrom(fileName), startOffset, endOffset, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to punch hole file");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to punch hole file" + fileName, t);
        }
    }

    @Override
    public void fileIsAboutToBeDeleted(long fileHash, long fileModTime, int fileSizeInPages)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mFileAboutToBeDeleted.invokeExact(fileHash, fileModTime, fileSizeInPages, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to clear native cache");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to clear native cache", t);
        }
    }

    @Override
    public void warmupElementOpen(MemorySegment warmUpState, MemorySegment context)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mWarmupElementOpen.invokeExact(warmUpState, context, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to warmupElementOpen");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to open warm up element", t);
        }
    }

    @Override
    public int warmupElementClose(MemorySegment warmUpState)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            return (int) mWarmupElementClose.invokeExact(warmUpState, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to warmupElementClose");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to close warm up element", t);
        }
    }

    @Override
    public void warmupVerifyQueryOffset(MemorySegment warmUpState)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mWarmupVerifyQueryOffset.invokeExact(warmUpState, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to warmupVerifyQueryOffset");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to warmup verify query offset", t);
        }
    }

    @Override
    public void warmupChunk(MemorySegment warmUpState, MemorySegment recordBufferParams, MemorySegment compressionState)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mWarmupChunk.invokeExact(warmUpState, recordBufferParams, compressionState, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to warmupChunk");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to warmup chunk", t);
        }
    }

    @Override
    public void warmupChunkExtRec(MemorySegment warmUpState, MemorySegment recordBufferParams)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mWarmupChunkExtRec.invokeExact(warmUpState, recordBufferParams, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to warmupChunkExtRec");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to warmup extended records", t);
        }
    }

    @Override
    public void matchOpen(MemorySegment matchState)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mMatchOpen.invokeExact(matchState, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to matchOpen");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to match open", t);
        }
    }

    @Override
    public int matchAgg(MemorySegment matchState, int startChunkIndex)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            return (int) mMatchAgg.invokeExact(matchState, (short) startChunkIndex, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to matchAgg");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to match aggregates", t);
        }
    }

    @Override
    public boolean matchLucenePrepare(MemorySegment matchState, int weIx, int chunkIndex)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            return (boolean) mMatchLucenePrepare.invokeExact(matchState, (short) weIx, (short) chunkIndex, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to matchLucenePrepare");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to match lucene prepare", t);
        }
    }

    @Override
    public void matchLuceneCompleted(MemorySegment matchState, int weIx, int chunkIndex, int numMatchedRecords)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mMatchLuceneCompleted.invokeExact(matchState, (short) weIx, (short) chunkIndex, numMatchedRecords, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to matchLuceneCompleted");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to match lucene completed", t);
        }
    }

    @Override
    public boolean match(MemorySegment matchState, int startChunkIndex, int numChunks)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            return (boolean) mMatch.invokeExact(matchState, (short) startChunkIndex, (short) numChunks, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to match");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to match", t);
        }
    }

    @Override
    public void matchClose(MemorySegment matchState)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mMatchClose.invokeExact(matchState, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to matchClose");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to match close", t);
        }
    }

    @Override
    public void collectOpen(MemorySegment collectState)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mCollectOpen.invokeExact(collectState, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to collectOpen");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to collect open", t);
        }
    }

    @Override
    public boolean openChunk(MemorySegment collectState, int chunkIndex)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            return (boolean) mCollectOpenChunk.invokeExact(collectState, (short) chunkIndex, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to openChunk");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to open chunk", t);
        }
    }

    @Override
    public void collectChunk(MemorySegment collectState, MemorySegment outQueryResultTypes)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mCollectCollectChunk.invokeExact(collectState, outQueryResultTypes, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to collectChunk");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to collectChunk", t);
        }
    }

    @Override
    public void collectClose(MemorySegment collectState, MemorySegment readStats)
    {
        try (NativeLogger.LogId logId = nativeLogger.getLogId(exceptionThrower)) {
            mCollectClose.invokeExact(collectState, readStats, logId.id());
        }
        catch (Throwable t) {
            if (t instanceof TrinoException te) {
                throw te;
            }
            shapingLogger.error(t, "failed to collectClose");
            throw new TrinoException(WarpErrorCode.WARP_GENERIC, "failed to collect close", t);
        }
    }
}
