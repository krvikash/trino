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
package io.trino.plugin.warp.juffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.BufferAllocatorStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.memory.PinnedGcArena;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.read.QueryParams;
import io.trino.plugin.warp.storage.write.WarmUpState;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.util.WarpInitializedServiceMarker;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.TinyintType;
import jakarta.annotation.PreDestroy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.query.classifier.NativeCollectClassifier.COLLECT_BUFFER_MAX_MEMORY;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@Singleton
public class BufferAllocator
        implements WarpInitializedServiceMarker
{
    private static final Logger logger = Logger.get(BufferAllocator.class);

    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final WorkerMemoryManager workerMemoryManager;
    private final MetricsManager metricsManager;
    private final NativeConfig nativeConfig;
    private final CatalogName catalogName;

    private final int maxRecLenForWarmupRecordBuffer;
    private final int maxRecLenForDataFixed;
    private final int maxRecLenForDataVarlen;
    private final int queryStringNullValueSize;
    private int extRecBuffSize;
    private int dataTempBufferSize;
    private int indexTempBufferSize;
    private int warmBufferSize;
    private int warmWriteBufferSize;
    private int warmContextBufferSize;
    private final BufferAllocatorStats stats;
    private MemorySegment predicateBundleMem;
    private PredicateBufferPool[] predicateBufferPools;
    private PinnedGcArena arena;
    private int[] buffTypeSizes;
    private int[] warmupRecordBufferSizes;
    private int[] collectFixedRecordBufferSizes;
    private int[] collectVarlenRecordBufferSizes;
    private int[] fixedCollectTxSizes;
    private int[] varlenCollectTxSizes;
    private int warmupIndexTxSize;

    @Inject
    public BufferAllocator(
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            NativeConfig nativeConfig,
            WorkerMemoryManager workerMemoryManager,
            MetricsManager metricsManager,
            WarpInitializedServiceRegistry warpInitializedServiceRegistry,
            CatalogName catalogName)
    {
        // services
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.metricsManager = requireNonNull(metricsManager);
        this.nativeConfig = requireNonNull(nativeConfig);
        this.workerMemoryManager = requireNonNull(workerMemoryManager);
        this.catalogName = requireNonNull(catalogName);
        warpInitializedServiceRegistry.addService(this);

        // constants
        this.maxRecLenForWarmupRecordBuffer = storageEngineConstants.getFixedLengthStringLimit();
        this.maxRecLenForDataFixed = 2 * Long.BYTES; // long decimal
        this.maxRecLenForDataVarlen = storageEngineConstants.getMaxRecLen();
        this.queryStringNullValueSize = storageEngineConstants.getQueryStringNullValueSize();

        initBufferTypeSizes();
        this.stats = BufferAllocatorStats.create();
    }

    @PreDestroy
    public void shutdown()
    {
        predicateBundleMem = null;
        arena.close();
    }

    private long initPredicateBundle()
    {
        final long[] poolSizes = {0, 2000, 1000, 100, 10};
        final long[] bufferSizes = {0, 100, 1000, 100 * 1000, 1000 * 1000};

        // create a set of the buffer types without invalid
        Set<PredicateBufferPoolType> bufferTypes = Arrays.stream(PredicateBufferPoolType.values())
                .filter(type -> !PredicateBufferPoolType.INVALID.equals(type))
                .collect(Collectors.toSet());

        // calculate total size and allocate it
        final long alignment = storageEngineConstants.getPageSize();
        long totalPoolSize = alignment;
        for (PredicateBufferPoolType type : bufferTypes) {
            totalPoolSize += poolSizes[type.ordinal()] * bufferSizes[type.ordinal()];
        }

        arena = workerMemoryManager.getPinnedGcArena();
        predicateBundleMem = arena.allocate(totalPoolSize, alignment);
        SegmentAllocator poolSlicer = SegmentAllocator.slicingAllocator(predicateBundleMem);

        // do another loop to actually create the pools
        this.predicateBufferPools = new PredicateBufferPool[PredicateBufferPoolType.values().length];
        for (PredicateBufferPoolType type : bufferTypes) {
            predicateBufferPools[type.ordinal()] = new PredicateBufferPool(type, bufferSizes[type.ordinal()], poolSizes[type.ordinal()], poolSlicer);
        }

        return totalPoolSize;
    }

    private void initBufferTypeSizes()
    {
        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        buffTypeSizes = new int[JbufType.JBUF_TYPE_NUM_OF.ordinal()];
        buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()] = chunkSize;
        buffTypeSizes[JbufType.JBUF_TYPE_CHUNKS.ordinal()] = storageEngineConstants.getChunksBufferMaxSize();
        buffTypeSizes[JbufType.JBUF_TYPE_SKIPLIST.ordinal()] = roundToPageSize(((chunkSize / storageEngineConstants.getVarlenMdGranularity()) + 1) * Integer.BYTES);

        this.dataTempBufferSize = storageEngineConstants.getWarmupDataTempBufferSize();
        this.indexTempBufferSize = storageEngineConstants.getWarmupIndexTempBufferSize();
        this.extRecBuffSize = storageEngineConstants.getRecordBufferMaxSize();

        // NOTE: all the array sizes above are with a +1 size to allow accessing them with the record length as index to the array without the need to -1
        // since 0 is not a valid length and the maximal value is
        this.warmupRecordBufferSizes = new int[maxRecLenForWarmupRecordBuffer + 1];
        for (int len = 1; len <= maxRecLenForWarmupRecordBuffer; len++) {
            warmupRecordBufferSizes[len] = storageEngine.getWarmupRecordBufferSize(len);
        }
        this.collectFixedRecordBufferSizes = new int[maxRecLenForDataFixed + 1];
        for (int len = 1; len <= maxRecLenForDataFixed; len++) {
            collectFixedRecordBufferSizes[len] = storageEngine.getFixedCollectRecordBufferSize(len);
        }
        this.collectVarlenRecordBufferSizes = new int[maxRecLenForDataVarlen + 1];
        for (int len = 1; len <= maxRecLenForDataVarlen; len++) {
            collectVarlenRecordBufferSizes[len] = storageEngine.getVarlenCollectRecordBufferSize(len);
        }

        this.fixedCollectTxSizes = new int[maxRecLenForDataFixed + 1];
        for (int len = 1; len <= maxRecLenForDataFixed; len++) {
            fixedCollectTxSizes[len] = storageEngine.getFixedCollectTxSize(len);
        }
        this.varlenCollectTxSizes = new int[maxRecLenForDataVarlen + 1];
        for (int len = 1; len <= maxRecLenForDataVarlen; len++) {
            varlenCollectTxSizes[len] = storageEngine.getVarlenCollectTxSize(len);
        }

        warmupIndexTxSize = Math.max((int) storageEngine.getWarmupBasicTxSize(), (int) storageEngine.getWarmupLuceneTxSize());

        int warmBufferSize = buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()] +
                buffTypeSizes[JbufType.JBUF_TYPE_CHUNKS.ordinal()] +
                storageEngineConstants.getPageSize() * 4; /* some page for skiplist and spare */
        int dataWarmBufferSize = warmBufferSize + warmupRecordBufferSizes[maxRecLenForWarmupRecordBuffer] + extRecBuffSize + dataTempBufferSize;
        int basicWarmBufferSize = warmBufferSize + calculateCrcBufferSize(RecTypeCode.REC_TYPE_DECIMAL_LONG, maxRecLenForDataFixed) + indexTempBufferSize;
        this.warmBufferSize = Math.max(dataWarmBufferSize, basicWarmBufferSize);
    }

    @Override
    public void init()
    {
        try {
            // 3 for records, extended records and metadata (we take spare for metadata)
            int dataWriteBufferSize = storageEngineConstants.getRecordBufferMaxSize() * 3;
            int basicWriteBufferSize = storageEngineConstants.getIndexChunkMaxSize();
            this.warmWriteBufferSize = Math.max(dataWriteBufferSize, basicWriteBufferSize);

            // we take the maximal size limit and multiply by 1024, in practice since not all WEs are in maximal size we can warm at once more
            this.warmContextBufferSize = storageEngineConstants.getMaxWeContextSize() * 1024;

            long predicateCacheSizeInBytes = initPredicateBundle();
            logger.info(
                    "catalog %s warmBufferSize %d warmWriteBufferSize %d warmContextBufferSize %d predicateCacheSizeInBytes %dMB",
                    catalogName,
                    warmBufferSize,
                    warmWriteBufferSize,
                    warmContextBufferSize,
                    predicateCacheSizeInBytes >> 20);

            metricsManager.registerMetric(this.stats);
        }
        catch (Throwable t) {
            logger.error(t, "failed to initialize buffer allocator");
            throw new TrinoException(WarpErrorCode.WARP_CATALOG_FAILED_TO_LOAD, "catalog " + catalogName + " failed to load", t);
        }
    }

    @VisibleForTesting
    public void clear()
    {
        if (predicateBufferPools != null) {
            for (PredicateBufferPool predicateBufferPool : predicateBufferPools) {
                if (predicateBufferPool != null) {
                    predicateBufferPool.clear();
                }
            }
        }
    }

    public int getPoolSize(PredicateBufferPoolType predicateBufferPoolType)
    {
        return predicateBufferPools[predicateBufferPoolType.ordinal()].getPoolSize();
    }

    public int getBufferSize(PredicateBufferPoolType predicateBufferPoolType)
    {
        return predicateBufferPools[predicateBufferPoolType.ordinal()].getBufSize();
    }

    public Optional<SegmentAllocator> createWarmMemoryAllocator(ThreadArena arena, boolean allocateCommonWarmUpState)
    {
        try {
            final long alignment = storageEngineConstants.getPageSize();
            final long contextSize = allocateCommonWarmUpState ? (long) storageEngineConstants.getMaxWeContextSize() : (long) warmContextBufferSize;
            final long allocSize = (long) warmBufferSize + (long) warmWriteBufferSize + contextSize + alignment;
            return Optional.of(SegmentAllocator.slicingAllocator(arena.allocate(allocSize, alignment)));
        }
        catch (Throwable t) {
            return Optional.empty();
        }
    }

    public MemorySegment allocateLoadSegment(SegmentAllocator warmMemoryAllocator)
    {
        return warmMemoryAllocator.allocate((long) warmBufferSize, storageEngineConstants.getPageSize());
    }

    public MemorySegment allocateLoadWriteBuffer(SegmentAllocator warmMemoryAllocator)
    {
        return warmMemoryAllocator.allocate((long) warmWriteBufferSize, storageEngineConstants.getPageSize());
    }

    public SegmentAllocator allocateLoadContextAllocator(SegmentAllocator warmMemoryAllocator, boolean allocateCommonWarmUpState)
    {
        if (allocateCommonWarmUpState) {
            return SegmentAllocator.prefixAllocator(warmMemoryAllocator.allocate((long) storageEngineConstants.getMaxWeContextSize(), ValueLayout.JAVA_INT.byteSize()));
        }
        return SegmentAllocator.slicingAllocator(warmMemoryAllocator.allocate((long) warmContextBufferSize, ValueLayout.JAVA_INT.byteSize()));
    }

    // return array of memory segments for java and sets the addresses inside the warm up state for storage engine
    public void setWarmBuffers(WarmUpElementAllocationParams weAllocParams, WarmUpState warmUpState)
    {
        if (weAllocParams.memorySegment() == null) {
            throw new RuntimeException("memory segment is null");
        }
        SegmentAllocator slicer = SegmentAllocator.slicingAllocator(weAllocParams.memorySegment());
        final long alignment = storageEngineConstants.getPageSize();

        MemorySegment jbufList = warmUpState.getJbufList();
        if (weAllocParams.isRecBufferNeeded()) {
            warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_REC, slicer.allocate(weAllocParams.recBuffSize(), alignment));
            if (weAllocParams.extRecBuffSize() > 0) {
                warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_EXT_RECS, slicer.allocate(weAllocParams.extRecBuffSize(), alignment));
            }
            // data type
            warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_TEMP, slicer.allocate(dataTempBufferSize, alignment));
        }
        else {
            // for all index types
            warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_TEMP, slicer.allocate(indexTempBufferSize, alignment));
        }

        if (weAllocParams.isCrcBufferNeeded()) {
            warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_CRC, slicer.allocate(weAllocParams.crcBuffSize(), alignment));
        }

        warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_NULL, slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()], alignment));

        if (weAllocParams.isMdBufferNeeded()) {
            warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_SKIPLIST, slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_SKIPLIST.ordinal()], alignment));
        }

        warmUpState.setJbufInList(jbufList, JbufType.JBUF_TYPE_CHUNKS, slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_CHUNKS.ordinal()], alignment));
    }

    public ByteBuffer memorySegment2RecBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_REC.ordinal()]);
    }

    public ByteBuffer memorySegment2ExtRecsBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_EXT_RECS.ordinal()]);
    }

    public ByteBuffer memorySegment2NullBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_NULL.ordinal()]);
    }

    public ByteBuffer memorySegment2ChunksBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_CHUNKS.ordinal()]);
    }

    public ByteBuffer memorySegment2CrcBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_CRC.ordinal()]);
    }

    public ByteBuffer memorySegment2PredicateBuff(MemorySegment buff)
    {
        return memorySegment2ByteBuffer(buff);
    }

    public ByteBuffer memorySegment2LuceneResultBM(MemorySegment buff, int offset)
    {
        return memorySegment2ByteBuffer(buff, offset);
    }

    public IntBuffer memorySegment2VarlenMdBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_SKIPLIST.ordinal()]).asIntBuffer();
    }

    private ByteBuffer memorySegment2ByteBuffer(MemorySegment buff)
    {
        return createBuffView(buff.asByteBuffer());
    }

    private ByteBuffer memorySegment2ByteBuffer(MemorySegment buff, int offset)
    {
        return createBuffView(buff.asByteBuffer().position(offset).slice());
    }

    public ByteBuffer createBuffView(ByteBuffer buf)
    {
        return buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }

    public PredicateBufferInfo allocPredicateBuffer(int size)
    {
        PredicateBufferInfo ret = null;

        for (int i = 1; i < predicateBufferPools.length; i++) {
            MemorySegment buff = predicateBufferPools[i].alloc(size);
            if (buff != null) {
                predicateBufferUpdateAllocStats(PredicateBufferPoolType.values()[i], true);
                ret = new PredicateBufferInfo(buff, PredicateBufferPoolType.values()[i]);
                break;
            }
        }
        return ret;
    }

    public PredicateBufferPoolType getRequiredPredicateBufferType(int size)
    {
        for (int i = 1; i < predicateBufferPools.length; i++) {
            if (predicateBufferPools[i].canHandle(size)) {
                return predicateBufferPools[i].getPredicateBufferPoolType();
            }
        }
        return PredicateBufferPoolType.INVALID;
    }

    public void freePredicateBuffer(PredicateCacheData predicateCacheData)
    {
        int poolId = predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType().ordinal();
        predicateBufferPools[poolId].free(predicateCacheData.getPredicateBufferInfo().buff());
        predicateBufferUpdateAllocStats(PredicateBufferPoolType.values()[poolId], false);
    }

    private void predicateBufferUpdateAllocStats(PredicateBufferPoolType predicateBuffer, boolean alloc)
    {
        int diff = alloc ? 1 : -1;
        switch (predicateBuffer) {
            case TINY -> stats.addpredicate_buffer_tiny_alloc(diff);
            case SMALL -> stats.addpredicate_buffer_small_alloc(diff);
            case MEDIUM -> stats.addpredicate_buffer_medium_alloc(diff);
            case LARGE -> stats.addpredicate_buffer_large_alloc(diff);
            default -> throw new RuntimeException("unknown predicateBuffer code " + predicateBuffer);
        }
    }

    public WarmUpElementAllocationParams calculateAllocationParams(WarmupElementWriteMetadata warmupElementWriteMetadata, MemorySegment loadSegment)
    {
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        RecTypeCode recTypeCode = warmUpElement.getRecTypeCode();
        int recTypeLength = warmUpElement.getRecTypeLength();

        boolean isRecBufferNeeded = isRecordBufferNeeded(recTypeCode);
        return new WarmUpElementAllocationParams(
                recTypeCode,
                recTypeLength,
                isRecBufferNeeded ? getWarmupRecordBufferSize(recTypeLength) : 0,
                isCrcBufferNeeded(warmUpElement.getWarmUpType(), recTypeCode) ? calculateCrcBufferSize(recTypeCode, recTypeLength) : 0,
                isRecBufferNeeded && isExtendedBufferNeeded(recTypeLength) ? extRecBuffSize : 0,
                isRecBufferNeeded && isMdBufferNeeded(recTypeCode),
                isLuceneBuffersNeeded(warmUpElement.getWarmUpType()),
                loadSegment);
    }

    // NOTE: this is called only for fixed length since variable length in insert is only above maxRecLenForWarmupRecordBuffer
    public int getWarmupRecordBufferSize(int recTypeLength)
    {
        return warmupRecordBufferSizes[Math.min(recTypeLength, maxRecLenForWarmupRecordBuffer)];
    }

    public int getCollectRecordBufferSizeMust(RecTypeCode recTypeCode, int recTypeLength)
    {
        int recordBufferSize = getCollectRecordBufferSize(recTypeCode, recTypeLength);
        return Math.min(storageEngineConstants.getRecordBufferMaxSize(), recordBufferSize - getCollectRecordBufferSizeOptional(recTypeCode, recTypeLength));
    }

    public int getCollectRecordBufferSize(RecTypeCode recTypeCode, int recTypeLength)
    {
        if (TypeUtils.isVarlenStr(recTypeCode)) {
            return collectVarlenRecordBufferSizes[Math.min(recTypeLength, maxRecLenForDataVarlen)];
        }
        return collectFixedRecordBufferSizes[Math.min(recTypeLength, maxRecLenForDataFixed)];
    }

    public int getCollectRecordBufferSizeOptional(RecTypeCode recTypeCode, int recTypeLength)
    {
        if (TypeUtils.isVarlenStr(recTypeCode) || (recTypeCode == RecTypeCode.REC_TYPE_DECIMAL_LONG)) {
            final int recordBufferSize = getCollectRecordBufferSize(recTypeCode, recTypeLength);
            if (recordBufferSize <= storageEngineConstants.getRecordBufferMaxSize()) {
                // no extra needed for this varchar
                return 0;
            }
            // extra is limited by max juffer size
            return Math.min(recordBufferSize - storageEngineConstants.getRecordBufferMaxSize(),
                    nativeConfig.getMaxRecJufferSize() - storageEngineConstants.getRecordBufferMaxSize());
        }
        return 0;
    }

    // NOTE: this is called only for fixed length since variable length does not support match collect (index is crc based)
    public int getMatchCollectRecordBufferSize(int recTypeLength)
    {
        return collectFixedRecordBufferSizes[recTypeLength];
    }

    public int getMappedMatchCollectBufferSize()
    {
        return collectFixedRecordBufferSizes[TinyintType.TINYINT.getFixedSize()];
    }

    public int getCollectTxSize(RecTypeCode recTypeCode, int recTypeLength)
    {
        if (TypeUtils.isVarlenStr(recTypeCode)) {
            return varlenCollectTxSizes[Math.min(recTypeLength, maxRecLenForDataVarlen)];
        }
        return fixedCollectTxSizes[Math.min(recTypeLength, maxRecLenForDataFixed)];
    }

    public int getWarmupIndexTxSize()
    {
        return warmupIndexTxSize;
    }

    public int getQueryNullBufferSize(RecTypeCode recTypeCode)
    {
        return buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()] * (TypeUtils.isVarlenStr(recTypeCode) ? queryStringNullValueSize : 1);
    }

    private int calculateCrcBufferSize(RecTypeCode recTypeCode, int recTypeLength)
    {
        int recSize;
        if (recTypeCode == RecTypeCode.REC_TYPE_DECIMAL_LONG) {
            recSize = recTypeLength;
        }
        else {
            recSize = switch (recTypeLength) {
                case 1 -> Byte.BYTES;
                case 2 -> Short.BYTES;
                case 4 -> Integer.BYTES;
                default -> Long.BYTES;
            };
        }
        return (recSize + Short.BYTES) * (1 << storageEngineConstants.getChunkSizeShift());
    }

    private boolean isExtendedBufferNeeded(int recTypeLength)
    {
        return recTypeLength > storageEngineConstants.getVarlenExtLimit();
    }

    private boolean isMdBufferNeeded(RecTypeCode recTypeCode)
    {
        return TypeUtils.isVarlenStr(recTypeCode);
    }

    private boolean isLuceneBuffersNeeded(WarmUpType columnWarmUpType)
    {
        return WarmUpType.WARM_UP_TYPE_LUCENE.equals(columnWarmUpType);
    }

    private boolean isRecordBufferNeeded(RecTypeCode recTypeCode)
    {
        return recTypeCode == RecTypeCode.REC_TYPE_BOOLEAN;
    }

    private boolean isCrcBufferNeeded(WarmUpType columnWarmUpType, RecTypeCode recTypeCode)
    {
        return WarmUpType.WARM_UP_TYPE_BASIC.equals(columnWarmUpType) && (recTypeCode != RecTypeCode.REC_TYPE_BOOLEAN);
    }

    private int roundToPageSize(int size)
    {
        int pageSize = storageEngineConstants.getPageSize();

        if (pageSize == 0) {
            return 0;
        }
        return ((size + pageSize - 1) / pageSize) * pageSize;
    }

    public int calcWeRecordBufferSize(int requestedSize, double satisfyPercentage)
    {
        return min(nativeConfig.getMaxRecJufferSize(), roundToPageSize((int) (requestedSize * satisfyPercentage)));
    }

    public double calculateSatisfyPercentage(long totalRequestedRecordBufferSize, long totalNullBuffs, QueryParams queryParams)
    {
        if (queryParams.getNumMatchElements() == 0 ||   // in full scan we lazy collect so we can assume to get MaxRecJufferSize
                totalRequestedRecordBufferSize + totalNullBuffs <= COLLECT_BUFFER_MAX_MEMORY) {
            return 1.0;
        }
        // adding numWes * page size so we will be able to round up the allocations
        long spareBuffer = COLLECT_BUFFER_MAX_MEMORY - (totalNullBuffs + (long) queryParams.getNumCollectElements() * storageEngineConstants.getPageSize());
        return (double) spareBuffer / totalRequestedRecordBufferSize;
    }
}
