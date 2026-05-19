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
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.PredicateInfo;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.CachePredicatesStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.read.predicates.AllPredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.InverseStringValuesPredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.InverseValuesPredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.LucenePredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.NonePredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.PredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.RangesPredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.StringRangesPredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.StringValuesPredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.ValuesPredicateFiller;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

@Singleton
public class PredicatesCacheService
{
    private static final Logger logger = Logger.get(PredicatesCacheService.class);
    private final ShapingLogger shapingLogger;
    private final Map<PredicateBufferPoolType, Map<Integer, PredicateCacheData>> predicateCachePool;
    private final Map<PredicateType, PredicateFiller> predicateTypeToFiller;
    private final BufferAllocator bufferAllocator;
    private final StorageEngineConstants storageEngineConstants;
    private final CachePredicatesStats cachePredicatesStats;
    private final DomainToMapBlockConvertor domainToMapBlockConvertor;
    private final Lock readLock;
    private final Lock writeLock;
    private final AtomicInteger activePredicatesTiny;
    private final AtomicInteger activePredicatesSmall;
    private final AtomicInteger activePredicatesMedium;
    private final AtomicInteger activePredicatesLarge;
    private static final int PREDICATE_SIZE1 = 100;
    private static final int PREDICATE_SIZE2 = 500;
    private static final int PREDICATE_SIZE3 = 1000;
    private static final int PREDICATE_SIZE4 = 5000;
    private static final int PREDICATE_SIZE5 = 10000;
    private static final int PREDICATE_SIZE6 = 50000;
    private static final int PREDICATE_SIZE7 = 100000;
    private static final int PREDICATE_SIZE8 = 500000;
    private static final int PREDICATE_SIZE9 = 1000000;

    private enum MetricsType
    {
        IN_USE,
        CACHE_HIT,
        CACHE_MISS,
        CACHE_MAX,
    }

    @Inject
    public PredicatesCacheService(
            BufferAllocator bufferAllocator,
            StorageEngineConstants storageEngineConstants,
            MetricsManager metricsManager,
            DomainToMapBlockConvertor domainToMapBlockConvertor,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.domainToMapBlockConvertor = domainToMapBlockConvertor;
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        this.predicateCachePool = new HashMap<>();
        this.predicateTypeToFiller = new HashMap<>();
        initPredicateCachePoll();
        initPredicateFillerMap();
        this.cachePredicatesStats = metricsManager.registerMetric(CachePredicatesStats.create());
        this.activePredicatesTiny = new AtomicInteger(0);
        this.activePredicatesSmall = new AtomicInteger(0);
        this.activePredicatesMedium = new AtomicInteger(0);
        this.activePredicatesLarge = new AtomicInteger(0);
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    private void initPredicateCachePoll()
    {
        for (PredicateBufferPoolType predicateBufferPoolType : PredicateBufferPoolType.values()) {
            predicateCachePool.put(predicateBufferPoolType, new HashMap<>());
        }
    }

    private void initPredicateFillerMap()
    {
        PredicateFiller predicateFiller = new RangesPredicateFiller(bufferAllocator, storageEngineConstants);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new LucenePredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        PredicateFiller stringPredicate = new StringValuesPredicateFiller(storageEngineConstants, bufferAllocator);
        predicateTypeToFiller.put(stringPredicate.getPredicateType(), stringPredicate);
        PredicateFiller stringRangesPredicate = new StringRangesPredicateFiller(storageEngineConstants, bufferAllocator);
        predicateTypeToFiller.put(stringRangesPredicate.getPredicateType(), stringRangesPredicate);
        predicateFiller = new ValuesPredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new AllPredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new NonePredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new InverseValuesPredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new InverseStringValuesPredicateFiller(storageEngineConstants, bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
    }

    private static boolean canMapMatchCollect(PredicateData predicateData, Domain domain)
    {
        if (domain == null) {
            return false;
        }

        return PredicateUtil.canMapMatchCollect(
                domain.getType(),
                predicateData.getPredicateInfo().predicateType(),
                predicateData.getPredicateInfo().functionType(),
                domain.getValues().getRanges().getRangeCount());
    }

    private void updatePredicateSizeStats(int size)
    {
        if (size < PREDICATE_SIZE1) {
            cachePredicatesStats.incsize1_minus();
        }
        else if (size < PREDICATE_SIZE2) {
            cachePredicatesStats.incsize1_size2();
        }
        else if (size < PREDICATE_SIZE3) {
            cachePredicatesStats.incsize2_size3();
        }
        else if (size < PREDICATE_SIZE4) {
            cachePredicatesStats.incsize3_size4();
        }
        else if (size < PREDICATE_SIZE5) {
            cachePredicatesStats.incsize4_size5();
        }
        else if (size < PREDICATE_SIZE6) {
            cachePredicatesStats.incsize5_size6();
        }
        else if (size < PREDICATE_SIZE7) {
            cachePredicatesStats.incsize6_size7();
        }
        else if (size < PREDICATE_SIZE8) {
            cachePredicatesStats.incsize7_size8();
        }
        else if (size < PREDICATE_SIZE9) {
            cachePredicatesStats.incsize8_size9();
        }
        else {
            cachePredicatesStats.incsize9_plus();
        }
    }

    public Optional<PredicateCacheData> createPredicateCacheData(PredicateData predicateData, Domain domain)
    {
        Optional<PredicateCacheData> ret = Optional.empty();
        PredicateBufferInfo predicateBufferInfo = bufferAllocator.allocPredicateBuffer(predicateData.getPredicateSize());
        if (predicateBufferInfo != null) {
            // The dictionary is generated for each predicate and cached with other predicate data, but its utilization is decided per split in createMatchCollect().
            Optional<Block> valuesDict = canMapMatchCollect(predicateData, domain) ? domainToMapBlockConvertor.convert(domain) : Optional.empty();
            ret = Optional.of(new PredicateCacheData(predicateBufferInfo, valuesDict));
        }
        return ret;
    }

    // utility function that allocates and fills the buffer but does not store it in cache
    public Optional<PredicateCacheData> predicateDataToBuffer(PredicateData predicateData, Domain domain)
    {
        Optional<PredicateCacheData> predicateCacheData = createPredicateCacheData(predicateData, domain);
        if (predicateCacheData.isPresent()) {
            PredicateInfo predicateInfo = predicateData.getPredicateInfo();
            ByteBuffer predicateBuffer = bufferAllocator.memorySegment2PredicateBuff(predicateCacheData.get().getPredicateBufferInfo().buff());
            PredicateType predicateType = predicateInfo.predicateType();
            predicateTypeToFiller.get(predicateType).fillPredicate(domain, predicateBuffer, predicateData);
        }
        return predicateCacheData;
    }

    public Optional<PredicateCacheData> getOrCreatePredicateBufferId(PredicateData predicateData, Domain domain)
    {
        Optional<PredicateCacheData> predicateCacheDataOpt = Optional.empty();
        PredicateBufferPoolType predicateBufferPoolType = bufferAllocator.getRequiredPredicateBufferType(predicateData.getPredicateSize());

        // this case is handled by the caller
        if (predicateBufferPoolType == PredicateBufferPoolType.INVALID) {
            return predicateCacheDataOpt;
        }

        Map<Integer, PredicateCacheData> predicateCache = predicateCachePool.get(predicateBufferPoolType);
        readLock.lock();
        try {
            PredicateCacheData predicateCacheData = predicateCache.get(predicateData.getPredicateHashCode());
            if (predicateCacheData != null) {
                predicateCacheDataOpt = Optional.of(predicateCacheData);
                incrementUse(predicateCacheData);
                incrementHit(predicateBufferPoolType);
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to get predicate buffer for queryMatchData=%s", predicateData);
            throw e;
        }
        finally {
            readLock.unlock();
        }
        if (predicateCacheDataOpt.isEmpty()) {
            predicateCacheDataOpt = createPredicate(predicateData, domain, predicateBufferPoolType);
        }
        return predicateCacheDataOpt; // can be empty if allocation failed since there are no buffers in the pool
    }

    public void markFinished(List<PredicateCacheData> predicateCacheDatas)
    {
        for (PredicateCacheData predicateCacheData : predicateCacheDatas) {
            if (predicateCacheData.getCurrentUse() > 0) {
                decrementUse(predicateCacheData);
            }
        }
    }

    private void incrementUse(PredicateCacheData predicateCacheData)
    {
        if (predicateCacheData.getCurrentUse() == 0) {
            synchronized (this) {
                if (predicateCacheData.getCurrentUse() == 0) {
                    updateActivePredicates(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), true);
                }
                predicateCacheData.incrementUse();
                updateMetrics(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), true, MetricsType.IN_USE);
            }
        }
        else {
            predicateCacheData.incrementUse();
            updateMetrics(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), true, MetricsType.IN_USE);
        }
    }

    private void decrementUse(PredicateCacheData predicateCacheData)
    {
        if (predicateCacheData.getCurrentUse() == 1) {
            synchronized (this) {
                if (predicateCacheData.getCurrentUse() == 1) {
                    updateActivePredicates(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), false);
                }
                predicateCacheData.decrementUse();
                updateMetrics(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), false, MetricsType.IN_USE);
            }
        }
        else {
            predicateCacheData.decrementUse();
            updateMetrics(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), false, MetricsType.IN_USE);
        }
    }

    private void incrementHit(PredicateBufferPoolType predicateBufferPoolType)
    {
        updateMetrics(predicateBufferPoolType, true, MetricsType.CACHE_HIT);
    }

    private void incrementMiss(PredicateBufferPoolType predicateBufferPoolType)
    {
        updateMetrics(predicateBufferPoolType, true, MetricsType.CACHE_MISS);
    }

    private void updateActivePredicates(PredicateBufferPoolType predicateBufferPoolType, boolean increase)
    {
        int newVal;
        int delta;
        if (increase) {
            switch (predicateBufferPoolType) {
                case TINY -> {
                    newVal = activePredicatesTiny.incrementAndGet();
                    delta = (int) (newVal - cachePredicatesStats.getmax_small());
                    if (delta > 0) {
                        cachePredicatesStats.addmax_tiny(delta);
                    }
                }
                case SMALL -> {
                    newVal = activePredicatesSmall.incrementAndGet();
                    delta = (int) (newVal - cachePredicatesStats.getmax_small());
                    if (delta > 0) {
                        cachePredicatesStats.addmax_small(delta);
                    }
                }
                case MEDIUM -> {
                    newVal = activePredicatesMedium.incrementAndGet();
                    delta = (int) (newVal - cachePredicatesStats.getmax_medium());
                    if (delta > 0) {
                        cachePredicatesStats.addmax_medium(delta);
                    }
                }
                case LARGE -> {
                    newVal = activePredicatesLarge.incrementAndGet();
                    delta = (int) (newVal - cachePredicatesStats.getmax_large());
                    if (delta > 0) {
                        cachePredicatesStats.addmax_large(delta);
                    }
                }
                default -> throw new TrinoException(WarpErrorCode.WARP_ILLEGAL_PARAMETER, "Uknown predicateBufferPoolType " + predicateBufferPoolType);
            }
        }
        else {
            switch (predicateBufferPoolType) {
                case TINY -> activePredicatesTiny.decrementAndGet();
                case SMALL -> activePredicatesSmall.decrementAndGet();
                case MEDIUM -> activePredicatesMedium.decrementAndGet();
                case LARGE -> activePredicatesLarge.decrementAndGet();
                default -> throw new TrinoException(WarpErrorCode.WARP_ILLEGAL_PARAMETER, "Uknown predicateBufferPoolType " + predicateBufferPoolType);
            }
        }
    }

    private void updateMetrics(PredicateBufferPoolType predicateBufferPoolType, boolean increase, MetricsType metricsType)
    {
        if (increase) {
            switch (predicateBufferPoolType) {
                case TINY -> {
                    switch (metricsType) {
                        case IN_USE -> cachePredicatesStats.incin_use_tiny();
                        case CACHE_HIT -> cachePredicatesStats.inchit_tiny();
                        case CACHE_MISS -> cachePredicatesStats.incmiss_tiny();
                        case CACHE_MAX -> throw new TrinoException(WarpErrorCode.WARP_GENERIC, "Unexpected to increase " + metricsType);
                    }
                }
                case SMALL -> {
                    switch (metricsType) {
                        case IN_USE -> cachePredicatesStats.incin_use_small();
                        case CACHE_HIT -> cachePredicatesStats.inchit_small();
                        case CACHE_MISS -> cachePredicatesStats.incmiss_small();
                        case CACHE_MAX -> throw new TrinoException(WarpErrorCode.WARP_GENERIC, "Unexpected to increase " + metricsType);
                    }
                }
                case MEDIUM -> {
                    switch (metricsType) {
                        case IN_USE -> cachePredicatesStats.incin_use_medium();
                        case CACHE_HIT -> cachePredicatesStats.inchit_medium();
                        case CACHE_MISS -> cachePredicatesStats.incmiss_medium();
                        case CACHE_MAX -> throw new TrinoException(WarpErrorCode.WARP_GENERIC, "Unexpected to increase " + metricsType);
                    }
                }
                case LARGE -> {
                    switch (metricsType) {
                        case IN_USE -> cachePredicatesStats.incin_use_large();
                        case CACHE_HIT -> cachePredicatesStats.inchit_large();
                        case CACHE_MISS -> cachePredicatesStats.incmiss_large();
                        case CACHE_MAX -> throw new TrinoException(WarpErrorCode.WARP_GENERIC, "Unexpected to increase " + metricsType);
                    }
                }
                default -> throw new TrinoException(WarpErrorCode.WARP_ILLEGAL_PARAMETER, "Uknown predicateBufferPoolType " + predicateBufferPoolType);
            }
        }
        else {
            if (metricsType != MetricsType.IN_USE) {
                throw new TrinoException(WarpErrorCode.WARP_GENERIC, "Unexpected to decrease " + metricsType);
            }
            else {
                switch (predicateBufferPoolType) {
                    case TINY -> cachePredicatesStats.addin_use_tiny(-1);
                    case SMALL -> cachePredicatesStats.addin_use_small(-1);
                    case MEDIUM -> cachePredicatesStats.addin_use_medium(-1);
                    case LARGE -> cachePredicatesStats.addin_use_large(-1);
                    default -> throw new TrinoException(WarpErrorCode.WARP_ILLEGAL_PARAMETER, "Uknown predicateBufferPoolType " + predicateBufferPoolType);
                }
            }
        }
    }

    private Optional<PredicateCacheData> createPredicate(
            PredicateData predicateData,
            Domain domain,
            PredicateBufferPoolType predicateBufferPoolType)
    {
        Optional<PredicateCacheData> predicateCacheDataOpt;
        writeLock.lock();
        try {
            PredicateCacheData predicateCacheData =
                    predicateCachePool.get(predicateBufferPoolType).get(predicateData.getPredicateHashCode());
            if (predicateCacheData == null) {
                // free cache if needed
                if (predicateCachePool.get(predicateBufferPoolType).size() == bufferAllocator.getPoolSize(predicateBufferPoolType)) {
                    if (!freeCache(predicateCachePool.get(predicateBufferPoolType), predicateBufferPoolType)) {
                        shapingLogger.warn(
                                "predicate cache for %s is full and could not be freed. maxSize=%d.",
                                predicateBufferPoolType,
                                bufferAllocator.getPoolSize(predicateBufferPoolType));
                        return Optional.empty();
                    }
                }
                // allocate and fill the buffer
                predicateCacheDataOpt = predicateDataToBuffer(predicateData, domain);
                if (predicateCacheDataOpt.isPresent()) {
                    predicateCacheData = predicateCacheDataOpt.get();
                    // put in cache
                    predicateCachePool.get(predicateBufferPoolType).put(predicateData.getPredicateHashCode(), predicateCacheData);
                    logger.debug(
                            "create new %s, key=%d, cacheSize=%d",
                            predicateCacheData,
                            predicateData.getPredicateHashCode(),
                            predicateCachePool.get(predicateBufferPoolType).size());
                    incrementMiss(predicateBufferPoolType);
                    updatePredicateSizeStats(predicateData.getPredicateSize());
                }
            }
            else {
                predicateCacheDataOpt = Optional.of(predicateCacheData);
                incrementHit(predicateBufferPoolType);
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to add new predicate to cache");
            throw e;
        }
        finally {
            writeLock.unlock();
        }
        predicateCacheDataOpt.ifPresent(this::incrementUse);
        return predicateCacheDataOpt;
    }

    private boolean freeCache(Map<Integer, PredicateCacheData> integerPredicateBufferMap, PredicateBufferPoolType predicateBufferPoolType)
    {
        int poolSize = bufferAllocator.getPoolSize(predicateBufferPoolType);
        int removeCount = poolSize < 10 ? 1 : poolSize / 10;

        List<Integer> toRemove = integerPredicateBufferMap
                .entrySet()
                .stream()
                .filter(x -> x.getValue().canRemove())
                .limit(removeCount)
                .map(Map.Entry::getKey).toList();
        if (toRemove.isEmpty()) {
            shapingLogger.warn("all predicates for type=%s are in use", predicateBufferPoolType);
            return false;
        }
        toRemove.forEach(key -> {
            PredicateCacheData removed = integerPredicateBufferMap.remove(key);
            bufferAllocator.freePredicateBuffer(removed);
        });
        return true;
    }

    @VisibleForTesting
    public int getHitTiny()
    {
        return (int) cachePredicatesStats.gethit_tiny();
    }

    @VisibleForTesting
    public int getMissTiny()
    {
        return (int) cachePredicatesStats.getmiss_tiny();
    }

    @VisibleForTesting
    public int getMaxTiny()
    {
        return (int) cachePredicatesStats.getmax_tiny();
    }

    @VisibleForTesting
    public Map<PredicateBufferPoolType, Map<Integer, PredicateCacheData>> getPredicateCachePool()
    {
        return predicateCachePool;
    }
}
