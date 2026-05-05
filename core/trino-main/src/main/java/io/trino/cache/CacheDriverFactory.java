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
package io.trino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.execution.ScheduledSplit;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.plugin.base.cache.CacheUtils;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.TrinoException;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.PageSourceProvider;
import io.trino.split.PageSourceProviderFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.getCacheDataReductionThreshold;
import static io.trino.SystemSessionProperties.isEnableDynamicRowFiltering;
import static io.trino.cache.CacheCommonSubqueries.LOAD_PAGES_ALTERNATIVE;
import static io.trino.cache.CacheCommonSubqueries.ORIGINAL_PLAN_ALTERNATIVE;
import static io.trino.cache.CacheCommonSubqueries.STORE_PAGES_ALTERNATIVE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.function.Function.identity;

public class CacheDriverFactory
{
    static final int MAX_UNENFORCED_PREDICATE_VALUE_COUNT = 1_000_000;
    public static final float TOO_BIG_SPLITS_THRESHOLD = 0.3f;
    public static final int MIN_PROCESSED_SPLITS = 16;
    public static final int MIN_PROCESSED_BYTES = 5 * 1024 * 1024; // 5 MB (total across all splits)

    private final Session session;
    private final PageSourceProvider pageSourceProvider;
    private final PlanSignature planSignature;
    private final SplitCache splitCache;
    private final JsonCodec<TupleDomain> tupleDomainCodec;
    private final TableHandle originalTableHandle;
    private final TupleDomain<CacheColumnId> enforcedPredicate;
    private final BiMap<ColumnHandle, CacheColumnId> commonColumnHandles;
    private final Map<CacheColumnId, Integer> projectedColumns;
    private final Supplier<StaticDynamicFilter> dynamicFilterSupplier;
    private final List<DriverFactory> alternatives;
    private final CacheMetrics cacheMetrics = new CacheMetrics();
    private final CacheStats cacheStats;
    private final CachePerformanceTracker cachePerformanceTracker;
    private final Ticker ticker = Ticker.systemTicker();

    public CacheDriverFactory(
            Session session,
            PageSourceProviderFactory pageSourceProvider,
            CacheManagerRegistry cacheManagerRegistry,
            JsonCodec<TupleDomain> tupleDomainCodec,
            TableHandle originalTableHandle,
            PlanSignatureWithPredicate planSignature,
            Map<CacheColumnId, ColumnHandle> commonColumnHandles,
            Supplier<StaticDynamicFilter> dynamicFilterSupplier,
            List<DriverFactory> alternatives,
            CacheStats cacheStats,
            CachePerformanceTracker cachePerformanceTracker)
    {
        requireNonNull(planSignature, "planSignature is null");
        this.session = requireNonNull(session, "session is null");
        this.planSignature = planSignature.signature();
        this.splitCache = requireNonNull(cacheManagerRegistry, "cacheManagerRegistry is null").getCacheManager().getSplitCache(planSignature.signature());
        this.tupleDomainCodec = requireNonNull(tupleDomainCodec, "tupleDomainCodec is null");
        this.originalTableHandle = requireNonNull(originalTableHandle, "originalTableHandle is null");
        this.enforcedPredicate = planSignature.predicate();
        this.commonColumnHandles = ImmutableBiMap.copyOf(requireNonNull(commonColumnHandles, "commonColumnHandles is null")).inverse();
        List<CacheColumnId> columns = planSignature.signature().getColumns();
        this.projectedColumns = IntStream.range(0, columns.size()).boxed()
                .collect(toImmutableMap(columns::get, identity()));
        this.dynamicFilterSupplier = requireNonNull(dynamicFilterSupplier, "dynamicFilterSupplier is null");
        this.alternatives = requireNonNull(alternatives, "alternatives is null");
        this.cacheStats = requireNonNull(cacheStats, "cacheStats is null");
        this.cachePerformanceTracker = requireNonNull(cachePerformanceTracker, "cachePerformanceTracker is null");
        this.pageSourceProvider = pageSourceProvider.createPageSourceProvider(originalTableHandle.catalogHandle());
    }

    public Driver createDriver(DriverContext driverContext, ScheduledSplit split, Optional<CacheSplitId> cacheSplitIdOptional)
    {
        DriverFactoryWithCacheContext driverFactory;
        long lookupStartNanos = ticker.read();
        try {
            driverFactory = chooseDriverFactory(split, cacheSplitIdOptional);
        }
        catch (Throwable t) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "SUBQUERY CACHE: create driver exception", t);
        }
        long lookupDurationNanos = ticker.read() - lookupStartNanos;
        cacheStats.getCacheLookupTime().addNanos(lookupDurationNanos);

        driverFactory.context()
                .map(context -> context.withMetrics(new Metrics(ImmutableMap.of(
                        "Cache lookup time (ms)", TDigestHistogram.fromValue(new Duration(lookupDurationNanos, NANOSECONDS).convertTo(MILLISECONDS).getValue())))))
                .ifPresent(driverContext::setCacheDriverContext);
        return driverFactory.factory().createDriver(driverContext);
    }

    private DriverFactoryWithCacheContext chooseDriverFactory(ScheduledSplit split, Optional<CacheSplitId> cacheSplitIdOptional)
    {
        if (cacheSplitIdOptional.isEmpty()) {
            // no split id, fallback to original plan
            cacheStats.recordMissingSplitId();
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }
        if (!split.split().isSplitAddressEnforced()) {
            // failed to schedule split on the preferred node, fallback to original plan
            cacheStats.recordSplitFailoverHappened();
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }
        CacheSplitId splitId = cacheSplitIdOptional.get();

        StaticDynamicFilter dynamicFilter = dynamicFilterSupplier.get();
        if (dynamicFilter.getCurrentDynamicFilterTupleDomain().getDomains().orElse(ImmutableMap.of())
                .values().stream().anyMatch(domain -> domain.getBloomfilterWithRange().isPresent())) {
            // bloom filters are not supported in cache
            cacheStats.recordDynamicFilterWithBloomFilter();
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }

        TupleDomain<CacheColumnId> enforcedPredicate = pruneEnforcedPredicate(split);
        TupleDomain<CacheColumnId> unenforcedPredicate = getDynamicRowFilteringUnenforcedPredicate(
                pageSourceProvider,
                session,
                split.split(),
                originalTableHandle,
                dynamicFilter.getCurrentPredicate())
                .transformKeys(handle -> requireNonNull(commonColumnHandles.get(handle)));

        // skip caching of completely filtered out splits
        if (enforcedPredicate.isNone() || unenforcedPredicate.isNone()) {
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }

        // skip caching if unenforced predicate becomes too big,
        // because large predicates are not likely to be reused in other subqueries
        if (getTupleDomainValueCount(unenforcedPredicate) > MAX_UNENFORCED_PREDICATE_VALUE_COUNT) {
            cacheStats.recordPredicateTooBig();
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }

        ProjectPredicate projectedEnforcedPredicate = projectPredicate(enforcedPredicate);
        ProjectPredicate projectedUnenforcedPredicate = projectPredicate(unenforcedPredicate);
        CacheSplitId splitIdWithPredicates = appendRemainingPredicates(splitId, projectedEnforcedPredicate, projectedUnenforcedPredicate);

        // load data from cache
        Optional<ConnectorPageSource> pageSource = splitCache.loadPages(splitIdWithPredicates, projectedEnforcedPredicate.predicate(), projectedUnenforcedPredicate.predicate());
        if (pageSource.isPresent()) {
            cacheStats.recordCacheHit();
            return new DriverFactoryWithCacheContext(
                    alternatives.get(LOAD_PAGES_ALTERNATIVE),
                    Optional.of(new CacheDriverContext(pageSource, Optional.empty(), dynamicFilter, splitId, planSignature, cacheMetrics, cacheStats, cachePerformanceTracker, Metrics.EMPTY)));
        }
        else {
            cacheStats.recordCacheMiss();
        }

        int processedSplitCount = cacheMetrics.getSplitNotCachedCount() + cacheMetrics.getSplitCachedCount();
        float tooBigSplitsRatio = processedSplitCount > MIN_PROCESSED_SPLITS ? cacheMetrics.getTooBigSplitCount() / (float) processedSplitCount : 0.0f;
        // try storing results instead
        // if splits are too large to be cached then do not try caching data as it adds extra computational cost
        if (tooBigSplitsRatio <= TOO_BIG_SPLITS_THRESHOLD) {
            double dataReductionRatio = processedSplitCount > MIN_PROCESSED_SPLITS && cacheMetrics.getInputCacheBytes() > MIN_PROCESSED_BYTES && cacheMetrics.getSourceBytes() > 0
                    ? cacheMetrics.getInputCacheBytes() / (double) cacheMetrics.getSourceBytes()
                    : 0d;
            if (dataReductionRatio <= getCacheDataReductionThreshold(session)) {
                Optional<ConnectorPageSink> pageSink = splitCache.storePages(splitIdWithPredicates, projectedEnforcedPredicate.predicate(), projectedUnenforcedPredicate.predicate());
                if (pageSink.isPresent()) {
                    return new DriverFactoryWithCacheContext(
                            alternatives.get(STORE_PAGES_ALTERNATIVE),
                            Optional.of(new CacheDriverContext(Optional.empty(), pageSink, dynamicFilter, splitId, planSignature, cacheMetrics, cacheStats, cachePerformanceTracker, Metrics.EMPTY)));
                }
                else {
                    cacheStats.recordSplitRejected();
                }
            }
            else {
                cacheStats.recordInsufficientDataReduction();
            }
        }
        else {
            cacheStats.recordTooBigSplit();
        }

        // fallback to original subplan
        return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
    }

    private record DriverFactoryWithCacheContext(DriverFactory factory, Optional<CacheDriverContext> context) {}

    private TupleDomain<CacheColumnId> pruneEnforcedPredicate(ScheduledSplit split)
    {
        return TupleDomain.intersect(ImmutableList.of(
                // prune scan domains of enforced predicate
                pageSourceProvider.prunePredicate(
                                session,
                                split.split(),
                                originalTableHandle,
                                enforcedPredicate
                                        .filter((columnId, _) -> commonColumnHandles.containsValue(columnId))
                                        .transformKeys(columnId -> commonColumnHandles.inverse().get(columnId)))
                        .transformKeys(commonColumnHandles::get),
                enforcedPredicate.filter((columnId, _) -> !commonColumnHandles.containsValue(columnId))));
    }

    private ProjectPredicate projectPredicate(TupleDomain<CacheColumnId> predicate)
    {
        return new ProjectPredicate(
                predicate.filter((columnId, _) -> projectedColumns.containsKey(columnId)),
                Optional.of(predicate.filter((columnId, _) -> !projectedColumns.containsKey(columnId)))
                        .filter(domain -> !domain.isAll())
                        .map(CacheUtils::normalizeTupleDomain)
                        .map(tupleDomainCodec::toJson));
    }

    private record ProjectPredicate(TupleDomain<CacheColumnId> predicate, Optional<String> remainingPredicate) {}

    private static CacheSplitId appendRemainingPredicates(CacheSplitId splitId, ProjectPredicate enforcedPredicate, ProjectPredicate unenforcedPredicate)
    {
        return appendRemainingPredicates(splitId, enforcedPredicate.remainingPredicate(), unenforcedPredicate.remainingPredicate());
    }

    @VisibleForTesting
    static CacheSplitId appendRemainingPredicates(CacheSplitId splitId, Optional<String> remainingEnforcedPredicate, Optional<String> remainingUnenforcedPredicate)
    {
        if (remainingEnforcedPredicate.isEmpty() && remainingUnenforcedPredicate.isEmpty()) {
            return splitId;
        }
        return new CacheSplitId(toStringHelper("SplitId")
                .add("splitId", splitId)
                .add("enforcedPredicate", remainingEnforcedPredicate.orElse("all"))
                .add("unenforcedPredicate", remainingUnenforcedPredicate.orElse("all"))
                .toString());
    }

    public void closeSplitCache()
    {
        try {
            splitCache.close();
        }
        catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @VisibleForTesting
    public static TupleDomain<ColumnHandle> getDynamicRowFilteringUnenforcedPredicate(
            PageSourceProvider delegatePageSourceProvider,
            Session session,
            Split split,
            TableHandle table,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        if (!isEnableDynamicRowFiltering(session)) {
            return delegatePageSourceProvider.getUnenforcedPredicate(session, split, table, dynamicFilter);
        }

        TupleDomain<ColumnHandle> unenforcedPredicate = delegatePageSourceProvider.getUnenforcedPredicate(session, split, table, dynamicFilter);
        if (unenforcedPredicate.isNone()) {
            // split is fully filtered out
            return TupleDomain.none();
        }

        // DynamicRowFilteringPageSourceProvider doesn't simplify dynamic predicate,
        // but we can still prune columns from dynamic filter, which are ineffective
        // in filtering split data
        return unenforcedPredicate.intersect(delegatePageSourceProvider.prunePredicate(session, split, table, dynamicFilter));
    }

    @VisibleForTesting
    public CacheMetrics getCacheMetrics()
    {
        return cacheMetrics;
    }

    private static int getTupleDomainValueCount(TupleDomain<?> tupleDomain)
    {
        return tupleDomain.getDomains()
                .map(domains -> domains.values().stream()
                        .mapToInt(CacheDriverFactory::getDomainValueCount)
                        .sum())
                .orElse(0);
    }

    private static int getDomainValueCount(Domain domain)
    {
        return domain.getValues().getValuesProcessor().transform(
                Ranges::getRangeCount,
                DiscreteValues::getValuesCount,
                _ -> 0);
    }
}
