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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.SizeOf;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.cache.NonEvictableCache;
import jakarta.annotation.Nullable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class RestResponseCache
{
    // Two-level cache: the outer cache maps each [TableIdentifier] to a [ResponsesForIdentifier] that
    // holds all query-param variants for that identifier.
    //
    // A flat `Cache<(TableIdentifier, QueryParams), CachedResponse>` would be simpler, but its
    // `invalidate(identifier)` would have to scan the key set with `removeIf`. Guava's `asMap()`
    // excludes entries that are still loading, so a concurrent load can survive the scan:
    //
    // cache: (A, q0) -> v0, (A, q1) -> v0
    // T1: starts load (A, q2)            -- miss, loading entry created (invisible to asMap())
    // T2: POST modifies table A          -- server moves to v1
    // T2: removeIf(key -> key.id == A)   -- removes (A, q0) and (A, q1); cannot see (A, q2)
    // T1: load completes                 -- stores (A, q2) -> v0 (stale)
    //
    // With the two-level design, `cache.invalidate(identifier)` is a single-key removal on the outer
    // cache. T1 already holds a reference to the [ResponsesForIdentifier], so its load stores into the
    // now-orphaned object. The `replace` call (weight recomputation) fails silently because the entry
    // was removed, and subsequent readers create a fresh [ResponsesForIdentifier].
    private final Cache<TableIdentifier, ResponsesForIdentifier> cache;
    private final Duration metastoreCacheTtl;
    private final long maximumEntrySize;
    private final Ticker ticker;

    public RestResponseCache(Duration metastoreCacheTtl, DataSize metastoreCacheMaximumSize, DataSize metastoreCacheMaximumEntrySize)
    {
        this(metastoreCacheTtl, metastoreCacheMaximumSize, metastoreCacheMaximumEntrySize, Ticker.systemTicker(), Optional.empty());
    }

    @VisibleForTesting
    RestResponseCache(Duration metastoreCacheTtl, DataSize metastoreCacheMaximumSize, DataSize metastoreCacheMaximumEntrySize, Ticker ticker, Optional<Integer> concurrencyLevel)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.maximumEntrySize = metastoreCacheMaximumEntrySize.toBytes();
        EvictableCacheBuilder<TableIdentifier, ResponsesForIdentifier> builder = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(metastoreCacheTtl.toJavaTime())
                .maximumWeight(metastoreCacheMaximumSize.toBytes())
                .weigher(RestResponseCache::calculateWeight)
                .shareNothingWhenDisabled()
                .recordStats();
        concurrencyLevel.ifPresent(builder::concurrencyLevel);
        this.cache = builder.build();
        this.metastoreCacheTtl = requireNonNull(metastoreCacheTtl, "metastoreCacheTtl is null");
    }

    private static int calculateWeight(TableIdentifier identifier, ResponsesForIdentifier entry)
    {
        long namespaceElementsSize = Arrays.stream(identifier.namespace().levels())
                .mapToLong(SizeOf::estimatedSizeOf)
                .sum();
        long keySizeInBytes = sizeOf(identifier.namespace().levels()) +
                namespaceElementsSize +
                estimatedSizeOf(identifier.name());
        long sizeInBytes = keySizeInBytes + entry.getRetainedSizeInBytes();
        return toIntExact(Math.min(sizeInBytes, Integer.MAX_VALUE));
    }

    /// Returns the load response for (`identifier`, `params`) as the given `objectType`, obtaining it
    /// from `loader` when it is not already known.
    ///
    /// A present result holds a response of the requested `objectType`. An empty result means no
    /// object of that type is associated with `identifier`.
    ///
    /// `loader` is consulted only when the answer is not already known, so it may not be called at
    /// all. It must return a non-null [RawResponse]. If `loader` throws, the exception propagates to the
    /// caller and nothing is retained.
    ///
    /// @param objectType whether the caller is loading a table or a view
    /// @param identifier the table/view identifier being loaded
    /// @param params the load request's query parameters
    /// @param loader supplies the response when it is not already known; must return non-null
    /// @return a response of the requested `objectType`, or empty if `identifier` denotes the other type
    public Optional<CachedResponse> get(ObjectType objectType, TableIdentifier identifier, QueryParams params, Supplier<RawResponse> loader)
    {
        ResponsesForIdentifier responseCache = uncheckedCacheGet(cache, identifier, () -> new ResponsesForIdentifier(metastoreCacheTtl, ticker));

        if (responseCache.knownToBeDifferentObjectType(objectType)) {
            return Optional.empty();
        }

        CachedResponse cached;
        AtomicBoolean loaded = new AtomicBoolean();
        try {
            cached = responseCache.get(params, () -> {
                RawResponse response = loader.get();
                CachedResponse result = new CachedResponse(response.json(), objectType);
                if (result.getRetainedSizeInBytes() > maximumEntrySize) {
                    throw new ResponseTooLargeException(result);
                }
                loaded.set(true);
                return result;
            });
        }
        catch (ResponseTooLargeException e) {
            cached = e.response();
        }
        if (loaded.get()) {
            // Recomputes weight and refreshes write time after mutating the holder
            cache.asMap().replace(identifier, responseCache, responseCache);
        }

        if (cached.objectType() != objectType) {
            return Optional.empty();
        }
        return Optional.of(cached);
    }

    public void invalidate(TableIdentifier identifier)
    {
        cache.invalidate(identifier);
    }

    public void invalidateAll()
    {
        cache.invalidateAll();
    }

    @Managed
    @Nested
    public CacheStatsMBean getStats()
    {
        return new CacheStatsMBean(cache);
    }

    public record QueryParams(Map<String, String> queryParams)
    {
        private static final QueryParams EMPTY = new QueryParams(ImmutableMap.of());
        private static final int INSTANCE_SIZE = instanceSize(QueryParams.class);

        public static QueryParams from(@Nullable Map<String, String> queryParams)
        {
            if (queryParams == null || queryParams.isEmpty()) {
                return EMPTY;
            }
            return new QueryParams(queryParams);
        }

        public QueryParams
        {
            queryParams = ImmutableMap.copyOf(requireNonNull(queryParams, "queryParams is null"));
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(queryParams, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
        }
    }

    private static class ResponsesForIdentifier
    {
        private static final int INSTANCE_SIZE = instanceSize(ResponsesForIdentifier.class);

        private final NonEvictableCache<QueryParams, CachedResponse> cache;

        public ResponsesForIdentifier(Duration metastoreCacheTtl, Ticker ticker)
        {
            this.cache = buildNonEvictableCache(
                    CacheBuilder.newBuilder()
                            .ticker(ticker)
                            .expireAfterWrite(metastoreCacheTtl.toJavaTime()));
        }

        public CachedResponse get(QueryParams queryParams, Supplier<CachedResponse> loader)
        {
            try {
                return uncheckedCacheGet(cache, queryParams, loader);
            }
            catch (UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw e;
            }
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(cache.asMap(), QueryParams::getRetainedSizeInBytes, CachedResponse::getRetainedSizeInBytes);
        }

        public boolean knownToBeDifferentObjectType(ObjectType objectType)
        {
            ConcurrentMap<QueryParams, CachedResponse> map = cache.asMap();
            if (map.isEmpty()) {
                return false;
            }
            return map.values().stream().allMatch(response -> response.objectType() != objectType);
        }
    }

    public enum ObjectType
    {
        TABLE,
        VIEW,
    }

    public record CachedResponse(String json, ObjectType objectType)
    {
        private static final int INSTANCE_SIZE = instanceSize(CachedResponse.class);

        public CachedResponse
        {
            requireNonNull(json, "json is null");
            requireNonNull(objectType, "objectType is null");
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(json);
        }

        @Override
        public String toString()
        {
            // 'json' might contain sensitive data, so we exclude it from the toString output.
            return toStringHelper(this)
                    .add("objectType", objectType)
                    .toString();
        }
    }

    private static class ResponseTooLargeException
            extends RuntimeException
    {
        private final CachedResponse response;

        public ResponseTooLargeException(CachedResponse response)
        {
            super(null, null, false, false);
            this.response = requireNonNull(response, "response is null");
        }

        public CachedResponse response()
        {
            return response;
        }
    }
}
