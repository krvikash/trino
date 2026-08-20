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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.iceberg.catalog.rest.RestResponseCache.CachedResponse;
import io.trino.plugin.iceberg.catalog.rest.RestResponseCache.ObjectType;
import io.trino.plugin.iceberg.catalog.rest.RestResponseCache.QueryParams;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static org.assertj.core.api.Assertions.assertThat;

final class TestRestResponseCache
{
    private static final TableIdentifier TABLE_A = TableIdentifier.of(Namespace.of("ns"), "table_a");
    private static final TableIdentifier TABLE_B = TableIdentifier.of(Namespace.of("ns"), "table_b");
    private static final QueryParams PARAMS = QueryParams.from(null);

    @Test
    void testWeightBasedEviction()
    {
        String json = "x".repeat(4096);
        RestResponseCache cache = createCacheWithCapacityFor(ObjectType.TABLE, TABLE_A, PARAMS, json);

        cache.get(ObjectType.TABLE, TABLE_B, PARAMS, () -> new RawResponse(json));

        AtomicInteger reloadCount = new AtomicInteger();
        cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> {
            reloadCount.incrementAndGet();
            return new RawResponse("{\"reloaded\":true}");
        });
        assertThat(reloadCount.get())
                .as("TABLE_A should have been evicted when TABLE_B was loaded")
                .isEqualTo(1);
    }

    @Test
    void testWeightIsRecomputedAfterInnerCacheGrows()
    {
        String smallJson = "{}";
        RestResponseCache cache = createCacheWithCapacityFor(ObjectType.TABLE, TABLE_A, PARAMS, smallJson);

        String largeJson = "x".repeat(10_000);
        QueryParams snapshotParams = QueryParams.from(ImmutableMap.of("snapshots", "all"));
        cache.get(ObjectType.TABLE, TABLE_A, snapshotParams, () -> new RawResponse(largeJson));

        AtomicInteger reloadCount = new AtomicInteger();
        cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> {
            reloadCount.incrementAndGet();
            return new RawResponse("{\"reloaded\":true}");
        });
        assertThat(reloadCount.get())
                .as("TABLE_A should have been evicted after its inner cache grew beyond maximum-size")
                .isEqualTo(1);
    }

    private static RestResponseCache createCacheWithCapacityFor(ObjectType objectType, TableIdentifier identifier, QueryParams params, String json)
    {
        long jsonSize = estimatedSizeOf(json);
        // Concurrency level 1 avoids splitting the weight budget across segments.
        // The 256-byte padding accounts for the entry key and internal cache overhead.
        RestResponseCache cache = new RestResponseCache(new Duration(10, TimeUnit.MINUTES), DataSize.ofBytes(jsonSize + 256), DataSize.of(10, DataSize.Unit.MEGABYTE), Ticker.systemTicker(), Optional.of(1));
        cache.get(objectType, identifier, params, () -> new RawResponse(json));
        Optional<CachedResponse> cached = cache.get(objectType, identifier, params, () -> {
            throw new AssertionError("loader should not be called for a cached entry");
        });
        assertThat(cached).isPresent();
        assertThat(cached.get().json()).isEqualTo(json);
        return cache;
    }

    @Test
    void testTtlExpiry()
    {
        TestingTicker ticker = new TestingTicker();
        Duration ttl = new Duration(5, TimeUnit.MINUTES);
        RestResponseCache cache = new RestResponseCache(ttl, DataSize.of(10, DataSize.Unit.MEGABYTE), DataSize.of(10, DataSize.Unit.MEGABYTE), ticker, Optional.empty());

        cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> new RawResponse("{\"v\":1}"));

        Optional<CachedResponse> cached = cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> {
            throw new AssertionError("should be cached");
        });
        assertThat(cached).isPresent();
        assertThat(cached.get().json()).isEqualTo("{\"v\":1}");

        ticker.increment(ttl.toMillis() + 1, TimeUnit.MILLISECONDS);

        AtomicInteger loadCount = new AtomicInteger();
        Optional<CachedResponse> afterExpiry = cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> {
            loadCount.incrementAndGet();
            return new RawResponse("{\"v\":2}");
        });
        assertThat(loadCount.get()).isEqualTo(1);
        assertThat(afterExpiry).isPresent();
        assertThat(afterExpiry.get().json()).isEqualTo("{\"v\":2}");
    }

    @Test
    void testInnerCacheTtlExpiry()
    {
        TestingTicker ticker = new TestingTicker();
        Duration ttl = new Duration(5, TimeUnit.MINUTES);
        RestResponseCache cache = new RestResponseCache(ttl, DataSize.of(10, DataSize.Unit.MEGABYTE), DataSize.of(10, DataSize.Unit.MEGABYTE), ticker, Optional.empty());

        cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> new RawResponse("{\"v\":1}"));

        QueryParams snapshotParams = QueryParams.from(ImmutableMap.of("snapshots", "all"));
        ticker.increment(ttl.toMillis() / 2, TimeUnit.MILLISECONDS);
        cache.get(ObjectType.TABLE, TABLE_A, snapshotParams, () -> new RawResponse("{\"v\":2}"));

        ticker.increment(ttl.toMillis() / 2 + 1, TimeUnit.MILLISECONDS);

        AtomicInteger reloadCount = new AtomicInteger();
        Optional<CachedResponse> defaultParamsResult = cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> {
            reloadCount.incrementAndGet();
            return new RawResponse("{\"v\":3}");
        });
        assertThat(reloadCount.get())
                .as("default params entry should have expired")
                .isEqualTo(1);
        assertThat(defaultParamsResult).isPresent();
        assertThat(defaultParamsResult.get().json()).isEqualTo("{\"v\":3}");

        Optional<CachedResponse> snapshotParamsResult = cache.get(ObjectType.TABLE, TABLE_A, snapshotParams, () -> {
            throw new AssertionError("snapshot params should still be cached");
        });
        assertThat(snapshotParamsResult).isPresent();
        assertThat(snapshotParamsResult.get().json()).isEqualTo("{\"v\":2}");
    }

    @Test
    void testOversizedEntryIsNotCached()
    {
        String largeJson = "x".repeat(10_000);
        RestResponseCache cache = new RestResponseCache(
                new Duration(10, TimeUnit.MINUTES),
                DataSize.of(10, DataSize.Unit.MEGABYTE),
                DataSize.ofBytes(100),
                Ticker.systemTicker(),
                Optional.empty());

        Optional<CachedResponse> result = cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> new RawResponse(largeJson));
        assertThat(result).isPresent();
        assertThat(result.get().json()).isEqualTo(largeJson);

        AtomicInteger reloadCount = new AtomicInteger();
        cache.get(ObjectType.TABLE, TABLE_A, PARAMS, () -> {
            reloadCount.incrementAndGet();
            return new RawResponse("{\"reloaded\":true}");
        });
        assertThat(reloadCount.get())
                .as("oversized entry should not have been cached")
                .isEqualTo(1);
    }
}
