/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.testing;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MockDataNodeStats
{
    public enum Key {
        SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT,
        REJECTED_DRAINING_ADD_DATA_PAGES_REQUEST_COUNT,
        REJECTED_EXCHANGE_FINISHED_ADD_DATA_PAGES_REQUEST_COUNT,
        SUCCESSFUL_LOCAL_GET_CHUNK_DATA_REQUEST_COUNT,
        SUCCESSFUL_GET_CHUNK_DATA_FROM_DRAINED_STORAGE_REQUEST_COUNT,
        FAILED_GET_CHUNK_DATA_CHUNK_DRAINED_REQUEST_COUNT,
        FAILED_GET_CHUNK_DATA_NOT_FOUND_IN_DRAINED_STORAGE_REQUEST_COUNT,
    }

    private final Map<Key, Long> stats;

    private MockDataNodeStats(Map<Key, Long> stats)
    {
        this.stats = ImmutableMap.copyOf(stats);
    }

    public long get(Key key)
    {
        return stats.getOrDefault(key, 0L);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ConcurrentMap<Key, AtomicLong> stats = new ConcurrentHashMap<>();

        public void increment(Key key)
        {
            stats.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
        }

        public MockDataNodeStats build()
        {
            ImmutableMap.Builder<Key, Long> finalStats = ImmutableMap.builder();

            for (Map.Entry<Key, AtomicLong> entry : stats.entrySet()) {
                finalStats.put(entry.getKey(), entry.getValue().get());
            }
            return new MockDataNodeStats(finalStats.build());
        }
    }
}
