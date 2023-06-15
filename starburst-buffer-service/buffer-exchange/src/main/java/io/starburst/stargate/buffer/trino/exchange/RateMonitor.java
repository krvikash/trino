/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.data.client.RateLimitInfo;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class RateMonitor
{
    private static final Logger log = Logger.get(RateMonitor.class);

    private static final long MAX_INTERVAL_IN_MILLIS = 5_000;

    private final Ticker ticker;

    private final Map<Long, Optional<RateLimitInfo>> rateLimitInfos = new ConcurrentHashMap<>();
    private final Map<Long, Deque<Long>> executionSchedules = new ConcurrentHashMap<>();

    public RateMonitor(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    /**
     * @param bufferNodeId target buffer node that we want to register to send an addDataPages request to
     * @return delay in milliseconds before we actually do the request
     */
    public long registerExecutionSchedule(long bufferNodeId)
    {
        Optional<RateLimitInfo> rateLimitInfo = rateLimitInfos.getOrDefault(bufferNodeId, Optional.empty());
        if (rateLimitInfo.isEmpty()) {
            return 0;
        }

        long expectedTotalTimeInMillis = (long) (1000 / rateLimitInfo.get().rateLimit()); // rate limit is per second
        long averageProcessTimeInMillis = rateLimitInfo.get().averageProcessTimeInMillis();
        long intervalInMillis = min(max(expectedTotalTimeInMillis - averageProcessTimeInMillis, 0), MAX_INTERVAL_IN_MILLIS); // just in case delay goes absurd
        if (intervalInMillis == MAX_INTERVAL_IN_MILLIS) {
            log.error("Rate limit interval %d larger than MAX_INTERVAL_IN_MILLIS %d", expectedTotalTimeInMillis - averageProcessTimeInMillis, MAX_INTERVAL_IN_MILLIS);
        }
        Deque<Long> executionSchedule = executionSchedules.computeIfAbsent(bufferNodeId, ignored -> new ArrayDeque<>());
        long now = tickerReadMillis();

        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (executionSchedule) {
            // remove execution schedules that have already finished
            while (!executionSchedule.isEmpty() && executionSchedule.peekFirst() <= now - averageProcessTimeInMillis) {
                executionSchedule.pollFirst();
            }

            if (executionSchedule.isEmpty()) {
                executionSchedule.add(now + intervalInMillis);
                return intervalInMillis;
            }

            long expectedExecutionTimestamp = (executionSchedule.peekLast() + averageProcessTimeInMillis) + intervalInMillis;
            executionSchedule.add(expectedExecutionTimestamp);
            return expectedExecutionTimestamp - now;
        }
    }

    /**
     * @param bufferNodeId target buffer node that we want to limit request rate on
     * @param rateLimitInfo Rate limit information around the buffer node
     */
    public void updateRateLimitInfo(long bufferNodeId, Optional<RateLimitInfo> rateLimitInfo)
    {
        rateLimitInfos.put(bufferNodeId, rateLimitInfo);
    }

    /**
     * Used to clean up stale information around rate limiting
     *
     * @param isBufferNodeStale predicate that determines whether a given buffer node is stale
     */
    public void cleanUp(Predicate<Long> isBufferNodeStale)
    {
        Set<Long> staleRateLimitInfos = rateLimitInfos.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.info("cleaning up stale rate limit infos for buffer nodes %s", staleRateLimitInfos);
        staleRateLimitInfos.forEach(rateLimitInfos::remove);

        Set<Long> staleExecutionSchedules = executionSchedules.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.info("cleaning up stale execution schedules for buffer nodes %s", staleExecutionSchedules);
        staleExecutionSchedules.forEach(executionSchedules::remove);
    }

    // for test only, so doesn't need to be synchronized
    @VisibleForTesting
    int getExecutionScheduleSize(long bufferNodeId)
    {
        return executionSchedules.getOrDefault(bufferNodeId, new ArrayDeque<>()).size();
    }

    private long tickerReadMillis()
    {
        return ticker.read() / 1_000_000;
    }
}
