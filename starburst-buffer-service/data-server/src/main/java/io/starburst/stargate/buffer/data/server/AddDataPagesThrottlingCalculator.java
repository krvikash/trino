/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.base.Ticker;
import io.airlift.log.Logger;
import io.airlift.stats.DecayCounter;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AddDataPagesThrottlingCalculator
{
    private static final Logger log = Logger.get(AddDataPagesThrottlingCalculator.class);

    private static final double DECAY_COUNT_RATE_LIMIT_THRESHOLD = 0.5;
    private static final double RATE_LIMIT_LOWER_BOUND = 1.0;
    private static final int PROCESS_TIME_MOVING_AVERAGE_CALCULATION_WINDOW = 50;
    private static final Duration REMOTE_HOST_STALENESS_THRESHOLD = succinctDuration(2, MINUTES);
    private static final Duration REMOTE_HOST_CLEANUP_INTERVAL = succinctDuration(30, SECONDS);

    private final DecayCounter decayCounter;
    private final int maxInProgressAddDataPagesRequests;
    private final int inProgressAddDataPagesRequestsRateLimitThreshold;

    private final Ticker ticker = Ticker.systemTicker();
    private final Map<String, CounterWithRate> counters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor();
    @GuardedBy("this")
    private final Queue<Long> recentProcessTimeQueue;
    @GuardedBy("this")
    private long movingProcessTimeSumInMillis;

    @Inject
    public AddDataPagesThrottlingCalculator(DataServerConfig config)
    {
        this.decayCounter = new DecayCounter(1 / config.getInProgressAddDataPagesRequestsThrottlingCounterDecayDuration().getValue(SECONDS));
        this.maxInProgressAddDataPagesRequests = config.getMaxInProgressAddDataPagesRequests();
        this.inProgressAddDataPagesRequestsRateLimitThreshold = config.getInProgressAddDataPagesRequestsRateLimitThreshold();
        this.recentProcessTimeQueue = new ArrayDeque<>(PROCESS_TIME_MOVING_AVERAGE_CALCULATION_WINDOW);
        // fill queue with 0s to begin
        for (int i = 0; i < PROCESS_TIME_MOVING_AVERAGE_CALCULATION_WINDOW; ++i) {
            recentProcessTimeQueue.add(0L);
        }
    }

    @PostConstruct
    public void start()
    {
        cleanupExecutor.scheduleWithFixedDelay(() -> {
            try {
                long now = ticker.read() / 1_000_000;
                long cleanupThreshold = now - REMOTE_HOST_STALENESS_THRESHOLD.toMillis();
                Iterator<Map.Entry<String, CounterWithRate>> iterator = counters.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, CounterWithRate> entry = iterator.next();
                    String remoteHost = entry.getKey();
                    long lastUpdatedTime = entry.getValue().getLastUpdateTimeMillis();
                    if (lastUpdatedTime < cleanupThreshold) {
                        log.info("Forgetting remote host %s, inactive for %s", remoteHost, succinctDuration(now - lastUpdatedTime, MILLISECONDS));
                        iterator.remove();
                    }
                }
            }
            catch (Throwable e) {
                log.error(e, "Error during cleaning up stale remote hosts in throttling calculator");
            }
        }, REMOTE_HOST_STALENESS_THRESHOLD.toMillis(), REMOTE_HOST_CLEANUP_INTERVAL.toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        cleanupExecutor.shutdown();
    }

    public void recordThrottlingEvent()
    {
        decayCounter.add(1);
    }

    public synchronized void recordProcessTimeInMillis(long processTimeInMillis)
    {
        checkState(recentProcessTimeQueue.size() == PROCESS_TIME_MOVING_AVERAGE_CALCULATION_WINDOW);

        long head = recentProcessTimeQueue.poll();
        recentProcessTimeQueue.add(processTimeInMillis);
        movingProcessTimeSumInMillis -= head;
        movingProcessTimeSumInMillis += processTimeInMillis;
    }

    public void updateCounterStat(String remoteHost, long count)
    {
        getCounterWithRate(remoteHost).add(count);
    }

    public OptionalDouble getRateLimit(String remoteHost, int inProgressAddDataPagesRequests)
    {
        // always call getRate first so timestamp and rate are up-to-date
        double rate = getCounterWithRate(remoteHost).getRate();
        if (rate == 0) {
            // this is possible as the buffer node may be stressed by some other workers
            return OptionalDouble.empty();
        }

        if (inProgressAddDataPagesRequests < inProgressAddDataPagesRequestsRateLimitThreshold && decayCounter.getCount() < DECAY_COUNT_RATE_LIMIT_THRESHOLD) {
            // if we are not overloaded, then no need to rate limit
            return OptionalDouble.empty();
        }

        if (inProgressAddDataPagesRequests > inProgressAddDataPagesRequestsRateLimitThreshold) {
            rate = rate - rate * (inProgressAddDataPagesRequests - inProgressAddDataPagesRequestsRateLimitThreshold) / maxInProgressAddDataPagesRequests;
        }

        // if we are far away from the hard throttling limit, we can relax the rate limiting for non-chatty clients,
        // otherwise lean towards stricter rate limit control
        double minRateBasedOnHeuristics = (maxInProgressAddDataPagesRequests - inProgressAddDataPagesRequests) / (counters.size() + 0.0001);

        return OptionalDouble.of(Math.max(rate, Math.max(RATE_LIMIT_LOWER_BOUND, minRateBasedOnHeuristics)));
    }

    public synchronized long getAverageProcessTimeInMillis()
    {
        return movingProcessTimeSumInMillis / PROCESS_TIME_MOVING_AVERAGE_CALCULATION_WINDOW;
    }

    private CounterWithRate getCounterWithRate(String remoteHost)
    {
        return counters.computeIfAbsent(remoteHost, ignored -> new CounterWithRate(ticker));
    }
}
