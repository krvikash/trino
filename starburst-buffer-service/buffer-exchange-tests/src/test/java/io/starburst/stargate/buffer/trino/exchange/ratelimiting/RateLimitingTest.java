/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.ratelimiting;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.trino.exchange.DataApiFacadeStats;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.Duration.succinctDuration;

public class RateLimitingTest
{
    private static final Logger log = Logger.get(RateLimitingTest.class);

    @Test(enabled = false)
    public void exampleRateLimitingTest()
            throws IOException
    {
        RateLimitingTestHarness.Builder harnessBuilder = RateLimitingTestHarness.builder();
        for (int nodeId = 0; nodeId < 10; ++nodeId) {
            for (int taskId = 0; taskId < 10; ++taskId) {
                harnessBuilder.addClient("node" + nodeId, succinctDuration(0, TimeUnit.MILLISECONDS), 10000, 100.0);
            }
        }
        harnessBuilder.setMaxInProgressAddDataPagesRequests(10);
        harnessBuilder.setInProgressAddDataPagesRequestsRateLimitThreshold(5);
        harnessBuilder.setRequestProcessingTime(succinctDuration(100, TimeUnit.MILLISECONDS));

        RateLimitingTestHarness harness = harnessBuilder.build();

        try {
            ListenableFuture<Void> harnessFuture = harness.run();
            while (true) {
                logStats(harness.getClientNodesStats());
                if (harnessFuture.isDone()) {
                    break;
                }
                sleepUninterruptibly(5, TimeUnit.SECONDS);
            }
            getFutureValue(harness.run());
        }
        finally {
            harness.close();
        }
    }

    private static void logStats(Map<String, DataApiFacadeStats> stats)
    {
        for (Map.Entry<String, DataApiFacadeStats> entry : new TreeMap<>(stats).entrySet()) {
            String nodeId = entry.getKey();
            DataApiFacadeStats nodeStats = entry.getValue();

            log.info("%s -> ok: %-7d | fail: %-7d | circuit: %-7d | overloaded: %-7d",
                    nodeId,
                    nodeStats.getAddDataPagesOperationStats().getSuccessOperationCount().getTotalCount(),
                    nodeStats.getAddDataPagesOperationStats().getAnyRequestErrorCount().getTotalCount(),
                    nodeStats.getAddDataPagesOperationStats().getCircuitBreakerOpenRequestErrorCount().getTotalCount(),
                    nodeStats.getAddDataPagesOperationStats().getOverloadedRequestErrorCount().getTotalCount());
        }
        log.info("stats: %s", stats);
    }
}
