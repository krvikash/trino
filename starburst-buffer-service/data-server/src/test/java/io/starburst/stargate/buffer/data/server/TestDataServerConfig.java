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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDataServerConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(DataServerConfig.class)
                .setDataIntegrityVerificationEnabled(true)
                .setTestingDropUploadedPages(false)
                .setHttpResponseThreads(100)
                .setTestingEnableStatsLogging(true)
                .setBroadcastInterval(succinctDuration(5, SECONDS))
                .setMinDrainingDuration(succinctDuration(30, SECONDS))
                .setDrainingMaxAttempts(4)
                .setMaxInProgressAddDataPagesRequests(500)
                .setChunkListTargetSize(1)
                .setChunkListMaxSize(100)
                .setChunkListPollTimeout(succinctDuration(100, MILLISECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("data-integrity-verification-enabled", "false")
                .put("testing.drop-uploaded-pages", "true")
                .put("http-response-threads", "88")
                .put("testing.enable-stats-logging", "false")
                .put("discovery-broadcast-interval", "102ms")
                .put("draining.min-duration", "103s")
                .put("draining.max-attempts", "5")
                .put("max-in-progress-add-data-pages-requests", "600")
                .put("chunk-list.target-size", "42")
                .put("chunk-list.max-size", "1000")
                .put("chunk-list.poll-timeout", "12345ms")
                .buildOrThrow();

        DataServerConfig expected = new DataServerConfig()
                .setDataIntegrityVerificationEnabled(false)
                .setTestingDropUploadedPages(true)
                .setHttpResponseThreads(88)
                .setTestingEnableStatsLogging(false)
                .setBroadcastInterval(succinctDuration(102, MILLISECONDS))
                .setMinDrainingDuration(succinctDuration(103, SECONDS))
                .setDrainingMaxAttempts(5)
                .setMaxInProgressAddDataPagesRequests(600)
                .setChunkListTargetSize(42)
                .setChunkListMaxSize(1000)
                .setChunkListPollTimeout(succinctDuration(12345, MILLISECONDS));

        assertFullMapping(properties, expected);
    }
}
