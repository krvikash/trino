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
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDataServerConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(DataServerConfig.class)
                .setIncludeChecksumInDataResponse(true)
                .setTestingDropUploadedPages(false)
                .setHttpResponseThreads(100)
                .setTestingEnableStatsLogging(true)
                .setBroadcastInterval(Duration.succinctDuration(5, TimeUnit.SECONDS))
                .setTestingDrainDurationLimit(Duration.succinctDuration(30, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("include-checksum-in-data-response", "false")
                .put("testing.drop-uploaded-pages", "true")
                .put("http-response-threads", "88")
                .put("testing.enable-stats-logging", "false")
                .put("discovery-broadcast-interval", "102ms")
                .put("testing.drain-duration", "11s")
                .buildOrThrow();

        DataServerConfig expected = new DataServerConfig()
                .setIncludeChecksumInDataResponse(false)
                .setTestingDropUploadedPages(true)
                .setHttpResponseThreads(88)
                .setTestingEnableStatsLogging(false)
                .setBroadcastInterval(Duration.succinctDuration(102, TimeUnit.MILLISECONDS))
                .setTestingDrainDurationLimit(Duration.succinctDuration(11, TimeUnit.SECONDS));

        assertFullMapping(properties, expected);
    }
}
