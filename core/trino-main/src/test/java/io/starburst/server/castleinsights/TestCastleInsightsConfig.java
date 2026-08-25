/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestCastleInsightsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CastleInsightsConfig.class)
                .setEnabled(false)
                .setMinQueryDuration(new Duration(0, SECONDS))
                .setThreadPoolSize("1C")
                .setMaxQueueSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("castle-insights.enabled", "true")
                .put("castle-insights.min-query-duration", "30s")
                .put("castle-insights.thread-pool-size", "2C")
                .put("castle-insights.max-queue-size", "500")
                .buildOrThrow();

        CastleInsightsConfig expected = new CastleInsightsConfig()
                .setEnabled(true)
                .setMinQueryDuration(new Duration(30, SECONDS))
                .setThreadPoolSize("2C")
                .setMaxQueueSize(500);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testPoolSizeValidation()
    {
        assertFailsValidation(
                new CastleInsightsConfig().setThreadPoolSize("0"),
                "threadPoolSize",
                "must be greater than or equal to 1",
                Min.class);

        assertFailsValidation(
                new CastleInsightsConfig().setMaxQueueSize(0),
                "maxQueueSize",
                "must be greater than or equal to 1",
                Min.class);
    }
}
