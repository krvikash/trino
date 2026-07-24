/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestProfilerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ProfilerConfig.class)
                .setProfilerEnabled(false)
                .setMinQueryDuration(new Duration(1, SECONDS))
                .setThreadPoolSize(2)
                .setMaxQueueSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("profiler.enabled", "true")
                .put("profiler.min-query-duration", "30s")
                .put("profiler.thread-pool-size", "4")
                .put("profiler.max-queue-size", "500")
                .buildOrThrow();

        ProfilerConfig expected = new ProfilerConfig()
                .setProfilerEnabled(true)
                .setMinQueryDuration(new Duration(30, SECONDS))
                .setThreadPoolSize(4)
                .setMaxQueueSize(500);

        assertFullMapping(properties, expected);
    }
}
