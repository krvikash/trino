/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server.failures;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;

class TestFailuresTrackingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FailuresTrackingConfig.class)
                .setFailuresCounterDecayDuration(succinctDuration(5, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("buffer.failures-tracking.failures-counter-decay-duration", "6m")
                .buildOrThrow();

        FailuresTrackingConfig expected = new FailuresTrackingConfig()
                .setFailuresCounterDecayDuration(succinctDuration(6, MINUTES));

        assertFullMapping(properties, expected);
    }
}
