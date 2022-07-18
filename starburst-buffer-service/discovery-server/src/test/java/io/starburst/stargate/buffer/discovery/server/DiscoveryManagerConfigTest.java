/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

class DiscoveryManagerConfigTest
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DiscoveryManagerConfig.class)
                .setBufferNodeDiscoveryStalenessThreshold(succinctDuration(5, MINUTES))
                .setStartGracePeriod(succinctDuration(30, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("buffer.discovery.buffer-node-info-staleness-threshold", "6m")
                .put("buffer.discovery.start-grace-period", "42s")
                .buildOrThrow();

        DiscoveryManagerConfig expected = new DiscoveryManagerConfig()
                .setBufferNodeDiscoveryStalenessThreshold(succinctDuration(6, MINUTES))
                .setStartGracePeriod(succinctDuration(42, SECONDS));

        assertFullMapping(properties, expected);
    }
}
