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

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDiscoveryApiConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(DiscoveryApiConfig.class)
                .setDiscoveryServiceUri(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("discovery-service.uri", "http://some-discovery-host:123")
                .buildOrThrow();

        DiscoveryApiConfig expected = new DiscoveryApiConfig()
                .setDiscoveryServiceUri(URI.create("http://some-discovery-host:123"));

        assertFullMapping(properties, expected);
    }
}
