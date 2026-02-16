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

public class TestHttpServerFeaturesConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(HttpServerFeaturesConfig.class)
                .setVirtualThreadsEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("virtual-threads.enabled", "true")
                .buildOrThrow();

        HttpServerFeaturesConfig expected = new HttpServerFeaturesConfig()
                .setVirtualThreadsEnabled(true);

        assertFullMapping(properties, expected);
    }
}
