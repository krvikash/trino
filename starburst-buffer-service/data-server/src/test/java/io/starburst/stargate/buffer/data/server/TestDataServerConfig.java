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

public class TestDataServerConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(DataServerConfig.class)
                .setDiscoveryServiceUri(null)
                .setIncludeChecksumInDataResponse(true)
                .setTestingDropUploadedPages(false)
                .setHttpResponseThreads(100)
                .setTestingEnableStatsLogging(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("discovery-service.uri", "http://some-discovery-host:123")
                .put("include-checksum-in-data-response", "false")
                .put("testing.drop-uploaded-pages", "true")
                .put("http-response-threads", "88")
                .put("testing.enable-stats-logging", "false")
                .buildOrThrow();

        DataServerConfig expected = new DataServerConfig()
                .setDiscoveryServiceUri(URI.create("http://some-discovery-host:123"))
                .setIncludeChecksumInDataResponse(false)
                .setTestingDropUploadedPages(true)
                .setHttpResponseThreads(88)
                .setTestingEnableStatsLogging(false);

        assertFullMapping(properties, expected);
    }
}
