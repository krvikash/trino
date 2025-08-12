/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.starburst.ai.client.AiClientConfig.StorageType;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAiClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AiClientConfig.class)
                .setStorageType(StorageType.NONE)
                .setClientCacheRefreshInterval(new Duration(1, TimeUnit.SECONDS))
                .setClientCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setClientCacheRefreshEnabled(false)
                .setBatchParallelism(4));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ai.client.models.storage", "FILE")
                .put("ai.client.cache.refresh.interval", "5s")
                .put("ai.client.cache.ttl", "30m")
                .put("ai.client.cache.refresh.enabled", "true")
                .put("ai.client.batch.parallelism", "8")
                .buildOrThrow();
        AiClientConfig expected = new AiClientConfig()
                .setStorageType(StorageType.FILE)
                .setClientCacheRefreshInterval(new Duration(5, TimeUnit.SECONDS))
                .setClientCacheTtl(new Duration(30, TimeUnit.MINUTES))
                .setClientCacheRefreshEnabled(true)
                .setBatchParallelism(8);

        assertFullMapping(properties, expected);
    }
}
