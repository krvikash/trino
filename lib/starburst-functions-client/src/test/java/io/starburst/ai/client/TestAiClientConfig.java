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
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

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
                .setBatchParallelism(4)
                .setBedrockApiTimeout(new Duration(5, TimeUnit.MINUTES))
                .setBedrockSocketTimeout(new Duration(3, TimeUnit.MINUTES))
                .setBedrockMaxRetries(10)
                .setOpenAiTimeout(new Duration(3, TimeUnit.MINUTES))
                .setOpenAiMaxRetries(2)
                .setOauth2RefreshSkew(new Duration(60, TimeUnit.SECONDS))
                .setOauthTokenCacheDuration(new Duration(30, TimeUnit.MINUTES))
                .setOauth2MaxCachedTokens(1000));
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
                .put("ai.client.bedrock-api-timeout", "10m")
                .put("ai.client.bedrock-socket-timeout", "10s")
                .put("ai.client.bedrock-max-retries", "15")
                .put("ai.client.openai-timeout", "4m")
                .put("ai.client.openai-max-retries", "5")
                .put("ai.client.oauth.refresh-skew", "2m")
                .put("ai.client.oauth.token-cache-duration", "12m")
                .put("ai.client.oauth.max-cached-tokens", "500")
                .buildOrThrow();
        AiClientConfig expected = new AiClientConfig()
                .setStorageType(StorageType.FILE)
                .setClientCacheRefreshInterval(new Duration(5, TimeUnit.SECONDS))
                .setClientCacheTtl(new Duration(30, TimeUnit.MINUTES))
                .setClientCacheRefreshEnabled(true)
                .setBatchParallelism(8)
                .setBedrockApiTimeout(new Duration(10, TimeUnit.MINUTES))
                .setBedrockSocketTimeout(new Duration(10, TimeUnit.SECONDS))
                .setBedrockMaxRetries(15)
                .setOpenAiMaxRetries(5)
                .setOpenAiTimeout(new Duration(4, TimeUnit.MINUTES))
                .setOauth2RefreshSkew(new Duration(2, TimeUnit.MINUTES))
                .setOauthTokenCacheDuration(new Duration(12, TimeUnit.MINUTES))
                .setOauth2MaxCachedTokens(500);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testClientRefreshLessThanOauthTokenCacheDuration()
    {
        assertFailsValidation(new AiClientConfig()
                        .setStorageType(StorageType.FILE)
                        .setClientCacheRefreshInterval(new Duration(5, TimeUnit.SECONDS))
                        .setClientCacheTtl(new Duration(30, TimeUnit.MINUTES))
                        .setClientCacheRefreshEnabled(true)
                        .setOauth2RefreshSkew(new Duration(2, TimeUnit.SECONDS)),
                "oauth2RefreshSkewValid",
                "ai.client.cache.refresh.interval must be less than ai.client.oauth.refresh-skew",
                AssertTrue.class);

        assertFailsValidation(new AiClientConfig()
                        .setStorageType(StorageType.FILE)
                        .setClientCacheRefreshInterval(new Duration(2, TimeUnit.SECONDS))
                        .setClientCacheTtl(new Duration(30, TimeUnit.MINUTES))
                        .setClientCacheRefreshEnabled(true)
                        .setOauth2RefreshSkew(new Duration(2, TimeUnit.SECONDS)),
                "oauth2RefreshSkewValid",
                "ai.client.cache.refresh.interval must be less than ai.client.oauth.refresh-skew",
                AssertTrue.class);
    }
}
