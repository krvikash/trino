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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class AiClientConfig
{
    private StorageType storageType = StorageType.NONE;
    private boolean clientCacheRefreshEnabled;
    private Duration clientCacheRefreshInterval = new Duration(1, TimeUnit.SECONDS);
    private Duration clientCacheTtl = new Duration(1, TimeUnit.HOURS);
    private int batchParallelism = 4;
    private Duration bedrockSocketTimeout = new Duration(3, TimeUnit.MINUTES);
    private Duration bedrockApiTimeout = new Duration(5, TimeUnit.MINUTES);
    private int bedrockMaxRetries = 10;
    private int openAiMaxRetries = 2; // Default retries for OpenAI SDK
    private Duration openAiTimeout = new Duration(3, TimeUnit.MINUTES);
    private Duration vertexAiTimeout = new Duration(3, TimeUnit.MINUTES);
    private int vertexAiMaxRetries = 2;
    private Duration oauth2RefreshSkew = new Duration(60, TimeUnit.SECONDS);
    private Duration oauthTokenCacheDuration = new Duration(30, TimeUnit.MINUTES);
    private int oauth2MaxCachedTokens = 1000;

    @NotNull
    public StorageType getStorageType()
    {
        return storageType;
    }

    @Config("ai.client.models.storage")
    public AiClientConfig setStorageType(StorageType storageType)
    {
        this.storageType = storageType;
        return this;
    }

    public boolean isClientCacheRefreshEnabled()
    {
        return clientCacheRefreshEnabled;
    }

    @Config("ai.client.cache.refresh.enabled")
    public AiClientConfig setClientCacheRefreshEnabled(boolean clientCacheRefreshEnabled)
    {
        this.clientCacheRefreshEnabled = clientCacheRefreshEnabled;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    @MaxDuration("10s")
    public Duration getClientCacheRefreshInterval()
    {
        return clientCacheRefreshInterval;
    }

    @Config("ai.client.cache.refresh.interval")
    @ConfigDescription("How often to refresh the AI client cache")
    public AiClientConfig setClientCacheRefreshInterval(Duration clientCacheRefreshInterval)
    {
        this.clientCacheRefreshInterval = clientCacheRefreshInterval;
        return this;
    }

    @NotNull
    @MinDuration("10m")
    @MaxDuration("3h")
    public Duration getClientCacheTtl()
    {
        return clientCacheTtl;
    }

    @Config("ai.client.cache.ttl")
    @ConfigDescription("How long to cache AI clients for")
    public AiClientConfig setClientCacheTtl(Duration clientCacheTtl)
    {
        this.clientCacheTtl = clientCacheTtl;
        return this;
    }

    @Min(1)
    public int getBatchParallelism()
    {
        return batchParallelism;
    }

    @Config("ai.client.batch.parallelism")
    @ConfigDescription("Per split parallelism level used in batch processing")
    public AiClientConfig setBatchParallelism(int batchParallelism)
    {
        this.batchParallelism = batchParallelism;
        return this;
    }

    public Duration getBedrockSocketTimeout()
    {
        return bedrockSocketTimeout;
    }

    @Config("ai.client.bedrock-socket-timeout")
    @ConfigDescription("Http socket timeout for AI client requests")
    public AiClientConfig setBedrockSocketTimeout(Duration bedrockSocketTimeout)
    {
        this.bedrockSocketTimeout = bedrockSocketTimeout;
        return this;
    }

    @Min(0)
    public int getBedrockMaxRetries()
    {
        return bedrockMaxRetries;
    }

    @Config("ai.client.bedrock-max-retries")
    @ConfigDescription("Maximum number of retries for AI client requests")
    public AiClientConfig setBedrockMaxRetries(int bedrockMaxRetries)
    {
        this.bedrockMaxRetries = bedrockMaxRetries;
        return this;
    }

    public Duration getBedrockApiTimeout()
    {
        return bedrockApiTimeout;
    }

    @Config("ai.client.bedrock-api-timeout")
    @ConfigDescription("Max duration of a single request to AWS Bedrock (including retries)")
    public AiClientConfig setBedrockApiTimeout(Duration bedrockApiTimeout)
    {
        this.bedrockApiTimeout = bedrockApiTimeout;
        return this;
    }

    @Min(0)
    public int getOpenAiMaxRetries()
    {
        return openAiMaxRetries;
    }

    @Config("ai.client.openai-max-retries")
    @ConfigDescription("Maximum number of retries for OpenAI client requests")
    public AiClientConfig setOpenAiMaxRetries(int openAiMaxRetries)
    {
        this.openAiMaxRetries = openAiMaxRetries;
        return this;
    }

    public Duration getOpenAiTimeout()
    {
        return openAiTimeout;
    }

    @Config("ai.client.openai-timeout")
    @ConfigDescription("Max duration of a single request to OpenAI (including retries)")
    public AiClientConfig setOpenAiTimeout(Duration openAiTimeout)
    {
        this.openAiTimeout = openAiTimeout;
        return this;
    }

    public Duration getVertexAiTimeout()
    {
        return vertexAiTimeout;
    }

    @Config("ai.client.vertex-ai-timeout")
    @ConfigDescription("Max duration of a single request to Vertex AI (including retries)")
    public AiClientConfig setVertexAiTimeout(Duration vertexAiTimeout)
    {
        this.vertexAiTimeout = vertexAiTimeout;
        return this;
    }

    @Min(0)
    public int getVertexAiMaxRetries()
    {
        return vertexAiMaxRetries;
    }

    @Config("ai.client.vertex-ai-max-retries")
    @ConfigDescription("Maximum number of retries for Vertex AI client requests")
    public AiClientConfig setVertexAiMaxRetries(int vertexAiMaxRetries)
    {
        this.vertexAiMaxRetries = vertexAiMaxRetries;
        return this;
    }

    @NotNull
    @MinDuration("5s")
    @MaxDuration("10m")
    public Duration getOauth2RefreshSkew()
    {
        return oauth2RefreshSkew;
    }

    @Config("ai.client.oauth.refresh-skew")
    @ConfigDescription("How early to refresh an OAuth2 access token before its expires_in elapses")
    public AiClientConfig setOauth2RefreshSkew(Duration oauth2RefreshSkew)
    {
        this.oauth2RefreshSkew = oauth2RefreshSkew;
        return this;
    }

    @NotNull
    @MinDuration("5s")
    @MaxDuration("10h")
    public Duration getOauthTokenCacheDuration()
    {
        return oauthTokenCacheDuration;
    }

    @Config("ai.client.oauth.token-cache-duration")
    @ConfigDescription("How long to keep OAuth tokens cached in memory. If a token expires during the time it's cached, it will be evicted; when an entry is evicted, a new token will be retrieved.")
    public AiClientConfig setOauthTokenCacheDuration(Duration oauthTokenCacheDuration)
    {
        this.oauthTokenCacheDuration = oauthTokenCacheDuration;
        return this;
    }

    @Min(1)
    public int getOauth2MaxCachedTokens()
    {
        return oauth2MaxCachedTokens;
    }

    @Config("ai.client.oauth.max-cached-tokens")
    @ConfigDescription("Maximum number of OAuth2 access tokens to cache in memory")
    public AiClientConfig setOauth2MaxCachedTokens(int oauth2MaxCachedTokens)
    {
        this.oauth2MaxCachedTokens = oauth2MaxCachedTokens;
        return this;
    }

    @AssertTrue(message = "ai.client.cache.refresh.interval must be less than ai.client.oauth.refresh-skew")
    public boolean isOauth2RefreshSkewValid()
    {
        return clientCacheRefreshInterval.compareTo(oauth2RefreshSkew) < 0;
    }

    public enum StorageType
    {
        NONE,
        FILE,
        EXTERNAL,
    }
}
