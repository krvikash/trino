/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import com.azure.storage.common.policy.RetryPolicyType;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class AzureBlobClientConfig
{
    private String connectionString;
    // Retry-related:
    private RetryPolicyType retryPolicyType;
    private Integer maxTries;
    private Duration tryTimeout = new Duration(10, SECONDS);
    private Duration retryDelay;
    private Duration maxRetryDelay;

    @Config("spooling.azure.connection-string")
    public AzureBlobClientConfig setConnectionString(String connectionString)
    {
        this.connectionString = connectionString;
        return this;
    }

    @NotNull
    public String getConnectionString()
    {
        return connectionString;
    }

    @Config("spooling.azure.retry-policy")
    @ConfigDescription("The retry pattern to use. EXPONENTIAL (default), or FIXED")
    public AzureBlobClientConfig setRetryPolicyType(RetryPolicyType retryPolicyType)
    {
        this.retryPolicyType = retryPolicyType;
        return this;
    }

    public RetryPolicyType getRetryPolicyType()
    {
        return retryPolicyType;
    }

    @Config("spooling.azure.max-tries")
    @ConfigDescription("Maximum number of spooling attempts per chunk. Default is 4")
    public AzureBlobClientConfig setMaxTries(Integer maxTries)
    {
        this.maxTries = maxTries;
        return this;
    }

    public Integer getMaxTries()
    {
        return maxTries;
    }

    @Config("spooling.azure.try-timeout")
    @ConfigDescription("Maximum time allowed before spooling is canceled. Default is 10 seconds.")
    public AzureBlobClientConfig setTryTimeout(Duration tryTimeout)
    {
        this.tryTimeout = tryTimeout;
        return this;
    }

    public Duration getTryTimeout()
    {
        return tryTimeout;
    }

    @Config("spooling.azure.retry-delay")
    @ConfigDescription("The amount of delay before retrying spooling. Default is 4ms for EXPONENTIAL and 30ms for FIXED retry-policy.")
    public AzureBlobClientConfig setRetryDelay(Duration retryDelay)
    {
        this.retryDelay = retryDelay;
        return this;
    }

    public Duration getRetryDelay()
    {
        return retryDelay;
    }

    @Config("spooling.azure.max-retry-delay")
    @ConfigDescription("The maximum delay allowed before retrying spooling. Default is 120ms.")
    public AzureBlobClientConfig setMaxRetryDelay(Duration maxRetryDelay)
    {
        this.maxRetryDelay = maxRetryDelay;
        return this;
    }

    public Duration getMaxRetryDelay()
    {
        return maxRetryDelay;
    }
}
