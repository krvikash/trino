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

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.time.Duration;

public class BlobServiceAsyncClientProvider
        implements Provider<BlobServiceAsyncClient>
{
    private final String connectionString;
    private final RetryPolicyType retryPolicyType;
    private final Integer maxTries;
    private final Duration tryTimeout;
    private final Duration retryDelay;
    private final Duration maxRetryDelay;

    @Inject
    public BlobServiceAsyncClientProvider(AzureBlobClientConfig azureBlobClientConfig)
    {
        this.connectionString = azureBlobClientConfig.getConnectionString();
        this.retryPolicyType = azureBlobClientConfig.getRetryPolicyType();
        this.maxTries = azureBlobClientConfig.getMaxTries();
        this.tryTimeout = convertDuration(azureBlobClientConfig.getTryTimeout());
        this.retryDelay = convertDuration(azureBlobClientConfig.getRetryDelay());
        this.maxRetryDelay = convertDuration(azureBlobClientConfig.getMaxRetryDelay());
    }

    private Duration convertDuration(io.airlift.units.Duration duration)
    {
        return duration == null ? null : Duration.ofMillis(duration.toMillis());
    }

    @Override
    public BlobServiceAsyncClient get()
    {
        return new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .retryOptions(new RequestRetryOptions(
                        retryPolicyType,
                        maxTries,
                        tryTimeout,
                        retryDelay,
                        maxRetryDelay,
                        // we do not support defining the secondary host
                        null))
                .buildAsyncClient();
    }
}
