/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.HttpDataClient;
import io.starburst.stargate.buffer.data.client.RetryingDataApi;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class RealBufferingServiceApiFactory
        implements ApiFactory
{
    private final HttpClient dataHttpClient;
    private final DiscoveryApi discoveryApi;
    private final boolean dataIntegrityVerificationEnabled;
    private final ScheduledExecutorService executorService;
    private final int maxRetries;
    private final Duration backoffInitial;
    private final Duration backoffMax;
    private final double backoffFactor;
    private final double backoffJitter;

    @Inject
    public RealBufferingServiceApiFactory(
            @ForBufferDataClient HttpClient dataHttpClient,
            DiscoveryApi discoveryApi,
            BufferExchangeConfig config,
            ScheduledExecutorService executorService)
    {
        this.dataHttpClient = requireNonNull(dataHttpClient, "dataHttpClient is null");
        this.discoveryApi = requireNonNull(discoveryApi, "discoveryApi is null");
        this.dataIntegrityVerificationEnabled = config.isDataIntegrityVerificationEnabled();
        this.executorService = executorService;
        this.maxRetries = config.getDataClientMaxRetries();
        this.backoffInitial = config.getDataClientRetryBackoffInitial();
        this.backoffMax = config.getDataClientRetryBackoffMax();
        this.backoffFactor = config.getDataClientRetryBackoffFactor();
        this.backoffJitter = config.getDataClientRetryBackoffJitter();
    }

    @Override
    public DiscoveryApi createDiscoveryApi()
    {
        return discoveryApi;
    }

    @Override
    public DataApi createDataApi(BufferNodeInfo bufferNodeInfo)
    {
        HttpDataClient httpDataClient = new HttpDataClient(bufferNodeInfo.uri(), dataHttpClient, dataIntegrityVerificationEnabled);
        return new RetryingDataApi(httpDataClient, maxRetries, backoffInitial, backoffMax, backoffFactor, backoffJitter, executorService);
    }
}
