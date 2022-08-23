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
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.HttpDataClient;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class RealBufferingServiceApiFactory
        implements ApiFactory
{
    private final HttpClient dataHttpClient;
    private final DiscoveryApi discoveryApi;
    private final boolean dataIntegrityVerificationEnabled;

    @Inject
    public RealBufferingServiceApiFactory(
            @ForBufferDataClient HttpClient dataHttpClient,
            DiscoveryApi discoveryApi,
            BufferExchangeConfig config)
    {
        this.dataHttpClient = requireNonNull(dataHttpClient, "dataHttpClient is null");
        this.discoveryApi = requireNonNull(discoveryApi, "discoveryApi is null");
        this.dataIntegrityVerificationEnabled = config.isDataIntegrityVerificationEnabled();
    }

    @Override
    public DiscoveryApi createDiscoveryApi()
    {
        return discoveryApi;
    }

    @Override
    public DataApi createDataApi(BufferNodeInfo bufferNodeInfo)
    {
        return new HttpDataClient(bufferNodeInfo.getUri(), dataHttpClient, dataIntegrityVerificationEnabled);
    }
}
