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

import com.google.inject.Inject;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiFactory;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class RealBufferingServiceApiFactory
        implements ApiFactory
{
    private final DiscoveryApi discoveryApi;
    private final DataApiFactory dataApiFactory;
    private final boolean useVirtualThreadsUri;

    @Inject
    public RealBufferingServiceApiFactory(
            DiscoveryApi discoveryApi,
            DataApiFactory dataApiFactory,
            BufferExchangeConfig config)
    {
        this.discoveryApi = requireNonNull(discoveryApi, "discoveryApi is null");
        this.dataApiFactory = requireNonNull(dataApiFactory, "dataApiFactory is null");
        this.useVirtualThreadsUri = requireNonNull(config, "config is null").isUseVirtualThreadsUri();
    }

    @Override
    public DiscoveryApi createDiscoveryApi()
    {
        return discoveryApi;
    }

    @Override
    public DataApi createDataApi(BufferNodeInfo bufferNodeInfo)
    {
        URI dataServerUri = useVirtualThreadsUri
                ? bufferNodeInfo.virtualThreadsUri().orElse(bufferNodeInfo.uri())
                : bufferNodeInfo.uri();
        return dataApiFactory.createDataApi(dataServerUri, bufferNodeInfo.nodeId());
    }
}
