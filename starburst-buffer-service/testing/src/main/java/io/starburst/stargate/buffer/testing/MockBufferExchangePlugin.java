/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.testing;

import com.google.common.collect.ImmutableList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.trino.exchange.ApiFactory;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManagerFactory;
import io.trino.spi.Plugin;
import io.trino.spi.exchange.ExchangeManagerFactory;

import static java.util.Objects.requireNonNull;

public class MockBufferExchangePlugin
        implements Plugin
{
    private final MockBufferService bufferService;

    public MockBufferExchangePlugin(MockBufferService bufferService)
    {
        this.bufferService = requireNonNull(bufferService, "bufferService is null");
    }

    @Override
    public Iterable<ExchangeManagerFactory> getExchangeManagerFactories()
    {
        return ImmutableList.of(BufferExchangeManagerFactory.withApiFactory(new ApiFactory()
        {
            @Override
            public DiscoveryApi createDiscoveryApi()
            {
                return bufferService.getDiscoveryApi();
            }

            @Override
            public DataApi createDataApi(BufferNodeInfo nodeInfo)
            {
                return bufferService.getDataApi(nodeInfo.getNodeId());
            }
        }));
    }
}
