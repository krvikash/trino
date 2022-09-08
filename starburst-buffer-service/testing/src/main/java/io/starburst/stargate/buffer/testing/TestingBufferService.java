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
import com.google.common.io.Closer;
import io.starburst.stargate.buffer.data.server.testing.TestingDataServer;
import io.starburst.stargate.buffer.discovery.server.testing.TestingDiscoveryServer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class TestingBufferService
        implements Closeable
{
    private final TestingDiscoveryServer discoveryServer;
    private final List<TestingDataServer> dataServers;

    private TestingBufferService(
            TestingDiscoveryServer.Builder discoveryServerBuilder,
            TestingDataServer.Builder dataServerBuilder,
            int dataServersCount)
    {
        discoveryServer = discoveryServerBuilder.build();

        // connect to discovery server
        dataServerBuilder.setDiscoveryBroadcastEnabled(true);
        dataServerBuilder.setConfigProperty("discovery-service.uri", discoveryServer.getBaseUri().toString());

        ImmutableList.Builder<TestingDataServer> dataServers = ImmutableList.builder();
        for (int i = 0; i < dataServersCount; ++i) {
            dataServers.add(dataServerBuilder.build());
        }
        this.dataServers = dataServers.build();
    }

    public TestingDiscoveryServer getDiscoveryServer()
    {
        return discoveryServer;
    }

    public List<TestingDataServer> getDataServers()
    {
        return dataServers;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        TestingDataServer.Builder dataServerBuilder = TestingDataServer.builder();
        TestingDiscoveryServer.Builder discoveryServerBuilder = TestingDiscoveryServer.builder();
        private int dataServersCount = 3;

        public Builder withDataServerBuilder(Consumer<TestingDataServer.Builder> dataServerBuilderConsumer)
        {
            dataServerBuilderConsumer.accept(dataServerBuilder);
            return this;
        }

        public Builder withDiscoveryServerBuilder(Consumer<TestingDiscoveryServer.Builder> discoverServerBuilderConsumer)
        {
            discoverServerBuilderConsumer.accept(discoveryServerBuilder);
            return this;
        }

        public Builder setDataServersCount(int dataServersCount)
        {
            this.dataServersCount = dataServersCount;
            return this;
        }

        public TestingBufferService build()
        {
            return new TestingBufferService(
                    discoveryServerBuilder,
                    dataServerBuilder,
                    dataServersCount);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        Closer closer = Closer.create();
        for (TestingDataServer dataServer : dataServers) {
            closer.register(dataServer);
        }
        closer.register(discoveryServer);
        closer.close();
    }
}
