/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.ratelimiting;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.HttpDataClient;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.trino.exchange.ApiFactory;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeConfig;
import io.starburst.stargate.buffer.trino.exchange.BufferNodeDiscoveryManager;
import io.starburst.stargate.buffer.trino.exchange.DataApiFacade;
import io.starburst.stargate.buffer.trino.exchange.DataApiFacadeStats;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.Duration.succinctDuration;
import static java.lang.Math.toIntExact;
import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RateLimitingTestHarness
        implements Cloneable
{
    private static final long NANOSECONDS_IN_SECOND = 1_000_000_000;
    private static final long NANOSECONDS_IN_MILLISECOND = 1_000_000;
    private static final long BUFFER_NODE_ID = 1;

    private static final Logger log = Logger.get(RateLimitingTestHarness.class);

    private final int maxInProgressAddDataPagesRequests;
    private final int inProgressAddDataPagesRequestsRateLimitThreshold;
    private final io.airlift.units.Duration inProgressAddDataPagesRequestsThrottlingCounterDecayDuration;
    private final io.airlift.units.Duration requestProcessingTime;
    private final Set<TestClientNode> clientNodes;
    private final ListeningExecutorService executorService;
    private final ScheduledExecutorService retryExecutor;

    private RateLimitingTestServer server;

    RateLimitingTestHarness(
            int maxInProgressAddDataPagesRequests,
            int inProgressAddDataPagesRequestsRateLimitThreshold,
            io.airlift.units.Duration inProgressAddDataPagesRequestsThrottlingCounterDecayDuration,
            io.airlift.units.Duration requestProcessingTime,
            Set<TestClientNode> clientNodes)
    {
        this.maxInProgressAddDataPagesRequests = maxInProgressAddDataPagesRequests;
        this.inProgressAddDataPagesRequestsRateLimitThreshold = inProgressAddDataPagesRequestsRateLimitThreshold;
        this.inProgressAddDataPagesRequestsThrottlingCounterDecayDuration = inProgressAddDataPagesRequestsThrottlingCounterDecayDuration;
        this.requestProcessingTime = requestProcessingTime;
        this.clientNodes = ImmutableSet.copyOf(clientNodes);
        this.executorService = listeningDecorator(newCachedThreadPool());
        this.retryExecutor = newScheduledThreadPool(8);
    }

    public ListenableFuture<Void> run()
    {
        server = new RateLimitingTestServer(
                maxInProgressAddDataPagesRequests,
                inProgressAddDataPagesRequestsRateLimitThreshold,
                inProgressAddDataPagesRequestsThrottlingCounterDecayDuration,
                requestProcessingTime);
        List<ListenableFuture<Void>> futures = clientNodes.stream().map(node -> node.run(executorService, server, retryExecutor)).collect(toImmutableList());
        return asVoid(allAsList(futures));
    }

    public Map<String, DataApiFacadeStats> getClientNodesStats()
    {
        return clientNodes.stream().collect(toImmutableMap(
                TestClientNode::getNodeId,
                TestClientNode::getDataApiFacadeStats));
    }

    public void close()
            throws IOException
    {
        retryExecutor.shutdown();
        executorService.shutdownNow();
        if (server != null) {
            server.close();
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int maxInProgressAddDataPagesRequests = 100;
        private int inProgressAddDataPagesRequestsRateLimitThreshold = 50;
        private io.airlift.units.Duration inProgressAddDataPagesRequestsThrottlingCounterDecayDuration = succinctDuration(5, SECONDS);
        private io.airlift.units.Duration requestProcessingTime = succinctDuration(10, MILLISECONDS);

        private final Multimap<String, TestClient> clients = HashMultimap.create();

        public Builder setMaxInProgressAddDataPagesRequests(int maxInProgressAddDataPagesRequests)
        {
            this.maxInProgressAddDataPagesRequests = maxInProgressAddDataPagesRequests;
            return this;
        }

        public Builder setInProgressAddDataPagesRequestsRateLimitThreshold(int inProgressAddDataPagesRequestsRateLimitThreshold)
        {
            this.inProgressAddDataPagesRequestsRateLimitThreshold = inProgressAddDataPagesRequestsRateLimitThreshold;
            return this;
        }

        public Builder setInProgressAddDataPagesRequestsThrottlingCounterDecayDuration(io.airlift.units.Duration inProgressAddDataPagesRequestsThrottlingCounterDecayDuration)
        {
            this.inProgressAddDataPagesRequestsThrottlingCounterDecayDuration = inProgressAddDataPagesRequestsThrottlingCounterDecayDuration;
            return this;
        }

        public Builder setRequestProcessingTime(io.airlift.units.Duration requestProcessingTime)
        {
            this.requestProcessingTime = requestProcessingTime;
            return this;
        }

        public Builder addClient(String nodeId, Duration start, long count, double rate)
        {
            clients.put(nodeId, new TestClient(start, count, rate));
            return this;
        }

        public RateLimitingTestHarness build()
        {
            ImmutableSet.Builder<TestClientNode> nodes = ImmutableSet.builder();

            for (Map.Entry<String, Collection<TestClient>> entry : clients.asMap().entrySet()) {
                String nodeId = entry.getKey();
                Set<TestClient> clients = ImmutableSet.copyOf(entry.getValue());
                nodes.add(new TestClientNode(nodeId, clients));
            }

            return new RateLimitingTestHarness(
                    maxInProgressAddDataPagesRequests,
                    inProgressAddDataPagesRequestsRateLimitThreshold,
                    inProgressAddDataPagesRequestsThrottlingCounterDecayDuration,
                    requestProcessingTime,
                    nodes.build());
        }
    }

    private static class TestClientNode
    {
        private final String nodeId; // TODO use
        private final Set<TestClient> clients;
        private final DataApiFacadeStats dataApiFacadeStats;

        public TestClientNode(String nodeId, Set<TestClient> clients)
        {
            this.nodeId = nodeId;
            this.clients = clients;
            this.dataApiFacadeStats = new DataApiFacadeStats();
        }

        public ListenableFuture<Void> run(ListeningExecutorService executorService, RateLimitingTestServer server, ScheduledExecutorService retryExecutor)
        {
            DataApiFacade dataApi = createApi(server, retryExecutor);
            List<ListenableFuture<Void>> futures = clients.stream().map(client -> client.run(executorService, dataApi)).collect(toImmutableList());
            return asVoid(allAsList(futures));
        }

        public String getNodeId()
        {
            return nodeId;
        }

        public DataApiFacadeStats getDataApiFacadeStats()
        {
            return dataApiFacadeStats;
        }

        private DataApiFacade createApi(RateLimitingTestServer server, ScheduledExecutorService retryExecutor)
        {
            return new DataApiFacade(
                    new BufferNodeDiscoveryManager()
                    {
                        @Override
                        public BufferNodesState getBufferNodes()
                        {
                            Instant now = Instant.now();
                            Optional<BufferNodeStats> dummyStats = Optional.of(new BufferNodeStats(100, 100, 100, 100, 100, 100, 100, 100, 100));
                            return new BufferNodesState(
                                    now.toEpochMilli(),
                                    ImmutableMap.of(BUFFER_NODE_ID, new BufferNodeInfo(BUFFER_NODE_ID, server.getBaseUri(), dummyStats, BufferNodeState.ACTIVE, now)));
                        }

                        @Override
                        public ListenableFuture<Void> forceRefresh()
                        {
                            return null;
                        }
                    },
                    new ApiFactory() {
                        @Override
                        public DiscoveryApi createDiscoveryApi()
                        {
                            throw new RuntimeException("not implemented");
                        }

                        @Override
                        public DataApi createDataApi(BufferNodeInfo nodeInfo)
                        {
                            SpooledChunkReader dummySpooledChunkReader = new SpooledChunkReader()
                            {
                                @Override
                                public ListenableFuture<List<DataPage>> getDataPages(SpoolingFile spoolingFile)
                                {
                                    throw new RuntimeException("not implemented");
                                }

                                @Override
                                public void close()
                                        throws Exception
                                {}
                            };
                            return new HttpDataClient(
                                    server.getBaseUri(),
                                    BUFFER_NODE_ID,
                                    new JettyHttpClient(),
                                    io.airlift.units.Duration.succinctDuration(30, TimeUnit.SECONDS),
                                    dummySpooledChunkReader,
                                    false,
                                    Optional.of(nodeId));
                        }
                    },
                    dataApiFacadeStats,
                    new BufferExchangeConfig(),
                    retryExecutor);
        }
    }

    private static class TestClient
    {
        private final Duration initialDelay;
        private final long count;
        private final double rate;

        public TestClient(Duration initialDelay, long count, double rate)
        {
            this.initialDelay = initialDelay;
            this.count = count;
            this.rate = rate;
        }

        public ListenableFuture<Void> run(ListeningExecutorService executorService, DataApiFacade api)
        {
            return asVoid(executorService.submit(() -> {
                try {
                    perform(api);
                }
                catch (Throwable e) {
                    log.warn(e, "Unexpected error");
                }
            }));
        }

        private void perform(DataApiFacade api)
                throws InterruptedException
        {
            sleep(initialDelay.toMillis());
            long start = System.nanoTime();
            for (int i = 0; i < count; ++i) {
                long now = System.nanoTime();
                long expectedRequestTime = (long) (start + i / rate * NANOSECONDS_IN_SECOND);
                if (now < expectedRequestTime) {
                    sleepNanos(expectedRequestTime - now);
                }
                else {
                    // todo: capture delay statistics
                }
                getFutureValue(api.addDataPages(BUFFER_NODE_ID, "xxx", 1, 1, 1, ImmutableListMultimap.of()));
            }
        }
    }

    private static void sleepNanos(long nanos)
            throws InterruptedException
    {
        long millis = nanos / NANOSECONDS_IN_MILLISECOND;
        long remainderNanos = nanos % NANOSECONDS_IN_MILLISECOND;
        sleep(millis, toIntExact(remainderNanos));
    }
}
