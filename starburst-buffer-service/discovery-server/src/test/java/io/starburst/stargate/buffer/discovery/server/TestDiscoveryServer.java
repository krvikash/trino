/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.testing.TestingTicker;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.BufferNodeStats;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.airlift.testing.Closeables.closeAll;
import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.RUNNING;
import static io.starburst.stargate.buffer.discovery.client.BufferNodeState.STARTING;
import static io.starburst.stargate.buffer.discovery.server.DiscoveryManagerConfig.DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD;
import static io.starburst.stargate.buffer.discovery.server.DiscoveryManagerConfig.DEFAULT_START_GRACE_PERIOD;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestDiscoveryServer
{
    private TestingDiscoveryServer discoveryServer;
    private DiscoveryManager discoveryManager;
    private HttpClient httpClient;

    private final TestingTicker ticker = new TestingTicker();

    @BeforeAll
    public void setup()
    {
        discoveryServer = TestingDiscoveryServer.builder()
                .setDiscoveryManagerTicker(ticker)
                .build();
        discoveryManager = discoveryServer.getInstance(DiscoveryManager.class);
        httpClient = new JettyHttpClient(new HttpClientConfig());
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        closeAll(discoveryServer, httpClient);
    }

    @Test
    void testHappy()
    {
        URI baseUri = discoveryServer.getBaseUri();
        HttpDiscoveryClient discoveryClient = new HttpDiscoveryClient(baseUri, httpClient);

        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        false,
                        Set.of()));

        BufferNodeStats node1Stats1 = new BufferNodeStats(10, 11, 12, 13, 14);
        BufferNodeInfo node1Info1 = new BufferNodeInfo(1, URI.create("http://address1"), Optional.of(node1Stats1), RUNNING);
        discoveryClient.updateBufferNode(node1Info1);

        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        false,
                        Set.of(node1Info1)));

        // still below START_GRACE_PERIOD
        ticker.increment(DEFAULT_START_GRACE_PERIOD.toMillis() - 10, TimeUnit.MILLISECONDS);
        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        false,
                        Set.of(node1Info1)));

        // add another node
        BufferNodeStats node2Stats1 = new BufferNodeStats(20, 21, 22, 23, 24);
        BufferNodeInfo node2Info1 = new BufferNodeInfo(2, URI.create("http://address2"), Optional.of(node1Stats1), STARTING);
        discoveryClient.updateBufferNode(node2Info1);
        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        false,
                        Set.of(node1Info1, node2Info1)));

        // move past the START_GRACE_PERIOD
        ticker.increment(11, TimeUnit.MILLISECONDS);
        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        true,
                        Set.of(node1Info1, node2Info1)));

        // move time and update node 1
        ticker.increment(1000, TimeUnit.MILLISECONDS);
        BufferNodeStats node1Stats2 = new BufferNodeStats(110, 111, 112, 113, 114);
        BufferNodeInfo node1Info2 = new BufferNodeInfo(1, URI.create("http://address1"), Optional.of(node1Stats2), RUNNING);
        discoveryClient.updateBufferNode(node1Info2);
        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        true,
                        Set.of(node1Info2, node2Info1)));

        // update both nodes and move time but still below MAX_NODE_INFO_STALENESS
        discoveryClient.updateBufferNode(node1Info2);
        discoveryClient.updateBufferNode(node2Info1);
        ticker.increment(DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD.toMillis() - 100, TimeUnit.MILLISECONDS);
        discoveryManager.cleanup(); // should not delete anything
        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        true,
                        Set.of(node1Info2, node2Info1)));

        // update just node1 and move time a bit
        discoveryClient.updateBufferNode(node1Info2);
        ticker.increment(200, TimeUnit.MILLISECONDS);
        discoveryManager.cleanup(); // should delete node2
        assertThat(discoveryClient.getBufferNodes()).isEqualTo(
                new BufferNodeInfoResponse(
                        true,
                        Set.of(node1Info2)));
    }
}
