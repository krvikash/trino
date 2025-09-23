/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jfr;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import io.starburst.server.troubleshooting.jfr.FlightRecorderHttpClient.WorkerNodesProvider;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.client.NodeVersion;
import io.trino.server.InternalCommunicationConfig;
import io.trino.spi.QueryId;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static java.nio.charset.StandardCharsets.UTF_8;

@ExtendWith(SoftAssertionsExtension.class)
final class TestFlightRecorderHttpClient
{
    @Test
    void testFailingWorker(SoftAssertions softly)
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            int badNodePort = 2;
            ServiceDescriptor badNode = worker("bad node", URI.create("http://localhost:" + badNodePort));
            WorkerNodesProvider workerNodesProvider = new WorkerNodesProvider(new StaticServiceSelector(badNode));
            TestingHttpClient.Processor processor = request -> {
                if (request.getUri().getPort() == badNodePort) {
                    return new TestingResponse(HttpStatus.fromStatusCode(500), ArrayListMultimap.create(), new byte[] {});
                }
                throw new IllegalArgumentException("request not supported " + request);
            };
            FlightRecorderHttpClient.Factory factory = new FlightRecorderHttpClient.Factory(
                    new TestingHttpClient(processor, executor),
                    scheduledExecutor,
                    workerNodesProvider,
                    new InternalCommunicationConfig().setHttpsRequired(false));
            FlightRecorderHttpClient client = factory.create(new QueryId("query"));
            ImmutableSet<String> nodeIds = ImmutableSet.of(badNode.getNodeId());

            softly.assertThatThrownBy(() -> client.start(nodeIds))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("Could not start recordings on nodes: \\[bad node] due to: \\[.*: Expected error code 2xx, got: 500]");
            softly.assertThatThrownBy(() -> client.finish(nodeIds))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("Could not finish recordings on nodes: \\[bad node] due to: \\[.*: Expected error code 2xx, got: 500]");
            softly.assertThatThrownBy(() -> client.remove(nodeIds))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("Could not remove recordings on nodes: \\[bad node] due to: \\[.*: Expected error code 2xx, got: 500]");
            softly.assertThatThrownBy(() -> client.getInputStreams(nodeIds))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("Could not get recordings from nodes: \\[bad node] due to: \\[.*: Expected status code to be 2xx but got 500]");
        }
        finally {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }
    }

    @Test
    void testNonExistentWorker(SoftAssertions softly)
    {
        try (ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor()) {
            ServiceDescriptor node = worker("good node", URI.create("http://localhost:1"));
            WorkerNodesProvider workerNodesProvider = new WorkerNodesProvider(new StaticServiceSelector(node));
            TestingHttpClient.Processor processor = _ -> new TestingResponse(HttpStatus.fromStatusCode(200), ArrayListMultimap.create(), "OK".getBytes(UTF_8));
            FlightRecorderHttpClient.Factory factory = new FlightRecorderHttpClient.Factory(
                    new TestingHttpClient(processor),
                    scheduledExecutor,
                    workerNodesProvider,
                    new InternalCommunicationConfig().setHttpsRequired(false));
            FlightRecorderHttpClient client = factory.create(new QueryId("query"));

            softly.assertThatCode(() -> client.start(ImmutableSet.of(node.getNodeId(), "non-existent-node")))
                    .doesNotThrowAnyException();
            softly.assertThatCode(() -> client.remove(ImmutableSet.of(node.getNodeId(), "non-existent-node")))
                    .doesNotThrowAnyException();
            softly.assertThatCode(() -> client.finish(ImmutableSet.of(node.getNodeId(), "non-existent-node")))
                    .doesNotThrowAnyException();
            softly.assertThatCode(() -> client.getInputStreams(ImmutableSet.of(node.getNodeId(), "non-existent-node")))
                    .doesNotThrowAnyException();
        }
    }

    private static ServiceDescriptor worker(String nodeId, URI internalUri)
    {
        return serviceDescriptor("trino")
                .setNodeId(nodeId)
                .addProperty("http", internalUri.toString())
                .addProperty("node_version", NodeVersion.UNKNOWN.getVersion())
                .addProperty("coordinator", Boolean.FALSE.toString())
                .build();
    }
}
