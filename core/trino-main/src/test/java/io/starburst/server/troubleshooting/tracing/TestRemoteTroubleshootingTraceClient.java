/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.tracing;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import io.starburst.server.troubleshooting.DownloadResult;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingHttpClient.Processor;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.client.NodeVersion;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.spi.Node;
import io.trino.spi.QueryId;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

@ExtendWith(SoftAssertionsExtension.class)
class TestRemoteTroubleshootingTraceClient
{
    @Test
    public void testDownloadIgnoreFailedWorkers(SoftAssertions softly)
            throws IOException
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            InternalNode goodNode = worker("good node", URI.create("http://localhost:1"));
            InternalNode badNode = worker("bad node", URI.create("http://localhost:2"));
            String goodResponse = "good response";
            Processor processor = request -> {
                if (request.getUri().getPort() == goodNode.getInternalUri().getPort()) {
                    return new TestingResponse(HttpStatus.fromStatusCode(200), ArrayListMultimap.create(), goodResponse.getBytes(UTF_8));
                }
                if (request.getUri().getPort() == badNode.getInternalUri().getPort()) {
                    return new TestingResponse(HttpStatus.fromStatusCode(500), ArrayListMultimap.create(), new byte[] {});
                }
                throw new IllegalArgumentException("request not supported " + request);
            };
            InMemoryNodeManager nodeManager = new InMemoryNodeManager(goodNode, badNode);

            RemoteTroubleshootingTraceClient client = new RemoteTroubleshootingTraceClient(nodeManager, new TestingHttpClient(processor, executor));

            Map<Node, DownloadResult> downloaded = client.download(new QueryId("query"), ImmutableSet.of(goodNode.getNodeIdentifier(), badNode.getNodeIdentifier()));
            softly.assertThat(downloaded).containsOnlyKeys(goodNode, badNode);
            DownloadResult goodResult = downloaded.get(goodNode);
            softly.assertThat(goodResult.isSuccessful()).isTrue();
            softly.assertThat(new String(goodResult.inputStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(goodResponse);
            DownloadResult badResult = downloaded.get(badNode);
            softly.assertThat(badResult.isSuccessful()).isFalse();
            softly.assertThat(new String(badResult.inputStream().readAllBytes(), StandardCharsets.UTF_8))
                    .contains("Caused by: io.trino.spi.TrinoException: request failed with http status code: 500");
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static InternalNode worker(String goodNode, URI internalUri)
    {
        return new InternalNode(goodNode, internalUri, NodeVersion.UNKNOWN, false);
    }
}
