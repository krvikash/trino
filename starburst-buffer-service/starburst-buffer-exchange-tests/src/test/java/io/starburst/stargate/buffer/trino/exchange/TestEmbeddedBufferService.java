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

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEmbeddedBufferService
        extends AbstractTestQueryFramework
{
    private final HttpClient httpClient = new JettyHttpClient();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        // setup embedded buffer service
        File exchangeManagerDirectory = createTempDirectory("exchange_manager").toFile();
        extraProperties.put("embedded-buffer-service-enabled", "true");
        extraProperties.put("buffer.spooling.directory", exchangeManagerDirectory.getAbsolutePath());
        extraProperties.put("buffer.testing.allow-local-spooling", "true");

        // By default, FaultTolerantExecutionConnectorTestHelper.getExtraProperties sets
        // executor-pool-size to 10. Such small value may cause queries to fail if tests are run in parallel.
        // The reason for that is currently same thread pool is used for long running jobs driving EventDrivenFaultTolerantQueryScheduler
        // as well as for future callback used during query processing. If all threads are used by EventDrivenFaultTolerantQueryScheduler jobs
        // then callbacks would not be executed and queries may get blocked.
        // TODO: update code in Trino so it is not needed
        extraProperties.put("query.executor-pool-size", "100");

        // exchange manager config
        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.use-embedded-buffer-service", "true")
                .put("exchange.sink-target-written-pages-count", "3") // small requests for better test coverage
                .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                .put("exchange.min-base-buffer-nodes-per-partition", "2")
                .put("exchange.max-base-buffer-nodes-per-partition", "2")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(extraProperties)
                .withExchange("buffer", exchangeManagerProperties)
                .build();
        return queryRunner;
    }

    @Test
    public void testBufferInternalAuth()
    {
        URI coordinatorBaseUrl = getQueryRunner().getCoordinator().getBaseUrl();
        URI workerBaseUrl = getDistributedQueryRunner().getServers().stream()
                .filter(server -> !server.isCoordinator())
                .findFirst()
                .orElseThrow()
                .getBaseUrl();

        assertInternalGetResource(coordinatorBaseUrl.resolve("/api/v1/buffer/discovery/nodes"));
        assertInternalPostResource(coordinatorBaseUrl.resolve("/api/v1/buffer/discovery/nodes/update"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/info"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/closedChunks"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/markAllClosedChunksReceived"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/setChunkDeliveryMode"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/1/pages/1/1"));
        assertInternalPostResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/addDataPages/1/1/1"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/register"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/finish"));
        assertInternalGetResource(workerBaseUrl.resolve("/api/v1/buffer/data/1/ping"));
    }

    private void assertInternalGetResource(URI uri)
    {
        Request request = prepareGet().setUri(uri).build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        assertInternalAuthFailure(response);
    }

    private void assertInternalPostResource(URI uri)
    {
        Request request = preparePost().setUri(uri).build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        assertInternalAuthFailure(response);
    }

    private static void assertInternalAuthFailure(StringResponse response)
    {
        assertThat(response.getStatusCode()).isEqualTo(403);
        assertThat(response.getBody()).isEqualTo("Error 403 Forbidden: Internal only resource");
    }
}
