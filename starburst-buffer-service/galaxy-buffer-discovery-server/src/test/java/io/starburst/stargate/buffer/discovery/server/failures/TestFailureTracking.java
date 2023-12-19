/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server.failures;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.testing.TestingTicker;
import io.starburst.stargate.buffer.discovery.client.failures.FailureInfo;
import io.starburst.stargate.buffer.discovery.client.failures.FailureTrackingApi;
import io.starburst.stargate.buffer.discovery.client.failures.FailuresStatusInfo;
import io.starburst.stargate.buffer.discovery.client.failures.HttpFailureTrackingClient;
import io.starburst.stargate.buffer.discovery.server.testing.TestingDiscoveryServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.testing.Closeables.closeAll;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFailureTracking
{
    private TestingDiscoveryServer server;
    private HttpClient httpClient;

    private final TestingTicker ticker = new TestingTicker();

    @BeforeEach
    public void setup()
    {
        server = TestingDiscoveryServer.builder()
                .setDiscoveryManagerTicker(ticker)
                .build();
        httpClient = new JettyHttpClient(new HttpClientConfig());
    }

    @AfterEach
    public void teardown()
            throws Exception
    {
        closeAll(server, httpClient);
    }

    @Test
    void testFailuresTracking()
    {
        URI baseUri = server.getBaseUri();
        FailureTrackingApi client = new HttpFailureTrackingClient(baseUri, httpClient);

        assertThat(client.getFailuresStatus()).isEqualTo(
                new FailuresStatusInfo(0));

        client.registerFailure(new FailureInfo(Optional.of("some exchange"), "blah", "some details"));
        client.registerFailure(new FailureInfo(Optional.empty(), "blah", "some details"));
        client.registerFailure(new FailureInfo(Optional.empty(), "blah", "some details"));
        client.registerFailure(new FailureInfo(Optional.empty(), "blah", "some details"));

        assertThat(client.getFailuresStatus()).isEqualTo(
                new FailuresStatusInfo(4));

        ticker.increment(2, TimeUnit.MINUTES);
        assertThat(client.getFailuresStatus()).isEqualTo(
                new FailuresStatusInfo(3));

        ticker.increment(2, TimeUnit.MINUTES);
        assertThat(client.getFailuresStatus()).isEqualTo(
                new FailuresStatusInfo(2));

        client.registerFailure(new FailureInfo(Optional.empty(), "blah", "some details"));
        assertThat(client.getFailuresStatus()).isEqualTo(
                new FailuresStatusInfo(3));

        ticker.increment(4, TimeUnit.MINUTES);
        assertThat(client.getFailuresStatus()).isEqualTo(
                new FailuresStatusInfo(1));

        ticker.increment(5, TimeUnit.MINUTES);
        assertThat(client.getFailuresStatus()).isEqualTo(
                new FailuresStatusInfo(0));
    }
}
