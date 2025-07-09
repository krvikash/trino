/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.starburst.stargate.buffer.data.server.testing.TestingDataServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;

import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestDataServerStatusProbes
{
    private TestingDataServer dataServer;
    private HttpClient httpClient;

    @BeforeEach
    public void setup()
    {
        dataServer = TestingDataServer.builder()
                .setConfigProperty("spooling.directory", System.getProperty("java.io.tmpdir") + "/spooling-storage")
                .build();
        httpClient = new JettyHttpClient(new HttpClientConfig());
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        closeAll(dataServer, httpClient);
    }

    @Test
    public void testStartedStatusProbe()
    {
        checkStatusProbeEndpoint("started");
    }

    @Test
    public void testReadyStatusProbe()
    {
        checkStatusProbeEndpoint("ready");
    }

    @Test
    public void testAliveStatusProbe()
    {
        checkStatusProbeEndpoint("alive");
    }

    private void checkStatusProbeEndpoint(String probe)
    {
        assertThat(httpClient.execute(
                        Request.builder()
                                .setMethod("GET")
                                .setUri(URI.create("%s/status/%s".formatted(dataServer.getBaseUri(), probe)))
                                .build(),
                        createStatusResponseHandler())
                .getStatusCode())
                .isEqualTo(200);
    }
}
