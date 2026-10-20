/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.vertexai;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.collect.ImmutableList;
import com.google.genai.Client;
import com.google.genai.types.HttpOptions;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.starburst.ai.client.StaticPromptDao;
import io.starburst.ai.client.TokenUsageContext;
import io.starburst.ai.client.TokenUsageListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Calls Vertex AI while the current span carries a vendor tracestate entry, the way Dynatrace
 * OneAgent tags traffic with keys like `f4feaf76-ce3ba1e0@dt`. Such keys are valid W3C tracestate
 * keys but OpenCensus rejects them, and the Google auth and HTTP libraries trace through
 * OpenCensus, which the OpenTelemetry shim bridges onto the current OpenTelemetry span.
 */
@TestInstance(PER_CLASS)
public class TestVertexAiTracestate
{
    private static final String DYNATRACE_TRACESTATE_KEY = "f4feaf76-ce3ba1e0@dt";

    private HttpServer server;

    @BeforeAll
    public void startServer()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/token", exchange -> respond(exchange,
                """
                {"access_token": "test-token", "expires_in": 3600, "token_type": "Bearer"}
                """));
        server.createContext("/", exchange -> respond(exchange,
                """
                {"candidates": [{"content": {"role": "model", "parts": [{"text": "positive"}]}}]}
                """));
        server.start();
    }

    @AfterAll
    public void stopServer()
    {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testGenerateWithVendorTracestateEntry()
            throws IOException
    {
        Span parent = Span.wrap(SpanContext.createFromRemoteParent(
                "4bf92f3577b34da6a3ce929d0e0e4736",
                "00f067aa0ba902b7",
                TraceFlags.getSampled(),
                TraceState.builder()
                        .put(DYNATRACE_TRACESTATE_KEY, "fw4;0;0;0;0;0;0;0")
                        .build()));

        try (Client client = createClient();
                SdkTracerProvider tracerProvider = SdkTracerProvider.builder().build();
                Scope _ = parent.makeCurrent()) {
            VertexAiLanguageModelClient languageModelClient = new VertexAiLanguageModelClient(
                    "gemini-2.5-flash",
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    new StaticPromptDao(),
                    directExecutor(),
                    1,
                    client,
                    tracerProvider.get("test"),
                    false,
                    TokenUsageListener.NOOP);

            assertThat(languageModelClient.generate("I love Starburst", TokenUsageContext.EMPTY)).isEqualTo("positive");
        }
    }

    private Client createClient()
            throws IOException
    {
        String serverUri = "http://localhost:" + server.getAddress().getPort();
        ServiceAccountCredentials credentials;
        try (InputStream key = getResource("vertex-ai-service-account.json").openStream()) {
            credentials = ServiceAccountCredentials.fromStream(key).toBuilder()
                    .setTokenServerUri(URI.create(serverUri + "/token"))
                    .build();
        }
        return Client.builder()
                .vertexAI(true)
                .project("test-project")
                .location("us-central1")
                .credentials(credentials.createScoped(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform")))
                .httpOptions(HttpOptions.builder().baseUrl(serverUri).build())
                .build();
    }

    private static void respond(HttpExchange exchange, String body)
            throws IOException
    {
        try (exchange; InputStream request = exchange.getRequestBody(); OutputStream response = exchange.getResponseBody()) {
            request.readAllBytes();
            byte[] bytes = body.getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            response.write(bytes);
        }
    }
}
