/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Forwards Glue requests to {@code target} and records them, since Floci ignores {@code CatalogId}.
 */
final class RecordingGlueProxy
        implements AutoCloseable
{
    private static final Set<String> RESTRICTED_HEADERS = ImmutableSet.of("connection", "content-length", "expect", "host", "upgrade", "transfer-encoding");

    public record GlueRequest(String operation, String body) {}

    private final URI target;
    private final Predicate<GlueRequest> denied;
    private final HttpClient client = HttpClient.newHttpClient();
    private final ConcurrentLinkedQueue<GlueRequest> requests = new ConcurrentLinkedQueue<>();
    private final HttpServer server;

    /**
     * @param denied requests answered with a Lake Formation {@code AccessDeniedException} instead of being forwarded
     */
    public RecordingGlueProxy(URI target, Predicate<GlueRequest> denied)
            throws IOException
    {
        this.target = requireNonNull(target, "target is null");
        this.denied = requireNonNull(denied, "denied is null");
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", this::forward);
        server.start();
    }

    public URI endpoint()
    {
        return URI.create("http://localhost:" + server.getAddress().getPort());
    }

    public List<GlueRequest> requests(String operation)
    {
        return requests.stream()
                .filter(request -> request.operation().equals(operation))
                .collect(toImmutableList());
    }

    private void forward(HttpExchange exchange)
            throws IOException
    {
        try (exchange) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String amzTarget = exchange.getRequestHeaders().getFirst("X-Amz-Target");
            if (amzTarget != null) {
                GlueRequest glueRequest = new GlueRequest(amzTarget.substring(amzTarget.indexOf('.') + 1), new String(body, UTF_8));
                requests.add(glueRequest);
                if (denied.test(glueRequest)) {
                    byte[] error = "{\"__type\":\"AccessDeniedException\",\"Message\":\"Insufficient Lake Formation permission(s): Required Describe\"}".getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.1");
                    exchange.sendResponseHeaders(400, error.length);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(error);
                    }
                    return;
                }
            }

            HttpRequest.Builder request = HttpRequest.newBuilder(target.resolve(exchange.getRequestURI()))
                    .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(body));
            for (Map.Entry<String, List<String>> header : exchange.getRequestHeaders().entrySet()) {
                if (!RESTRICTED_HEADERS.contains(header.getKey().toLowerCase(ENGLISH))) {
                    header.getValue().forEach(value -> request.header(header.getKey(), value));
                }
            }
            HttpResponse<byte[]> response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());

            response.headers().map().forEach((name, values) -> {
                if (!name.startsWith(":") && !RESTRICTED_HEADERS.contains(name.toLowerCase(ENGLISH))) {
                    exchange.getResponseHeaders().put(name, ImmutableList.copyOf(values));
                }
            });
            exchange.sendResponseHeaders(response.statusCode(), response.body().length == 0 ? -1 : response.body().length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response.body());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException(e));
        }
    }

    @Override
    public void close()
    {
        server.stop(0);
    }
}
