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
package io.trino.plugin.warp.execution;

import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.jackson.io.JacksonSerializer;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.plugin.warp.extension.di.HttpServerLifeCycleHandler;
import io.trino.spi.catalog.CatalogName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unchecked")
@Singleton
public class WarpClient
{
    public static final JsonCodec<Void> VOID_RESULTS_CODEC = JsonCodec.jsonCodec(Void.class);
    private static final Logger logger = Logger.get(WarpClient.class);
    private final CatalogName catalogName;
    private final HttpClient httpClient;
    private final WarpExtensionConfig warpExtensionConfig;
    private final Optional<Supplier<JwtBuilder>> jwtBuilder;

    @Inject
    public WarpClient(CatalogName catalogName,
            @ForWarp HttpClient httpClient,
            WarpExtensionConfig warpExtensionConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalog name is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.warpExtensionConfig = requireNonNull(warpExtensionConfig);

        jwtBuilder = warpExtensionConfig.getInternalCommunicationSharedSecret() != null ?
                Optional.of(() -> new DefaultJwtBuilder()
                        .serializeToJsonWith(new JacksonSerializer<>())
                        .signWith(hmacShaKeyFor(Hashing.sha256().hashString(warpExtensionConfig.getInternalCommunicationSharedSecret(), UTF_8).asBytes()))
                        .subject(warpExtensionConfig.getClusterUUID())
                        .expiration(Date.from(ZonedDateTime.now(ZoneId.systemDefault()).plusMinutes(5).toInstant()))) :
                Optional.empty();
    }

    public static boolean isOk(int statusCode)
    {
        return (statusCode == HttpStatus.OK.code()) ||
                (statusCode == HttpStatus.NO_CONTENT.code());
    }

    public <T extends TaskData> void sendWithRetry(HttpUriBuilder uri, T taskData)
    {
        if (Objects.isNull(uri)) {
            throw new RuntimeException("uri is null");
        }
        URI fullUri = uri.appendPath("/v1/ext/" + catalogName.toString())
                .appendPath(taskData.getTaskName()).build();
        String callerName = taskData.getClass().getSimpleName();
        Request.Builder builder = Request.Builder.preparePost()
                .setUri(fullUri)
                .addHeaders(taskData.getTaskHeaders())
                .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(taskData.getCodec(), taskData))
                .addHeader("X-Trino-User", "internal");
        handleBearer(builder);

        sendWithRetry(builder.build(), FullJsonResponseHandler.createFullJsonResponseHandler(VOID_RESULTS_CODEC), callerName);
    }

    public <T> T sendWithRetry(Request request, FullJsonResponseHandler<T> responseHandler)
    {
        return sendWithRetry(request, responseHandler, "");
    }

    public <T> T sendWithRetry(Request request, FullJsonResponseHandler<T> responseHandler, String callerName)
    {
        Request.Builder builder = Request.Builder.fromRequest(request)
                .addHeader("X-Trino-User", "internal");
        handleBearer(builder);
        Request requestWithUser = builder.build();
        return invokeWithRetry(() -> {
            JsonResponse<T> response = httpClient.execute(requestWithUser, responseHandler);
            if (isOk(response.getStatusCode())) {
                if (response.getJson() != null) {
                    return response.getValue();
                }
                else {
                    return null;
                }
            }
            else {
                logger.error("error response for %s : %d=>%s / %s",
                        requestWithUser.getUri(),
                        response.getStatusCode(),
                        response.getJson(),
                        response.getException());
                if (response.getException() != null) {
                    throw response.getException();
                }
                throw new IOException("failed response " + response.getStatusCode());
            }
        }, request.getUri(), callerName);
    }

    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        Request.Builder builder = Request.Builder.fromRequest(request)
                .addHeader("X-Trino-User", "internal");
        handleBearer(builder);
        return httpClient.executeAsync(builder.build(), responseHandler);
    }

    public <T> T invokeWithRetry(Callable<T> func, URI uri, String callerName)
    {
        return Failsafe.with(RetryPolicy.builder()
                        .withMaxAttempts(2)
                        .onFailedAttempt((executionAttemptedEvent) -> logger.warn("failed executing %s REST command to URI %s", callerName, uri))
                        .withDelay(Duration.ofSeconds(10))
                        .handle(IOException.class, UncheckedIOException.class, IllegalStateException.class)
                        .build())
                .get(func::call);
    }

    public HttpUriBuilder getRestEndpoint(URI nodeUri)
    {
        return getRestEndpoint(nodeUri, true);
    }

    public HttpUriBuilder getRestEndpoint(URI nodeUri, boolean isInternal)
    {
        int restPort;
        try {
            restPort = nodeUri.getPort() > 0 ? nodeUri.getPort() : URI.create(nodeUri.toString()).toURL().getDefaultPort();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        if (!warpExtensionConfig.isUseHttpServerPort()) {
            restPort = HttpServerLifeCycleHandler.getWarpRestPort(restPort)
                    .orElse(getRestHttpPort(warpExtensionConfig));
            if (restPort == 0) {
                throw new IllegalStateException("Warp REST port is not configured");
            }
        }
        HttpUriBuilder uriBuilder = HttpUriBuilder.uriBuilderFrom(nodeUri)
                .port(restPort);
        if (warpExtensionConfig.isUseHttpServerPort()) {
            uriBuilder.appendPath("ext");
            if (isInternal && warpExtensionConfig.getInternalCommunicationSharedSecret() != null) {
                uriBuilder.appendPath("internal");
            }
            uriBuilder.appendPath(catalogName.toString());
        }
        return uriBuilder;
    }

    private void handleBearer(Request.Builder builder)
    {
        jwtBuilder.ifPresent(jwtBuilderSupplier -> builder.addHeader("X-Trino-Internal-Bearer", jwtBuilderSupplier.get().compact()));
    }

    public static int getRestHttpPort(WarpExtensionConfig warpExtensionConfig)
    {
        return Integer.parseInt(getRestHttpPortStr(warpExtensionConfig));
    }

    public static String getRestHttpPortStr(WarpExtensionConfig warpExtensionConfig)
    {
        if (!warpExtensionConfig.isUseHttpServerPort() && !warpExtensionConfig.isRestHttpDefaultPortEnabled()) {
            return "0";
        }
        return String.valueOf(warpExtensionConfig.getRestHttpPort());
    }
}
