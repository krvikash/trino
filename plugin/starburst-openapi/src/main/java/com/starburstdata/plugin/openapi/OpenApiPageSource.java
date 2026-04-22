/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.starburstdata.plugin.openapi.authentication.OpenApiAuthenticator;
import com.starburstdata.plugin.openapi.conversions.decoder.OpenApiDecoder;
import com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_AUTHORIZATION_ERROR;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_GENERIC_EXTERNAL_ERROR;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * @param <S> Pagination state type.
 */
public class OpenApiPageSource<S>
        implements ConnectorPageSource
{
    private static final int MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_AFTER_MILLIS = 5_000L;
    private static final long MAX_RETRY_AFTER_MILLIS = 60_000L;

    private final HttpClient httpClient;
    private final OpenApiPaginationStrategy<S> paginationStrategy;
    private final OpenApiDecoder decoder;
    private final List<ColumnHandle> columnHandles;
    private CompletableFuture<OpenApiResult<S>> pageFuture;
    private Iterator<SourcePage> currentIterator = emptyIterator();
    private S currentState;
    private Request nextRequest;
    private final OpenApiAuthenticator authenticator;
    private final ObjectMapper objectMapper;
    private final ResponseHandler<OpenApiResult<S>, RuntimeException> jsonResponseHandler = new ReadFromJson();
    private final RetryPolicy<OpenApiResult<S>> retryPolicy;

    public OpenApiPageSource(
            HttpClient httpClient,
            OpenApiPaginationStrategy<S> paginationStrategy,
            Request initialRequest,
            OpenApiDecoder decoder,
            List<ColumnHandle> columnHandles,
            ObjectMapper objectMapper,
            OpenApiAuthenticator authenticator)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.paginationStrategy = requireNonNull(paginationStrategy, "paginationStrategy is null");
        this.currentState = paginationStrategy.initialState();
        this.nextRequest = requireNonNull(initialRequest, "initialRequest is null");
        this.decoder = requireNonNull(decoder, "decoder is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.authenticator = requireNonNull(authenticator, "authenticator is null");
        this.retryPolicy = RetryPolicy.<OpenApiResult<S>>builder()
                .handle(RetryableException.class)
                .withMaxRetries(MAX_RETRIES)
                .withDelayFn(ctx -> ctx.getLastException() instanceof RetryableException e
                        ? Duration.ofMillis(e.retryAfterMillis())
                        : Duration.ofMillis(DEFAULT_RETRY_AFTER_MILLIS))
                .build();
    }

    private class ReadFromJson
            implements ResponseHandler<OpenApiResult<S>, RuntimeException>
    {
        @Override
        public OpenApiResult<S> handleException(Request request, Exception exception)
                throws RuntimeException
        {
            throw new TrinoException(
                    OPENAPI_GENERIC_EXTERNAL_ERROR,
                    "Encountered unexpected error (%s) while requesting %s".formatted(
                            exception.getMessage(),
                            request.getUri().getPath()),
                    exception);
        }

        @Override
        public OpenApiResult<S> handle(Request request, Response response)
                throws RuntimeException
        {
            int statusCode = response.getStatusCode();
            return switch (statusCode) {
                case 200 -> {
                    JsonNode root;
                    try {
                        root = objectMapper.reader()
                                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                                .readTree(response.getInputStream());
                    }
                    catch (IOException e) {
                        throw new TrinoException(
                                OPENAPI_GENERIC_EXTERNAL_ERROR,
                                "Failed to read JSON from response",
                                e);
                    }
                    yield new OpenApiResult<>(
                            decoder.decodeFromRoot(root, columnHandles),
                            paginationStrategy.nextStateFromResponse(currentState, response, root));
                }
                case 400 -> throw new TrinoException(
                        OPENAPI_GENERIC_EXTERNAL_ERROR,
                        "Bad request (status 400) for %s - check query parameters".formatted(request.getUri().getPath()));
                case 401 -> {
                    String wwwAuthenticate = response.getHeader("WWW-Authenticate");
                    String hint = wwwAuthenticate != null ? " (WWW-Authenticate: %s)".formatted(wwwAuthenticate) : "";
                    throw new TrinoException(
                            OPENAPI_AUTHORIZATION_ERROR,
                            "Unauthorized (status 401) for %s - check authentication configuration%s".formatted(request.getUri().getPath(), hint));
                }
                case 403 -> throw new TrinoException(
                        OPENAPI_AUTHORIZATION_ERROR,
                        "Forbidden (status 403) for %s - insufficient permissions".formatted(request.getUri().getPath()));
                case 404 -> throw new TrinoException(
                        OPENAPI_GENERIC_EXTERNAL_ERROR,
                        "Not found (status 404) for %s - the route or resource may not exist, or access may be restricted".formatted(request.getUri().getPath()));
                case 429, 503 -> throw new RetryableException(statusCode, parseRetryAfterMillis(response));
                // A redirect status reaching here means Jetty exhausted its redirect limit (8 by default).
                case 301, 302, 303, 307, 308 -> throw new TrinoException(
                        OPENAPI_GENERIC_EXTERNAL_ERROR,
                        "Too many redirects for %s - possible redirect loop".formatted(request.getUri().getPath()));
                default -> throw new TrinoException(
                        OPENAPI_GENERIC_EXTERNAL_ERROR,
                        "Unexpected response status (%s) for %s".formatted(statusCode, request.getUri().getPath()));
            };
        }
    }

    @Override
    public long getCompletedBytes()
    {
        // TODO
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        // TODO
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return paginationStrategy.isFinished(currentState) && !currentIterator.hasNext();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            return null;
        }

        if (currentIterator.hasNext()) {
            return currentIterator.next();
        }

        if (pageFuture != null && pageFuture.isDone()) {
            OpenApiResult<S> result = getFutureValue(pageFuture);
            currentState = result.newPaginationState();
            currentIterator = result.pageIterator();
            if (!isFinished()) {
                nextRequest = paginationStrategy.nextRequestFromState(nextRequest, currentState);
                return currentIterator.next();
            }
        }

        if (pageFuture == null) {
            pageFuture = nextFuture();
        }
        return null;
    }

    private CompletableFuture<OpenApiResult<S>> nextFuture()
    {
        checkState(!isFinished(), "Unexpectedly tried to make request when finished.");
        try {
            return Failsafe.with(retryPolicy).getAsync(() ->
                    httpClient.execute(authenticator.filterRequest(nextRequest), jsonResponseHandler));
        }
        catch (RetryableException e) {
            throw new TrinoException(
                    OPENAPI_GENERIC_EXTERNAL_ERROR,
                    "Request failed (status %s) after %s retries for %s".formatted(e.statusCode(), MAX_RETRIES, nextRequest.getUri().getPath()));
        }
    }

    private static long parseRetryAfterMillis(Response response)
    {
        String retryAfter = response.getHeader("Retry-After");
        if (retryAfter == null) {
            return DEFAULT_RETRY_AFTER_MILLIS;
        }
        try {
            return Math.min(Long.parseLong(retryAfter.trim()) * 1000L, MAX_RETRY_AFTER_MILLIS);
        }
        catch (NumberFormatException e) {
            return DEFAULT_RETRY_AFTER_MILLIS;
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        if (pageFuture != null) {
            pageFuture.cancel(true);
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return pageFuture == null ? NOT_BLOCKED : pageFuture;
    }

    record OpenApiResult<P>(Iterator<SourcePage> pageIterator, P newPaginationState)
    {
    }

    private static final class RetryableException
            extends RuntimeException
    {
        private final int statusCode;
        private final long retryAfterMillis;

        RetryableException(int statusCode, long retryAfterMillis)
        {
            super("Retryable status %s, retry after %sms".formatted(statusCode, retryAfterMillis), null, true, false);
            this.statusCode = statusCode;
            this.retryAfterMillis = retryAfterMillis;
        }

        int statusCode()
        {
            return statusCode;
        }

        long retryAfterMillis()
        {
            return retryAfterMillis;
        }
    }
}
