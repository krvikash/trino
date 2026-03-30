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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.starburstdata.plugin.openapi.authentication.OpenApiAuthenticator;
import com.starburstdata.plugin.openapi.conversions.OpenApiDecoder;
import com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;

/**
 * @param <S> Pagination state type.
 */
public class OpenApiPageSource<S>
        implements ConnectorPageSource
{
    private final HttpClient httpClient;
    private final OpenApiPaginationStrategy<S> paginationStrategy;
    private final OpenApiDecoder decoder;
    private final List<ColumnHandle> columnHandles;
    private CompletableFuture<OpenApiResult<S>> pageFuture;
    private S currentState;
    private Request currentRequest;
    private final OpenApiAuthenticator authenticator;
    private final ObjectMapper objectMapper;
    private final ResponseHandler<OpenApiResult<S>, RuntimeException> jsonResponseHandler = new ReadFromJson();

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
        this.currentRequest = requireNonNull(initialRequest, "initialRequest is null");
        this.decoder = requireNonNull(decoder, "decoder is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.authenticator = requireNonNull(authenticator, "authenticator is null");
    }

    private class ReadFromJson
            implements ResponseHandler<OpenApiResult<S>, RuntimeException>
    {
        @Override
        public OpenApiResult<S> handleException(Request request, Exception exception)
                throws RuntimeException
        {
            throw new RuntimeException(exception);
        }

        @Override
        public OpenApiResult<S> handle(Request request, Response response)
                throws RuntimeException
        {
            int statusCode = response.getStatusCode();
            if (statusCode != HttpStatus.OK.code()) {
                throw new RuntimeException("Non-200 response status: %s".formatted(statusCode));
            }
            final JsonNode root;
            try {
                root = objectMapper.readTree(response.getInputStream());
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read JSON from response", e);
            }
            return new OpenApiResult<>(
                    decoder.decodeToPage(root, columnHandles),
                    paginationStrategy.nextStateFromResponse(currentState, response));
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
        return paginationStrategy.isFinished(currentState);
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            return null;
        }

        if (pageFuture != null && pageFuture.isDone()) {
            OpenApiResult<S> result = getFutureValue(pageFuture);
            currentState = result.newPaginationState();
            if (!isFinished()) {
                currentRequest = paginationStrategy.nextRequestFromState(currentRequest, currentState);
                pageFuture = nextFuture();
            }
            return SourcePage.create(result.page());
        }

        if (pageFuture == null) {
            pageFuture = nextFuture();
        }
        return null;
    }

    private CompletableFuture<OpenApiResult<S>> nextFuture()
    {
        checkState(!isFinished(), "Unexpectedly tried to make request when finished.");
        return toCompletableFuture(httpClient.executeAsync(
                authenticator.filterRequest(currentRequest),
                jsonResponseHandler));
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

    record OpenApiResult<P>(Page page, P newPaginationState)
    {
    }
}
