/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.pagination;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.trino.spi.TrinoException;

import java.util.Set;

import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_GENERIC_EXTERNAL_ERROR;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

/**
 * Pagination strategy for page-number-based APIs.
 *
 * <p>Termination is determined by a boolean field in the response body, located via a
 * JSON Pointer (RFC 6901). When that field is {@code true} the last page has been reached.
 *
 * <p>The first request is issued as-is (the API defaults to page 1). Subsequent requests
 * have the configured page query parameter set explicitly.
 */
public class PageNumberPaginationStrategy
        implements OpenApiPaginationStrategy<PageNumberPaginationStrategy.PageState>
{
    private static final long FIRST_PAGE = 1;

    private final String pageParameterName;
    private final JsonPointer isLastPageFieldJsonPointer;

    public PageNumberPaginationStrategy(String pageParameterName, String isLastPageFieldJsonPointer)
    {
        this.pageParameterName = requireNonNull(pageParameterName, "pageParameterName is null");
        this.isLastPageFieldJsonPointer = JsonPointer.compile(requireNonNull(isLastPageFieldJsonPointer, "isLastPageFieldJsonPointer is null"));
    }

    @Override
    public PageState initialState()
    {
        return new PageState(FIRST_PAGE, false);
    }

    @Override
    public PageState nextStateFromResponse(PageState currentState, Response response, JsonNode responseBody)
    {
        JsonNode isLastPageNode = responseBody.at(isLastPageFieldJsonPointer);
        if (isLastPageNode.isMissingNode() || isLastPageNode.isNull() || isLastPageNode.asText().isEmpty()) {
            throw new TrinoException(OPENAPI_GENERIC_EXTERNAL_ERROR, "Failed to read whether last page flag from response");
        }
        boolean isLastPage = isLastPageNode.booleanValue();
        return new PageState(currentState.nextPage() + 1, isLastPage);
    }

    @Override
    public Request nextRequestFromState(Request currentRequest, PageState state)
    {
        if (isFinished(state)) {
            throw new IllegalStateException("nextRequestFromState called on a finished state");
        }
        return Request.Builder.fromRequest(currentRequest)
                .setUri(uriBuilderFrom(currentRequest.getUri())
                        .replaceParameter(pageParameterName, String.valueOf(state.nextPage()))
                        .build())
                .build();
    }

    @Override
    public boolean isFinished(PageState state)
    {
        return state.finished();
    }

    @Override
    public Set<String> getParameterNames()
    {
        return ImmutableSet.of(pageParameterName);
    }

    public record PageState(long nextPage, boolean finished) {}
}
