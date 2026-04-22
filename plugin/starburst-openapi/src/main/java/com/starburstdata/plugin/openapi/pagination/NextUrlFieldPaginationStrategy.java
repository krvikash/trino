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
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Pagination strategy that follows a next-page URL embedded in the response body.
 *
 * <p>The URL is located via {@code nextUrlFieldJsonPointer}, a JSON Pointer (RFC 6901)
 * into the response body (e.g. {@code /paging/next}). When that field is absent, null,
 * or empty the last page has been reached. The URL replaces the entire request URI for
 * the next page, so all query parameters are taken from the URL itself — no additional
 * parameters are injected.
 *
 * <p>Example (Instagram Graph API-style): given
 * {@code {"paging": {"next": "https://api.example.com/v1/items?after=abc&limit=2"}}}
 * the next request will use that URL verbatim.
 *
 * @see <a href="https://developers.facebook.com/docs/graph-api/results">Facebook Graph API Paginated Results</a>
 */
public class NextUrlFieldPaginationStrategy
        implements OpenApiPaginationStrategy<NextUrlFieldPaginationStrategy.NextUrlState>
{
    private final JsonPointer nextUrlFieldJsonPointer;

    public NextUrlFieldPaginationStrategy(String nextUrlFieldJsonPointer)
    {
        this.nextUrlFieldJsonPointer = JsonPointer.compile(requireNonNull(nextUrlFieldJsonPointer, "nextUrlFieldJsonPointer is null"));
    }

    @Override
    public NextUrlState initialState()
    {
        return new NextUrlState(Optional.empty(), false);
    }

    @Override
    public NextUrlState nextStateFromResponse(NextUrlState currentState, Response response, JsonNode responseBody)
    {
        JsonNode urlNode = responseBody.at(nextUrlFieldJsonPointer);
        if (urlNode.isMissingNode() || urlNode.isNull() || urlNode.asText().isEmpty()) {
            return new NextUrlState(Optional.empty(), true);
        }
        return new NextUrlState(Optional.of(urlNode.asText()), false);
    }

    @Override
    public Request nextRequestFromState(Request currentRequest, NextUrlState state)
    {
        if (isFinished(state)) {
            throw new IllegalStateException("nextRequestFromState called on a finished state");
        }
        if (state.nextUrl().isEmpty()) {
            return currentRequest;
        }
        return Request.Builder.fromRequest(currentRequest)
                .setUri(URI.create(state.nextUrl().get()))
                .build();
    }

    @Override
    public boolean isFinished(NextUrlState state)
    {
        return state.finished();
    }

    public record NextUrlState(Optional<String> nextUrl, boolean finished) {}
}
