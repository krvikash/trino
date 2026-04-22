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

import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.http.client.HeaderNames;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import jakarta.ws.rs.core.Link;

import java.net.URI;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Pagination strategy that follows a next-page URL from the HTTP {@code Link} response header.
 *
 * <p>Each response is inspected for a {@code Link} header containing a relation of type
 * {@code next} (RFC 5988 / RFC 8288). When such a link is found, its URI replaces the full
 * request URI for the next page — all query parameters come from the link URL itself.
 * Pagination stops when no {@code rel="next"} link is present.
 *
 * <p>Example (GitHub REST API style):
 * {@code Link: <https://api.example.com/v1/items?page=2>; rel="next", <https://api.example.com/v1/items?page=5>; rel="last"}
 */
public class LinkHeaderPaginationStrategy
        implements OpenApiPaginationStrategy<LinkHeaderPaginationStrategy.LinkHeaderState>
{
    public LinkHeaderPaginationStrategy() {}

    @Override
    public LinkHeaderState initialState()
    {
        return new LinkHeaderState(Optional.empty(), false);
    }

    @Override
    public LinkHeaderState nextStateFromResponse(LinkHeaderState currentState, Response response, JsonNode responseBody)
    {
        Optional<String> linkHeader = response.getHeader(HeaderNames.LINK);
        if (linkHeader.isEmpty()) {
            return new LinkHeaderState(Optional.empty(), true);
        }
        Optional<String> nextUrl = Arrays.stream(linkHeader.orElseThrow().split(",\\s*(?=<)"))
                .map(segment -> {
                    try {
                        return Link.valueOf(segment.trim());
                    }
                    catch (IllegalArgumentException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .filter(link -> "next".equals(link.getRel()))
                .map(link -> link.getUri().toString())
                .findFirst();
        return new LinkHeaderState(nextUrl, nextUrl.isEmpty());
    }

    @Override
    public Request nextRequestFromState(Request currentRequest, LinkHeaderState state)
    {
        if (isFinished(state)) {
            throw new IllegalStateException("nextRequestFromState called on a finished state");
        }
        if (state.nextUrl().isEmpty()) {
            // Initial state before any response has been received — use the original request URI.
            return currentRequest;
        }
        return Request.Builder.fromRequest(currentRequest)
                .setUri(URI.create(state.nextUrl().get()))
                .build();
    }

    @Override
    public boolean isFinished(LinkHeaderState state)
    {
        return state.finished();
    }

    public record LinkHeaderState(Optional<String> nextUrl, boolean finished) {}
}
