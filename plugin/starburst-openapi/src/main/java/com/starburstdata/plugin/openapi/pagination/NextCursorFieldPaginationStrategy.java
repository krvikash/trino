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
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;

import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

/**
 * Pagination strategy for cursor-based APIs that return the next cursor in a dedicated response field.
 *
 * <p>The cursor is read using {@code nextCursorFieldJsonPointer}, a JSON Pointer (RFC 6901) into the
 * response body (e.g. {@code /response_metadata/next_cursor}). A missing, null, or empty value
 * signals the last page.
 *
 * <p>The first request is issued without a cursor parameter. Subsequent requests include the cursor
 * returned by the previous response as a query parameter named {@code cursorParameterName}.
 *
 * <p>Example (Slack-style): given {@code {"response_metadata": {"next_cursor": "abc123"}}} with
 * {@code nextCursorFieldJsonPointer = "/response_metadata/next_cursor"} and
 * {@code cursorParameterName = "cursor"}, the next request will include {@code ?cursor=abc123}.
 */
public class NextCursorFieldPaginationStrategy
        implements OpenApiPaginationStrategy<NextCursorFieldPaginationStrategy.CursorState>
{
    private final String cursorParameterName;
    private final JsonPointer nextCursorFieldJsonPointer;

    public NextCursorFieldPaginationStrategy(String cursorParameterName, String nextCursorFieldJsonPointer)
    {
        this.cursorParameterName = requireNonNull(cursorParameterName, "cursorParameterName is null");
        this.nextCursorFieldJsonPointer = JsonPointer.compile(requireNonNull(nextCursorFieldJsonPointer, "nextCursorField is null"));
    }

    @Override
    public CursorState initialState()
    {
        return new CursorState(Optional.empty(), false);
    }

    @Override
    public CursorState nextStateFromResponse(CursorState currentState, Response response, JsonNode responseBody)
    {
        JsonNode cursorNode = responseBody.at(nextCursorFieldJsonPointer);
        if (cursorNode.isMissingNode() || cursorNode.isNull() || cursorNode.asText().isEmpty()) {
            return new CursorState(Optional.empty(), true);
        }
        return new CursorState(Optional.of(cursorNode.asText()), false);
    }

    @Override
    public Request nextRequestFromState(Request currentRequest, CursorState state)
    {
        if (isFinished(state)) {
            throw new IllegalStateException("nextRequestFromState called on a finished state");
        }
        HttpUriBuilder uriBuilder = uriBuilderFrom(currentRequest.getUri());
        state.cursor().ifPresent(cursor -> uriBuilder.replaceParameter(cursorParameterName, cursor));
        return Request.Builder.fromRequest(currentRequest)
                .setUri(uriBuilder.build())
                .build();
    }

    @Override
    public boolean isFinished(CursorState state)
    {
        return state.finished();
    }

    public record CursorState(Optional<String> cursor, boolean finished) {}
}
