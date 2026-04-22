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
import java.util.Set;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

/**
 * Pagination strategy that derives the next cursor from the last element of a response array.
 *
 * <p>When {@code dataFieldJsonPointer} is present, the array is located via JSON Pointer (RFC 6901)
 * (e.g. {@code /data}). When absent, the response body itself is expected to be the array.
 * The cursor value is read via {@code cursorFieldJsonPointer} (RFC 6901) on the last element of
 * that array (e.g. {@code /id}). When the array is absent or empty the last page has been reached.
 *
 * <p>Example (Stripe-style): given {@code {"data": [..., {"id": "cus_123"}]}} with
 * {@code dataFieldJsonPointer = "/data"}, {@code cursorFieldJsonPointer = "/id"}, and {@code cursorParameterName = "starting_after"},
 * the next request will include {@code ?starting_after=cus_123}.
 */
public class LastElementCursorFieldPaginationStrategy
        implements OpenApiPaginationStrategy<LastElementCursorFieldPaginationStrategy.CursorState>
{
    private final String cursorParameterName;
    private final Optional<JsonPointer> dataFieldJsonPointer;
    private final JsonPointer cursorFieldJsonPointer;

    public LastElementCursorFieldPaginationStrategy(
            String cursorParameterName,
            Optional<String> dataFieldJsonPointer,
            String cursorFieldJsonPointer)
    {
        this.cursorParameterName = requireNonNull(cursorParameterName, "cursorParameterName is null");
        this.dataFieldJsonPointer = requireNonNull(dataFieldJsonPointer, "dataFieldJsonPointer is null").map(JsonPointer::compile);
        this.cursorFieldJsonPointer = JsonPointer.compile(requireNonNull(cursorFieldJsonPointer, "cursorFieldJsonPointer is null"));
    }

    @Override
    public CursorState initialState()
    {
        return new CursorState(Optional.empty(), false);
    }

    @Override
    public CursorState nextStateFromResponse(CursorState currentState, Response response, JsonNode responseBody)
    {
        JsonNode array = dataFieldJsonPointer.map(responseBody::at).orElse(responseBody);
        if (array.isMissingNode() || !array.isArray() || array.isEmpty()) {
            return CursorState.done();
        }
        JsonNode lastElement = array.get(array.size() - 1);
        JsonNode cursorNode = lastElement.at(cursorFieldJsonPointer);
        if (cursorNode.isMissingNode() || cursorNode.isNull() || cursorNode.asText().isEmpty()) {
            return CursorState.done();
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

    @Override
    public Set<String> getParameterNames()
    {
        return Set.of(cursorParameterName);
    }

    public record CursorState(Optional<String> cursor, boolean finished)
    {
        public static CursorState done()
        {
            return new CursorState(Optional.empty(), true);
        }
    }
}
