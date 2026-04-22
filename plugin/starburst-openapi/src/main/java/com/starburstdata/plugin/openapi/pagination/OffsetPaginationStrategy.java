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

import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

/**
 * Pagination strategy for offset-based APIs ({@code offset} / {@code limit}).
 *
 * <p>Termination is determined by a boolean field in the response body, located via a
 * JSON Pointer (RFC 6901). When that field is {@code true} the last page has been reached.
 *
 * <p>The first request is issued as-is. Subsequent requests have {@code offset} and {@code limit}
 * query parameters set explicitly.
 */
public class OffsetPaginationStrategy
        implements OpenApiPaginationStrategy<OffsetPaginationStrategy.OffsetState>
{
    private final String offsetParameterName;
    private final Optional<JsonPointer> dataFieldJsonPointer;

    public OffsetPaginationStrategy(String offsetParameterName, Optional<String> dataFieldJsonPointer)
    {
        this.offsetParameterName = requireNonNull(offsetParameterName, "offsetParameterName is null");
        this.dataFieldJsonPointer = requireNonNull(dataFieldJsonPointer, "dataFieldJsonPointer is null").map(JsonPointer::compile);
    }

    @Override
    public OffsetState initialState()
    {
        return new OffsetState(0, 0, false);
    }

    @Override
    public OffsetState nextStateFromResponse(OffsetState currentState, Response response, JsonNode responseBody)
    {
        JsonNode array = dataFieldJsonPointer.map(responseBody::at).orElse(responseBody);
        if (array.isMissingNode() || !array.isArray() || array.isEmpty()) {
            return new OffsetState(0, 0, true);
        }
        return new OffsetState(currentState.offset() + array.size(), currentState.pageSize(), false);
    }

    @Override
    public Request nextRequestFromState(Request currentRequest, OffsetState state)
    {
        if (isFinished(state)) {
            throw new IllegalStateException("nextRequestFromState called on a finished state");
        }
        return Request.Builder.fromRequest(currentRequest)
                .setUri(uriBuilderFrom(currentRequest.getUri())
                        .replaceParameter(offsetParameterName, String.valueOf(state.offset()))
                        .build())
                .build();
    }

    @Override
    public boolean isFinished(OffsetState state)
    {
        return state.finished();
    }

    public record OffsetState(long offset, long pageSize, boolean finished) {}
}
