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

import java.util.Set;

/**
 * @param <S> The class representing the pagination "state".
 */
public interface OpenApiPaginationStrategy<S>
{
    S initialState();

    S nextStateFromResponse(S currentState, Response response, JsonNode responseBody);

    Request nextRequestFromState(Request currentRequest, S state);

    boolean isFinished(S state);

    /**
     * Returns the names of all query parameters that this strategy may add to requests.
     * Used to exclude pagination parameters from the connector's advertised table-function arguments.
     */
    default Set<String> getParameterNames()
    {
        return Set.of();
    }

    /**
     * Returns JSON Pointers (RFC 6901) to fields in the response body that this strategy reads
     * to determine pagination state. The caller uses this set to verify that the required fields
     * are present before invoking the strategy; if any are absent it may fall back to a read-once
     * strategy.
     *
     * <p>The default implementation returns an empty set, meaning the strategy makes no
     * assumptions about response body structure.
     */
    default Set<JsonPointer> requiredResponseColumnsPaths()
    {
        return Set.of();
    }
}
