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

import io.airlift.http.client.Request;
import io.airlift.http.client.Response;

/**
 * @param <S> The class representing the pagination "state".
 */
public interface OpenApiPaginationStrategy<S>
{
    OpenApiPaginationStrategy<?> READ_ONCE_STRATEGY = new ReadOnceStrategy();

    S initialState();

    S nextStateFromResponse(S currentState, Response response);

    Request nextRequestFromState(Request currentRequest, S state);

    boolean isFinished(S state);
}
