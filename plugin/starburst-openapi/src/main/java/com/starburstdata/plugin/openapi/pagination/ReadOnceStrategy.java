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

public class ReadOnceStrategy
        implements OpenApiPaginationStrategy<Boolean>
{
    @Override
    public Boolean initialState()
    {
        // Where boolean represents "have you read one response yet?"
        return false;
    }

    @Override
    public Boolean nextStateFromResponse(Boolean currentState, Response response)
    {
        return true;
    }

    @Override
    public Request nextRequestFromState(Request currentRequest, Boolean state)
    {
        return currentRequest;
    }

    @Override
    public boolean isFinished(Boolean state)
    {
        return state;
    }
}
