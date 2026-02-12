/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.units.Duration;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.CLIENT_ID_HEADER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class DataRequestHelper
{
    private static final Duration CLIENT_MAX_WAIT_LIMIT = succinctDuration(60, TimeUnit.SECONDS);

    private DataRequestHelper() {}

    public static Duration getAsyncTimeout(@Nullable Duration clientMaxWait)
    {
        if (clientMaxWait == null || clientMaxWait.toMillis() == 0 || clientMaxWait.compareTo(CLIENT_MAX_WAIT_LIMIT) > 0) {
            return CLIENT_MAX_WAIT_LIMIT;
        }
        return succinctDuration(clientMaxWait.toMillis() * 0.95, MILLISECONDS);
    }

    public static String getClientId(HttpServletRequest request)
    {
        String clientId = request.getHeader(CLIENT_ID_HEADER);
        if (clientId == null) {
            clientId = request.getRemoteHost();
        }
        return clientId;
    }
}
