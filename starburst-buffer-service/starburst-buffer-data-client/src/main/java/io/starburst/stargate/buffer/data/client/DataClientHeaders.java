/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import io.airlift.http.client.HeaderName;

public final class DataClientHeaders
{
    public static final String MAX_WAIT = "X-Trino-Data-Client-Max-Wait";
    public static final HeaderName MAX_WAIT_HEADER = HeaderName.of(MAX_WAIT);

    private DataClientHeaders() {}
}
