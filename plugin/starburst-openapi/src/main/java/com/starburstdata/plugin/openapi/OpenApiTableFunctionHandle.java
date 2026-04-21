/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import static java.util.Objects.requireNonNull;

public record OpenApiTableFunctionHandle(OpenApiRequestTableHandle requestHandle)
        implements ConnectorTableFunctionHandle
{
    public OpenApiTableFunctionHandle
    {
        requireNonNull(requestHandle, "requestHandle is null");
    }
}
