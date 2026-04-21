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

import io.trino.spi.connector.ConnectorTableHandle;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public record OpenApiRequestTableHandle(String path, URI uri)
        implements ConnectorTableHandle
{
    public OpenApiRequestTableHandle
    {
        requireNonNull(path, "path is null");
        requireNonNull(uri, "uri is null");
    }
}
