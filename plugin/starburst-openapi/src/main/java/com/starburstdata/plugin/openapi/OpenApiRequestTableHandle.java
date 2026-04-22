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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorTableHandle;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public record OpenApiRequestTableHandle(
        String path,
        URI uri,
        List<String> parameterNames,
        List<String> providedParameters)
        implements ConnectorTableHandle
{
    public OpenApiRequestTableHandle
    {
        requireNonNull(path, "path is null");
        requireNonNull(uri, "uri is null");
        parameterNames = ImmutableList.copyOf(requireNonNull(parameterNames, "parameterNames is null"));
        providedParameters = ImmutableList.copyOf(requireNonNull(providedParameters, "providedParameters is null"));
    }
}
