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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.airlift.http.client.HeaderNames;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class OpenApiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final OpenApiSpec openApiSpec;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    @Inject
    public OpenApiPageSourceProvider(
            OpenApiSpec openApiSpec,
            ObjectMapper objectMapper,
            @ForOpenApi HttpClient httpClient)
    {
        this.openApiSpec = requireNonNull(openApiSpec, "openApiSpec is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        OpenApiRequestTableHandle handle = (OpenApiRequestTableHandle) table;
        return new OpenApiPageSource<>(
                httpClient,
                openApiSpec.getPaginationStrategy(handle.path()),
                toInitialRequest(handle),
                openApiSpec.getDecoder(handle.path()),
                columns,
                objectMapper,
                openApiSpec.getAuthenticator(handle.path()));
    }

    private static Request toInitialRequest(OpenApiRequestTableHandle handle)
    {
        return Request.builder()
                .setMethod("GET")
                .setUri(handle.uri())
                // Jetty caps redirect chains at 8 by default; exceeding it throws an exception
                // that surfaces through ReadFromJson.handleException rather than looping forever.
                .setFollowRedirects(true)
                .addHeader(HeaderNames.USER_AGENT, "starburst-openapi")
                .addHeader(HeaderNames.CONTENT_TYPE, JSON_UTF_8.toString())
                .addHeader(HeaderNames.ACCEPT, JSON_UTF_8.toString())
                .build();
    }
}
