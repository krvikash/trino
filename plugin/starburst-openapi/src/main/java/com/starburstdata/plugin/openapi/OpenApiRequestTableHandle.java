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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.trino.spi.connector.ConnectorTableHandle;

import java.net.URI;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.net.MediaType.JSON_UTF_8;

public record OpenApiRequestTableHandle(
        @JsonProperty("path") String path)
        implements ConnectorTableHandle
{
    private static final String USER_AGENT_VALUE = "starburst-openapi";
    private static final String JSON_MEDIA_TYPE = JSON_UTF_8.toString();

    @JsonIgnore
    public Request toInitialRequest(URI baseUri)
    {
        return Request.builder()
                .setMethod("GET")
                .setUri(HttpUriBuilder.uriBuilderFrom(baseUri)
                        .appendPath(path)
                        .build())
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(CONTENT_TYPE, JSON_MEDIA_TYPE)
                .addHeader(ACCEPT, JSON_MEDIA_TYPE)
                .build();
    }
}
