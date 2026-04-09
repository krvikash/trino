/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.authentication;

import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.swagger.v3.oas.models.security.SecurityScheme.In;

import java.net.HttpCookie;

import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

public class ApiKeyAuthenticator
        implements OpenApiAuthenticator
{
    private final String apiKey;
    private final In in;
    private final String name;

    public ApiKeyAuthenticator(
            String apiKey,
            In in,
            String name)
    {
        this.apiKey = requireNonNull(apiKey, "apiKey is null");
        this.in = requireNonNull(in, "in is null");
        this.name = requireNonNull(name, "name is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        Request.Builder baseBuilder = Request.Builder.fromRequest(request);
        Request.Builder withApiKeyBuilder = switch (in) {
            case In.COOKIE -> baseBuilder.addHeader(COOKIE, new HttpCookie(name, apiKey).toString());
            case In.HEADER -> baseBuilder.setHeader(HeaderName.of(name), apiKey);
            case In.QUERY -> baseBuilder.setUri(uriBuilderFrom(request.getUri())
                    .addParameter(name, apiKey)
                    .build());
        };
        return withApiKeyBuilder.build();
    }
}
