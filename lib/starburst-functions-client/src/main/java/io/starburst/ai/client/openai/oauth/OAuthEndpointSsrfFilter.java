/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai.oauth;

import com.google.inject.Inject;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.starburst.stargate.security.OAuthServerUrlValidator;
import io.starburst.stargate.security.RejectedUrlException;

import static java.util.Objects.requireNonNull;

public class OAuthEndpointSsrfFilter
        implements HttpRequestFilter
{
    private final OAuthServerUrlValidator urlValidator;

    @Inject
    public OAuthEndpointSsrfFilter(OAuthServerUrlValidator urlValidator)
    {
        this.urlValidator = requireNonNull(urlValidator, "urlValidator is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        try {
            urlValidator.validateOAuthEndpointUrl(request.getUri());
        }
        catch (RejectedUrlException e) {
            throw new IllegalArgumentException(e.clientMessage(), e);
        }
        return request;
    }
}
