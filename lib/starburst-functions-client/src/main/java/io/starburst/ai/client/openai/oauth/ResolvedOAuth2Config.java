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

import io.starburst.ai.model.ConnectionInfo.OAuth2GrantType;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Post-secrets-resolution snapshot of an OAuth2Config. Never serialized back to the public model;
 * kept internal to the functions-client module so resolved secrets do not leak into the JSON layer.
 */
public record ResolvedOAuth2Config(
        OAuth2GrantType grantType,
        String tokenUrl,
        String clientId,
        String clientSecret,
        Optional<String> scope,
        Optional<String> audience)
{
    public ResolvedOAuth2Config
    {
        requireNonNull(grantType, "grantType is null");
        requireNonNull(tokenUrl, "tokenUrl is null");
        requireNonNull(clientId, "clientId is null");
        requireNonNull(clientSecret, "clientSecret is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(audience, "audience is null");
    }

    @Override
    public String toString()
    {
        return format(
                "ResolvedOAuth2Config{grantType=%s, tokenUrl=%s, clientId=***, clientSecret=***, scope=%s, audience=%s}",
                grantType,
                tokenUrl,
                scope,
                audience);
    }
}
