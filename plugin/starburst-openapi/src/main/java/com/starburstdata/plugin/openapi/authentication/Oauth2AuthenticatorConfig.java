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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Set;

public class Oauth2AuthenticatorConfig
{
    private String clientId;
    private String clientSecret;
    private URI tokenUrl;
    private Set<String> scopes;

    @NotNull
    public String getClientId()
    {
        return clientId;
    }

    @Config("openapi.security-scheme.client-id")
    @ConfigDescription("The client-id for a client-credentials flow in an oauth2 security scheme.")
    public Oauth2AuthenticatorConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    @SuppressWarnings("unused")
    @AssertTrue(message = "Client-id cannot use : character.")
    public boolean isClientIdValid()
    {
        return clientId == null || !clientId.contains(":");
    }

    @NotNull
    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("openapi.security-scheme.client-secret")
    @ConfigDescription("The client-secret for a client-credentials flow in an oauth2 security scheme.")
    @ConfigSecuritySensitive
    public Oauth2AuthenticatorConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @NotNull
    public URI getTokenUrl()
    {
        return tokenUrl;
    }

    @Config("openapi.security-scheme.token-url")
    @ConfigDescription("The token URL of a client credentials flow in an oauth2 security scheme.")
    public Oauth2AuthenticatorConfig setTokenUrl(URI tokenUrl)
    {
        this.tokenUrl = tokenUrl;
        return this;
    }

    public Set<String> getScopes()
    {
        return scopes;
    }

    @Config("openapi.security-scheme.scopes")
    @ConfigDescription("The scopes to request in a client credentials flow in an oauth2 security scheme.")
    public Oauth2AuthenticatorConfig setScopes(Set<String> scopes)
    {
        this.scopes = scopes;
        return this;
    }

    @SuppressWarnings("unused")
    @AssertTrue(message = "Scopes must be non-empty and contain characters within the ranges defined by RFC-6749")
    public boolean isScopesValid()
    {
        return scopes == null || scopes.stream().allMatch(Oauth2Authenticator::validScope);
    }
}
