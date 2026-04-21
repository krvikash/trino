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
import io.swagger.v3.oas.models.security.SecurityScheme;
import jakarta.validation.constraints.NotNull;

public class ApiKeyAuthenticatorConfig
{
    private String secret;
    private SecurityScheme.In in;
    private String name;

    @NotNull
    public String getSecret()
    {
        return secret;
    }

    @Config("openapi.security-scheme.secret")
    @ConfigDescription("The API-key secret of an api-key security scheme.")
    @ConfigSecuritySensitive
    public ApiKeyAuthenticatorConfig setSecret(String secret)
    {
        this.secret = secret;
        return this;
    }

    @NotNull
    public SecurityScheme.In getIn()
    {
        return in;
    }

    @Config("openapi.security-scheme.in")
    @ConfigDescription("The location to place the secret in an api-key security scheme.")
    public ApiKeyAuthenticatorConfig setIn(SecurityScheme.In in)
    {
        this.in = in;
        return this;
    }

    @NotNull
    public String getName()
    {
        return name;
    }

    @Config("openapi.security-scheme.name")
    @ConfigDescription("The name of the parameter, header, or cookie value where the secret is used in an api-key security scheme.")
    public ApiKeyAuthenticatorConfig setName(String name)
    {
        this.name = name;
        return this;
    }
}
