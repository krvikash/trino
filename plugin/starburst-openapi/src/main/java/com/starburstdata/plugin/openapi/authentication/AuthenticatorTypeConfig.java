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
import jakarta.validation.constraints.NotNull;

public class AuthenticatorTypeConfig
{
    public enum AuthenticatorType
    {
        NONE,
        APIKEY,
        OAUTH2,
        /**/
    }

    private AuthenticatorType type = AuthenticatorType.NONE;

    @NotNull
    public AuthenticatorType getType()
    {
        return type;
    }

    @Config("openapi.security-scheme.type")
    @ConfigDescription("The type of a configuration-defined security scheme to apply to requests.")
    public AuthenticatorTypeConfig setType(AuthenticatorType type)
    {
        this.type = type;
        return this;
    }
}
