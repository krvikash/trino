/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class SalesforceCdpConfig
{
    private String user;
    private String password;
    private String clientId;
    private String clientSecret;

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("salesforce-cdp.user")
    @ConfigDescription("Username used to log in to Salesforce")
    public SalesforceCdpConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("salesforce-cdp.password")
    @ConfigDescription("Password used to log in to Salesforce")
    @ConfigSecuritySensitive
    public SalesforceCdpConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @NotNull
    public String getClientId()
    {
        return clientId;
    }

    @Config("salesforce-cdp.oauth2.client-id")
    @ConfigDescription("Client Id of Salesforce connected App")
    @ConfigSecuritySensitive
    public SalesforceCdpConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    @NotNull
    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("salesforce-cdp.oauth2.client-secret")
    @ConfigDescription("Client Secret of Salesforce connected App")
    @ConfigSecuritySensitive
    public SalesforceCdpConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }
}
