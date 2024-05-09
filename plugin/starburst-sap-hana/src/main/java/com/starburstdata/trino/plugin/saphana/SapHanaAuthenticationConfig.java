/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class SapHanaAuthenticationConfig
{
    public static final String PASSWORD = "PASSWORD";
    private String authenticationType = PASSWORD;

    @NotNull
    public String getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("sap-hana.authentication.type")
    @ConfigDescription("SAP HANA authentication mechanism type")
    public SapHanaAuthenticationConfig setAuthenticationType(String authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }
}
