/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.cloud;

import io.trino.filesystem.azure.AzureAuth;
import io.trino.filesystem.azure.AzureAuthOauth;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

public class TestTrinoFsCloudSpoolingStorageOnAzureOAuthFlat
        extends AbstractTestTrinoFsCloudSpoolingStorageOnAzure
{
    @Override
    protected String account()
    {
        return requireEnv("ABFS_FLAT_ACCOUNT");
    }

    @Override
    protected AzureAuth azureAuth()
    {
        String tenantId = requireEnv("ABFS_OAUTH_TENANT_ID");
        String clientId = requireEnv("ABFS_OAUTH_CLIENT_ID");
        String clientSecret = requireEnv("ABFS_OAUTH_CLIENT_SECRET");
        String clientEndpoint = "https://login.microsoftonline.com/%s/oauth2/v2.0/token".formatted(tenantId);
        return new AzureAuthOauth(clientEndpoint, tenantId, clientId, clientSecret);
    }

    @Override
    protected AccountKind accountKind()
    {
        return AccountKind.FLAT;
    }
}
