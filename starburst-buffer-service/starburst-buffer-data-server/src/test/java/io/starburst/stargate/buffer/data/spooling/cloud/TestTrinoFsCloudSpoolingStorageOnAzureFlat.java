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
import io.trino.filesystem.azure.AzureAuthAccessKey;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

public class TestTrinoFsCloudSpoolingStorageOnAzureFlat
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
        return new AzureAuthAccessKey(requireEnv("ABFS_FLAT_ACCESS_KEY"));
    }

    @Override
    protected AccountKind accountKind()
    {
        return AccountKind.FLAT;
    }
}
