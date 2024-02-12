/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.license.LicenseVerifier;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static java.util.Objects.requireNonNull;

public class StargateParallelPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(getConnectorFactory(() -> true, false));
    }

    @VisibleForTesting
    Iterable<ConnectorFactory> getConnectorFactories(LicenseVerifier licenseVerifier, boolean enableWrites)
    {
        return ImmutableList.of(getConnectorFactory(licenseVerifier, enableWrites));
    }

    private ConnectorFactory getConnectorFactory(LicenseVerifier licenseVerifier, boolean enableWrites)
    {
        requireNonNull(licenseVerifier, "licenseVerifier is null");
        return new StargateParallelConnectorFactory("stargate_parallel", enableWrites);
    }
}
