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

import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.NOOP_LICENSE_MANAGER;

public class TestingStargateParallelPlugin
        extends StargateParallelPlugin
{
    private final boolean enableWrites;

    public TestingStargateParallelPlugin(boolean enableWrites)
    {
        this.enableWrites = enableWrites;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactories(NOOP_LICENSE_MANAGER, enableWrites);
    }
}
