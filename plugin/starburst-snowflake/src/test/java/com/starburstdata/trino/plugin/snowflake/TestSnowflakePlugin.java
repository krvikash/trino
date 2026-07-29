/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestSnowflakePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        getOnlyElement(plugin.getConnectorFactories())
                .create("test",
                        ImmutableMap.of(
                                "connection-url", "jdbc:snowflake:test",
                                "connection-user", "test",
                                "snowflake.connection-private-key", "test",
                                "snowflake.connection-private-key.passphrase", "test",
                                "snowflake.role", "test",
                                "snowflake.database", "test",
                                "snowflake.warehouse", "test"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testCreateConnectorWithProxySettings()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                        "test",
                        ImmutableMap.of(
                                "connection-url", "jdbc:snowflake:test",
                                "connection-user", "test",
                                "snowflake.connection-private-key", "test",
                                "snowflake.connection-private-key.passphrase", "test",
                                "snowflake.database", "test",
                                "snowflake.warehouse", "test",
                                "snowflake.proxy.enabled", "true",
                                "snowflake.proxy.host", "localhost",
                                "snowflake.proxy.port", "9000"),
                        new TestingConnectorContext())
                .shutdown();
    }
}
