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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSnowflakePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertThat(connectorFactories).hasSize(3);

        connectorFactory(connectorFactories, "deprecated_snowflake_jdbc")
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

        connectorFactory(connectorFactories, "snowflake_parallel")
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
    public void testSnowflakeJdbcConnectorNameThrows()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());

        assertThatThrownBy(() -> connectorFactory(connectorFactories, "snowflake_jdbc")
                .create("my_catalog",
                        ImmutableMap.of(),
                        new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        """
                        The snowflake_jdbc connector is DEPRECATED.
                        It will be removed in a future release.
                        Please migrate to the snowflake_parallel connector.
                        If you need to continue using the JDBC connector temporarily, set connector.name=deprecated_snowflake_jdbc.
                        """);
    }

    @Test
    public void testCreateConnectorWithProxySettings()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());

        for (ConnectorFactory factory : connectorFactories) {
            if (factory.getName().equals("snowflake_jdbc")) {
                continue;
            }
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

    private static ConnectorFactory connectorFactory(List<ConnectorFactory> factories, String name)
    {
        return factories.stream()
                .filter(f -> f.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No factory named: " + name));
    }
}
