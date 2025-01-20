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
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void testGetSecuritySensitivePropertyNames()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertThat(connectorFactories).hasSize(1);

        Map<String, String> config = ImmutableMap.of(
                "non-existent-property", "value",
                "bootstrap.quiet", "true",
                "snowflake.proxy.enabled", "true",
                "snowflake.proxy.password", "password",
                "snowflake.proxy.username", "user");

        for (ConnectorFactory factory : connectorFactories) {
            Set<String> sensitiveProperties = factory.getSecuritySensitivePropertyNames("catalog", config, new TestingConnectorContext());

            assertThat(sensitiveProperties).containsExactlyInAnyOrder("non-existent-property", "snowflake.proxy.password");
        }
    }
}
