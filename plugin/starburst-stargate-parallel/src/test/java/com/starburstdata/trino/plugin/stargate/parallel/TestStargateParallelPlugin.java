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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TestStargateParallelPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StargateParallelPlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertThat(connectorFactories).hasSize(1);

        ConnectorFactory factory = connectorFactories.get(0);

        Map<String, String> properties = ImmutableMap.of(
                "connection-url", "jdbc:trino://localhost:8080/test",
                "connection-user", "presto",
                "ssl.enabled", "true",
                "ssl.truststore.password", "password");

        var ignored = factory.create("test", properties, new TestingConnectorContext());
    }
}
