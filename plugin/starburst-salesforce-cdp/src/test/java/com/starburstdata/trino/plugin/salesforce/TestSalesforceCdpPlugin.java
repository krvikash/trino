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

import com.google.common.collect.ImmutableMap;
import com.google.inject.CreationException;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSalesforceCdpPlugin
{
    @Test
    void testCreateConnector()
    {
        Plugin plugin = new SalesforceCdpPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                        "test",
                        ImmutableMap.of(
                                "salesforce-cdp.user", "user",
                                "salesforce-cdp.password", "password",
                                "salesforce-cdp.oauth2.client-id", "clientid",
                                "salesforce-cdp.oauth2.client-secret", "secret"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    void testCreateConnectorWithForcedMapping()
    {
        Plugin plugin = new SalesforceCdpPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "salesforce-cdp.user", "user",
                        "salesforce-cdp.password", "password",
                        "jdbc-types-mapped-to-varchar", "varchar",
                        "salesforce-cdp.oauth2.client-id", "clientid",
                        "salesforce-cdp.oauth2.client-secret", "secret"),
                new TestingConnectorContext()))
                .isInstanceOf(CreationException.class)
                .hasStackTraceContaining("VerifyException: Salesforce CDP does not support forced type mapping to VARCHAR");
    }
}
