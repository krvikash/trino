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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestSalesforceCdpConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SalesforceCdpConfig.class)
                .setUser(null)
                .setPassword(null)
                .setClientId(null)
                .setClientSecret(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("salesforce-cdp.user", "user")
                .put("salesforce-cdp.password", "password")
                .put("salesforce-cdp.oauth2.client-id", "client-id")
                .put("salesforce-cdp.oauth2.client-secret", "client-secret")
                .buildOrThrow();

        SalesforceCdpConfig expected = new SalesforceCdpConfig()
                .setUser("user")
                .setPassword("password")
                .setClientId("client-id")
                .setClientSecret("client-secret");

        assertFullMapping(properties, expected);
    }
}
