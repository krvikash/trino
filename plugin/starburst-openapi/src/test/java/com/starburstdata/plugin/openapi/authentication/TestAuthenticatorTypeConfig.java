/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.authentication;

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.starburstdata.plugin.openapi.authentication.AuthenticatorTypeConfig.AuthenticatorType.NONE;
import static com.starburstdata.plugin.openapi.authentication.AuthenticatorTypeConfig.AuthenticatorType.OAUTH2;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

final class TestAuthenticatorTypeConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AuthenticatorTypeConfig.class)
                .setType(NONE));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.security-scheme.type", "OAUTH2")
                .buildOrThrow();

        AuthenticatorTypeConfig expected = new AuthenticatorTypeConfig()
                .setType(OAUTH2);

        assertFullMapping(properties, expected);
    }

    @Test
    void testTypeValidation()
    {
        assertFailsValidation(
                new AuthenticatorTypeConfig()
                        .setType(null),
                "type",
                "must not be null",
                NotNull.class);
    }
}
