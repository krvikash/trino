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

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.swagger.v3.oas.models.security.SecurityScheme.In.HEADER;

final class TestApiKeyAuthenticatorConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ApiKeyAuthenticatorConfig.class)
                .setSecret(null)
                .setIn(null)
                .setName(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.security-scheme.secret", "my-api-key")
                .put("openapi.security-scheme.in", "HEADER")
                .put("openapi.security-scheme.name", "X-API-Key")
                .buildOrThrow();

        ApiKeyAuthenticatorConfig expected = new ApiKeyAuthenticatorConfig()
                .setSecret("my-api-key")
                .setIn(HEADER)
                .setName("X-API-Key");

        assertFullMapping(properties, expected);
    }

    @Test
    void testSecretValidation()
    {
        assertFailsValidation(
                new ApiKeyAuthenticatorConfig()
                        .setIn(HEADER)
                        .setName("X-API-Key"),
                "secret",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testInValidation()
    {
        assertFailsValidation(
                new ApiKeyAuthenticatorConfig()
                        .setSecret("my-api-key")
                        .setName("X-API-Key"),
                "in",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testNameValidation()
    {
        assertFailsValidation(
                new ApiKeyAuthenticatorConfig()
                        .setSecret("my-api-key")
                        .setIn(HEADER),
                "name",
                "must not be null",
                NotNull.class);
    }
}
