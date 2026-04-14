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
import com.google.common.collect.ImmutableSet;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

final class TestOauth2AuthenticatorConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(Oauth2AuthenticatorConfig.class)
                .setClientId(null)
                .setClientSecret(null)
                .setTokenUrl(null)
                .setScopes(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.security-scheme.client-id", "my-client-id")
                .put("openapi.security-scheme.client-secret", "my-client-secret")
                .put("openapi.security-scheme.token-url", "https://auth.example.com/token")
                .put("openapi.security-scheme.scopes", "read,write")
                .buildOrThrow();

        Oauth2AuthenticatorConfig expected = new Oauth2AuthenticatorConfig()
                .setClientId("my-client-id")
                .setClientSecret("my-client-secret")
                .setTokenUrl(URI.create("https://auth.example.com/token"))
                .setScopes(ImmutableSet.of("read", "write"));

        assertFullMapping(properties, expected);
    }

    @Test
    void testClientIdValidation()
    {
        assertFailsValidation(
                new Oauth2AuthenticatorConfig()
                        .setClientSecret("my-client-secret")
                        .setTokenUrl(URI.create("https://auth.example.com/token"))
                        .setScopes(ImmutableSet.of("read")),
                "clientId",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testClientSecretValidation()
    {
        assertFailsValidation(
                new Oauth2AuthenticatorConfig()
                        .setClientId("my-client-id")
                        .setTokenUrl(URI.create("https://auth.example.com/token"))
                        .setScopes(ImmutableSet.of("read")),
                "clientSecret",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testTokenUrlValidation()
    {
        assertFailsValidation(
                new Oauth2AuthenticatorConfig()
                        .setClientId("my-client-id")
                        .setClientSecret("my-client-secret")
                        .setScopes(ImmutableSet.of("read")),
                "tokenUrl",
                "must not be null",
                NotNull.class);
    }

    @Test
    void testScopesWithInvalidCharactersValidation()
    {
        assertFailsValidation(
                new Oauth2AuthenticatorConfig()
                        .setClientId("my-client-id")
                        .setClientSecret("my-client-secret")
                        .setTokenUrl(URI.create("https://auth.example.com/token"))
                        .setScopes(ImmutableSet.of("invalid scope")),
                "scopesValid",
                "Scopes must be non-empty and contain characters within the ranges defined by RFC-6749",
                AssertTrue.class);

        assertFailsValidation(
                new Oauth2AuthenticatorConfig()
                        .setClientId("my-client-id")
                        .setClientSecret("my-client-secret")
                        .setTokenUrl(URI.create("https://auth.example.com/token"))
                        .setScopes(ImmutableSet.of("")),
                "scopesValid",
                "Scopes must be non-empty and contain characters within the ranges defined by RFC-6749",
                AssertTrue.class);
    }

    @Test
    void testClientIdWithColonValidation()
    {
        assertFailsValidation(
                new Oauth2AuthenticatorConfig()
                        .setClientId("client:id")
                        .setClientSecret("my-client-secret")
                        .setTokenUrl(URI.create("https://auth.example.com/token"))
                        .setScopes(ImmutableSet.of("read")),
                "clientIdValid",
                "Client-id cannot use : character.",
                AssertTrue.class);
    }
}
