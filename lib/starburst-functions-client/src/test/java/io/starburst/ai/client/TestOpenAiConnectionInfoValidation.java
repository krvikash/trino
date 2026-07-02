/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.ai.model.ConnectionInfo.OAuth2Config;
import io.starburst.ai.model.ConnectionInfo.OAuth2GrantType;
import io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOpenAiConnectionInfoValidation
{
    private static final OAuth2Config VALID_OAUTH2 = new OAuth2Config(
            OAuth2GrantType.CLIENT_CREDENTIALS,
            "https://idp.example/token",
            "the-client-id",
            "${ENV:CLIENT_SECRET}",
            Optional.empty(),
            Optional.empty());

    @Test
    void rejectsPlaintextClientSecret()
    {
        assertThatThrownBy(() -> new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "https://idp.example/token",
                "the-client-id",
                "plaintext-value",
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a secret reference");
    }

    @Test
    void acceptsSecretReferenceForClientSecret()
    {
        assertThatCode(() -> new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "https://idp.example/token",
                "the-client-id",
                "${ENV:CLIENT_SECRET}",
                Optional.of("scope"),
                Optional.of("aud")))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsHttpTokenUrlUnlessLocalhost()
    {
        assertThatThrownBy(() -> new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "http://idp.example/token",
                "cid",
                "${ENV:CS}",
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("https");

        assertThatCode(() -> new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "http://localhost:9000/token",
                "cid",
                "${ENV:CS}",
                Optional.empty(),
                Optional.empty()))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsBlankOAuth2Fields()
    {
        assertThatThrownBy(() -> new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "",
                "cid",
                "${ENV:CS}",
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new OAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "https://idp.example/token",
                "",
                "${ENV:CS}",
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnsupportedGrantType()
    {
        assertThatThrownBy(() -> new OAuth2Config(
                OAuth2GrantType.AUTHORIZATION_CODE,
                "https://idp.example/token",
                "cid",
                "${ENV:CS}",
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CLIENT_CREDENTIALS");
    }

    @Test
    void rejectsOAuth2CombinedWithApiKey()
    {
        assertThatThrownBy(() -> new OpenAiConnectionInfo(
                Optional.of("https://llm.example/v1"),
                Optional.of("sk-abc"),
                ImmutableMap.of(),
                Optional.of(VALID_OAUTH2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void allowsEmptyApiKeyStringAlongsideOAuth2()
    {
        // An empty apiKey should be treated as absent.
        assertThatCode(() -> new OpenAiConnectionInfo(
                Optional.of("https://llm.example/v1"),
                Optional.of(""),
                ImmutableMap.of(),
                Optional.of(VALID_OAUTH2)))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsOAuth2CombinedWithStaticAuthorizationHeader()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "Authorization", ImmutableList.of("Bearer tok"));
        assertThatThrownBy(() -> new OpenAiConnectionInfo(
                Optional.of("https://llm.example/v1"),
                Optional.empty(),
                headers,
                Optional.of(VALID_OAUTH2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Authorization");

        // Case-insensitive check.
        Map<String, List<String>> lowerHeaders = ImmutableMap.of(
                "authorization", ImmutableList.of("Bearer tok"));
        assertThatThrownBy(() -> new OpenAiConnectionInfo(
                Optional.of("https://llm.example/v1"),
                Optional.empty(),
                lowerHeaders,
                Optional.of(VALID_OAUTH2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Authorization");
    }

    @Test
    void allowsOtherAdditionalHeadersAlongsideOAuth2()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "X-Tenant", ImmutableList.of("acme"));
        assertThatCode(() -> new OpenAiConnectionInfo(
                Optional.of("https://llm.example/v1"),
                Optional.empty(),
                headers,
                Optional.of(VALID_OAUTH2)))
                .doesNotThrowAnyException();
    }

    @Test
    void toStringRedactsSecrets()
    {
        String rendered = VALID_OAUTH2.toString();
        assertThat(rendered).contains("clientId=***");
        assertThat(rendered).contains("clientSecret=***");
        assertThat(rendered).doesNotContain("${ENV:CLIENT_SECRET}");
        assertThat(rendered).doesNotContain("the-client-id");
    }
}
