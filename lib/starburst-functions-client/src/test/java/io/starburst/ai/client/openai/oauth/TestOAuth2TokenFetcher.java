/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai.oauth;

import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.starburst.ai.model.ConnectionInfo.OAuth2GrantType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOAuth2TokenFetcher
{
    private static final ResolvedOAuth2Config CONFIG = new ResolvedOAuth2Config(
            OAuth2GrantType.CLIENT_CREDENTIALS,
            "https://idp.example/token",
            "the-client-id",
            "s3cret!/value",
            Optional.of("scope-a scope-b"),
            Optional.of("aud-1"));

    @Test
    public void sendsClientCredentialsForm()
    {
        AtomicReference<Request> capturedRequest = new AtomicReference<>();
        AtomicReference<String> capturedBody = new AtomicReference<>();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            capturedRequest.set(request);
            capturedBody.set(readBody(request));
            return TestingResponse.mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"abc\",\"expires_in\":3600,\"token_type\":\"Bearer\"}");
        });

        OAuth2TokenFetcher fetcher = new OAuth2TokenFetcher(httpClient);
        OAuth2TokenResponse response = fetcher.fetch(CONFIG);

        assertThat(response.accessToken()).isEqualTo("abc");
        assertThat(response.expiresInSeconds()).isEqualTo(3600);

        Request sent = capturedRequest.get();
        assertThat(sent.getMethod()).isEqualTo("POST");
        assertThat(sent.getUri().toString()).isEqualTo("https://idp.example/token");
        assertThat(sent.getHeaders().get(HeaderName.of("Content-Type"))).containsExactly("application/x-www-form-urlencoded");
        assertThat(sent.getHeaders().get(HeaderName.of("Accept"))).containsExactly("application/json");

        String authorization = sent.getHeader(HeaderName.of("Authorization"));
        assertThat(authorization).startsWith("Basic ");
        String decoded = new String(Base64.getDecoder().decode(authorization.substring("Basic ".length())), StandardCharsets.UTF_8);
        // clientId:clientSecret, each URL-encoded per RFC 6749 §2.3.1
        assertThat(decoded).isEqualTo("the-client-id:s3cret%21%2Fvalue");

        String body = capturedBody.get();
        assertThat(body).contains("grant_type=client_credentials");
        assertThat(body).contains("scope=scope-a+scope-b");
        assertThat(body).contains("audience=aud-1");
    }

    @Test
    public void wrapsNonSuccessStatusAsRedactedException()
    {
        TestingHttpClient httpClient = new TestingHttpClient(_ -> new TestingResponse(
                HttpStatus.UNAUTHORIZED,
                ImmutableListMultimap.of(),
                "{\"error\":\"invalid_client\",\"error_description\":\"secret-shaped-detail\"}".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> new OAuth2TokenFetcher(httpClient).fetch(CONFIG))
                .isInstanceOf(OAuth2TokenException.class)
                .hasMessageContaining("401")
                .hasMessageContaining("idp.example")
                .hasMessageNotContaining("s3cret")
                .hasMessageNotContaining("invalid_client")
                .hasMessageNotContaining("secret-shaped-detail");
    }

    @Test
    public void rejectsMalformedJson()
    {
        TestingHttpClient httpClient = new TestingHttpClient(_ -> TestingResponse.mockResponse(HttpStatus.OK, JSON_UTF_8, "not-json"));

        assertThatThrownBy(() -> new OAuth2TokenFetcher(httpClient).fetch(CONFIG))
                .isInstanceOf(OAuth2TokenException.class)
                .hasMessageContaining("idp.example");
    }

    @Test
    void rejectsResponseMissingAccessToken()
    {
        TestingHttpClient httpClient = new TestingHttpClient(_ -> TestingResponse.mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"expires_in\":600}"));

        assertThatThrownBy(() -> new OAuth2TokenFetcher(httpClient).fetch(CONFIG))
                .isInstanceOf(OAuth2TokenException.class);
    }

    @Test
    public void rejectsResponseMissingExpiresIn()
    {
        TestingHttpClient httpClient = new TestingHttpClient(_ -> TestingResponse.mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"tok\"}"));

        assertThatThrownBy(() -> new OAuth2TokenFetcher(httpClient).fetch(CONFIG))
                .isInstanceOf(OAuth2TokenException.class);
    }

    private static String readBody(Request request)
    {
        if (request.getBodyGenerator() instanceof StaticBodyGenerator body) {
            return new String(body.getBody(), StandardCharsets.UTF_8);
        }
        return "";
    }
}
