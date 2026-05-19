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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.UrlEscapers.urlFormParameterEscaper;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_AUTHORIZATION_ERROR;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_GENERIC_EXTERNAL_ERROR;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNSUPPORTED_AUTHENTICATION;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class Oauth2Authenticator
        implements OpenApiAuthenticator
{
    // https://datatracker.ietf.org/doc/html/rfc6749#section-3.3
    private static final CharMatcher VALID_SCOPE_TOKEN = CharMatcher.is('!') // %x21
            .or(CharMatcher.inRange('#', '[')) // %x23-5B
            .or(CharMatcher.inRange(']', '~')); // %x5D-7E
    private static final long ACCESS_TOKEN_VALIDITY_SLOP_MILLISECONDS = 1000;
    private static final long ACCESS_TOKEN_DEFAULT_EXPIRY_SECONDS = 60;

    private final Oauth2ResponseHandler responseHandler;

    private final Request accessTokenRequest;

    private final HttpClient httpClient;

    private String accessToken;
    private long accessTokenValidUntilMillis;

    public Oauth2Authenticator(
            URI tokenUrl,
            Optional<Set<String>> scopes,
            String clientId,
            String clientSecret,
            HttpClient httpClient,
            ObjectMapper objectMapper)
    {
        requireNonNull(scopes, "scopes is null");
        checkArgument(scopes.stream()
                        .flatMap(Set::stream)
                        .allMatch(Oauth2Authenticator::validScope),
                "Invalid scopes");
        this.accessTokenRequest = createAccessTokenRequest(
                requireNonNull(clientId, "clientId is null"),
                requireNonNull(clientSecret, "clientSecret is null"),
                scopes,
                requireNonNull(tokenUrl, "tokenUrl is null"));
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.responseHandler = new Oauth2ResponseHandler(requireNonNull(objectMapper, "objectMapper is null"));
    }

    public static boolean validScope(String scope)
    {
        return VALID_SCOPE_TOKEN.matchesAllOf(scope) && !scope.isEmpty();
    }

    private static Request createAccessTokenRequest(
            String clientId,
            String clientSecret,
            Optional<Set<String>> scopes,
            URI tokenUrl)
    {
        // https://datatracker.ietf.org/doc/html/rfc2617#section-2
        String authorizationValue = "Basic %s".formatted(Base64.getEncoder()
                .encodeToString("%s:%s".formatted(clientId, clientSecret).getBytes(UTF_8)));
        // https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2
        ImmutableMap.Builder<String, String> formBuilder = ImmutableMap.<String, String>builder()
                .put("grant_type", "client_credentials");
        if (scopes.isPresent()) {
            formBuilder = formBuilder.put("scope", join(" ", scopes.get()));
        }
        BodyGenerator bodyGenerator = StaticBodyGenerator.createStaticBodyGenerator(
                Joiner.on("&")
                        .withKeyValueSeparator("=")
                        .join(Maps.transformValues(
                                formBuilder.buildOrThrow(),
                                urlFormParameterEscaper()::escape)),
                UTF_8);
        return Request.builder()
                .setMethod("POST")
                .setUri(tokenUrl)
                .setBodyGenerator(bodyGenerator)
                .setHeader(AUTHORIZATION, authorizationValue)
                .setHeader(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .setHeader(ACCEPT, "application/json")
                .build();
    }

    private synchronized String getAccessToken()
    {
        if (accessToken != null &&
                (accessTokenValidUntilMillis - ACCESS_TOKEN_VALIDITY_SLOP_MILLISECONDS) > System.currentTimeMillis()) {
            return accessToken;
        }
        return getAndSetAccessToken();
    }

    private String getAndSetAccessToken()
    {
        OAuth2AccessTokenResponse response = httpClient.execute(accessTokenRequest, responseHandler);
        if (!"bearer".equalsIgnoreCase(response.tokenType())) {
            throw new TrinoException(
                    OPENAPI_UNSUPPORTED_AUTHENTICATION,
                    "Authorization server responded with '%s' token type, only bearer is supported".formatted(
                            String.valueOf(response.tokenType()).toLowerCase(ENGLISH)));
        }
        if (response.accessToken() == null) {
            throw new TrinoException(OPENAPI_GENERIC_EXTERNAL_ERROR, "Authorization response did not include access_token");
        }
        this.accessToken = response.accessToken();
        this.accessTokenValidUntilMillis =
                System.currentTimeMillis() +
                        response.expiresInSeconds().orElse(ACCESS_TOKEN_DEFAULT_EXPIRY_SECONDS) * 1000;
        return accessToken;
    }

    // https://datatracker.ietf.org/doc/html/rfc6749#section-5.1 (but no refresh token and assuming bearer format)
    public record OAuth2AccessTokenResponse(
            @JsonProperty("access_token")
            String accessToken,
            @JsonProperty("token_type")
            String tokenType,
            @JsonProperty("expires_in")
            Optional<Long> expiresInSeconds) {}

    // https://datatracker.ietf.org/doc/html/rfc6749#section-5.2
    public record Oauth2ErrorResponse(
            @JsonProperty("error")
            String error) {}

    private static class Oauth2ResponseHandler
            implements ResponseHandler<OAuth2AccessTokenResponse, TrinoException>
    {
        private final ObjectMapper objectMapper;

        Oauth2ResponseHandler(ObjectMapper objectMapper)
        {
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        }

        @Override
        public OAuth2AccessTokenResponse handleException(
                Request request,
                Exception exception)
        {
            throw new TrinoException(
                    OPENAPI_GENERIC_EXTERNAL_ERROR,
                    "Failed to read OAUTH2 client credentials authorization response (%s)".formatted(
                            exception.getMessage()),
                    exception);
        }

        @Override
        public OAuth2AccessTokenResponse handle(
                Request request,
                Response response)
        {
            if (response.getStatusCode() == HttpStatus.OK.code()) {
                try {
                    return objectMapper.readValue(response.getInputStream(), OAuth2AccessTokenResponse.class);
                }
                catch (IOException e) {
                    throw new TrinoException(
                            OPENAPI_GENERIC_EXTERNAL_ERROR,
                            "Unexpected error reading JSON of authorization response (%s)".formatted(e.getMessage()),
                            e);
                }
            }
            if (response.getStatusCode() == HttpStatus.BAD_REQUEST.code()) {
                try {
                    Oauth2ErrorResponse errorResponse = objectMapper.readValue(
                            response.getInputStream(),
                            Oauth2ErrorResponse.class);
                    throw new TrinoException(
                            OPENAPI_AUTHORIZATION_ERROR,
                            "Error authorizing with oauth2 '%s'".formatted(
                                    String.valueOf(errorResponse.error())));
                }
                catch (IOException e) {
                    throw new TrinoException(
                            OPENAPI_GENERIC_EXTERNAL_ERROR,
                            "Unexpected error reading oauth2 authorization error response (%s)".formatted(
                                    e.getMessage()),
                            e);
                }
            }
            throw new TrinoException(
                    OPENAPI_GENERIC_EXTERNAL_ERROR,
                    "Unexpected oauth2 authorization response status code (%s)".formatted(response.getStatusCode()));
        }
    }

    @Override
    public Request filterRequest(Request request)
    {
        return Request.Builder.fromRequest(request)
                .setHeader(AUTHORIZATION, "Bearer " + getAccessToken())
                .build();
    }
}
