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

import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.starburst.ai.model.ConnectionInfo.OAuth2GrantType;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class OAuth2TokenFetcher
{
    private static final String CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";
    private static final String APPLICATION_JSON = "application/json";
    private static final JsonCodec<OAuth2TokenResponse> RESPONSE_CODEC = JsonCodec.jsonCodec(OAuth2TokenResponse.class);

    private final HttpClient httpClient;

    @Inject
    public OAuth2TokenFetcher(@ForAiOAuth2 HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public OAuth2TokenResponse fetch(ResolvedOAuth2Config config)
    {
        requireNonNull(config, "config is null");
        URI tokenUri = parseTokenUri(config.tokenUrl());
        Request request = preparePost()
                .setUri(tokenUri)
                .addHeader("Content-Type", CONTENT_TYPE_FORM)
                .addHeader("Accept", APPLICATION_JSON)
                .addHeader("Authorization", basicAuthHeader(config.clientId(), config.clientSecret()))
                .setBodyGenerator(createStaticBodyGenerator(formBody(config), UTF_8))
                .build();

        JsonResponse<OAuth2TokenResponse> response;
        try {
            response = httpClient.execute(request, FullJsonResponseHandler.createFullJsonResponseHandler(RESPONSE_CODEC));
        }
        catch (RuntimeException e) {
            throw new OAuth2TokenException(format("OAuth2 token fetch failed for host %s", tokenUri.getHost()), e);
        }

        int status = response.getStatusCode();
        if (status < 200 || status >= 300) {
            throw new OAuth2TokenException(format("OAuth2 token fetch returned HTTP %s from host %s", status, tokenUri.getHost()));
        }
        if (!response.hasValue()) {
            throw new OAuth2TokenException(
                    format("OAuth2 token response from host %s could not be parsed as JSON", tokenUri.getHost()),
                    response.getException());
        }
        return response.getValue();
    }

    private static URI parseTokenUri(String tokenUrl)
    {
        try {
            return new URI(tokenUrl);
        }
        catch (URISyntaxException e) {
            throw new OAuth2TokenException("OAuth2 tokenUrl is not a valid URI", e);
        }
    }

    private static String basicAuthHeader(String clientId, String clientSecret)
    {
        // RFC 6749 §2.3.1: URL-encode each component before Base64.
        String encoded = URLEncoder.encode(clientId, UTF_8) + ":" + URLEncoder.encode(clientSecret, UTF_8);
        return "Basic " + Base64.getEncoder().encodeToString(encoded.getBytes(UTF_8));
    }

    private static String formBody(ResolvedOAuth2Config config)
    {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("grant_type", grantTypeValue(config.grantType()));
        config.scope().ifPresent(value -> params.put("scope", value));
        config.audience().ifPresent(value -> params.put("audience", value));
        StringBuilder body = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (!body.isEmpty()) {
                body.append('&');
            }
            body.append(URLEncoder.encode(entry.getKey(), UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(entry.getValue(), UTF_8));
        }
        return body.toString();
    }

    private static String grantTypeValue(OAuth2GrantType grantType)
    {
        return switch (grantType) {
            case CLIENT_CREDENTIALS -> "client_credentials";
            case AUTHORIZATION_CODE -> throw new OAuth2TokenException("AUTHORIZATION_CODE grant type is not supported");
        };
    }
}
