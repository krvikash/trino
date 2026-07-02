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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Minimal RFC 6749 §5.1 successful token response DTO.
 * We rely on `access_token` and `expires_in`; other fields are ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OAuth2TokenResponse(String accessToken, Long expiresInSeconds)
{
    public OAuth2TokenResponse
    {
        if (accessToken == null || accessToken.isBlank()) {
            throw new IllegalArgumentException("access_token cannot be null or empty");
        }
        if (expiresInSeconds == null || expiresInSeconds <= 0) {
            throw new IllegalArgumentException("expires_in missing or non-positive");
        }
    }

    @JsonCreator
    public static OAuth2TokenResponse fromJson(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("expires_in") Long expiresInSeconds)
    {
        return new OAuth2TokenResponse(accessToken, expiresInSeconds);
    }

    @Override
    public String toString()
    {
        return "OAuth2TokenResponse{accessToken=***, expiresInSeconds=" + expiresInSeconds + "}";
    }
}
