/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.VertexAiConnectionInfo;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "provider")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OpenAiConnectionInfo.class, name = "OPENAI"),
        @JsonSubTypes.Type(value = AwsBedrockConnectionInfo.class, name = "AWS_BEDROCK"),
        @JsonSubTypes.Type(value = VertexAiConnectionInfo.class, name = "VERTEX_AI"),
})
public sealed interface ConnectionInfo
        permits AwsBedrockConnectionInfo,
                OpenAiConnectionInfo,
                VertexAiConnectionInfo
{
    enum OAuth2GrantType
    {
        CLIENT_CREDENTIALS,
        AUTHORIZATION_CODE,
    }

    record OpenAiConnectionInfo(
            Optional<String> endpoint,
            Optional<String> apiKey,
            Map<String, List<String>> additionalHeaders,
            Optional<OAuth2Config> oauthConfig)
            implements ConnectionInfo
    {
        public OpenAiConnectionInfo
        {
            requireNonNull(endpoint, "endpoint is null");
            requireNonNull(apiKey, "apiKey is null");
            additionalHeaders = requireNonNullElse(additionalHeaders, Map.of());
            oauthConfig = requireNonNullElse(oauthConfig, Optional.empty());
            if (oauthConfig.isPresent()) {
                if (apiKey.isPresent() && !apiKey.get().isEmpty()) {
                    throw new IllegalArgumentException("apiKey and oauthConfig are mutually exclusive");
                }
                for (String headerName : additionalHeaders.keySet()) {
                    if (headerName.toLowerCase(Locale.ROOT).equals("authorization")) {
                        throw new IllegalArgumentException("Authorization header cannot be set in additionalHeaders when oauthConfig is configured");
                    }
                }
            }
        }
    }

    record OAuth2Config(
            OAuth2GrantType grantType,
            String tokenUrl,
            String clientId,
            String clientSecret,
            Optional<String> scope,
            Optional<String> audience)
    {
        private static final Pattern SECRET_REFERENCE = Pattern.compile("^\\$\\{[^:}]+:[^}]+}$");

        public OAuth2Config
        {
            requireNonNull(grantType, "grantType is null");
            if (grantType != OAuth2GrantType.CLIENT_CREDENTIALS) {
                throw new IllegalArgumentException("Only CLIENT_CREDENTIALS OAuth grant type is supported");
            }
            requireNonNull(tokenUrl, "tokenUrl is null");
            requireNonNull(clientId, "clientId is null");
            requireNonNull(clientSecret, "clientSecret is null");
            requireNonNull(scope, "scope is null");
            requireNonNull(audience, "audience is null");
            if (tokenUrl.isBlank()) {
                throw new IllegalArgumentException("tokenUrl is blank");
            }
            validateTokenUrl(tokenUrl);
            if (clientId.isBlank()) {
                throw new IllegalArgumentException("clientId is blank");
            }
            if (clientSecret.isBlank()) {
                throw new IllegalArgumentException("clientSecret is blank");
            }
            if (!SECRET_REFERENCE.matcher(clientSecret).matches()) {
                throw new IllegalArgumentException("oauthConfig.clientSecret must be a secret reference (e.g. ${ENV:VAR_NAME}); plaintext values are not allowed");
            }
        }

        public static void validateTokenUrl(String tokenUrl)
        {
            URI uri;
            try {
                uri = new URI(tokenUrl);
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("oauthConfig.tokenUrl is not a valid URI: " + tokenUrl, e);
            }
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (scheme == null || host == null) {
                throw new IllegalArgumentException("oauthConfig.tokenUrl must include a scheme and host: " + tokenUrl);
            }
            String lowerScheme = scheme.toLowerCase(Locale.ROOT);
            String lowerHost = host.toLowerCase(Locale.ROOT);
            if (lowerScheme.equals("https")) {
                return;
            }
            if (lowerScheme.equals("http") && (lowerHost.equals("localhost") || lowerHost.equals("127.0.0.1"))) {
                return;
            }
            throw new IllegalArgumentException("oauthConfig.tokenUrl must use https:// (http:// is allowed only for localhost): " + tokenUrl);
        }

        // Don't include the clientId/clientSecret if printing or logging this
        @Override
        public String toString()
        {
            return format(
                    "OAuth2Config{grantType=%s, tokenUrl=%s, clientId=***, clientSecret=***, scope=%s, audience=%s}",
                    grantType,
                    tokenUrl,
                    scope,
                    audience);
        }
    }

    record AwsBedrockConnectionInfo(
            Optional<String> awsAccessKey,
            Optional<String> awsSecretKey,
            Optional<String> region,
            Optional<String> iamRole,
            boolean isUseAnonymousCredentials,
            Optional<String> externalId,
            Optional<String> endpoint,
            Map<String, List<String>> additionalHeaders)
            implements ConnectionInfo
    {
        public AwsBedrockConnectionInfo
        {
            requireNonNull(awsAccessKey, "awsAccessKey is null");
            requireNonNull(awsSecretKey, "awsSecretKey is null");
            requireNonNull(region, "region is null");
            requireNonNull(iamRole, "iamRole is null");
            requireNonNull(externalId, "externalId is null");
            requireNonNull(endpoint, "endpoint is null");
            additionalHeaders = requireNonNullElse(additionalHeaders, Map.of());
        }
    }

    record VertexAiConnectionInfo(
            Optional<String> serviceAccountKey,
            Optional<String> projectId,
            String location,
            Map<String, List<String>> additionalHeaders)
            implements ConnectionInfo
    {
        public VertexAiConnectionInfo
        {
            requireNonNull(serviceAccountKey, "serviceAccountKey is null");
            requireNonNull(projectId, "projectId is null");
            requireNonNull(location, "location is null");
            additionalHeaders = requireNonNullElse(additionalHeaders, Map.of());
        }
    }
}
