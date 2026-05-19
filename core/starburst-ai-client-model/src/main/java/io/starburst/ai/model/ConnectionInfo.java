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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "provider")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OpenAiConnectionInfo.class, name = "OPENAI"),
        @JsonSubTypes.Type(value = AwsBedrockConnectionInfo.class, name = "AWS_BEDROCK"),
})
public sealed interface ConnectionInfo
        permits OpenAiConnectionInfo, AwsBedrockConnectionInfo
{
    record OpenAiConnectionInfo(Optional<String> endpoint, Optional<String> apiKey, Map<String, List<String>> additionalHeaders)
            implements ConnectionInfo
    {
        public OpenAiConnectionInfo
        {
            requireNonNull(endpoint, "endpoint is null");
            requireNonNull(apiKey, "apiKey is null");
            additionalHeaders = requireNonNullElse(additionalHeaders, Map.of());
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
}
