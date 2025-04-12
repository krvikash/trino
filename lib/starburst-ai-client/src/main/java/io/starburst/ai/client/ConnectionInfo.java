/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.starburst.ai.client;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "provider")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ConnectionInfo.OpenAiConnectionInfo.class, name = "OPENAI"),
    @JsonSubTypes.Type(value = ConnectionInfo.AwsBedrockConnectionInfo.class, name = "AWS_BEDROCK")
})
public sealed interface ConnectionInfo
        permits ConnectionInfo.OpenAiConnectionInfo, ConnectionInfo.AwsBedrockConnectionInfo
{
    record OpenAiConnectionInfo(Optional<String> endpoint, Optional<String> apiKey)
            implements ConnectionInfo
    {
        public OpenAiConnectionInfo
        {
            requireNonNull(endpoint, "endpoint is null");
            requireNonNull(apiKey, "apiKey is null");
        }
    }

    record AwsBedrockConnectionInfo(
            Optional<String> awsAccessKey,
            Optional<String> awsSecretKey,
            Optional<String> region,
            Optional<String> iamRole,
            Optional<String> externalId)
            implements ConnectionInfo
    {
        public AwsBedrockConnectionInfo
        {
            requireNonNull(awsAccessKey, "awsAccessKey is null");
            requireNonNull(awsSecretKey, "awsSecretKey is null");
            requireNonNull(region, "region is null");
            requireNonNull(iamRole, "iamRole is null");
            requireNonNull(externalId, "externalId is null");
        }
    }
}
