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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemLoader.getCustomAwsCredentialsProvider;
import static java.util.Objects.requireNonNull;

public record S3SecurityMappingResult(
        Optional<AwsCredentials> credentials,
        Optional<CustomCredentialsProviderContext> customCredentialsProviderContext,
        Optional<String> iamRole,
        Optional<String> roleSessionName,
        Optional<String> kmsKeyId,
        Optional<String> sseCustomerKey,
        Optional<String> endpoint,
        Optional<String> region)
{
    public S3SecurityMappingResult
    {
        requireNonNull(credentials, "credentials is null");
        requireNonNull(customCredentialsProviderContext, "customCredentialsProviderContext is null");
        requireNonNull(iamRole, "iamRole is null");
        requireNonNull(roleSessionName, "roleSessionName is null");
        requireNonNull(kmsKeyId, "kmsKeyId is null");
        requireNonNull(sseCustomerKey, "sseCustomerKey is null");
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(region, "region is null");
    }

    public Optional<AwsCredentialsProvider> credentialsProvider()
    {
        if (credentials.isPresent()) {
            return credentials.map(StaticCredentialsProvider::create);
        }
        return customCredentialsProviderContext.map(CustomCredentialsProviderContext::createCredentialProvider);
    }

    public record CustomCredentialsProviderContext(String customCredentialsClass, Map<String, String> customCredentialsClassArguments)
    {
        public CustomCredentialsProviderContext
        {
            requireNonNull(customCredentialsClass, "customCredentialsClass is null");
            customCredentialsClassArguments = ImmutableMap.copyOf(requireNonNull(customCredentialsClassArguments, "customCredentialsClassArguments is null"));
        }

        public AwsCredentialsProvider createCredentialProvider()
        {
            return getCustomAwsCredentialsProvider(customCredentialsClass, customCredentialsClassArguments);
        }
    }
}
