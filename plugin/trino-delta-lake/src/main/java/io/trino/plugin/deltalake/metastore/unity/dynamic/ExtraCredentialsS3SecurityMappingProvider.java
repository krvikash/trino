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
package io.trino.plugin.deltalake.metastore.unity.dynamic;

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3SecurityMappingProvider;
import io.trino.filesystem.s3.S3SecurityMappingResult;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Map;
import java.util.Optional;

public class ExtraCredentialsS3SecurityMappingProvider
        implements S3SecurityMappingProvider
{
    private final String s3AwsAccessKeyCredentialName;
    private final String s3AwsSecretKeyCredentialName;
    private final String s3AwsRegionCredentialName;
    private final Optional<String> s3AwsAccountIdCredentialName;
    private final Optional<String> s3AwsSessionTokenCredentialName;
    private final Optional<String> s3AwsEndpointCredentialName;

    @Inject
    public ExtraCredentialsS3SecurityMappingProvider(DynamicS3CredentialConfig s3CredentialConfig)
    {
        s3AwsAccessKeyCredentialName = s3CredentialConfig.getS3AwsAccessKeyCredentialName();
        s3AwsSecretKeyCredentialName = s3CredentialConfig.getS3AwsSecretKeyCredentialName();
        s3AwsRegionCredentialName = s3CredentialConfig.getS3RegionCredentialName();
        s3AwsAccountIdCredentialName = s3CredentialConfig.getS3AwsAccountIdCredentialName();
        s3AwsSessionTokenCredentialName = s3CredentialConfig.getS3AwsSessionTokenCredentialName();
        s3AwsEndpointCredentialName = s3CredentialConfig.getS3AwsEndpointCredentialName();
    }

    @Override
    public Optional<S3SecurityMappingResult> getMapping(ConnectorIdentity identity, Location location)
    {
        Map<String, String> extraCredentials = identity.getExtraCredentials();
        if (s3AwsAccessKeyCredentialName == null || !extraCredentials.containsKey(s3AwsAccessKeyCredentialName)) {
            // No direct S3 credentials configured or provided — the identity already carries Unity-vended credentials.
            return Optional.empty();
        }
        AwsCredentials credentials;
        if (s3AwsSessionTokenCredentialName.isPresent() && extraCredentials.containsKey(s3AwsSessionTokenCredentialName.get())) {
            AwsSessionCredentials.Builder awsSessionCredentialsBuilder = AwsSessionCredentials.builder()
                    .accessKeyId(extraCredentials.get(s3AwsAccessKeyCredentialName))
                    .secretAccessKey(extraCredentials.get(s3AwsSecretKeyCredentialName))
                    .sessionToken(extraCredentials.get(s3AwsSessionTokenCredentialName.get()));
            s3AwsAccountIdCredentialName.ifPresent(propertyName -> awsSessionCredentialsBuilder.accountId(extraCredentials.get(propertyName)));
            credentials = awsSessionCredentialsBuilder.build();
        }
        else {
            AwsBasicCredentials.Builder awsBasicCredentialsBuilder = AwsBasicCredentials.builder()
                    .accessKeyId(extraCredentials.get(s3AwsAccessKeyCredentialName))
                    .secretAccessKey(extraCredentials.get(s3AwsSecretKeyCredentialName));
            s3AwsAccountIdCredentialName.ifPresent(propertyName -> awsBasicCredentialsBuilder.accountId(extraCredentials.get(propertyName)));
            credentials = awsBasicCredentialsBuilder.build();
        }
        return Optional.of(new S3SecurityMappingResult(
                Optional.of(credentials),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                s3AwsEndpointCredentialName.map(credentialName -> identity.getExtraCredentials().get(credentialName)),
                Optional.ofNullable(identity.getExtraCredentials().get(s3AwsRegionCredentialName))));
    }
}
