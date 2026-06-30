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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class DynamicS3CredentialConfig
{
    private String s3RegionCredentialName;
    private String s3AwsAccessKeyCredentialName;
    private String s3AwsSecretKeyCredentialName;
    private Optional<String> s3AwsAccountIdCredentialName = Optional.empty();
    private Optional<String> s3AwsSessionTokenCredentialName = Optional.empty();
    private Optional<String> s3AwsEndpointCredentialName = Optional.empty();

    public String getS3RegionCredentialName()
    {
        return s3RegionCredentialName;
    }

    @Config("dynamic.s3.region.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the S3 region")
    public DynamicS3CredentialConfig setS3RegionCredentialName(String s3RegionCredentialName)
    {
        this.s3RegionCredentialName = s3RegionCredentialName;
        return this;
    }

    public String getS3AwsAccessKeyCredentialName()
    {
        return s3AwsAccessKeyCredentialName;
    }

    @Config("dynamic.s3.aws-access-key.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the S3 AWS access key")
    public DynamicS3CredentialConfig setS3AwsAccessKeyCredentialName(String s3AwsAccessKeyCredentialName)
    {
        this.s3AwsAccessKeyCredentialName = s3AwsAccessKeyCredentialName;
        return this;
    }

    public String getS3AwsSecretKeyCredentialName()
    {
        return s3AwsSecretKeyCredentialName;
    }

    @Config("dynamic.s3.aws-secret-key.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the S3 AWS secret key")
    public DynamicS3CredentialConfig setS3AwsSecretKeyCredentialName(String s3AwsSecretKeyCredentialName)
    {
        this.s3AwsSecretKeyCredentialName = s3AwsSecretKeyCredentialName;
        return this;
    }

    public Optional<String> getS3AwsAccountIdCredentialName()
    {
        return s3AwsAccountIdCredentialName;
    }

    @Config("dynamic.s3.aws-account-id.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the S3 AWS account ID")
    public DynamicS3CredentialConfig setS3AwsAccountIdCredentialName(String s3AwsAccountIdCredentialName)
    {
        this.s3AwsAccountIdCredentialName = Optional.ofNullable(s3AwsAccountIdCredentialName);
        return this;
    }

    public Optional<String> getS3AwsSessionTokenCredentialName()
    {
        return s3AwsSessionTokenCredentialName;
    }

    @Config("dynamic.s3.aws-session-token.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the S3 AWS session token")
    public DynamicS3CredentialConfig setS3AwsSessionTokenCredentialName(String s3AwsSessionTokenCredentialName)
    {
        this.s3AwsSessionTokenCredentialName = Optional.ofNullable(s3AwsSessionTokenCredentialName);
        return this;
    }

    public Optional<String> getS3AwsEndpointCredentialName()
    {
        return s3AwsEndpointCredentialName;
    }

    @Config("dynamic.s3.aws-endpoint.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the S3 endpoint URL")
    public DynamicS3CredentialConfig setS3AwsEndpointCredentialName(String s3AwsEndpointCredentialName)
    {
        this.s3AwsEndpointCredentialName = Optional.ofNullable(s3AwsEndpointCredentialName);
        return this;
    }
}
