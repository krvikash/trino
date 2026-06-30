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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestS3AndUnityMetastoreWithDynamicPropertiesStsDeltaConnectorSmokeTest
        extends BaseDynamicS3AndUnityMetastoreDeltaConnectorSmokeTest
{
    private static final String DATABRICKS_BOB_AWS_ROLE_ARN = "arn:aws:iam::888469412714:role/sep-unity-bob-iam-role";

    @Override
    protected Map<String, String> getDeltaLakeProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.metastore.unity.vended-credentials-enabled", "false")
                .putAll(getDynamicProperties())
                .buildOrThrow();
    }

    private Map<String, String> getDynamicProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("dynamic.hive-metastore-unity-host.credential-name", "dynamic_host")
                .put("dynamic.hive-metastore-unity-token.credential-name", "dynamic_token")
                .put("dynamic.hive-metastore-unity-catalog-name.credential-name", "dynamic_catalog")
                .put("dynamic.s3.aws-access-key.credential-name", "dynamic_access_key")
                .put("dynamic.s3.aws-secret-key.credential-name", "dynamic_secret_key")
                .put("dynamic.s3.aws-session-token.credential-name", "dynamic_session_token")
                .put("dynamic.s3.region.credential-name", "dynamic_region")
                .put("dynamic.s3.aws-account-id.credential-name", "dynamic_account_id")
                .buildOrThrow();
    }

    @Override
    protected Map<String, String> getExtraCredentialsForAlice()
    {
        return ImmutableMap.<String, String>builder()
                .put("dynamic_access_key", ALICE_AWS_ACCESS_KEY_ID)
                .put("dynamic_secret_key", ALICE_AWS_SECRET_KEY)
                .put("dynamic_region", DATABRICKS_AWS_REGION)
                .buildOrThrow();
    }

    @Override
    protected Map<String, String> getExtraCredentialsForBob()
    {
        Credentials bobCredentials = getStsCredentials(
                DATABRICKS_AWS_REGION,
                BOB_AWS_ACCESS_KEY_ID,
                BOB_AWS_SECRET_KEY,
                DATABRICKS_BOB_AWS_ROLE_ARN);
        return ImmutableMap.<String, String>builder()
                .put("dynamic_access_key", bobCredentials.accessKeyId())
                .put("dynamic_secret_key", bobCredentials.secretAccessKey())
                .put("dynamic_session_token", bobCredentials.sessionToken())
                .put("dynamic_region", DATABRICKS_AWS_REGION)
                .put("dynamic_account_id", AWS_ACCOUNT_ID)
                .buildOrThrow();
    }

    private Credentials getStsCredentials(String region, String staticAccessKey, String staticSecretKey, String role)
    {
        try (StsClient stsClient = StsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(staticAccessKey, staticSecretKey)))
                .build()) {
            AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(role)
                    .roleSessionName("dynamic-configuration-session-" + randomNameSuffix())
                    .durationSeconds(3600)
                    .build();

            AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);
            return assumeRoleResponse.credentials();
        }
    }
}
