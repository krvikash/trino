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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestDynamicS3CredentialConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicS3CredentialConfig.class)
                .setS3RegionCredentialName(null)
                .setS3AwsAccessKeyCredentialName(null)
                .setS3AwsSecretKeyCredentialName(null)
                .setS3AwsAccountIdCredentialName(null)
                .setS3AwsSessionTokenCredentialName(null)
                .setS3AwsEndpointCredentialName(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("dynamic.s3.region.credential-name", "s3_region")
                .put("dynamic.s3.aws-access-key.credential-name", "s3_access_key")
                .put("dynamic.s3.aws-secret-key.credential-name", "s3_secret_key")
                .put("dynamic.s3.aws-account-id.credential-name", "s3_account_id")
                .put("dynamic.s3.aws-session-token.credential-name", "s3_session_token")
                .put("dynamic.s3.aws-endpoint.credential-name", "s3_endpoint")
                .buildOrThrow();

        DynamicS3CredentialConfig expected = new DynamicS3CredentialConfig()
                .setS3RegionCredentialName("s3_region")
                .setS3AwsAccessKeyCredentialName("s3_access_key")
                .setS3AwsSecretKeyCredentialName("s3_secret_key")
                .setS3AwsAccountIdCredentialName("s3_account_id")
                .setS3AwsSessionTokenCredentialName("s3_session_token")
                .setS3AwsEndpointCredentialName("s3_endpoint");

        assertFullMapping(properties, expected);
    }
}
