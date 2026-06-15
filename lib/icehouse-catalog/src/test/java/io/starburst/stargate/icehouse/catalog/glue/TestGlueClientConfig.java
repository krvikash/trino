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
package io.starburst.stargate.icehouse.catalog.glue;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGlueClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GlueClientConfig.class)
                .setGlueRegion(null)
                .setGlueEndpointUrl(null)
                .setGlueStsRegion(null)
                .setGlueStsEndpointUrl(null)
                .setPinGlueClientToCurrentRegion(false)
                .setMaxGlueConnections(30)
                .setMaxGlueErrorRetries(10)
                .setIamRole(null)
                .setExternalId(null)
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setUseWebIdentityTokenCredentialsProvider(false)
                .setCatalogId(null)
                .setSkipArchive(true));
    }

    @Test
    void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.glue.region", "us-west-2")
                .put("hive.metastore.glue.endpoint-url", "http://glue.example.com")
                .put("hive.metastore.glue.sts.region", "eu-west-1")
                .put("hive.metastore.glue.sts.endpoint", "http://sts.example.com")
                .put("hive.metastore.glue.pin-client-to-current-region", "true")
                .put("hive.metastore.glue.max-connections", "50")
                .put("hive.metastore.glue.max-error-retries", "5")
                .put("hive.metastore.glue.iam-role", "arn:aws:iam::123456789012:role/GlueRole")
                .put("hive.metastore.glue.external-id", "external-id-123")
                .put("hive.metastore.glue.aws-access-key", "AKIAIOSFODNN7EXAMPLE")
                .put("hive.metastore.glue.aws-secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .put("hive.metastore.glue.use-web-identity-token-credentials-provider", "true")
                .put("hive.metastore.glue.catalogid", "123456789012")
                .put("hive.metastore.glue.skip-archive", "false")
                .buildOrThrow();

        GlueClientConfig expected = new GlueClientConfig()
                .setGlueRegion("us-west-2")
                .setGlueEndpointUrl(URI.create("http://glue.example.com"))
                .setGlueStsRegion("eu-west-1")
                .setGlueStsEndpointUrl(URI.create("http://sts.example.com"))
                .setPinGlueClientToCurrentRegion(true)
                .setMaxGlueConnections(50)
                .setMaxGlueErrorRetries(5)
                .setIamRole("arn:aws:iam::123456789012:role/GlueRole")
                .setExternalId("external-id-123")
                .setAwsAccessKey("AKIAIOSFODNN7EXAMPLE")
                .setAwsSecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .setUseWebIdentityTokenCredentialsProvider(true)
                .setCatalogId("123456789012")
                .setSkipArchive(false);

        assertFullMapping(properties, expected);
    }
}
