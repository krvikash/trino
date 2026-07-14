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
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

final class TestDynamicUnityMetastoreConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicUnityMetastoreConfig.class)
                .setUnityHostCredentialName(null)
                .setUnityTokenCredentialName(null)
                .setUnityCatalogNameCredentialName(null)
                .setVendedCredentialsCredentialName(null)
                .setVendedCredentialsEnabled(false)
                .setCatalogManagedTableEnabled(false)
                .setConnectTimeout(new Duration(30, SECONDS))
                .setReadTimeout(new Duration(60, SECONDS)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("dynamic.hive-metastore-unity-host.credential-name", "unity_host")
                .put("dynamic.hive-metastore-unity-token.credential-name", "unity_token")
                .put("dynamic.hive-metastore-unity-catalog-name.credential-name", "catalog_name")
                .put("dynamic.hive-metastore-unity-vended-credentials-enabled.credential-name", "vended_credentials")
                .put("hive.metastore.unity.vended-credentials-enabled", "true")
                .put("hive.metastore.unity.catalog-managed-table-enabled", "true")
                .put("hive.metastore.unity.connect-timeout", "5s")
                .put("hive.metastore.unity.read-timeout", "45s")
                .buildOrThrow();

        DynamicUnityMetastoreConfig expected = new DynamicUnityMetastoreConfig()
                .setUnityHostCredentialName("unity_host")
                .setUnityTokenCredentialName("unity_token")
                .setUnityCatalogNameCredentialName("catalog_name")
                .setVendedCredentialsCredentialName("vended_credentials")
                .setVendedCredentialsEnabled(true)
                .setCatalogManagedTableEnabled(true)
                .setConnectTimeout(new Duration(5, SECONDS))
                .setReadTimeout(new Duration(45, SECONDS));

        assertFullMapping(properties, expected);
    }
}
