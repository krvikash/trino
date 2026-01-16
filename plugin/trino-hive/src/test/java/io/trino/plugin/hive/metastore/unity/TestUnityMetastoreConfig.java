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
package io.trino.plugin.hive.metastore.unity;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestUnityMetastoreConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(UnityMetastoreConfig.class)
                .setCatalogName(null)
                .setToken(null)
                .setHost(null)
                .setCatalogManagedTableEnabled(false)
                .setVendedCredentialsEnabled(false));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.unity.catalog-name", "catalog")
                .put("hive.metastore.unity.token", "token")
                .put("hive.metastore.unity.host", "host")
                .put("hive.metastore.unity.catalog-managed-table-enabled", "true")
                .put("hive.metastore.unity.vended-credentials-enabled", "true")
                .buildOrThrow();

        UnityMetastoreConfig expected = new UnityMetastoreConfig()
                .setCatalogName("catalog")
                .setToken("token")
                .setHost("host")
                .setCatalogManagedTableEnabled(true)
                .setVendedCredentialsEnabled(true);

        assertFullMapping(properties, expected);
    }
}
