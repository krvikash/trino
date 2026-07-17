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
package io.trino.server.starburst.accesscontrol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxySystemAccessControlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxySystemAccessControlConfig.class)
                .setBackgroundProcessingThreads(8)
                .setVisibilityBatchSize(2_000)
                .setSystemRuntimeFilterOtherUsers(false)
                .setExpectedQueryParallelism(100)
                .setPermissionsCacheExpireAfterWriteDuration("PT10M")
                .setReadOnlyCatalogs("")
                .setSharedCatalogSchemaNames("")
                .setAlwaysVisibleCatalogSystemTables("")
                .setGalaxyEntityPrivilegesEnabled(false)
                .setAccessControlMode("galaxy"));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.access-control-background-threads", "3")
                .put("galaxy.visibility-batch-size", "3000")
                .put("galaxy.expected-query-parallelism", "200")
                .put("galaxy.system-runtime-filter-other-users", "true")
                .put("galaxy.permissions-cache-expiration", "PT1M")
                .put("galaxy.read-only-catalogs", "sillycatalog,funnycatalog")
                .put("galaxy.shared-catalog-schemas", "my_catalog->foo,his_catalog->*broken,her_catalog->*")
                .put("galaxy.catalog-always-visible-system-tables", "my_catalog->system.table1,his_catalog->system.table2")
                .put("galaxy.entity-privileges.enabled", "true")
                .put("galaxy.access-control-mode", "sep")
                .buildOrThrow();

        GalaxySystemAccessControlConfig expected = new GalaxySystemAccessControlConfig()
                .setBackgroundProcessingThreads(3)
                .setVisibilityBatchSize(3000)
                .setExpectedQueryParallelism(200)
                .setSystemRuntimeFilterOtherUsers(true)
                .setPermissionsCacheExpireAfterWriteDuration("PT1M")
                .setReadOnlyCatalogs(ImmutableSet.of("sillycatalog", "funnycatalog"))
                .setSharedCatalogSchemaNames(Optional.of(ImmutableMap.of(
                        "my_catalog", new SharedSchemaNameAndAccepted("foo", true),
                        "his_catalog", new SharedSchemaNameAndAccepted("broken", false),
                        "her_catalog", new SharedSchemaNameAndAccepted(null, false))))
                .setAlwaysVisibleCatalogSystemTables(ImmutableSetMultimap.of(
                        "my_catalog",
                        new SchemaTableName("system", "table1"),
                        "his_catalog",
                        new SchemaTableName("system", "table2")))
                .setGalaxyEntityPrivilegesEnabled(true)
                .setAccessControlMode("sep");

        assertFullMapping(properties, expected);
    }
}
