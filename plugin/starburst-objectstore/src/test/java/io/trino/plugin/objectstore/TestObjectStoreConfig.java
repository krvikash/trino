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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergFileFormat;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestObjectStoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ObjectStoreConfig.class)
                .setTableType(TableType.HIVE)
                .setMaxMetadataQueriesProcessingThreads(32)
                .setDefaultIcebergFileFormat(IcebergFileFormat.PARQUET)
                .setIcebergRestCatalogUsed(false)
                .setGalaxyLocationSecurityEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("object-store.table-type", "ICEBERG")
                .put("object-store.information-schema-queries-threads", "13")
                .put("object-store.iceberg-default-file-format", "ORC")
                .put("object-store.iceberg-rest-catalog-used", "true")
                .put("galaxy.location-security.enabled", "true")
                .buildOrThrow();

        ObjectStoreConfig expected = new ObjectStoreConfig()
                .setTableType(TableType.ICEBERG)
                .setMaxMetadataQueriesProcessingThreads(13)
                .setDefaultIcebergFileFormat(IcebergFileFormat.ORC)
                .setIcebergRestCatalogUsed(true)
                .setGalaxyLocationSecurityEnabled(true);

        assertFullMapping(properties, expected);
    }
}
