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
package io.trino.plugin.iceberg.procedure;

import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;

final class TestIcebergRestCatalogCaseInsensitiveFlushMetadataCacheProcedure
        extends BaseTestIcebergRestCatalogFlushMetadataCacheProcedure
{
    TestIcebergRestCatalogCaseInsensitiveFlushMetadataCacheProcedure()
    {
        super(true);
    }

    @Test
    void testFlushCaseInsensitiveTableMappingCache()
    {
        String schemaName = "test_case_insensitive_flush_" + randomNameSuffix();
        String tableName = "table_name";

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + getSchemaLocation() + "/" + schemaName + "')");
        assertUpdate("CREATE TABLE " + schemaName + "." + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        // populate cache
        assertQuery("SELECT count(*) FROM " + schemaName + "." + tableName, "VALUES 25");

        renameTableOutsideTrino(schemaName, tableName, tableName.toUpperCase(ENGLISH));

        assertUpdate("CALL system.flush_metadata_cache()");

        // Use a modifying operation to verify the cache was flushed.
        // A SELECT would succeed even with stale cache since the REST catalog resolves names case-insensitively.
        assertUpdate("INSERT INTO " + schemaName + "." + tableName + " SELECT * FROM tpch.tiny.nation", 25);

        assertUpdate("DROP TABLE " + schemaName + "." + tableName);
        assertUpdate("DROP SCHEMA " + schemaName);
    }
}
