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

import io.trino.testing.AbstractTestQueryFramework;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;

abstract class BaseTestIcebergFlushMetadataCacheProcedure
        extends AbstractTestQueryFramework
{
    @Test
    void testFlushMetadataCacheForTable()
    {
        String schemaName = "test_flush_" + randomNameSuffix();
        String tableName = "test_table";
        String renamedTableName = "renamed_table";

        try {
            assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + getSchemaLocation() + "/" + schemaName + "')");
            assertUpdate("CREATE TABLE " + schemaName + "." + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);

            // Populate cache by querying the table
            assertQuery("SELECT count(*) FROM " + schemaName + "." + tableName, "VALUES 25");

            // Rename table outside Trino
            renameTableOutsideTrino(schemaName, tableName, renamedTableName);

            // Cache should still allow querying old table name
            assertQuery("SELECT count(*) FROM " + schemaName + "." + tableName, "VALUES 25");

            // Flush cache for the table
            assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + schemaName + "', table_name => '" + tableName + "')");

            // Old table name should no longer work
            assertQueryFails("SELECT * FROM " + schemaName + "." + tableName, ".*Table '.*' does not exist");

            // New table name should work
            assertQuery("SELECT count(*) FROM " + schemaName + "." + renamedTableName, "VALUES 25");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + renamedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    void testFlushEntireMetadataCache()
    {
        String schemaName = "test_flush_all_" + randomNameSuffix();
        String tableName1 = "test_table_1";
        String tableName2 = "test_table_2";
        String renamedTableName1 = "renamed_table_1";
        String renamedTableName2 = "renamed_table_2";

        try {
            assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + getSchemaLocation() + "/" + schemaName + "')");
            assertUpdate("CREATE TABLE " + schemaName + "." + tableName1 + " AS SELECT 1 AS id", 1);
            assertUpdate("CREATE TABLE " + schemaName + "." + tableName2 + " AS SELECT 2 AS id", 1);

            // Populate cache
            assertQuery("SELECT * FROM " + schemaName + "." + tableName1, "VALUES 1");
            assertQuery("SELECT * FROM " + schemaName + "." + tableName2, "VALUES 2");

            // Rename tables outside Trino
            renameTableOutsideTrino(schemaName, tableName1, renamedTableName1);
            renameTableOutsideTrino(schemaName, tableName2, renamedTableName2);

            // Cache should still allow querying old table names
            assertQuery("SELECT * FROM " + schemaName + "." + tableName1, "VALUES 1");
            assertQuery("SELECT * FROM " + schemaName + "." + tableName2, "VALUES 2");

            // Flush entire cache
            assertUpdate("CALL system.flush_metadata_cache()");

            // Old table names should no longer work
            assertQueryFails("SELECT * FROM " + schemaName + "." + tableName1, ".*Table '.*' does not exist");
            assertQueryFails("SELECT * FROM " + schemaName + "." + tableName2, ".*Table '.*' does not exist");

            // New table names should work
            assertQuery("SELECT * FROM " + schemaName + "." + renamedTableName1, "VALUES 1");
            assertQuery("SELECT * FROM " + schemaName + "." + renamedTableName2, "VALUES 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName1);
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName2);
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + renamedTableName1);
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + renamedTableName2);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    void testFlushMetadataCacheNonExistentTable()
    {
        // Flushing cache for non-existent table should not fail
        assertUpdate("CALL system.flush_metadata_cache(schema_name => 'test_not_existing_schema', table_name => 'test_not_existing_table')");
    }

    @Test
    void testInvalidArguments()
    {
        assertQueryFails("CALL system.flush_metadata_cache(schema_name => '', table_name => 'test_not_existing_table')", "schemaName is empty");
        assertQueryFails("CALL system.flush_metadata_cache(schema_name => 'test_not_existing_schema', table_name => '')", "tableName is empty");
        assertQueryFails(
                "CALL system.flush_metadata_cache(schema_name => 'test_not_existing_schema')",
                "Illegal parameter set passed\\. Both schema_name and table_name must be provided, or neither\\.");
        assertQueryFails(
                "CALL system.flush_metadata_cache(table_name => 'test_not_existing_table')",
                "Illegal parameter set passed\\. Both schema_name and table_name must be provided, or neither\\.");
    }

    protected abstract String getSchemaLocation();

    protected abstract void renameTableOutsideTrino(String schemaName, String sourceTableName, String targetTableName);
}
