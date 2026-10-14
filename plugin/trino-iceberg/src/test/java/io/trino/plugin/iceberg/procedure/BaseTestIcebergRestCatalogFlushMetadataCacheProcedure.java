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

import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;

abstract class BaseTestIcebergRestCatalogFlushMetadataCacheProcedure
        extends BaseTestIcebergFlushMetadataCacheProcedure
{
    @TempDir
    private static Path warehouseLocation;
    private final boolean caseInsensitiveNameMatching;
    protected JdbcCatalog backend;

    protected BaseTestIcebergRestCatalogFlushMetadataCacheProcedure(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .setBaseDataDir(Optional.of(warehouseLocation))
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.rest-catalog.metastore-cache.ttl", "10m")
                .addIcebergProperty("iceberg.rest-catalog.case-insensitive-name-matching", Boolean.toString(caseInsensitiveNameMatching))
                .disableSchemaInitializer()
                .build();
    }

    @Override
    protected String getSchemaLocation()
    {
        return warehouseLocation.toAbsolutePath().toString();
    }

    @Override
    protected void renameTableOutsideTrino(String schemaName, String sourceTableName, String targetTableName)
    {
        backend.renameTable(
                TableIdentifier.of(Namespace.of(schemaName), sourceTableName),
                TableIdentifier.of(Namespace.of(schemaName), targetTableName));
    }

    @Test
    void testFlushTableDoesNotFlushOtherTables()
    {
        String schemaName = "test_flush_table_only_" + randomNameSuffix();
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

            renameTableOutsideTrino(schemaName, tableName1, renamedTableName1);
            renameTableOutsideTrino(schemaName, tableName2, renamedTableName2);

            assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + schemaName + "', table_name => '" + tableName1 + "')");

            assertQueryFails("SELECT * FROM " + schemaName + "." + tableName1, ".*Table '.*' does not exist");
            // The other table is still served from the cache
            assertQuery("SELECT * FROM " + schemaName + "." + tableName2, "VALUES 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + renamedTableName1);
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + renamedTableName2);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    void testFlushTableDoesNotFlushSameTableInOtherSchema()
    {
        String suffix = randomNameSuffix();
        String schemaName1 = "test_flush_table_schema_1_" + suffix;
        String schemaName2 = "test_flush_table_schema_2_" + suffix;
        String tableName = "test_table";
        String renamedTableName = "renamed_table";

        try {
            assertUpdate("CREATE SCHEMA " + schemaName1 + " WITH (location = '" + getSchemaLocation() + "/" + schemaName1 + "')");
            assertUpdate("CREATE SCHEMA " + schemaName2 + " WITH (location = '" + getSchemaLocation() + "/" + schemaName2 + "')");
            assertUpdate("CREATE TABLE " + schemaName1 + "." + tableName + " AS SELECT 1 AS id", 1);
            assertUpdate("CREATE TABLE " + schemaName2 + "." + tableName + " AS SELECT 2 AS id", 1);

            // Populate cache
            assertQuery("SELECT * FROM " + schemaName1 + "." + tableName, "VALUES 1");
            assertQuery("SELECT * FROM " + schemaName2 + "." + tableName, "VALUES 2");

            renameTableOutsideTrino(schemaName1, tableName, renamedTableName);
            renameTableOutsideTrino(schemaName2, tableName, renamedTableName);

            assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + schemaName1 + "', table_name => '" + tableName + "')");

            assertQueryFails("SELECT * FROM " + schemaName1 + "." + tableName, ".*Table '.*' does not exist");
            // The table with the same name in the other schema is still served from the cache
            assertQuery("SELECT * FROM " + schemaName2 + "." + tableName, "VALUES 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName1 + "." + renamedTableName);
            assertUpdate("DROP TABLE IF EXISTS " + schemaName2 + "." + renamedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName1);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName2);
        }
    }
}
