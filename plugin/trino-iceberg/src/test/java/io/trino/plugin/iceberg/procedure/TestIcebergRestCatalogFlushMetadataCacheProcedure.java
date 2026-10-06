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
import static java.util.Locale.ENGLISH;

final class TestIcebergRestCatalogFlushMetadataCacheProcedure
        extends BaseTestIcebergFlushMetadataCacheProcedure
{
    @TempDir
    private static Path warehouseLocation;
    private JdbcCatalog backend;

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
                .addIcebergProperty("iceberg.rest-catalog.case-insensitive-name-matching", "true")
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
