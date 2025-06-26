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
package io.trino.plugin.iceberg.catalog.rest;

import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergLakekeeperCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private TestingLakekeeperCatalog lakekeeperCatalog;

    public TestIcebergLakekeeperCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        lakekeeperCatalog = closeAfterClass(new TestingLakekeeperCatalog());

        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .addIcebergProperty("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.nested-namespace-enabled", "true")
                .addIcebergProperty("iceberg.rest-catalog.uri", lakekeeperCatalog.restUri() + "/catalog")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", TestingLakekeeperCatalog.WAREHOUSE)
                .addIcebergProperty("s3.endpoint", lakekeeperCatalog.externalMinioAddress())
                .addIcebergProperty("fs.native-s3.enabled", "true")
                .addIcebergProperty("s3.region", "dummy")
                .addIcebergProperty("s3.path-style-access", "true")
                .addIcebergProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addIcebergProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        lakekeeperCatalog.dropWithoutPurge(getSession().getSchema().orElseThrow(), tableName);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        TrinoCatalog trinoCatalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
        BaseTable table = trinoCatalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    @Test
    @Disabled("Disabled since unregister -> register is currently unsupported by lakekeeper due to defaulting to purgeRequested = true.")
    public void testUnregisterTable() {}

    @Override
    @Test
    @Disabled("Disabled since unregister -> register is currently unsupported by lakekeeper due to defaulting to purgeRequested = true.")
    public void testRepeatUnregisterTable() {}

    @Override
    protected String schemaPath()
    {
        return format("s3://%s/%s", TestingLakekeeperCatalog.BUCKET, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            // we're using s3 which doesn't have the notation of a directory,
            // so we just check if there are any files
            return fileSystem.listFiles(Location.of(location)).hasNext();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testCreateTableWithTrailingSpaceInLocation).hasStackTraceContaining("Trailing whitespaces are forbidden");
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testCreateTableWithTrailingSpaceInLocation).hasStackTraceContaining("Trailing whitespaces are forbidden");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("CREATE TABLE iceberg." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        // lakekeeper uses namespaceId instead of schemaName in the location
                        format("   location = 's3://" + TestingLakekeeperCatalog.BUCKET + "/.*/region.*',\n" +
                                "   max_commit_retry = 4\n") +
                        "\\)");
    }

    @Override
    protected void assertLocationNotExists(Location tableLocation)
            throws IOException
    {
        assertEventually(() -> super.assertLocationNotExists(tableLocation));
    }

    @Test
    @Override
    public void testDeleteWithV3Format()
    {
        assertThatThrownBy(super::testDeleteWithV3Format)
                .hasMessageContaining("Failed to create transaction")
                .hasStackTraceContaining("Invalid format version specified in table_properties: 3");
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        String tableName = "test_register_table_with_dropped_table_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = tableName + "_new";
        // Drop table to verify register_table call fails when no metadata can be found (table doesn't exist)
        assertUpdate(format("DROP TABLE %s", tableName));

        // Use Thread.sleep for this specific test as it seems to require the exact timing
        // that was previously working with the original implementation
        try {
            Thread.sleep(1100);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertQueryFails(format("CALL system.register_table (CURRENT_SCHEMA, '%s', '%s')", tableNameNew, tableLocation),
                ".*No versioned metadata file exists at location.*");
    }

    @Test
    @Override
    @Disabled("Disabled due to https://github.com/trinodb/trino/issues/23941")
    public void testDeleteRowsConcurrently() {}

    @Test
    @Override
    public void testUnregisterTableNotExistingTable()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingTable).hasStackTraceContaining("Forbidden: Table not found or action can_drop forbidden for Anonymous");
    }

    @Test
    void testNestedNamespace()
    {
        String parentNamespace = "level1_" + randomNameSuffix();
        String nestedNamespace = parentNamespace + ".level2_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + parentNamespace);
        assertUpdate("CREATE SCHEMA \"" + nestedNamespace + "\"");
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains(parentNamespace, nestedNamespace);

        assertUpdate("DROP SCHEMA \"" + nestedNamespace + "\"");
        assertUpdate("DROP SCHEMA " + parentNamespace);
    }
}
