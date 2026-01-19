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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergDatabricksUnityRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final String DATABRICKS_HOST = requireEnv("DATABRICKS_HOST");
    private static final String DATABRICKS_TOKEN = requireEnv("DATABRICKS_TOKEN");
    private static final String DATABRICKS_UNITY_CATALOG_NAME = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
    private static final String DATABRICKS_AWS_ACCESS_KEY_ID = requireEnv("DATABRICKS_AWS_ACCESS_KEY_ID");
    private static final String DATABRICKS_AWS_SECRET_ACCESS_KEY = requireEnv("DATABRICKS_AWS_SECRET_ACCESS_KEY");
    private static final String DATABRICKS_AWS_REGION = requireEnv("DATABRICKS_AWS_REGION");

    private static final String SCHEMA = "test_iceberg_smoke_" + randomNameSuffix();

    private TrinoCatalog catalog;

    public TestIcebergDatabricksUnityRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder(SCHEMA)
                .addIcebergProperty("iceberg.format-version", "3") // Databricks skipped support for Iceberg v2
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.view-endpoints-enabled", "false")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", DATABRICKS_UNITY_CATALOG_NAME)
                .addIcebergProperty("iceberg.rest-catalog.uri", "https://%s/api/2.1/unity-catalog/iceberg-rest".formatted(DATABRICKS_HOST))
                .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                .addIcebergProperty("iceberg.rest-catalog.oauth2.server-uri", "https://%s/oidc/v1/token".formatted(DATABRICKS_HOST))
                .addIcebergProperty("iceberg.rest-catalog.oauth2.token", DATABRICKS_TOKEN)
                .addIcebergProperty("iceberg.rest-catalog.oauth2.scope", "all-apis")
                .addIcebergProperty("iceberg.rest-catalog.vended-credentials-enabled", "true")
                .addIcebergProperty("fs.s3.enabled", "true")
                .addIcebergProperty("s3.region", DATABRICKS_AWS_REGION)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @BeforeAll
    public void setup()
    {
        fileSystem = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(DATABRICKS_AWS_ACCESS_KEY_ID)
                        .setAwsSecretKey(DATABRICKS_AWS_SECRET_ACCESS_KEY)
                        .setRegion(DATABRICKS_AWS_REGION),
                new S3FileSystemStats()).create(SESSION);
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        catalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
    }

    @Override
    protected void createSchema(String schemaName)
    {
        assertUpdate("CREATE SCHEMA " + schemaName);
    }

    @Override
    protected void dropSchema(String schema)
    {
        assertUpdate("DROP SCHEMA " + schema);
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
    }

    @Override
    protected String schemaPath()
    {
        return format("s3://starburstdata-unity/%s", getSession().getSchema().orElseThrow());
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        BaseTable table = catalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String getTableLocation(String tableName)
    {
        BaseTable table = catalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().location();
    }

    @Override
    protected Location getTableDataLocation(String tableName)
    {
        BaseTable table = catalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return Location.of(table.properties().get(WRITE_DATA_LOCATION));
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            deleteRecursively(Path.of(location), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override // Databricks Unity catalog sets additional table properties by default
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("" +
                        "CREATE TABLE iceberg." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   compression_codec = 'ZSTD',\n" +
                        "   data_location = '.*',\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        "   location = '.*/" + schemaName + "/.*',\n" +
                        "   max_previous_versions = 100,\n" +
                        "   object_store_layout_enabled = true\n" +
                        "\\)");
    }

    @Test
    @Override // Databricks Unity Catalog ignores the specified location
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        String tableName = "test_create_table_with_trailing_space_" + randomNameSuffix();
        String tableLocationWithTrailingSpace = schemaPath() + "/" + tableName + " ";

        assertQuerySucceeds(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS a, 'INDIA' AS b, true AS c", tableName, tableLocationWithTrailingSpace));
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        assertThat(getTableLocation(tableName)).isNotEqualTo(tableLocationWithTrailingSpace);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override // TODO Investigate why SHOW STATS returns incorrect NDV
    public void testAnalyze()
    {
        Session noStatsOnWrite = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", COLLECT_EXTENDED_STATISTICS_ON_WRITE, "false")
                .build();

        try (TestTable table = newTrinoTable("test_analyze", "(id int)")) {
            assertUpdate(noStatsOnWrite, "INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);

            assertUpdate("ANALYZE " + table.getName());
            assertThat(query("SHOW STATS FOR " + table.getName())).result()
                    .projected("column_name", "distinct_values_count")
                    .matches("VALUES (VARCHAR 'id', CAST(null AS double)), (null, null)");
        }
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
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testRegisterView()
    {
        assertThatThrownBy(super::testRegisterView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
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
    public void testCreateOrReplaceTable()
    {
        assertThatThrownBy(super::testCreateOrReplaceTable)
                .hasMessageContaining("Malformed request: REPLACE TABLE is not yet supported on IcebergCompatV3 tables");
    }

    @Test
    @Override
    public void testCreateOrReplaceWithTableChangesFunction()
    {
        assertThatThrownBy(super::testCreateOrReplaceWithTableChangesFunction)
                .hasMessageContaining("Malformed request: REPLACE TABLE is not yet supported on IcebergCompatV3 tables");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        assertThatThrownBy(super::testCreateOrReplaceTableChangeColumnNamesAndTypes)
                .hasMessageContaining("Malformed request: REPLACE TABLE is not yet supported on IcebergCompatV3 tables");
    }

    @Test
    @Override
    public void testMetadataDeleteAfterCommitEnabled()
    {
        assertThatThrownBy(super::testMetadataDeleteAfterCommitEnabled)
                .hasMessageContaining("Malformed request: INVALID_PARAMETER_VALUE: Table properties contain prohibited keys: write.metadata.previous-versions-max");
    }

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithTableLocation() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithComments() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithShowCreateTable() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithReInsert() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithDroppedTable() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithDifferentTableName() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithMetadataFile() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRegisterTableWithTrailingSpaceInLocation() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testUnregisterTable() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testRepeatUnregisterTable() {}

    @Test
    @Override // The procedure is disabled because of credential vending
    public void testUnregisterBrokenTable() {}

    @Test
    @Override // TODO Enable after fixing flaky timeout: "Task 0 did not complete in time"
    public void testDeleteRowsConcurrently() {}
}
