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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.filesystem.Location;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.QueryId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.Metric;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergRestScanPlanningConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    @TempDir
    private static Path path;

    public TestIcebergRestScanPlanningConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
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

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        JdbcCatalog backend = closeAfterClass(ServerScanPlanningRestCatalogAdapter.buildBackendCatalog(path));
        TestingHttpServer restServer = ServerScanPlanningRestCatalogAdapter.startTestServer(new ServerScanPlanningRestCatalogAdapter(backend));
        closeAfterClass(restServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(path))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("fs.hadoop.enabled", "true")
                                .put("iceberg.rest-catalog.uri", restServer.getBaseUrl().toString())
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .buildOrThrow())
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                        .withSchemaProperties(Map.of("location", "'file://" + path + "'"))
                        .build())
                .build();
    }

    @Test
    public void testServerSideScanPlanning()
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT * FROM region");
        assertServerSideScanPlanningUsed(result.queryId());
    }

    @Test
    public void testHiddenColumnFilePath()
    {
        assertThat(query("SELECT count(DISTINCT \"$path\"), count(*) FROM region")).result()
                .matches("VALUES (BIGINT '1', BIGINT '5')");
        assertThat((String) computeScalar("SELECT \"$path\" FROM region LIMIT 1"))
                .contains("/region")
                .endsWith("." + format.name().toLowerCase(Locale.ROOT));
    }

    @Test
    public void testHiddenColumnFileModifiedTime()
    {
        assertThat(query("SELECT count(DISTINCT \"$file_modified_time\"), count(*) FROM region")).result()
                .matches("VALUES (BIGINT '1', BIGINT '5')");
    }

    @Test
    public void testHistoryMetadataTable()
    {
        assertThat(query("SELECT parent_id, is_current_ancestor FROM \"region$history\""))
                .matches("VALUES (CAST(NULL AS BIGINT), true)");
    }

    @Test
    public void testPropertiesMetadataTable()
    {
        assertThat((Long) computeScalar("SELECT count(*) FROM \"region$properties\""))
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    @Override
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
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        "   location = '.*'\n" +
                        "\\)");
    }

    @Test
    @Override
    public void testAnalyze()
    {
        Session noStatsOnWrite = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", COLLECT_EXTENDED_STATISTICS_ON_WRITE, "false")
                .build();

        // Server-side scan planning does not support stats
        try (TestTable table = newTrinoTable("test_analyze", "(id int)")) {
            assertUpdate(noStatsOnWrite, "INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            assertUpdate("ANALYZE " + table.getName());
            assertThat(query("SHOW STATS FOR " + table.getName())).result()
                    .projected("column_name", "distinct_values_count")
                    .matches("VALUES (VARCHAR 'id', CAST(NULL AS DOUBLE)), (CAST(NULL AS VARCHAR), CAST(NULL AS DOUBLE))");
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

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
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
    protected String schemaPath()
    {
        return format("%s/%s", path, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return Files.exists(Path.of(location));
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .isInstanceOf(QueryFailedException.class)
                .cause()
                .hasMessageMatching("Failed to read file: .*avro")
                .hasNoCause();
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Table location should not exist");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageMatching("Failed to load table: (.*)");
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

    private void assertServerSideScanPlanningUsed(QueryId queryId)
    {
        QueryStats stats = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats();
        OperatorStats tableScan = stats.getOperatorSummaries().stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                .collect(MoreCollectors.onlyElement());

        Map<String, Metric<?>> metrics = tableScan.getConnectorMetrics().getMetrics();
        assertThat(metrics).containsKey("serverSideScanCount");

        long server = ((LongCount) metrics.getOrDefault("serverSideScanCount", new LongCount(0))).getTotal();
        long client = ((LongCount) metrics.getOrDefault("clientSideScanCount", new LongCount(0))).getTotal();
        assertThat(server).isGreaterThan(0);
        assertThat(client).isEqualTo(0);
    }
}
