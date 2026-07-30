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

import com.google.common.collect.MoreCollectors;
import io.trino.execution.QueryStats;
import io.trino.filesystem.Location;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Metric;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.TestingConnectorBehavior;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergDatabricksUnityScanPlanningConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final String DATABRICKS_UNITY_JDBC_URL = requireEnv("DATABRICKS_UNITY_JDBC_URL");
    private static final String DATABRICKS_HOST = requireEnv("DATABRICKS_HOST");
    private static final String DATABRICKS_TOKEN = requireEnv("DATABRICKS_TOKEN");
    private static final String DATABRICKS_AWS_REGION = requireEnv("DATABRICKS_AWS_REGION");

    private String testSchema;

    public TestIcebergDatabricksUnityScanPlanningConnectorSmokeTest()
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
        testSchema = "test_scan_planning_" + randomNameSuffix();
        populateFineGrainAccessControlTables();
        closeAfterClass(() -> executeOnDatabricks("DROP SCHEMA IF EXISTS main.%s CASCADE".formatted(testSchema)));

        return IcebergQueryRunner.builder()
                .amendSession(sessionBuilder -> sessionBuilder.setSchema(testSchema))
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.security", "read_only")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", format("https://%s/api/2.1/unity-catalog/iceberg-rest", DATABRICKS_HOST))
                .addIcebergProperty("iceberg.rest-catalog.warehouse", "main")
                .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                .addIcebergProperty("iceberg.rest-catalog.oauth2.token", DATABRICKS_TOKEN)
                .addIcebergProperty("iceberg.rest-catalog.vended-credentials-enabled", "true")
                .addIcebergProperty("fs.s3.enabled", "true")
                .addIcebergProperty("s3.region", DATABRICKS_AWS_REGION)
                .disableSchemaInitializer()
                .build();
    }

    private void populateFineGrainAccessControlTables()
    {
        String qualifiedSchema = "main.%s".formatted(testSchema);
        executeOnDatabricks("CREATE SCHEMA " + qualifiedSchema);

        executeOnDatabricks((
                "CREATE TABLE %s.region USING ICEBERG " +
                        "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported') AS " +
                        "SELECT r_regionkey AS regionkey, r_name AS name, r_comment AS comment FROM samples.tpch.region")
                .formatted(qualifiedSchema));
        executeOnDatabricks((
                "CREATE TABLE %s.nation USING ICEBERG " +
                        "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported') AS " +
                        "SELECT n_nationkey AS nationkey, n_name AS name, n_regionkey AS regionkey, n_comment AS comment FROM samples.tpch.nation")
                .formatted(qualifiedSchema));

        // Add passthrough column masks to enable server-side scan planning without affecting data
        executeOnDatabricks("CREATE OR REPLACE FUNCTION %s.passthrough_string(s STRING) RETURN s".formatted(qualifiedSchema));
        executeOnDatabricks("ALTER TABLE %s.region ALTER COLUMN comment SET MASK %s.passthrough_string".formatted(qualifiedSchema, qualifiedSchema));
        executeOnDatabricks("ALTER TABLE %s.nation ALTER COLUMN comment SET MASK %s.passthrough_string".formatted(qualifiedSchema, qualifiedSchema));

        // Add non-trivial column mask redacting `comment` to '***'
        executeOnDatabricks((
                "CREATE TABLE %s.nation_with_mask USING ICEBERG " +
                        "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported') AS " +
                        "SELECT * FROM %s.nation")
                .formatted(qualifiedSchema, qualifiedSchema));
        executeOnDatabricks("CREATE OR REPLACE FUNCTION %s.redact_string(s STRING) RETURN '***'".formatted(qualifiedSchema));
        executeOnDatabricks("ALTER TABLE %s.nation_with_mask ALTER COLUMN comment SET MASK %s.redact_string".formatted(qualifiedSchema, qualifiedSchema));

        // Add row filter limiting visible rows to nationkey < 10
        executeOnDatabricks((
                "CREATE TABLE %s.nation_with_filter USING ICEBERG " +
                        "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported') AS " +
                        "SELECT * FROM %s.nation")
                .formatted(qualifiedSchema, qualifiedSchema));
        executeOnDatabricks("CREATE OR REPLACE FUNCTION %s.nationkey_lt_ten(k BIGINT) RETURN k < 10".formatted(qualifiedSchema));
        executeOnDatabricks("ALTER TABLE %s.nation_with_filter SET ROW FILTER %s.nationkey_lt_ten ON (nationkey)".formatted(qualifiedSchema, qualifiedSchema));
    }

    private void executeOnDatabricks(String sql)
    {
        try (Connection connection = DriverManager.getConnection(DATABRICKS_UNITY_JDBC_URL, "token", DATABRICKS_TOKEN);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute on Databricks: " + sql, e);
        }
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        // File system access is not supported
    }

    @Test
    public void testServerSideScanPlanningUsed()
    {
        MaterializedResultWithPlan regionResult = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT count(*) FROM region");
        assertServerSideScanPlanningUsed(regionResult.queryId());

        MaterializedResultWithPlan nationResult = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT count(*) FROM nation");
        assertServerSideScanPlanningUsed(nationResult.queryId());
    }

    @Test
    public void testColumnMaskingApplied()
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT DISTINCT comment FROM nation_with_mask");
        assertThat(result.result().getOnlyColumnAsSet()).containsExactly("***");
        assertServerSideScanPlanningUsed(result.queryId());
    }

    @Test
    public void testRowFilteringApplied()
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT MAX(nationkey) FROM nation_with_filter");
        assertThat(result.result().getOnlyValue()).isEqualTo(9L);
        assertServerSideScanPlanningUsed(result.queryId());
    }

    @Test
    public void testHiddenColumnFilePath()
    {
        assertThat(query("SELECT count(DISTINCT \"$path\"), count(*) FROM region")).result()
                .matches("VALUES (BIGINT '1', BIGINT '5')");
        assertThat((String) computeScalar("SELECT \"$path\" FROM region LIMIT 1"))
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
    public void testSnapshotsMetadataTableRejected()
    {
        assertMetadataTableRejected("region$snapshots");
    }

    @Test
    public void testFilesMetadataTableRejected()
    {
        assertMetadataTableRejected("region$files");
    }

    @Test
    public void testManifestsMetadataTableRejected()
    {
        assertMetadataTableRejected("region$manifests");
    }

    @Test
    public void testPartitionsMetadataTable()
    {
        assertThat((Long) computeScalar("SELECT count(*) FROM \"region$partitions\""))
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    public void testRefsMetadataTableRejected()
    {
        assertMetadataTableRejected("region$refs");
    }

    private void assertMetadataTableRejected(String metadataTable)
    {
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT count(*) FROM \"" + metadataTable + "\""))
                .hasMessageMatching("Failed to initialize the vended credentials from the provided fileIoProperties|Malformed request: .*ErrorCode: 4000.*");
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

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("(?s)" +
                        "CREATE TABLE iceberg." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(.*" +
                        "   format = '" + format.name() + "',.*" +
                        "   format_version = 2,.*" +
                        "   location = '.*'.*" +
                        "\\)");
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String schemaPath()
    {
        // File system access is not supported. Return an empty string to verify "Access Denied" exceptions.
        return "";
    }

    @Override
    protected boolean locationExists(String location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void deleteDirectory(String location)
    {
        throw new UnsupportedOperationException();
    }

    @Test
    @Override
    public void testView()
    {
        testFailsDueToReadOnlyCatalog(super::testView);
    }

    @Test
    @Override
    public void testRegisterView()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterView);
    }

    @Test
    @Override
    public void testCommentView()
    {
        testFailsDueToReadOnlyCatalog(super::testCommentView);
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        testFailsDueToReadOnlyCatalog(super::testCommentViewColumn);
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        testFailsDueToReadOnlyCatalog(super::testMaterializedView);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        testFailsDueToReadOnlyCatalog(super::testRenameSchema);
    }

    @Test
    @Override
    public void testRenameTable()
    {
        testFailsDueToReadOnlyCatalog(super::testRenameTable);
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        testFailsDueToReadOnlyCatalog(super::testRenameTableAcrossSchemas);
    }

    @Test
    @Override
    public void testCreateTable()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTable);
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTableAsSelect);
    }

    @Test
    @Override
    public void testUpdate()
    {
        testFailsDueToReadOnlyCatalog(super::testUpdate);
    }

    @Test
    @Override
    public void testInsert()
    {
        testFailsDueToReadOnlyCatalog(super::testInsert);
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        testFailsDueToReadOnlyCatalog(super::testRowLevelDelete);
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        testFailsDueToReadOnlyCatalog(super::testDeleteAllDataFromTable);
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        testFailsDueToReadOnlyCatalog(super::testDeleteRowsConcurrently);
    }

    @Test
    @Override
    public void testDeleteWithV3Format()
    {
        testFailsDueToReadOnlyCatalog(super::testDeleteWithV3Format);
    }

    @Test
    @Override
    public void testDefaultColumnValue()
    {
        testFailsDueToReadOnlyCatalog(super::testDefaultColumnValue);
    }

    @Test
    @Override
    public void testVariantType()
    {
        testFailsDueToReadOnlyCatalog(super::testVariantType);
    }

    @Test
    @Override
    public void testCreateOrReplaceTable()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateOrReplaceTable);
    }

    @Test
    @Override
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateOrReplaceTableChangeColumnNamesAndTypes);
    }

    @Test
    @Override
    public void testRecreateTableWithSameName()
    {
        testFailsDueToReadOnlyCatalog(super::testRecreateTableWithSameName);
    }

    @Test
    @Override
    public void testRegisterTableWithTableLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithTableLocation);
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithComments);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        testFailsDueToReadOnlyCatalog(super::testRowLevelUpdate);
    }

    @Test
    @Override
    public void testMerge()
    {
        testFailsDueToReadOnlyCatalog(super::testMerge);
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateSchema);
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateSchemaWithNonLowercaseOwnerName);
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithShowCreateTable);
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithReInsert);
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithDroppedTable);
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithDifferentTableName);
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithMetadataFile);
    }

    @Test
    @Override
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTableWithTrailingSpaceInLocation);
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithTrailingSpaceInLocation);
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        testFailsDueToReadOnlyCatalog(super::testUnregisterTable);
    }

    @Test
    @Override
    public void testUnregisterBrokenTable()
    {
        testFailsDueToReadOnlyCatalog(super::testUnregisterBrokenTable);
    }

    @Test
    @Override
    public void testUnregisterTableNotExistingTable()
    {
        testFailsDueToReadOnlyCatalog(super::testUnregisterTableNotExistingTable);
    }

    @Test
    @Override
    public void testUnregisterTableNotExistingSchema()
    {
        testFailsDueToReadOnlyCatalog(super::testUnregisterTableNotExistingSchema);
    }

    @Test
    @Override
    public void testRepeatUnregisterTable()
    {
        testFailsDueToReadOnlyCatalog(super::testRepeatUnregisterTable);
    }

    @Test
    @Override
    public void testUnregisterTableAccessControl()
    {
        testFailsDueToReadOnlyCatalog(super::testUnregisterTableAccessControl);
    }

    @Test
    @Override
    public void testCreateTableWithNonExistingSchemaVerifyLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTableWithNonExistingSchemaVerifyLocation);
    }

    @Test
    @Override
    public void testSortedNationTable()
    {
        testFailsDueToReadOnlyCatalog(super::testSortedNationTable);
    }

    @Test
    @Override
    public void testFileSortingWithLargerTable()
    {
        testFailsDueToReadOnlyCatalog(super::testFileSortingWithLargerTable);
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingMetadataFile);
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingSnapshotFile);
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingManifestListFile);
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingDataFile);
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithNonExistentTableLocation);
    }

    @Test
    @Override
    public void testMetadataTables()
    {
        testFailsDueToReadOnlyCatalog(super::testMetadataTables);
    }

    @Test
    @Override
    public void testPartitionFilterRequired()
    {
        testFailsDueToReadOnlyCatalog(super::testPartitionFilterRequired);
    }

    @Test
    @Override
    public void testTableChangesFunction()
    {
        testFailsDueToReadOnlyCatalog(super::testTableChangesFunction);
    }

    @Test
    @Override
    public void testRowLevelDeletesWithTableChangesFunction()
    {
        testFailsDueToReadOnlyCatalog(super::testRowLevelDeletesWithTableChangesFunction);
    }

    @Test
    @Override
    public void testCreateOrReplaceWithTableChangesFunction()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateOrReplaceWithTableChangesFunction);
    }

    @Test
    @Override
    public void testTruncateTable()
    {
        testFailsDueToReadOnlyCatalog(super::testTruncateTable);
    }

    @Test
    @Override
    public void testMetadataDeleteAfterCommitEnabled()
    {
        testFailsDueToReadOnlyCatalog(super::testMetadataDeleteAfterCommitEnabled);
    }

    @Test
    @Override
    public void testAnalyze()
    {
        testFailsDueToReadOnlyCatalog(super::testAnalyze);
    }

    @Test
    @Override
    public void testIcebergTablesSystemTable()
    {
        testFailsDueToReadOnlyCatalog(super::testIcebergTablesSystemTable);
    }

    private static void testFailsDueToReadOnlyCatalog(ThrowingCallable callable)
    {
        String[] expectedReasons = Stream.of(
                        "Cannot create schema",
                        "Cannot rename schema",
                        "Cannot create table",
                        "Cannot create materialized view",
                        "Cannot create view",
                        "Cannot execute procedure")
                .map(reason -> "Access Denied: " + reason)
                .toArray(String[]::new);
        assertThatThrownBy(callable)
                .satisfies(throwable -> assertThat(getStackTraceAsString(throwable))
                        .containsAnyOf(expectedReasons));
    }
}
