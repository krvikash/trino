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
package io.trino.plugin.warp;

import io.trino.Session;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PASS_THROUGH_DISPATCHER;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestWarpSpeedWithHiveProxiedConnectorTest
        extends BaseConnectorTest
{
    private static final String CATALOG_NAME = "warp_speed";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path hiveDir = Files.createTempDirectory("hive_catalog_");

        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.empty(),
                3,
                Map.of(),
                Map.of(
                        "http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME,
                        PASS_THROUGH_DISPATCHER, HIVE_CONNECTOR_NAME),  // so the results would be correct
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                CATALOG_NAME,
                new WarpPlugin(),
                Map.of(
                        // SQL functions
                        "sql.path", CATALOG_NAME + ".functions",
                        "sql.default-function-catalog", CATALOG_NAME,
                        "sql.default-function-schema", "functions"));

        // Install TPCH plugin for source data
        queryRunner.installPlugin(new io.trino.plugin.tpch.TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        // Create schema and copy TPCH tables using the queryRunner's default session
        String schemaName = queryRunner.getDefaultSession().getSchema().orElseThrow();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        copyTpchTables(queryRunner, "tpch", "tiny", queryRunner.getDefaultSession(), REQUIRED_TPCH_TABLES);

        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CTE_REUSE,
                 SUPPORTS_CREATE_FUNCTION,
                 SUPPORTS_MULTI_STATEMENT_WRITES,
                 SUPPORTS_REPORTING_WRITTEN_BYTES -> true;
            case SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_ADD_FIELD,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_DROP_FIELD,
                 SUPPORTS_MERGE,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_FIELD,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("WarpSpeed with Hive proxied connector does not support column default values");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE nation"))
                .matches("(?s)\\QCREATE TABLE warp_speed." + schema + ".nation (\n" +
                        "   nationkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   regionkey bigint,\n" +
                        "   comment varchar(152)\n" +
                        ")\\E\n" +
                        "WITH \\(.*");
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .matches("CREATE SCHEMA warp_speed." + schemaName + "\n" +
                        "WITH \\(\n" +
                        "   location = '.*'\n" +
                        "\\)");
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        // Hive-specific column name restrictions
        return switch (columnName) {
            case " aleadingspace" -> "Hive column names must not start with a space: ' aleadingspace'".equals(exception.getMessage());
            case "atrailingspace " -> "Hive column names must not end with a space: 'atrailingspace '".equals(exception.getMessage());
            case "a,comma" -> "Hive column names must not contain commas: 'a,comma'".equals(exception.getMessage());
            default -> false;
        };
    }

    @Test
    @Override
    public void testDropAndAddColumnWithSameName()
    {
        // Override because Hive connector can access old data after dropping and adding a column with same name
        assertThatThrownBy(super::testDropAndAddColumnWithSameName)
                .hasMessageContaining(
                        """
                        Actual rows (up to 100 of 1 extra rows shown, 1 rows in total):
                            [1, 2]\
                        """);
    }

    @Override
    protected void verifySelectAfterInsertFailurePermissible(Throwable e)
    {
        assertThat(getStackTraceAsString(e))
                .containsPattern("io.trino.spi.TrinoException: Cannot read from a table \\w+\\.test_insert_select_\\w+ that was modified within transaction");
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Schema name must be shorter than or equal to '128' characters but got '129'");
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Table name must be shorter than or equal to '128' characters but got .*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        // Hive doesn't support TIME or TIMESTAMP WITH TIME ZONE types
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        if (typeName.equals("timestamp(6)")) {
            // Supported depending on hive timestamp precision configuration
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected boolean supportsPhysicalPushdown()
    {
        // Hive table is created using default format which is ORC. Currently ORC reader has issue
        // pruning dereferenced struct fields https://github.com/trinodb/trino/issues/17201
        return false;
    }

    @Override
    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey, OptionalInt updateCount)
    {
        assertUpdate(createTable + " WITH (transactional = true)");
    }

    @Test
    @Override
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithVarcharPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateMultipleCondition()
    {
        assertThatThrownBy(super::testUpdateMultipleCondition)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateWithNullValues()
    {
        assertThatThrownBy(super::testUpdateWithNullValues)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateCaseSensitivity()
    {
        assertThatThrownBy(super::testUpdateCaseSensitivity)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateRowConcurrently()
            throws Exception
    {
        assertThatThrownBy(super::testUpdateRowConcurrently)
                .hasMessage("Unexpected concurrent update failure")
                .cause()
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateWithPredicates()
    {
        assertThatThrownBy(super::testUpdateWithPredicates)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateRowType()
    {
        assertThatThrownBy(super::testUpdateRowType)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateAllValues()
    {
        assertThatThrownBy(super::testUpdateAllValues)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void verifySupportsMergeDeclaration()
    {
        assertThatThrownBy(super::verifySupportsMergeDeclaration)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Disabled
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_small_file_threshold", "0B")
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_tiny_stripe_threshold", "0B")
                .build();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }
}
