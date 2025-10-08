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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COMPOSITE_SPLITS_ENABLED;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PASS_THROUGH_DISPATCHER;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestWarpSpeedWithIcebergProxiedConnectorTest
        extends BaseConnectorTest
{
    private static final String CATALOG_NAME = "warp_speed";
    private static final IcebergFileFormat format = PARQUET;
    private static final int formatVersion = 2;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path icebergDir = Files.createTempDirectory("iceberg_catalog_");

        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.empty(),
                3,
                Map.of(),
                Map.ofEntries(
                        Map.entry("http-server.log.enabled", "false"),
                        Map.entry(USE_HTTP_SERVER_PORT, "false"),
                        Map.entry("node.environment", "warp"),
                        Map.entry("iceberg.catalog.type", "TESTING_FILE_METASTORE"),
                        Map.entry("iceberg.file-format", format.name()),
                        Map.entry("iceberg.format-version", Integer.toString(formatVersion)),
                        // Only allow some extra properties. Add "sorted_by" so that we can test that the property is disallowed by the connector explicitly.
                        Map.entry("iceberg.allowed-extra-properties", "extra.property.one,extra.property.two,extra.property.three,sorted_by"),
                        // Allows testing the sorting writer flushing to the file system with smaller tables
                        Map.entry("iceberg.writer-sort-buffer-size", "1MB"),
                        // Disable partition statistics to make diff from Trino smaller
                        Map.entry("iceberg.partition-statistics.enabled", "false"),
                        Map.entry(PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME),
                        Map.entry(PASS_THROUGH_DISPATCHER, ICEBERG_CONNECTOR_NAME)),  // so the results would be correct
                icebergDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                CATALOG_NAME,
                new WarpPlugin(),
                Map.of(
                        // SQL functions
                        "sql.path", CATALOG_NAME + ".functions",
                        "sql.default-function-catalog", CATALOG_NAME,
                        "sql.default-function-schema", "functions"));

        // Register Iceberg internal functions
        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());

        // Install TPCH plugin for source data
        queryRunner.installPlugin(new TpchPlugin());
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
            case SUPPORTS_CREATE_OR_REPLACE_TABLE,
                 SUPPORTS_CTE_REUSE,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_REPORTING_WRITTEN_BYTES -> true;
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_REFRESH_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS,
                 SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .matches(format("CREATE SCHEMA %s.%s\\s+", CATALOG_NAME, schemaName) +
                        format("AUTHORIZATION USER %s\\s+" +
                                "WITH \\(\n" +
                                "\\s+location = '.*'\n" +
                                "\\)", getSession().getIdentity().getUser()));
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        try (TestTable table = newTrinoTable(
                "test_char_varchar",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS CHAR(3))), " +
                        "   (3, CAST('   ' AS CHAR(3)))," +
                        "   (6, CAST('x  ' AS CHAR(3)))")) {
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('' AS varchar(2))", "VALUES (3, '')");

            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x' AS varchar(2))", "VALUES (6, 'x')");

            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('   ' AS varchar(3))")).returnsEmptyResult();
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))")).returnsEmptyResult();
        }
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat(e)
                .hasMessageMatching("Version pointer type is not supported: .*|" +
                        "Unsupported type for temporal table version: .*|" +
                        "Unsupported type for table version: .*|" +
                        "No version history table " + schemaName + ".nation at or before .*|" +
                        "Iceberg snapshot ID does not exists: .*|" +
                        "Cannot find snapshot with reference name: .*");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches(format("\\QCREATE TABLE %s.%s.orders (\n", CATALOG_NAME, schemaName) +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = " + formatVersion + ",\n" +
                        "   location = '\\E.*orders-.*\\Q'\n" +
                        ")\\E");
    }

    @Test
    @Override
    public void testAddDefaultColumn()
    {
        assertThatThrownBy(super::testAddDefaultColumn)
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Test
    @Override
    public void testDropRowFieldWhenDuplicates()
    {
        // Iceberg doesn't allow duplicated field names in a row type
        assertThatThrownBy(super::testDropRowFieldWhenDuplicates)
                .hasMessage("Field name 'a' specified more than once");
    }

    @Test
    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        // Iceberg doesn't allow ambiguous field names in row types
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity)
                .hasMessage("Field name 'some_field' specified more than once");
    }

    @Test
    @Override
    public void testSetDefaultColumn()
    {
        assertThatThrownBy(super::testSetDefaultColumn)
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Test
    @Override
    public void testDropDefaultColumn()
    {
        assertThatThrownBy(super::testDropDefaultColumn)
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("timestamp(3) with time zone")) {
            // The connector returns UTC instead of the given time zone
            return Optional.of(setup.withNewValueLiteral("TIMESTAMP '2020-02-12 14:03:00.123000 +00:00'"));
        }
        return switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            // TODO https://github.com/trinodb/trino/issues/15822 The connector returns incorrect NULL when a field in row type doesn't exist in Parquet files
            case "row(x integer) -> row(\"y\" integer)" -> Optional.of(setup.withNewValueLiteral("NULL"));
            // Iceberg allows updating column types if the update is safe. Safe updates are:
            // - int to bigint
            // - float to double
            // - decimal(P,S) to decimal(P2,S) when P2 > P (scale cannot change)
            // https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table--alter-column
            case "tinyint -> smallint",
                 "bigint -> integer",
                 "bigint -> smallint",
                 "bigint -> tinyint",
                 "decimal(5,3) -> decimal(5,2)",
                 "char(25) -> char(20)",
                 "varchar -> char(20)",
                 "time(6) -> time(3)",
                 "timestamp(6) -> timestamp(3)",
                 // Iceberg cannot update map keys
                 "map(integer, varchar) -> map(bigint, varchar)" -> Optional.of(setup.asUnsupported());
            // Iceberg connector ignores the varchar length
            case "varchar(100) -> varchar(50)" -> Optional.empty();
            default -> Optional.of(setup);
        };
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*(Failed to set column type: Cannot change (column type:|type from .* to )" +
                "|Time precision \\(3\\) not supported for Iceberg. Use \"time\\(6\\)\" instead" +
                "|Timestamp precision \\(3\\) not supported for Iceberg. Use \"timestamp\\(6\\)\" or \"timestamp\\(9\\)\" instead" +
                "|Type not supported for Iceberg: (tinyint|smallint|char\\(20\\))" +
                "|Cannot update map keys).*");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetFieldTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("timestamp(3) with time zone")) {
            // The connector returns UTC instead of the given time zone
            return Optional.of(setup.withNewValueLiteral("TIMESTAMP '2020-02-12 14:03:00.123000 +00:00'"));
        }
        return switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            // TODO https://github.com/trinodb/trino/issues/15822 The connector returns incorrect NULL when a field in row type doesn't exist in Parquet files
            // Skip this test entirely, as the newValueLiteral is always wrapped in a row
            case "row(x integer) -> row(\"y\" integer)" -> Optional.empty();

            // Iceberg allows updating column types if the update is safe. Safe updates are:
            // - int to bigint
            // - float to double
            // - decimal(P,S) to decimal(P2,S) when P2 > P (scale cannot change)
            // https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table--alter-column
            case "tinyint -> smallint",
                 "bigint -> integer",
                 "bigint -> smallint",
                 "bigint -> tinyint",
                 "decimal(5,3) -> decimal(5,2)",
                 "char(25) -> char(20)",
                 "varchar -> char(20)",
                 "time(6) -> time(3)",
                 "timestamp(6) -> timestamp(3)",
                 // Iceberg cannot update map keys
                 "map(integer, varchar) -> map(bigint, varchar)" -> Optional.of(setup.asUnsupported());
            // Iceberg connector ignores the varchar length
            case "varchar(100) -> varchar(50)" -> Optional.empty();
            default -> Optional.of(setup);
        };
    }

    @Test
    @Override
    public void testSetFieldMapKeyType()
    {
        // Iceberg doesn't support change a map 'key' column. Only map values can be changed.
        assertThatThrownBy(super::testSetFieldMapKeyType)
                .hasMessageContaining("Failed to set field type: Cannot alter map keys");
    }

    @Test
    @Override
    public void testSetNestedFieldMapKeyType()
    {
        // Iceberg doesn't support change a map 'key' column. Only map values can be changed.
        assertThatThrownBy(super::testSetNestedFieldMapKeyType)
                .hasMessageContaining("Failed to set field type: Cannot alter map keys");
    }

    @Override
    protected void verifySetFieldTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*(Failed to set field type: Cannot change (column type:|type from .* to )" +
                "|Time precision \\(3\\) not supported for Iceberg. Use \"time\\(6\\)\" instead" +
                "|Timestamp precision \\(3\\) not supported for Iceberg. Use \"timestamp\\(6\\)\" or \"timestamp\\(9\\)\" instead" +
                "|Type not supported for Iceberg: (tinyint|smallint|char\\(20\\))" +
                "|Cannot update map keys).*");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Schema name must be shorter than or equal to '128' characters but got '129'");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*Table name must be shorter than or equal to '128' characters but got .*");
    }

    @Test
    @Override
    public void testCreateTableWithDefaultColumn()
    {
        String tableName = "test_default_value_" + randomNameSuffix();
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + " (x int DEFAULT 1)"))
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Test
    @Override
    public void testInsertDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testInsertDefaultNullIntoNotNullColumn)
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("WarpSpeed with Iceberg proxied connector does not support column default values");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("Failed to commit the transaction during write.*|" +
                "Failed to commit during write.*");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageStartingWith("Failed to add column: Failed to replace table due to concurrent updates")
                .rootCause()
                .hasMessageContaining("Cannot update Iceberg table: supplied previous location does not match current location");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(3)")) {
            // Iceberg coerces char to varchar by trimming trailing spaces, so use a literal without trailing whitespace
            return Optional.of(new DataMappingTestSetup(typeName, "'ab'", dataMappingTestSetup.getHighValueLiteral()));
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Test
    @Override
    public void testMergeWithDefaultColumnValue()
    {
        assertThatThrownBy(super::testMergeWithDefaultColumnValue)
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Test
    @Override
    public void testMergeDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testMergeDefaultNullIntoNotNullColumn)
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Test
    void testCompositeSplits()
    {
        String tableName = "test_composite_splits_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (a INT, b VARCHAR)");
            for (int i = 0; i < 5; i++) {
                assertUpdate("INSERT INTO " + tableName + " VALUES (" + i + ", 'value" + i + "')", 1);
            }

            Session compositeSplitsSession = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), COMPOSITE_SPLITS_ENABLED, "true")
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "max_split_size", "100MB")
                    .build();

            assertQuery(
                    compositeSplitsSession,
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 'value0'), (1, 'value1'), (2, 'value2'), (3, 'value3'), (4, 'value4')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
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
    protected Map<String, String> getBehaviorAlteringCatalogProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.allowed-extra-properties", "normallynotallowed")
                .buildOrThrow();
    }

    @Override
    protected void assertAlteredCatalogBehavior(String catalogName)
    {
        String schemaName = "test_schema_without_location" + randomNameSuffix();
        String schemaLocation = "/tmp/" + schemaName;
        String tableName = "test_create_external" + randomNameSuffix();
        TrinoFileSystem fileSystem = ((IcebergConnector) ((StarburstWarpConnector) getDistributedQueryRunner().getCoordinator().getConnector(CATALOG_NAME)).getProxiedConnector())
                .getInjector().getInstance(TrinoFileSystemFactory.class).create(SESSION);

        try {
            fileSystem.createDirectory(Location.of(schemaLocation));
            assertUpdate(format("CREATE SCHEMA %s.%s WITH (location = '%s')", catalogName, schemaName, schemaLocation));
            String createTableSql = format(
                    """
                    CREATE TABLE %s.%s.%s WITH (
                        extra_properties = MAP(ARRAY['normallynotallowed'], ARRAY['foo'])
                    ) AS SELECT 1 as c1""",
                    catalogName,
                    schemaName,
                    tableName);
            assertQuerySucceeds(createTableSql);
            fileSystem.deleteDirectory(Location.of(schemaLocation));
        }
        catch (IOException exception) {
            // Let the test fail up higher on IO exceptions.
            throw new RuntimeException(exception);
        }
    }
}
