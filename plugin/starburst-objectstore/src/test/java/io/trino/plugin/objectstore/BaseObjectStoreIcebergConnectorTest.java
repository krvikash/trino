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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests ObjectStore connector with Iceberg backend.
 *
 * @see BaseTestObjectStoreIcebergFeaturesConnectorTest
 */
public abstract class BaseObjectStoreIcebergConnectorTest
        extends BaseObjectStoreConnectorTest
{
    private long v1SnapshotId;
    private long v1EpochMillis;
    private long v2SnapshotId;
    private long v2EpochMillis;
    private long incorrectSnapshotId;

    public BaseObjectStoreIcebergConnectorTest(boolean isGalaxyMetastore)
    {
        super(isGalaxyMetastore, ICEBERG);
    }

    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        boolean connectorHasBehavior = new GetIcebergConnectorTestBehavior().hasBehavior(connectorBehavior);

        return switch (connectorBehavior) {
            case SUPPORTS_DROP_SCHEMA_CASCADE -> true;
            case SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS -> {
                // TODO when this changes, remove this flag from other BaseObjectStoreConnectorTest subclasses
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                yield false;
            }
            // ObjectStore adds support for refreshing views using Hive
            case SUPPORTS_REFRESH_VIEW -> {
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                yield true;
            }
            case SUPPORTS_CREATE_FUNCTION -> false;
            // By default, declare all behaviors/features supported by Iceberg connector
            default -> connectorHasBehavior;
        };
    }

    @BeforeAll
    public void setUp()
            throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_iceberg_read_versioned_table(a_string varchar, an_integer integer)");
        assertQuerySucceeds("INSERT INTO test_iceberg_read_versioned_table VALUES ('a', 1)");
        if (isGalaxyMetastore) {
            getQueryRunner().execute("GRANT SELECT ON \"test_iceberg_read_versioned_table$snapshots\" TO ROLE accountadmin");
        }

        v1SnapshotId = getLatestSnapshotId("test_iceberg_read_versioned_table");
        v1EpochMillis = getCommittedAtInEpochMilliSeconds("test_iceberg_read_versioned_table", v1SnapshotId);
        TimeUnit.MILLISECONDS.sleep(1);
        assertQuerySucceeds("INSERT INTO test_iceberg_read_versioned_table VALUES ('b', 2)");
        v2SnapshotId = getLatestSnapshotId("test_iceberg_read_versioned_table");
        v2EpochMillis = getCommittedAtInEpochMilliSeconds("test_iceberg_read_versioned_table", v2SnapshotId);
        incorrectSnapshotId = v2SnapshotId + 1;
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
        if (isGalaxyMetastore) {
            assertThat(e)
                    .hasMessageContaining("Cannot update Iceberg table: supplied previous location does not match current location");
        }
        else {
            assertThat(e)
                    .hasMessageMatching("Failed to add column: Metadata location .* is not same as table metadata location .* for .*");
        }
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("timestamp(3) with time zone")) {
            // The connector returns UTC instead of the given time zone
            return Optional.of(setup.withNewValueLiteral("TIMESTAMP '2020-02-12 14:03:00.123000 +00:00'"));
        }
        // Iceberg allows updating column types if the update is safe. Safe updates are:
        // - int to bigint
        // - float to double
        // - decimal(P,S) to decimal(P2,S) when P2 > P (scale cannot change)
        // https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table--alter-column
        return switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            // TODO https://github.com/trinodb/trino/issues/15822 The connector returns incorrect NULL when a field in row type doesn't exist in Parquet files
            case "row(x integer) -> row(\"y\" integer)" -> Optional.of(setup.withNewValueLiteral("NULL"));
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
        assertThat(e).hasMessageMatching(".*(Cannot change column type|not supported for Iceberg|Not a primitive type|Cannot change type |Cannot update map keys).*");
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Version pointer type is not supported: .*|" +
                        "Unsupported type for temporal table version: .*|" +
                        "Unsupported type for table version: .*|" +
                        "No version history table tpch.nation at or before .*|" +
                        "Iceberg snapshot ID does not exists: .*|" +
                        "Cannot find snapshot with reference name: .*");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        if (isGalaxyMetastore) {
            return super.maxTableNameLength();
        }
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        if (isGalaxyMetastore) {
            return super.maxSchemaNameLength();
        }
        return OptionalInt.of(128);
    }

    @Test
    @Override
    public void testRenameSchemaToLongName()
    {
        if (isGalaxyMetastore) {
            super.testRenameSchemaToLongName();
        }
        else {
            assertThatThrownBy(super::testRenameSchemaToLongName)
                    .hasMessage("Hive metastore does not support renaming schemas");
        }
    }

    @Test
    @Override
    public void testDropSchemaCascadeFailure()
    {
        if (isGalaxyMetastore) {
            super.testDropSchemaCascadeFailure();
        }
        else {
            assertThatThrownBy(super::testDropSchemaCascadeFailure)
                    .hasMessageContaining("test_system_table$partitions is not a valid object name");
        }
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.orders (\n" +
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
                "   format = 'PARQUET',\n" +
                "   format_version = 3,\n" +
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/orders-\\E.*\\Q',\n" +
                "   type = 'ICEBERG'\n" +
                ")\\E");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        if (isGalaxyMetastore) {
            super.testRenameSchema();
        }
        else {
            assertThatThrownBy(super::testRenameSchema)
                    .hasMessage("Hive metastore does not support renaming schemas");
        }
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
    public void testCreateTableWithTableLayoutDataLocation()
    {
        skipTestUnless(isGalaxyMetastore);

        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CREATE TABLE test_location (a int) WITH (object_store_layout_enabled = true, data_location = '" + location + "/test_location')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location + "/test_location");

        assertQueryFails("CREATE TABLE test_location_ctas WITH (object_store_layout_enabled = true, data_location = '" + location + "') AS SELECT 123 x",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
    }

    @Test
    public void testAlterTablePropertiesWithTableLayoutDataLocation()
    {
        skipTestUnless(isGalaxyMetastore);

        String location = "s3://%s/denied".formatted(bucketName);
        assertUpdate("CREATE TABLE test_location_alter AS SELECT 123 x", 1);
        assertQueryFails(format("ALTER TABLE test_location_alter SET PROPERTIES object_store_layout_enabled = true, data_location = '%s'", location),
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
        assertUpdate("DROP TABLE test_location_alter");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // with char->varchar coercion on table creation, this is essentially varchar/varchar comparison
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_char_varchar",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS CHAR(3))), " +
                        "   (3, CAST('   ' AS CHAR(3)))," +
                        "   (6, CAST('x  ' AS CHAR(3)))")) {
            // varchar of length shorter than column's length
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))")).returnsEmptyResult();
            // varchar of length longer than column's length
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('    ' AS varchar(4))")).returnsEmptyResult();
            // value that's not all-spaces
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))")).returnsEmptyResult();
            // exact match
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('   ' AS varchar(3))", "VALUES (3, '   ')");
        }
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();

        if (typeName.equals("char(3)")) {
            // Use explicitly padded literal in char mapping test due to whitespace padding on coercion to varchar
            return Optional.of(new DataMappingTestSetup(typeName, "'ab '", dataMappingTestSetup.getHighValueLiteral()));
        }

        // According to Iceberg specification all time and timestamp values are stored with microsecond precision.
        if (typeName.equals("time")) {
            return Optional.of(new DataMappingTestSetup("time(6)", "TIME '15:03:00'", "TIME '23:59:59.999999'"));
        }

        if (typeName.equals("timestamp")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999999'"));
        }

        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999999 +12:00'"));
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Test
    @Override
    public void testHiveSpecificTableProperty()
    {
        assertThatThrownBy(super::testHiveSpecificTableProperty)
                .hasMessage("Table property 'auto_purge' not supported for Iceberg tables");
    }

    @Test
    @Override
    public void testHiveSpecificColumnProperty()
    {
        assertThat(query(
                """
                CREATE TABLE test_hive_specific_column_property(
                   xyz bigint,
                   abc bigint WITH (partition_projection_type = 'INTEGER', partition_projection_range = ARRAY['0', '10'])
                )"""))
                .failure().hasMessage("Iceberg tables do not support column properties [partition_projection_type, partition_projection_range]");
    }

    @Test
    @Override
    public void testDeltaSpecificTableProperty()
    {
        assertThatThrownBy(super::testDeltaSpecificTableProperty)
                .hasMessage("Table property 'checkpoint_interval' not supported for Iceberg tables");
    }

    @Override
    protected Double basicTableStatisticsExpectedNdv(int actualNdv)
    {
        return (double) actualNdv;
    }

    @Override
    protected void verifyRefreshMaterializedViewFailureWithoutMultiWriteInTransactionSupport(AbstractThrowableAssert abstractThrowableAssert)
    {
        if (isGalaxyMetastore) {
            super.verifyRefreshMaterializedViewFailureWithoutMultiWriteInTransactionSupport(abstractThrowableAssert);
        }
        else {
            abstractThrowableAssert.hasMessageMatching("Catalog only supports writes using autocommit: \\w+");
        }
    }

    @Test
    public void testCallRollbackToSnapshot()
    {
        assertUpdate("CREATE TABLE test_rollback AS SELECT 123 x", 1);
        if (isGalaxyMetastore) {
            getQueryRunner().execute("GRANT SELECT ON \"test_rollback$snapshots\" TO ROLE accountadmin");
        }

        long snapshotId = (long) computeActual("SELECT snapshot_id FROM \"test_rollback$snapshots\"").getOnlyValue();
        assertUpdate("INSERT INTO test_rollback VALUES (456)", 1);
        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", snapshotId));
        assertQuery("SELECT * FROM test_rollback", "VALUES 123");
    }

    @Test
    public void testOptimize()
    {
        assertUpdate("CREATE TABLE test_optimize (LIKE nation) WITH (format_version = 1)");
        if (isGalaxyMetastore) {
            getQueryRunner().execute("GRANT SELECT ON \"test_optimize$files\" TO ROLE accountadmin");
        }

        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO test_optimize SELECT * FROM nation", "SELECT count(*) FROM nation");
        }
        long fileCount = (long) computeActual("SELECT count(DISTINCT file_path) FROM \"test_optimize$files\"").getOnlyValue();

        assertUpdate("ALTER TABLE test_optimize EXECUTE OPTIMIZE");

        long newFileCount = (long) computeActual("SELECT count(DISTINCT file_path) FROM \"test_optimize$files\"").getOnlyValue();
        assertThat(newFileCount).isLessThan(fileCount);

        assertQuerySucceeds("ALTER TABLE test_optimize EXECUTE expire_snapshots");
        assertQuerySucceeds("ALTER TABLE test_optimize EXECUTE remove_orphan_files");
    }

    @Test
    public void testOptimizeManifests()
    {
        try (TestTable table = newTrinoTable("test_optimize_manifests", "(x int)")) {
            if (isGalaxyMetastore) {
                assertUpdate("GRANT SELECT ON \"" + table.getName() + "$manifests\" TO ROLE accountadmin");
            }

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            String countManifests = "SELECT count(*) FROM \"" + table.getName() + "$manifests\"";
            assertThat((long) computeScalar(countManifests)).isEqualTo(2);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat((long) computeScalar(countManifests)).isEqualTo(1);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1, 2");
        }
    }

    @Test
    public void testAnalyze()
    {
        assertUpdate("CREATE TABLE test_analyze (x BIGINT)");
        assertUpdate("ANALYZE test_analyze");
        assertQuery(
                "SHOW STATS FOR test_analyze",
                """
                VALUES
                  ('x', 0, 0, 1, null, null, null),
                  (null, null, null, null, 0, null, null)""");

        assertUpdate("DROP TABLE test_analyze");
    }

    // Versioned table tests from TestIcebergReadVersionedTable

    @Test
    public void testSelectTableWithEndSnapshotId()
    {
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + v1SnapshotId, "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + v2SnapshotId, "VALUES ('a', 1), ('b', 2)");
        assertQueryFails("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + incorrectSnapshotId, "Iceberg snapshot ID does not exists: " + incorrectSnapshotId);
    }

    @Test
    public void testSelectTableWithEndShortTimestampWithTimezone()
    {
        assertQueryFails(
                "SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'",
                "\\QNo version history table tpch.\"test_iceberg_read_versioned_table\" at or before 1970-01-01T00:00:00.001Z");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9), "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9), "VALUES ('a', 1), ('b', 2)");
    }

    @Test
    public void testSelectTableWithEndLongTimestampWithTimezone()
    {
        assertQueryFails(
                "SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'",
                "\\QNo version history table tpch.\"test_iceberg_read_versioned_table\" at or before 1970-01-01T00:00:00.001Z");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9), "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9), "VALUES ('a', 1), ('b', 2)");
    }

    @Test
    public void testEndVersionInTableNameAndForClauseShouldFail()
    {
        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table@" + v1SnapshotId + "\" FOR VERSION AS OF " + v1SnapshotId,
                ".*Table 'objectstore.tpch.\"test_iceberg_read_versioned_table@%d\"' does not exist".formatted(v1SnapshotId));

        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table@" + v1SnapshotId + "\" FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9),
                ".*Table 'objectstore.tpch.\"test_iceberg_read_versioned_table@%d\"' does not exist".formatted(v1SnapshotId));
    }

    @Test
    public void testSortedTable()
    {
        Session withSmallRowGroups = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_writer_max_stripe_rows", "7")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_table",
                "(id INTEGER, name VARCHAR) WITH (sorted_by = ARRAY['name'], format = 'ORC')")) {
            if (isGalaxyMetastore) {
                assertUpdate(format("GRANT SELECT ON \"%s$files\" TO ROLE accountadmin", table.getName()));
            }

            String values = " VALUES" +
                    "(5, 'name5')," +
                    "(10, 'name10')," +
                    "(4, 'name4')," +
                    "(1, 'name1')," +
                    "(6, 'name6')," +
                    "(9, 'name9')," +
                    "(8, 'name8')," +
                    "(3, 'name3')," +
                    "(7, 'name7')," +
                    "(2, 'name2')";
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + values, 10);
            assertQuery("SELECT * FROM " + table.getName(), values.trim());
            TrinoFileSystem fileSystem = getTrinoFileSystem();
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(checkOrcFileSorting(fileSystem, Location.of((String) filePath), "name")).isTrue();
            }
        }
    }

    @Test
    @Override
    public void testDropRowFieldWhenDuplicates()
    {
        // Override because Iceberg doesn't allow duplicated field names in a row type
        assertThatThrownBy(super::testDropRowFieldWhenDuplicates)
                .hasMessage("Field name 'a' specified more than once");
    }

    @Test
    @Override // Override because ambiguous field name is disallowed in the connector
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity)
                .hasMessage("Field name 'some_field' specified more than once");
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
    protected Optional<SetColumnTypeSetup> filterSetFieldTypesDataProvider(SetColumnTypeSetup setup)
    {
        return new GetIcebergConnectorTestBehavior().filterSetFieldTypesDataProvider(setup);
    }

    @Override
    protected void verifySetFieldTypeFailurePermissible(Throwable e)
    {
        new GetIcebergConnectorTestBehavior().verifySetFieldTypeFailurePermissible(e);
    }

    @Test
    public void testSystemTables()
    {
        // TODO https://github.com/trinodb/trino/issues/12920
        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table$partitions\" FOR VERSION AS OF " + v1SnapshotId,
                ".*Views do not support versioning");
    }

    @Test
    public void testIcebergTablesSystemTable()
    {
        ImmutableList.Builder<String> expectedSchemasBuilder = ImmutableList.<String>builder()
                .add("information_schema")
                .add("system")
                .add("tpch");
        if (!isGalaxyMetastore) {
            expectedSchemasBuilder.add("default");
        }

        List<String> expectedSchemas = expectedSchemasBuilder.build();
        Set<Object> actualSchemas = computeActual("SHOW SCHEMAS").getOnlyColumnAsSet();

        List<MaterializedRow> expectedSchemataRows = expectedSchemas.stream()
                .map(schema -> new MaterializedRow(ImmutableList.of("objectstore", schema)))
                .collect(toImmutableList());
        List<MaterializedRow> actualSchemataRows = computeActual("SELECT * FROM information_schema.schemata").getMaterializedRows();

        if (isGalaxyMetastore) {
            assertThat(actualSchemas)
                    .containsExactlyInAnyOrderElementsOf(expectedSchemas);
            assertThat(actualSchemataRows)
                    .containsExactlyInAnyOrderElementsOf(expectedSchemataRows);
        }
        else {
            // Avoid using exact match since other tests may create additional schemas
            assertThat(actualSchemas)
                    .containsAll(expectedSchemas);
            assertThat(actualSchemataRows)
                    .containsAll(expectedSchemataRows);
        }

        assertThat(computeActual("SHOW TABLES FROM system").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("iceberg_tables");

        assertQuery(
                "SELECT * FROM information_schema.tables WHERE table_schema = 'system'",
                "VALUES ('objectstore', 'system', 'iceberg_tables', 'BASE TABLE')");
    }

    @Test
    public void testVariantType()
    {
        try (TestTable table = newTrinoTable("test_variant", "(x variant) WITH (format_version = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES VARIANT 'true'", 1);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES VARIANT 'true'");
        }
    }

    @Test
    public void testCreateChangelogViewProcedure()
    {
        // Verifies the Iceberg create_changelog_view procedure is exposed through the Object Store connector (see FeatureExposures).
        try (TestTable table = newTrinoTable("test_changelog_view", "(id INT NOT NULL, value VARCHAR) WITH (merge_mode = 'copy-on-write')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'a'), (2, 'b')", 2);
            long startSnapshot = getLatestSnapshotId(table.getName());
            assertUpdate("UPDATE " + table.getName() + " SET value = 'a2' WHERE id = 1", 1);
            long endSnapshot = getLatestSnapshotId(table.getName());

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE create_changelog_view(" +
                    "start_snapshot_id => " + startSnapshot + ", " +
                    "end_snapshot_id => " + endSnapshot + ", " +
                    "identifier_columns => ARRAY['id'])");
            try {
                assertThat(query("SELECT id, value, _change_type FROM " + table.getName() + "_changes"))
                        .matches("VALUES " +
                                "(INT '1', VARCHAR 'a2', VARCHAR 'update_after'), " +
                                "(INT '1', VARCHAR 'a', VARCHAR 'update_before')");
            }
            finally {
                assertUpdate("DROP VIEW " + table.getName() + "_changes");
            }
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

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeActual(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyValue();
    }

    private long getCommittedAtInEpochMilliSeconds(String tableName, long snapshotId)
    {
        return ((ZonedDateTime) computeActual(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id=%s LIMIT 1", tableName, snapshotId)).getOnlyValue())
                .toInstant().toEpochMilli();
    }

    private static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern("'TIMESTAMP '''uuuu-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }
}
