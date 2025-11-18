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

import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.TASK_MIN_WRITER_COUNT;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * @see BaseTestObjectStoreHiveFeaturesConnectorTest
 */
public abstract class BaseObjectStoreHiveConnectorTest
        extends BaseObjectStoreConnectorTest
{
    private static final String MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE = "Modifying Hive table rows is constrained to deletes of whole partitions";

    public BaseObjectStoreHiveConnectorTest(boolean isGalaxyMetastore)
    {
        super(isGalaxyMetastore, HIVE);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        boolean connectorHasBehavior = new GetHiveConnectorTestBehavior().hasBehavior(connectorBehavior);

        switch (connectorBehavior) {
            case SUPPORTS_MULTI_STATEMENT_WRITES: // multi-statement transaction support is disabled in ObjectStore
                // when this fails remove the `case` for given flag
                verify(connectorHasBehavior, "Expected support for: %s", connectorBehavior);
                return false;

            // GetHiveConnectorTestBehavior sets false because Hive connector doesn't support updates on non-ACID tables
            case SUPPORTS_UPDATE:
            case SUPPORTS_ROW_LEVEL_UPDATE:
                return true;

            // Declared to be had since MERGE-related test cases are overridden
            case SUPPORTS_MERGE:
                return true;

            case SUPPORTS_DROP_SCHEMA_CASCADE:
                return true;

            // ObjectStore adds support for materialized views using Iceberg
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW_WHEN_STALE:
            case SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
//            case SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS: -- not supported by Iceberg:
            case SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN:
                // when this fails remove the `case` for given flag
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return true;

            case SUPPORTS_CREATE_FUNCTION:
                return false;

            default:
                // By default, declare all behaviors/features supported by Hive connector
                return connectorHasBehavior;
        }
    }

    @Override
    protected boolean supportsPhysicalPushdown()
    {
        // Hive table is created using default format which is ORC. Currently ORC reader has issue
        // pruning dereferenced struct fields https://github.com/trinodb/trino/issues/17201
        return false;
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        if (typeName.equals("timestamp(6)")) {
            // It's supported depending on hive timestamp precision configuration, so the exception message doesn't match the expected for asUnsupported().
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return switch (columnName) {
            case " aleadingspace" -> "Hive column names must not start with a space: ' aleadingspace'".equals(exception.getMessage());
            case "atrailingspace " -> "Hive column names must not end with a space: 'atrailingspace '".equals(exception.getMessage());
            case "a,comma" -> "Hive column names must not contain commas: 'a,comma'".equals(exception.getMessage());
            default -> false;
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("Hive connector does not support column default values");
    }

    @Test
    @Override
    public void testCreateTableWithLocation()
    {
        skipTestUnless(isGalaxyMetastore);

        assertThatThrownBy(super::testCreateTableWithLocation)
                .hasStackTraceContaining("Table property 'location' not supported for Hive tables");

        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (external_location = '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (external_location = '" + location + "/test_location_create')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location + "/test_location_create");
    }

    @Test
    @Override
    public void testCreateTableAsWithLocation()
    {
        skipTestUnless(isGalaxyMetastore);

        assertThatThrownBy(super::testCreateTableAsWithLocation)
                .hasStackTraceContaining("Table property 'location' not supported for Hive tables");

        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CREATE TABLE test_location_ctas WITH (external_location = '" + location + "') AS SELECT 123 x",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertQueryFails("CREATE TABLE test_location_ctas WITH (external_location = '" + location + "/test_location_ctas') AS SELECT 123 x",
                "Access Denied: Role accountadmin is not allowed to use location: " + location + "/test_location_ctas");
    }

    @Test
    public void testRegisterPartitionWithLocation()
    {
        skipTestUnless(isGalaxyMetastore);

        String tableName = "test_register_partition_for_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  partitioned_by = ARRAY['part'] " +
                ")");

        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CALL system.register_partition('tpch', '" + tableName + "', ARRAY['part'], ARRAY['first'], '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                "Hive tables do not support NOT NULL columns");
    }

    @Test
    @Override
    public void testDropAndAddColumnWithSameName()
    {
        // Override because Hive connector can access old data after dropping and adding a column with same name
        assertThatThrownBy(super::testDropAndAddColumnWithSameName)
                .hasMessageContaining("""
                        Actual rows (up to 100 of 1 extra rows shown, 1 rows in total):
                            [1, 2]""");
    }

    @Test
    @Override
    public void testDropRowFieldInArray()
    {
        // Dropping row field type is not supported, but a non standard exception message is thrown.
        assertThatThrownBy(super::testDropRowFieldInArray)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Dropping fields from Hive tables is not supported"
                        to match regex:
                          ".*does not support.*"
                        but did not.""");
    }

    @Test
    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        // Override because the connector throws a slightly different error message
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col", "(a_varchar varchar)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar NOT NULL",
                    "Hive tables do not support NOT NULL columns");
        }
    }

    @Test
    @Override
    public void testCreateTableWithDefaultColumn()
    {
        // Override because the connector throws a slightly different error message
        String tableName = "test_default_value_" + randomNameSuffix();
        assertQueryFails("CREATE TABLE " + tableName + " (x int DEFAULT 1)", "Hive tables do not support DEFAULT columns");
    }

    @Test
    @Override
    public void testAddRowFieldInArray()
    {
        // Adding row field type is not supported, but a non standard exception message is thrown.
        assertThatThrownBy(super::testAddRowFieldInArray)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Adding fields to Hive tables is not supported"
                        to match regex:
                          ".*does not support.*"
                        but did not.""");
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
    public void testMergeDeleteWithCTAS()
    {
        assertThatThrownBy(super::testMergeDeleteWithCTAS)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    public void testSortedTable()
    {
        Session withSmallRowGroups = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_optimized_writer_max_stripe_rows", "6")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_table",
                "(id INTEGER, name VARCHAR) WITH (bucketed_by = ARRAY[ 'name' ], bucket_count = 2, sorted_by = ARRAY['name'], format = 'ORC')")) {
            String values = " VALUES" +
                    "(5, 'name5')," +
                    "(10, 'name10')," +
                    "(4, 'name4')," +
                    "(11, 'name11')," +
                    "(15, 'name15')," +
                    "(17, 'name17')," +
                    "(20, 'name20')," +
                    "(12, 'name12')," +
                    "(13, 'name13')," +
                    "(1, 'name1')," +
                    "(6, 'name6')," +
                    "(19, 'name19')," +
                    "(18, 'name18')," +
                    "(9, 'name9')," +
                    "(16, 'name16')," +
                    "(8, 'name8')," +
                    "(3, 'name3')," +
                    "(14, 'name14')," +
                    "(7, 'name7')," +
                    "(2, 'name2')";
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + values, 20);
            assertQuery("SELECT * FROM " + table.getName(), values.trim());
            TrinoFileSystem fileSystem = getTrinoFileSystem();
            for (Object filePath : computeActual("SELECT DISTINCT \"$path\" FROM " + table.getName()).getOnlyColumnAsSet()) {
                assertThat(checkOrcFileSorting(fileSystem, Location.of((String) filePath), "name")).isTrue();
            }
        }
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
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
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
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateRowType()
    {
        assertThatThrownBy(super::testUpdateRowType).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateAllValues()
    {
        assertThatThrownBy(super::testUpdateAllValues).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateWithPredicates()
    {
        assertThatThrownBy(super::testUpdateWithPredicates).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateMultipleCondition()
    {
        assertThatThrownBy(super::testUpdateMultipleCondition).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateCaseSensitivity()
    {
        assertThatThrownBy(super::testUpdateCaseSensitivity).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeLarge()
    {
        assertThatThrownBy(super::testMergeLarge).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeSimpleSelect()
    {
        assertThatThrownBy(super::testMergeSimpleSelect).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeFruits()
    {
        assertThatThrownBy(super::testMergeFruits).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeMultipleOperations()
    {
        assertThatThrownBy(super::testMergeMultipleOperations).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeSimpleQuery()
    {
        assertThatThrownBy(super::testMergeSimpleQuery).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeAllInserts()
    {
        assertThatThrownBy(super::testMergeAllInserts).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeFalseJoinCondition()
    {
        assertThatThrownBy(super::testMergeFalseJoinCondition).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeAllColumnsUpdated()
    {
        assertThatThrownBy(super::testMergeAllColumnsUpdated).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeAllMatchesDeleted()
    {
        assertThatThrownBy(super::testMergeAllMatchesDeleted).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeMultipleRowsMatchFails()
    {
        assertThatThrownBy(super::testMergeAllMatchesDeleted).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeQueryWithStrangeCapitalization()
    {
        assertThatThrownBy(super::testMergeQueryWithStrangeCapitalization).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeWithoutTablesAliases()
    {
        assertThatThrownBy(super::testMergeWithoutTablesAliases).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeWithUnpredictablePredicates()
    {
        assertThatThrownBy(super::testMergeWithUnpredictablePredicates).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeWithSimplifiedUnpredictablePredicates()
    {
        assertThatThrownBy(super::testMergeWithSimplifiedUnpredictablePredicates).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeCasts()
    {
        assertThatThrownBy(super::testMergeCasts).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeSubqueries()
    {
        assertThatThrownBy(super::testMergeSubqueries).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMergeWrittenStats()
    {
        assertThatThrownBy(super::testMergeWrittenStats).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat(computeScalar("SHOW CREATE TABLE orders")).isEqualTo("" +
                "CREATE TABLE objectstore.tpch.orders (\n" +
                "   orderkey bigint,\n" +
                "   custkey bigint,\n" +
                "   orderstatus varchar(1),\n" +
                "   totalprice double,\n" +
                "   orderdate date,\n" +
                "   orderpriority varchar(15),\n" +
                "   clerk varchar(15),\n" +
                "   shippriority integer,\n" +
                "   comment varchar(79)\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   type = 'HIVE'\n" +
                ")");
    }

    @Test
    @Override
    public void testIcebergSpecificTableProperty()
    {
        assertThatThrownBy(super::testIcebergSpecificTableProperty)
                .hasMessage("Table property 'partitioning' not supported for Hive tables");
    }

    @Test
    @Override
    public void testDeltaSpecificTableProperty()
    {
        assertThatThrownBy(super::testDeltaSpecificTableProperty)
                .hasMessage("Table property 'checkpoint_interval' not supported for Hive tables");
    }

    @Override
    protected Double basicTableStatisticsExpectedNdv(int actualNdv)
    {
        return 1.0;
    }

    @Test
    @Override
    public void testRegisterTableProcedure()
    {
        assertThatThrownBy(super::testRegisterTableProcedure)
                .hasMessage("Unsupported table type");
    }

    @Test
    @Override
    public void testUnregisterTableProcedure()
    {
        assertThatThrownBy(super::testUnregisterTableProcedure)
                .hasMessage("Unsupported table type");
    }

    @Override
    protected String getTableLocation(String tableName)
    {
        return "s3://%s/tpch/%s".formatted(bucketName, tableName);
    }

    @Test
    public void testCreatePartitionedBucketedTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + "test_create_partitioned_bucketed_table" + " " +
                "WITH (" +
                "format = 'RCBINARY', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                Session.builder(getSession())
                        .setSystemProperty(TASK_MIN_WRITER_COUNT, "4")
                        .build(),
                createTable,
                "SELECT count(*) FROM orders");

        if (isGalaxyMetastore) {
            getQueryRunner().execute("GRANT SELECT ON \"test_create_partitioned_bucketed_table$partitions\" TO ROLE accountadmin");
        }

        assertQuery(
                "SELECT count(*) FROM \"test_create_partitioned_bucketed_table$partitions\"",
                "SELECT 3");

        // verify that we create bucket_count files in each partition
        assertEqualsIgnoreOrder(
                computeActual("SELECT orderstatus, COUNT(DISTINCT \"$path\") FROM test_create_partitioned_bucketed_table GROUP BY 1"),
                resultBuilder(getSession(), createVarcharType(1), BIGINT)
                        .row("F", 11L)
                        .row("O", 11L)
                        .row("P", 11L)
                        .build());

        assertQuery(
                "SELECT * FROM test_create_partitioned_bucketed_table",
                "SELECT custkey, custkey, comment, orderstatus FROM orders");

        for (int i = 1; i <= 30; i++) {
            assertQuery(
                    format("SELECT * FROM test_create_partitioned_bucketed_table WHERE custkey = %d AND custkey2 = %d", i, i),
                    format("SELECT custkey, custkey, comment, orderstatus FROM orders WHERE custkey = %d", i));
        }
    }

    @Test
    public void testCallCreateEmptyPartition()
    {
        assertUpdate("" +
                "CREATE TABLE test_call_create_empty_partition (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'part' ] " +
                ")");
        if (isGalaxyMetastore) {
            getQueryRunner().execute("GRANT SELECT ON \"test_call_create_empty_partition$partitions\" TO ROLE accountadmin");
        }

        assertQuery("SELECT count(*) FROM \"test_call_create_empty_partition$partitions\"", "SELECT 0");

        assertUpdate("CALL system.create_empty_partition('tpch', 'test_call_create_empty_partition', ARRAY['part'], ARRAY['empty'])");
        assertQuery("SELECT count(*) FROM \"test_call_create_empty_partition$partitions\"", "SELECT 1");
    }

    @Test
    public void testOptimize()
    {
        assertUpdate("CREATE TABLE test_optimize (LIKE nation)");
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO test_optimize SELECT * FROM nation", "SELECT count(*) FROM nation");
        }
        assertThatThrownBy(() -> computeActual("ALTER TABLE test_optimize EXECUTE optimize(file_size_threshold => '10kB')"))
                .hasMessage("Executing OPTIMIZE on Hive tables is not supported");
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        String tableName = "test_analyze_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // No column stats after running an empty analyze
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), 0);
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on 3 partitions including a null partition and a duplicate partition
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]])", tableName), 12);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        // expect empty basic stats for empty partition due to clearRowCountWhenAllPartitionsHaveNoRows()
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, 9, 9), " +
                        "(null, null, null, null, null, null, null)");
        // expect empty basic stats for empty partition due to clearRowCountWhenAllPartitionsHaveNoRows()
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, 9, 9), " +
                        "(null, null, null, null, null, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3'), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        // expect empty basic stats for empty partition due to clearRowCountWhenAllPartitionsHaveNoRows()
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, 9, 9), " +
                        "(null, null, null, null, null, null, null)");
        // expect empty basic stats for empty partition due to clearRowCountWhenAllPartitionsHaveNoRows()
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, 9, 9), " +
                        "(null, null, null, null, null, null, null)");

        // Drop the partitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testUpdateNotNullColumn()
    {
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                format("Hive tables do not support NOT NULL columns"));
    }

    @Test
    public void testAnalyzeUnpartitionedTable()
    {
        String tableName = "test_analyze_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.375, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7'), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7'), " +
                        "('c_timestamp', null, 10.0, 0.375, null, null, null), " +
                        "('c_varchar', 40.0, 10.0, 0.375, null, null, null), " +
                        "('c_varbinary', 20.0, null, 0.375, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Drop the unpartitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    protected void createPartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, true);
    }

    protected void createUnpartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, false);
    }

    private void createTableForAnalyzeTest(String tableName, boolean partitioned)
    {
        Session defaultSession = getSession();

        // Disable column statistics collection when creating the table
        Session disableColumnStatsSession = Session.builder(defaultSession)
                .setCatalogSessionProperty(defaultSession.getCatalog().get(), "collect_column_statistics_on_write", "false")
                .build();

        assertUpdate(
                disableColumnStatsSession,
                "" +
                        "CREATE TABLE " +
                        tableName +
                        (partitioned ? " WITH (partitioned_by = ARRAY['p_varchar', 'p_bigint'])\n" : " ") +
                        "AS " +
                        "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint " +
                        "FROM ( " +
                        "  VALUES " +
                        // p_varchar = 'p1', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00:00.000', 'abc1', X'bcd1', 'p1', BIGINT '7'), " +
                        "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00:00.000', 'abc2', X'bcd2', 'p1', BIGINT '7'), " +
                        // p_varchar = 'p2', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00:00.000', 'cba1', X'dcb1', 'p2', BIGINT '7'), " +
                        "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00:00.000', 'cba2', X'dcb2', 'p2', BIGINT '7'), " +
                        // p_varchar = 'p3', p_bigint = BIGINT '8'
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (true, BIGINT '3', DOUBLE '4.4', TIMESTAMP '2012-10-10 01:00:00.000', 'bca1', X'cdb1', 'p3', BIGINT '8'), " +
                        "    (false, BIGINT '2', DOUBLE '3.4', TIMESTAMP '2012-10-10 00:00:00.000', 'bca2', X'cdb2', 'p3', BIGINT '8'), " +
                        // p_varchar = NULL, p_bigint = NULL
                        "    (false, BIGINT '7', DOUBLE '7.7', TIMESTAMP '1977-07-07 07:07:00.000', 'efa1', X'efa1', NULL, NULL), " +
                        "    (false, BIGINT '6', DOUBLE '6.7', TIMESTAMP '1977-07-07 07:06:00.000', 'efa2', X'efa2', NULL, NULL), " +
                        "    (false, BIGINT '5', DOUBLE '5.7', TIMESTAMP '1977-07-07 07:05:00.000', 'efa3', X'efa3', NULL, NULL), " +
                        "    (false, BIGINT '4', DOUBLE '4.7', TIMESTAMP '1977-07-07 07:04:00.000', 'efa4', X'efa4', NULL, NULL) " +
                        ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint)", 16);

        if (partitioned) {
            // Create empty partitions
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e1", "9"));
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e2", "9"));
        }
    }

    @Test
    @Override
    public void testRenameRowField()
    {
        // Renaming row field is not supported, but a non-standard exception message is thrown.
        assertThatThrownBy(super::testRenameRowField)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Renaming fields in Hive tables is not supported"
                        to match regex:
                          "This connector does not support renaming fields"
                        but did not.""");
    }

    @Test
    @Override
    public void testSetFieldType()
    {
        // Setting row field type is not supported, but a non standard exception message is thrown.
        assertThatThrownBy(super::testSetFieldType)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Setting field type in Hive tables is not supported"
                        to match regex:
                          "This connector does not support setting field types"
                        but did not.""");
    }

    @Test
    @Override
    public void testSetFieldTypeInArray()
    {
        // Setting row field type is not supported, but a non standard exception message is thrown.
        assertThatThrownBy(super::testSetFieldTypeInArray)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Setting field type in Hive tables is not supported"
                        to match regex:
                          ".*does not support.*"
                        but did not.""");
    }

    @Test
    @Override
    public void testSetFieldMapKeyType()
    {
        // Setting map key type is not supported, but a non standard exception message is thrown.
        assertThatThrownBy(super::testSetFieldMapKeyType)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Setting field type in Hive tables is not supported"
                        to match regex:
                          ".*does not support.*"
                        but did not.""");
    }

    @Test
    @Override
    public void testSetFieldMapValueType()
    {
        // Setting map value type is not supported, but a non standard exception message is thrown.
        assertThatThrownBy(super::testSetFieldMapValueType)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Setting field type in Hive tables is not supported"
                        to match regex:
                          ".*does not support.*"
                        but did not.""");
    }

    @Test
    @Override
    public void testUpdateWithSubquery()
    {
        assertThatThrownBy(super::testUpdateWithSubquery).hasMessage("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Test
    @Override
    public void testUpdateWithNullValues()
    {
        assertThatThrownBy(super::testUpdateWithNullValues).hasMessage("Modifying Hive table rows is constrained to deletes of whole partitions");
    }
}
