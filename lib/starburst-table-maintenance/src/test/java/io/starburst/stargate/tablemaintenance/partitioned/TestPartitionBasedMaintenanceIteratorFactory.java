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
package io.starburst.stargate.tablemaintenance.partitioned;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.starburst.stargate.tablemaintenance.PartitionColumnBasedOptimizeStatus;
import io.starburst.stargate.tablemaintenance.partitioned.UnoptimizedPartitionValue.ColumnValue;
import io.trino.spi.connector.CatalogSchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.starburst.stargate.tablemaintenance.partitioned.FileType.determineFileType;
import static io.starburst.stargate.tablemaintenance.partitioned.UnoptimizedPartitionValue.unoptimizedPartitionValue;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitionBasedMaintenanceIteratorFactory
{
    private static final DataSize FILE_SIZE_THRESHOLD = DataSize.of(67, DataSize.Unit.MEGABYTE);
    private static final CatalogSchemaTableName TEST_TABLE = new CatalogSchemaTableName("test_catalog", "test_schema", "test_table");
    private static final String EXPECTED_GET_COLUMNS_QUERY =
            """
            SELECT
                column_name, data_type
            FROM
                "test_catalog".information_schema.columns
            WHERE
                table_catalog = 'test_catalog'
                AND table_schema = 'test_schema'
                AND table_name = 'test_table'
            ORDER BY
                ordinal_position ASC
            """;

    private static final String EXPECTED_SHOW_CREATE_TABLE_QUERY =
            """
            SHOW CREATE TABLE "test_catalog"."test_schema"."test_table"
            """;

    private static final String EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1 =
            """
            SELECT DISTINCT
              CAST(partition."col1" AS VARCHAR) as partition_column_value_0,
              any_value(typeof(partition."col1")) as partition_column_type_0,
              CASE content
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'POSITION_DELETES'
                WHEN 2 THEN 'EQUALITY_DELETES'
              END as file_type
            FROM
              "test_catalog"."test_schema"."test_table$files"
            WHERE
              (content = 0 AND file_size_in_bytes < 70254592)
              OR content IN (1, 2)
            GROUP BY
              partition."col1",
              partition,
              content
            HAVING
              (content = 0 AND COUNT(*) >= 2)
              OR content IN (1, 2)
            """;

    private static final String EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL2 =
            """
            SELECT DISTINCT
              CAST(partition."col2" AS VARCHAR) as partition_column_value_0,
              any_value(typeof(partition."col2")) as partition_column_type_0,
              CASE content
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'POSITION_DELETES'
                WHEN 2 THEN 'EQUALITY_DELETES'
              END as file_type
            FROM
              "test_catalog"."test_schema"."test_table$files"
            WHERE
              (content = 0 AND file_size_in_bytes < 70254592)
              OR content IN (1, 2)
            GROUP BY
              partition."col2",
              partition,
              content
            HAVING
              (content = 0 AND COUNT(*) >= 2)
              OR content IN (1, 2)
            """;

    private static final String EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1_DAY =
            """
            SELECT DISTINCT
              CAST(partition."col1_day" AS VARCHAR) as partition_column_value_0,
              any_value(typeof(partition."col1_day")) as partition_column_type_0,
              CASE content
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'POSITION_DELETES'
                WHEN 2 THEN 'EQUALITY_DELETES'
              END as file_type
            FROM
              "test_catalog"."test_schema"."test_table$files"
            WHERE
              (content = 0 AND file_size_in_bytes < 70254592)
              OR content IN (1, 2)
            GROUP BY
              partition."col1_day",
              partition,
              content
            HAVING
              (content = 0 AND COUNT(*) >= 2)
              OR content IN (1, 2)
            """;

    private static final String EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_EVENT_TS_DAY =
            """
            SELECT DISTINCT
              CAST(partition."event_ts_day" AS VARCHAR) as partition_column_value_0,
              any_value(typeof(partition."event_ts_day")) as partition_column_type_0,
              CASE content
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'POSITION_DELETES'
                WHEN 2 THEN 'EQUALITY_DELETES'
              END as file_type
            FROM
              "test_catalog"."test_schema"."test_table$files"
            WHERE
              (content = 0 AND file_size_in_bytes < 70254592)
              OR content IN (1, 2)
            GROUP BY
              partition."event_ts_day",
              partition,
              content
            HAVING
              (content = 0 AND COUNT(*) >= 2)
              OR content IN (1, 2)
            """;

    private static final String EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1_COL2 =
            """
            SELECT DISTINCT
              CAST(partition."col1" AS VARCHAR) as partition_column_value_0,
              any_value(typeof(partition."col1")) as partition_column_type_0,
              CAST(partition."col2" AS VARCHAR) as partition_column_value_1,
              any_value(typeof(partition."col2")) as partition_column_type_1,
              CASE content
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'POSITION_DELETES'
                WHEN 2 THEN 'EQUALITY_DELETES'
              END as file_type
            FROM
              "test_catalog"."test_schema"."test_table$files"
            WHERE
              (content = 0 AND file_size_in_bytes < 70254592)
              OR content IN (1, 2)
            GROUP BY
              partition."col1",
              partition."col2",
              partition,
              content
            HAVING
              (content = 0 AND COUNT(*) >= 2)
              OR content IN (1, 2)
            """;

    private static final String EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1_DAY_COL2 =
            """
            SELECT DISTINCT
              CAST(partition."col1_day" AS VARCHAR) as partition_column_value_0,
              any_value(typeof(partition."col1_day")) as partition_column_type_0,
              CAST(partition."col2" AS VARCHAR) as partition_column_value_1,
              any_value(typeof(partition."col2")) as partition_column_type_1,
              CASE content
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'POSITION_DELETES'
                WHEN 2 THEN 'EQUALITY_DELETES'
              END as file_type
            FROM
              "test_catalog"."test_schema"."test_table$files"
            WHERE
              (content = 0 AND file_size_in_bytes < 70254592)
              OR content IN (1, 2)
            GROUP BY
              partition."col1_day",
              partition."col2",
              partition,
              content
            HAVING
              (content = 0 AND COUNT(*) >= 2)
              OR content IN (1, 2)
            """;

    @Test
    public void testTableWithSinglePartition()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "varchar"), new TableColumn("col2", "integer")),
                "CREATE TABLE test_table (col1 varchar, col2 integer) WITH (partitioning = ARRAY['col1'])",
                ImmutableList.of(
                        unoptimizedPartitionValue("value1", "varchar", "DATA"),
                        unoptimizedPartitionValue("value2", "varchar", "DATA"),
                        unoptimizedPartitionValue("value2", "varchar", "EQUALITY_DELETES"),
                        unoptimizedPartitionValue("value2", "varchar", "POSITION_DELETES")));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value1' AS varchar)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value2' AS varchar)
                        """);
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
        assertThat(queryRunner.getCapturedGetColumnsQuery()).isEqualTo(EXPECTED_GET_COLUMNS_QUERY);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1);
    }

    @Test
    public void testPartitionValueWithSingleQuote()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "varchar"), new TableColumn("col2", "integer")),
                "CREATE TABLE test_table (col1 varchar, col2 integer) WITH (partitioning = ARRAY['col1'])",
                ImmutableList.of(
                        unoptimizedPartitionValue("O'Reilly", "varchar", "DATA"),
                        unoptimizedPartitionValue("value2", "varchar", "DATA"),
                        unoptimizedPartitionValue("value2", "varchar", "POSITION_DELETES")));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('O''Reilly' AS varchar)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value2' AS varchar)
                        """);
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
        assertThat(queryRunner.getCapturedGetColumnsQuery()).isEqualTo(EXPECTED_GET_COLUMNS_QUERY);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1);
    }

    @Test
    public void testTableWithMultiplePartitions()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "varchar"), new TableColumn("col2", "integer"), new TableColumn("col3", "date")),
                """
                CREATE TABLE test_table (col1 varchar, col2 integer, col3 date)
                WITH
                  (
                    type = 'ICEBERG',
                    partitioning = ARRAY['col2', 'col1'],
                    sorted_by = ARRAY['col3']
                  )
                """,
                ImmutableList.of(
                        unoptimizedPartitionValue("10", "integer", "DATA"),
                        unoptimizedPartitionValue("20", "integer", "DATA")));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col2"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col2" = CAST('10' AS integer)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col2" = CAST('20' AS integer)
                        """);
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
        assertThat(queryRunner.getCapturedGetColumnsQuery()).isEqualTo(EXPECTED_GET_COLUMNS_QUERY);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL2);
    }

    @Test
    public void testTableWithMultiplePartitionsWithTransformColumns()
    {
        // partitioning = day(col1), col3, bucket(16, col2). Optimizable order: day(col1), col3. Single-column request → day(col1).
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "timestamp(6)"), new TableColumn("col2", "integer"), new TableColumn("col3", "varchar")),
                "CREATE TABLE test_table (col1 timestamp(6), col2 integer, col3 varchar) WITH (partitioning = ARRAY['day(col1)', 'col3', 'bucket(16, col2)'])",
                ImmutableList.of(
                        unoptimizedPartitionValue("2025-02-11", "date", "DATA"),
                        unoptimizedPartitionValue("2025-02-12", "date", "DATA"),
                        unoptimizedPartitionValue("2025-02-12", "date", "POSITION_DELETES")));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1"), ImmutableList.of("col1_day"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE date("col1") = CAST('2025-02-11' AS date)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE date("col1") = CAST('2025-02-12' AS date)
                        """);
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
        assertThat(queryRunner.getCapturedGetColumnsQuery()).isEqualTo(EXPECTED_GET_COLUMNS_QUERY);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1_DAY);
    }

    @Test
    public void testTableWithOnlyUnsupportedTransformedPartitions()
    {
        // No identity and no day(...) — purely hour/bucket/truncate → no partition-based optimize.
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(
                        new TableColumn("event_id", "bigint"),
                        new TableColumn("event_type", "varchar"),
                        new TableColumn("event_date", "date"),
                        new TableColumn("event_timestamp", "timestamp(6) with time zone")),
                """
                CREATE TABLE test (
                  event_id BIGINT,
                  event_type VARCHAR,
                  event_date DATE,
                  event_timestamp TIMESTAMP(6)
                  WITH
                    TIME ZONE
                )
                WITH
                  (
                    type = 'ICEBERG',
                    format = 'PARQUET',
                    partitioning = ARRAY[
                      'bucket(event_type, 16)',
                      'truncate(event_type, 2)',
                      'hour(event_timestamp)'
                    ]
                  )
                """,
                ImmutableList.of());
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE);
        assertThat(partitionedBasedMaintenanceDetails).isEmpty();
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
        assertThat(queryRunner.getCapturedGetColumnsQuery()).isEqualTo(EXPECTED_GET_COLUMNS_QUERY);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isNull();
    }

    @Test
    public void testDayTransformGeneratesDatePredicate()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("event_ts", "timestamp(6)")),
                "CREATE TABLE test_table (event_ts timestamp(6)) WITH (partitioning = ARRAY['day(event_ts)'])",
                ImmutableList.of(
                        unoptimizedPartitionValue("2025-02-11", "date", "DATA"),
                        unoptimizedPartitionValue("2025-02-12", "date", "DATA")));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> details = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE);
        assertThat(details.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("event_ts"), ImmutableList.of("event_ts_day"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(details.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE date("event_ts") = CAST('2025-02-11' AS date)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE date("event_ts") = CAST('2025-02-12' AS date)
                        """);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_EVENT_TS_DAY);
    }

    @Test
    public void testMixedIdentityAndDayTransform()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("event_type", "varchar"), new TableColumn("event_ts", "timestamp(6)")),
                "CREATE TABLE test_table (event_type varchar, event_ts timestamp(6)) WITH (partitioning = ARRAY['event_type', 'day(event_ts)'])",
                ImmutableList.of(
                        new UnoptimizedPartitionValue(
                                ImmutableList.of(new ColumnValue("CREATE", "varchar"), new ColumnValue("2025-02-11", "date")),
                                determineFileType("DATA")),
                        new UnoptimizedPartitionValue(
                                ImmutableList.of(new ColumnValue("UPDATE", "varchar"), new ColumnValue("2025-02-12", "date")),
                                determineFileType("DATA"))));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> details = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE, 2);
        assertThat(details.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("event_type", "event_ts"), ImmutableList.of("event_type", "event_ts_day"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(details.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "event_type" = CAST('CREATE' AS varchar) AND date("event_ts") = CAST('2025-02-11' AS date)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "event_type" = CAST('UPDATE' AS varchar) AND date("event_ts") = CAST('2025-02-12' AS date)
                        """);
    }

    @Test
    public void testDeleteFilesContributeToPartitionOptimize()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "varchar"), new TableColumn("col2", "integer")),
                "CREATE TABLE test_table (col1 varchar, col2 integer) WITH (partitioning = ARRAY['col1'])",
                ImmutableList.of(
                        unoptimizedPartitionValue("value0", "varchar", "DATA"),
                        unoptimizedPartitionValue("value1", "varchar", "EQUALITY_DELETES"),
                        unoptimizedPartitionValue("value2", "varchar", "POSITION_DELETES")));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value0' AS varchar)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value1' AS varchar)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value2' AS varchar)
                        """);
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
        assertThat(queryRunner.getCapturedGetColumnsQuery()).isEqualTo(EXPECTED_GET_COLUMNS_QUERY);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1);
    }

    @Test
    public void testQueryFormatVersionV2()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(),
                "CREATE TABLE test_table (col1 varchar) WITH (type = 'ICEBERG', format_version = 2, partitioning = ARRAY['col1'])",
                ImmutableList.of());
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        assertThat(iteratorFactory.queryFormatVersion(TEST_TABLE)).hasValue(2);
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
    }

    @Test
    public void testQueryFormatVersionV1()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(),
                "CREATE TABLE test_table (col1 varchar) WITH (type = 'ICEBERG', format_version = 1)",
                ImmutableList.of());
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        assertThat(iteratorFactory.queryFormatVersion(TEST_TABLE)).hasValue(1);
    }

    @Test
    public void testQueryFormatVersionError()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(),
                "unused",
                ImmutableList.of())
        {
            @Override
            public String runShowCreateTableQuery(String query)
            {
                super.runShowCreateTableQuery(query);
                throw new RuntimeException("Cluster query failed");
            }
        };
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        assertThat(iteratorFactory.queryFormatVersion(TEST_TABLE)).isEmpty();
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
    }

    @Test
    public void testQueryFormatVersionMissing()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(),
                "CREATE TABLE test_table (col1 varchar) WITH (type = 'ICEBERG')",
                ImmutableList.of());
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        assertThat(iteratorFactory.queryFormatVersion(TEST_TABLE)).isEmpty();
    }

    @Test
    public void testTwoPartitionColumnsOptimize()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "varchar"), new TableColumn("col2", "integer"), new TableColumn("col3", "date")),
                """
                CREATE TABLE test_table (col1 varchar, col2 integer, col3 date)
                WITH
                  (
                    type = 'ICEBERG',
                    partitioning = ARRAY['col1', 'col2'],
                    sorted_by = ARRAY['col3']
                  )
                """,
                ImmutableList.of(
                        new UnoptimizedPartitionValue(ImmutableList.of(new ColumnValue("value1", "varchar"), new ColumnValue("10", "integer")), determineFileType("DATA")),
                        new UnoptimizedPartitionValue(ImmutableList.of(new ColumnValue("value1", "varchar"), new ColumnValue("20", "integer")), determineFileType("DATA")),
                        new UnoptimizedPartitionValue(ImmutableList.of(new ColumnValue("value2", "varchar"), new ColumnValue("10", "integer")), determineFileType("DATA"))));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE, 2);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1", "col2"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value1' AS varchar) AND "col2" = CAST('10' AS integer)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value1' AS varchar) AND "col2" = CAST('20' AS integer)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value2' AS varchar) AND "col2" = CAST('10' AS integer)
                        """);
        assertThat(queryRunner.getCapturedShowCreateTableQuery()).isEqualTo(EXPECTED_SHOW_CREATE_TABLE_QUERY);
        assertThat(queryRunner.getCapturedGetColumnsQuery()).isEqualTo(EXPECTED_GET_COLUMNS_QUERY);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1_COL2);
    }

    @Test
    public void testMultipleColumnsRequestedFallsBackWhenFewerAvailable()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "varchar"), new TableColumn("col2", "integer")),
                "CREATE TABLE test_table (col1 varchar, col2 integer) WITH (partitioning = ARRAY['col1'])",
                ImmutableList.of(
                        unoptimizedPartitionValue("value1", "varchar", "DATA"),
                        unoptimizedPartitionValue("value2", "varchar", "DATA")));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        // requesting optimize on two columns, while partition is on one column, should result in query having only one column
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE, 2);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value1' AS varchar)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE "col1" = CAST('value2' AS varchar)
                        """);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1);
    }

    @Test
    public void testMultipleColumnsWithDayAndIdentityPartitions()
    {
        TestingDeterminePartitionOptimizeQueryRunner queryRunner = new TestingDeterminePartitionOptimizeQueryRunner(
                ImmutableList.of(new TableColumn("col1", "timestamp(6)"), new TableColumn("col2", "integer"), new TableColumn("col3", "date")),
                "CREATE TABLE test_table (col1 timestamp(6), col2 integer, col3 date) WITH (partitioning = ARRAY['day(col1)', 'col2', 'col3'])",
                ImmutableList.of(
                        new UnoptimizedPartitionValue(ImmutableList.of(new ColumnValue("2025-01-01", "date"), new ColumnValue("10", "integer")), determineFileType("DATA")),
                        new UnoptimizedPartitionValue(ImmutableList.of(new ColumnValue("2025-01-02", "date"), new ColumnValue("20", "integer")), determineFileType("DATA"))));
        PartitionBasedMaintenanceIteratorFactory iteratorFactory = new PartitionBasedMaintenanceIteratorFactory(queryRunner, FILE_SIZE_THRESHOLD);
        Optional<PartitionedBasedMaintenanceDetails> partitionedBasedMaintenanceDetails = iteratorFactory.createPartitionedTableOptimizeQueriesIterator(TEST_TABLE, 2);
        assertThat(partitionedBasedMaintenanceDetails.orElseThrow().optimizeStatus())
                .isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1", "col2"), ImmutableList.of("col1_day", "col2"), ImmutableList.of()));
        assertThat(ImmutableList.copyOf(partitionedBasedMaintenanceDetails.orElseThrow().queryIterator()))
                .containsExactly(
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE date("col1") = CAST('2025-01-01' AS date) AND "col2" = CAST('10' AS integer)
                        """,
                        """
                        ALTER TABLE "test_catalog"."test_schema"."test_table" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE date("col1") = CAST('2025-01-02' AS date) AND "col2" = CAST('20' AS integer)
                        """);
        assertThat(queryRunner.getCapturedGetUnoptimizedPartitionColumnValuesQuery()).isEqualTo(EXPECTED_UNOPTIMIZED_PARTITIONS_QUERY_COL1_DAY_COL2);
    }

    private static class TestingDeterminePartitionOptimizeQueryRunner
            implements DeterminePartitionOptimizeQueryRunner
    {
        private final List<TableColumn> tableColumns;
        private final String showCreateTableResult;
        private final List<UnoptimizedPartitionValue> unoptimizedPartitionValues;
        private String capturedGetColumnsQuery;
        private String capturedShowCreateTableQuery;
        private String capturedGetUnoptimizedPartitionColumnValuesQuery;

        private TestingDeterminePartitionOptimizeQueryRunner(
                List<TableColumn> tableColumns,
                String showCreateTableResult,
                List<UnoptimizedPartitionValue> unoptimizedPartitionValues)
        {
            this.tableColumns = ImmutableList.copyOf(tableColumns);
            this.showCreateTableResult = requireNonNull(showCreateTableResult, "showCreateTableResult is null");
            this.unoptimizedPartitionValues = ImmutableList.copyOf(unoptimizedPartitionValues);
        }

        @Override
        public List<TableColumn> runGetColumnsQuery(String query)
        {
            capturedGetColumnsQuery = query;
            return tableColumns;
        }

        @Override
        public String runShowCreateTableQuery(String query)
        {
            capturedShowCreateTableQuery = query;
            return showCreateTableResult;
        }

        @Override
        public List<UnoptimizedPartitionValue> runGetUnoptimizedPartitionColumnValuesQuery(String query, int partitionColumnCount)
        {
            capturedGetUnoptimizedPartitionColumnValuesQuery = query;
            return unoptimizedPartitionValues;
        }

        public String getCapturedGetColumnsQuery()
        {
            return capturedGetColumnsQuery;
        }

        public String getCapturedShowCreateTableQuery()
        {
            return capturedShowCreateTableQuery;
        }

        public String getCapturedGetUnoptimizedPartitionColumnValuesQuery()
        {
            return capturedGetUnoptimizedPartitionColumnValuesQuery;
        }
    }
}
