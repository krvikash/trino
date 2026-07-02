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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the incremental-column materialized view refresh feature
 * gated by {@code iceberg.materialized-view-incremental-column-refresh.enabled}.
 */
public class TestIcebergIncrementalColumnMaterializedViewRefresh
        extends AbstractTestQueryFramework
{
    private static final String HIVE_CATALOG = "hive";
    private static final String ICEBERG_TEST_SCHEMA = "incremental_column_test_" + randomNameSuffix();
    private static final String HIVE_TEST_SCHEMA = "incremental_column_test_hive_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.materialized-view-incremental-column-refresh.enabled", "true"))
                .build();
        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data")));
        queryRunner.createCatalog(HIVE_CATALOG, "hive", ImmutableMap.of("hive.security", "allow-all"));
        queryRunner.execute("CREATE SCHEMA " + ICEBERG_CATALOG + "." + ICEBERG_TEST_SCHEMA);
        queryRunner.execute("CREATE SCHEMA " + HIVE_CATALOG + "." + HIVE_TEST_SCHEMA);
        return queryRunner;
    }

    @AfterAll
    public final void cleanupSchema()
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + ICEBERG_TEST_SCHEMA + " CASCADE");
        assertUpdate("DROP SCHEMA IF EXISTS " + HIVE_CATALOG + "." + HIVE_TEST_SCHEMA + " CASCADE");
    }

    @Test
    public void testCreateRejectsIncrementalColumnNotInOutput()
    {
        String source = ICEBERG_TEST_SCHEMA + ".create_reject_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".create_reject_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts BIGINT)");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'nonexistent_column') AS SELECT id, ts FROM " + source,
                ".*incremental_column 'nonexistent_column' is not part of the materialized view output columns.*");
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testCreateRejectsUnsupportedIncrementalColumnType()
    {
        String source = ICEBERG_TEST_SCHEMA + ".create_unsupported_type_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".create_unsupported_type_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source +
                " (id BIGINT, ts_tz TIMESTAMP(6) WITH TIME ZONE, flag BOOLEAN, weight DOUBLE)");

        // TIMESTAMP WITH TIME ZONE — MV storage downgrades to VARCHAR, source keeps original type.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts_tz') AS SELECT id, ts_tz FROM " + source,
                ".*incremental_column type is not supported: timestamp\\(6\\) with time zone.*");

        // BOOLEAN / DOUBLE — no formatter and not orderable in a useful way.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'flag') AS SELECT id, flag FROM " + source,
                ".*incremental_column type is not supported: boolean.*");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'weight') AS SELECT id, weight FROM " + source,
                ".*incremental_column type is not supported: double.*");

        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testCreateRejectsHighPrecisionIncrementalColumnType()
    {
        String source = ICEBERG_TEST_SCHEMA + ".precision_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".precision_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts TIMESTAMP(6), t TIME(6))");

        // TIMESTAMP(9) — precision above microsecond limit; Iceberg storage would downgrade to VARCHAR.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts') AS SELECT id, CAST(ts AS TIMESTAMP(9)) AS ts FROM " + source,
                ".*incremental_column type is not supported: timestamp\\(9\\).*");

        // TIME(7) — just above the microsecond boundary (precision 6).
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 't') AS SELECT id, CAST(t AS TIME(7)) AS t FROM " + source,
                ".*incremental_column type is not supported: time\\(7\\).*");

        // TIME(9) — further above the boundary.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 't') AS SELECT id, CAST(t AS TIME(9)) AS t FROM " + source,
                ".*incremental_column type is not supported: time\\(9\\).*");

        // TIME WITH TIME ZONE — not a plain TimeType, not preserved natively in Iceberg storage.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 't_tz') AS SELECT id, (t AT TIME ZONE 'UTC') AS t_tz FROM " + source,
                ".*incremental_column type is not supported: time\\(6\\) with time zone.*");

        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnCannotBeChangedAfterCreate()
    {
        String source = ICEBERG_TEST_SCHEMA + ".immutable_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".immutable_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts BIGINT, ts2 BIGINT)");

        // Without incremental_column at create — cannot be added later.
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " AS SELECT id, ts, ts2 FROM " + source);
        assertQueryFails(
                "ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES incremental_column = 'ts'",
                ".*The following properties cannot be updated: incremental_column.*");
        assertUpdate("DROP MATERIALIZED VIEW " + mvName);

        // With incremental_column set — cannot be replaced or cleared.
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts') AS SELECT id, ts, ts2 FROM " + source);
        assertQueryFails(
                "ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES incremental_column = 'ts2'",
                ".*The following properties cannot be updated: incremental_column.*");
        assertQueryFails(
                "ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES incremental_column = DEFAULT",
                ".*The following properties cannot be updated: incremental_column.*");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnAppendsOnlyDelta()
    {
        String source = ICEBERG_TEST_SCHEMA + ".append_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".append_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts') AS SELECT id, ts FROM " + source);
        // First refresh: storage is empty, so all source rows are appended.
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, ts FROM " + mvName))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200')");

        // Add new rows above and below the checkpoint to confirm only above-checkpoint rows are added.
        assertUpdate("INSERT INTO " + source + " VALUES (3, 50), (4, 300), (5, 400)", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, ts FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '4', BIGINT '300'), (BIGINT '5', BIGINT '400')");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testFirstRefreshInsertsEverythingWhenStorageEmpty()
    {
        String source = ICEBERG_TEST_SCHEMA + ".first_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".first_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts TIMESTAMP(6))");
        assertUpdate(
                "INSERT INTO " + source + " VALUES " +
                        "(1, TIMESTAMP '2026-01-01 00:00:00.000000'), " +
                        "(2, TIMESTAMP '2026-02-01 00:00:00.000000')",
                2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts') AS SELECT id, ts FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT count(*) FROM " + mvName)).matches("VALUES BIGINT '2'");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalRefreshOnDateColumn()
    {
        String source = ICEBERG_TEST_SCHEMA + ".date_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".date_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, extract_date DATE)");
        assertUpdate(
                "INSERT INTO " + source + " VALUES (1, DATE '2026-01-01'), (2, DATE '2026-02-01')",
                2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'extract_date') AS SELECT id, extract_date FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);

        assertUpdate("INSERT INTO " + source + " VALUES (3, DATE '2026-01-15'), (4, DATE '2026-03-01')", 2);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 1);
        assertThat(query("SELECT id FROM " + mvName + " ORDER BY id"))
                .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '4'");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalRefreshWithPartitioning()
    {
        String source = ICEBERG_TEST_SCHEMA + ".part_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".part_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, region VARCHAR, ts BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 'us', 100), (2, 'eu', 200), (3, 'us', 150)", 3);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (" +
                        "partitioning = ARRAY['region'], " +
                        "incremental_column = 'ts') AS " +
                        "SELECT id, region, ts FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 3);

        // Verify the storage table is partitioned as requested.
        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + mvName))
                .contains("partitioning = ARRAY['region']");

        // Add rows across partitions: some below checkpoint (skipped), some above (appended).
        assertUpdate("INSERT INTO " + source + " VALUES (4, 'us', 50), (5, 'eu', 300), (6, 'us', 400)", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, region, ts FROM " + mvName + " ORDER BY id"))
                .matches("VALUES " +
                        "(BIGINT '1', VARCHAR 'us', BIGINT '100'), " +
                        "(BIGINT '2', VARCHAR 'eu', BIGINT '200'), " +
                        "(BIGINT '3', VARCHAR 'us', BIGINT '150'), " +
                        "(BIGINT '5', VARCHAR 'eu', BIGINT '300'), " +
                        "(BIGINT '6', VARCHAR 'us', BIGINT '400')");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }

    /**
     * Cross-catalog scenario: MV lives in the iceberg catalog, source table in the hive
     * catalog. With the {@code INCREMENTAL_COLUMN} refresh type the connector skips the
     * delete-before-insert step regardless of source-table origin, so pre-checkpoint rows
     * already in storage are preserved and only above-checkpoint rows are appended.
     */
    @Test
    public void testIncrementalRefreshWithSourceFromDifferentCatalog()
    {
        String hiveSource = HIVE_CATALOG + "." + HIVE_TEST_SCHEMA + ".cross_catalog_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".cross_catalog_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + hiveSource + " (id BIGINT, ts BIGINT)");
        assertUpdate("INSERT INTO " + hiveSource + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts') AS SELECT id, ts FROM " + hiveSource);
        // First refresh: storage is empty, no checkpoint, all source rows appended.
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, ts FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200')");

        // Add rows across the checkpoint. Only above-checkpoint rows are appended; old rows
        // remain in storage because INCREMENTAL_COLUMN refresh skips the wipe.
        assertUpdate("INSERT INTO " + hiveSource + " VALUES (3, 50), (4, 300), (5, 400)", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, ts FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '4', BIGINT '300'), (BIGINT '5', BIGINT '400')");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + hiveSource);
    }

    @Test
    public void testIncrementalRefreshWithMultipleSources()
    {
        String hiveSource = HIVE_CATALOG + "." + HIVE_TEST_SCHEMA + ".cross_catalog_src_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + hiveSource + " (id BIGINT, ts BIGINT)");
        assertUpdate("INSERT INTO " + hiveSource + " VALUES (1, 100), (2, 200)", 2);

        String hiveSource2 = HIVE_CATALOG + "." + HIVE_TEST_SCHEMA + ".cross_catalog_src2_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + hiveSource2 + " (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO " + hiveSource2 + " VALUES (1, 'Foo'), (2, 'Bar')", 2);

        String mvName = ICEBERG_TEST_SCHEMA + ".cross_catalog_mv_" + randomNameSuffix();
        String mvSelect =
                """
                SELECT id, first.ts, second.name
                FROM %s AS first
                JOIN %s AS second
                USING (id)
                """.formatted(hiveSource, hiveSource2);
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts') AS " + mvSelect);
        // First refresh: storage is empty, no checkpoint, all source rows appended.
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, ts, name FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100', VARCHAR 'Foo'), (BIGINT '2', BIGINT '200', VARCHAR 'Bar')");

        // Add rows across the checkpoint on both sources. The INCREMENTAL_COLUMN refresh
        // path is shape-agnostic — the analyzer's WHERE predicate trims the join's output
        // and the connector appends only above-checkpoint rows; pre-checkpoint rows already
        // in storage are preserved.
        assertUpdate("INSERT INTO " + hiveSource + " VALUES (3, 50), (4, 300), (5, 400)", 3);
        assertUpdate("INSERT INTO " + hiveSource2 + " VALUES (3, 'Baz'), (4, 'Bax'), (5, 'Bav')", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, ts, name FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100', VARCHAR 'Foo'), (BIGINT '2', BIGINT '200', VARCHAR 'Bar'), (BIGINT '4', BIGINT '300', VARCHAR 'Bax'), (BIGINT '5', BIGINT '400', VARCHAR 'Bav')");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + hiveSource);
        assertUpdate("DROP TABLE " + hiveSource2);
    }

    @Test
    public void testIncrementalColumnRejectedOnAggregatingMV()
    {
        String source = ICEBERG_TEST_SCHEMA + ".agg_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".agg_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, region VARCHAR, ts BIGINT)");
        String errorPattern = ".*CREATE MATERIALIZED VIEW with incremental_column is not supported when the MV definition contains aggregations or GROUP BY.*";

        // GROUP BY with aggregation.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'max_ts') AS " +
                        "SELECT region, max(ts) AS max_ts FROM " + source + " GROUP BY region",
                errorPattern);

        // Scalar aggregation (no GROUP BY).
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'max_ts') AS " +
                        "SELECT max(ts) AS max_ts FROM " + source,
                errorPattern);

        // No aggregation: same query without GROUP BY and without aggregate functions must succeed.
        assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                "SELECT id, region, ts FROM " + source);
        assertUpdate("DROP MATERIALIZED VIEW " + mvName);

        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnRejectedOnNestedAggregatingMV()
    {
        String source = ICEBERG_TEST_SCHEMA + ".nested_agg_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".nested_agg_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, region VARCHAR, ts BIGINT)");
        String errorPattern = ".*CREATE MATERIALIZED VIEW with incremental_column is not supported when the MV definition contains aggregations or GROUP BY.*";

        // UNION ALL where one branch has GROUP BY with aggregation.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'max_ts') AS " +
                        "SELECT region, max(ts) AS max_ts FROM " + source + " GROUP BY region " +
                        "UNION ALL " +
                        "SELECT region, ts AS max_ts FROM " + source,
                errorPattern);

        // CTE that aggregates.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'max_ts') AS " +
                        "WITH a AS (SELECT region, max(ts) AS max_ts FROM " + source + " GROUP BY region) " +
                        "SELECT region, max_ts FROM a",
                errorPattern);

        // FROM-subquery that aggregates.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'max_ts') AS " +
                        "SELECT * FROM (SELECT region, max(ts) AS max_ts FROM " + source + " GROUP BY region)",
                errorPattern);

        // GROUP BY in a FROM-subquery with no top-level aggregation.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'region') AS " +
                        "SELECT region FROM (SELECT region FROM " + source + " GROUP BY region)",
                errorPattern);

        // Positive control: UNION ALL of two plain non-aggregating selects must succeed.
        assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                "SELECT id, region, ts FROM " + source + " WHERE id < 10 " +
                "UNION ALL " +
                "SELECT id, region, ts FROM " + source + " WHERE id >= 10");
        assertUpdate("DROP MATERIALIZED VIEW " + mvName);

        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnRejectedOnNonMonotonicConstructs()
    {
        String source = ICEBERG_TEST_SCHEMA + ".nonmonotonic_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".nonmonotonic_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, region VARCHAR, ts BIGINT)");
        String messagePrefix = "Materialized view with incremental_column is not supported when its definition contains ";

        // DISTINCT
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT DISTINCT id, region, ts FROM " + source,
                ".*" + messagePrefix + "DISTINCT.*");

        // DISTINCT nested in a FROM-subquery
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts FROM (SELECT DISTINCT id, region, ts FROM " + source + ")",
                ".*" + messagePrefix + "DISTINCT.*");

        // LIMIT
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts FROM " + source + " LIMIT 10",
                ".*" + messagePrefix + "LIMIT or FETCH.*");

        // OFFSET
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts FROM " + source + " OFFSET 5 ROWS",
                ".*" + messagePrefix + "OFFSET.*");

        // UNION (distinct)
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts FROM " + source + " WHERE id < 10 " +
                        "UNION " +
                        "SELECT id, region, ts FROM " + source + " WHERE id >= 10",
                ".*" + messagePrefix + "INTERSECT, EXCEPT or UNION DISTINCT.*");

        // INTERSECT
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts FROM " + source + " " +
                        "INTERSECT " +
                        "SELECT id, region, ts FROM " + source,
                ".*" + messagePrefix + "INTERSECT, EXCEPT or UNION DISTINCT.*");

        // EXCEPT
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts FROM " + source + " " +
                        "EXCEPT " +
                        "SELECT id, region, ts FROM " + source,
                ".*" + messagePrefix + "INTERSECT, EXCEPT or UNION DISTINCT.*");

        // Window function
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts, row_number() OVER (ORDER BY ts) AS rn FROM " + source,
                ".*" + messagePrefix + "window functions.*");

        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnRefreshDoesNotCausePerpetuallyStaleMV()
    {
        String source = ICEBERG_TEST_SCHEMA + ".staleness_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".staleness_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                "SELECT id, ts FROM " + source);

        // First refresh populates the MV.
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);

        // Insert new rows above the current watermark.
        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);

        // Second refresh is incremental — appends the new row.
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 1);

        // Insert nothing new. A third refresh must append 0 rows, not be treated as a full refresh.
        // If the MV self-scan were recorded in DEPENDS_ON_TABLES the MV would appear perpetually
        // stale and this count could be incorrect (all rows refreshed instead of 0).
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 0);

        // Confirm the MV contents are correct (no duplicates).
        assertThat(query("SELECT id, ts FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300')");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnMVNonDeterministicFunction()
    {
        String source = ICEBERG_TEST_SCHEMA + ".nondet_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".nondet_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, region VARCHAR, ts BIGINT)");
        String errorPattern = ".*CREATE MATERIALIZED VIEW with incremental_column is not supported when non-deterministic functions used in MV definition.*";

        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts, CURRENT_TIMESTAMP AS current_ts FROM " + source,
                errorPattern);

        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts, CURRENT_DATE AS current_d FROM " + source,
                errorPattern);

        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts, LOCALTIME AS lt FROM " + source,
                errorPattern);

        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts, LOCALTIMESTAMP AS lts FROM " + source,
                errorPattern);

        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts, random() AS r FROM " + source,
                errorPattern);

        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName + " WITH (incremental_column = 'ts') AS " +
                        "SELECT id, region, ts, uuid() AS u FROM " + source,
                errorPattern);
    }

    /**
     * Exercises every supported {@code incremental_column} type end-to-end:
     * the source table column type drives the storage table column type, the
     * {@code SELECT max(col) FROM mv} subquery returns a value of that type, and
     * the comparison against the source column must succeed natively for every
     * supported checkpoint type.
     */
    @ParameterizedTest
    @MethodSource("incrementalColumnTypeCases")
    public void testIncrementalRefreshByColumnType(IncrementalColumnTypeCase testCase)
    {
        String sourceTable = ICEBERG_TEST_SCHEMA + ".typed_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".typed_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTable + " (id BIGINT, checkpoint " + testCase.columnType() + ")");
        assertUpdate(
                "INSERT INTO " + sourceTable + " VALUES " +
                        "(1, " + testCase.initialLow() + "), " +
                        "(2, " + testCase.initialHigh() + ")",
                2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'checkpoint') AS SELECT id, checkpoint FROM " + sourceTable);

        // First refresh: storage is empty, so all rows are appended.
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);

        // Mix rows below and above the current checkpoint.
        assertUpdate(
                "INSERT INTO " + sourceTable + " VALUES " +
                        "(3, " + testCase.belowCheckpoint() + "), " +
                        "(4, " + testCase.aboveCheckpointLow() + "), " +
                        "(5, " + testCase.aboveCheckpointHigh() + ")",
                3);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);

        assertThat(query("SELECT id FROM " + mvName + " ORDER BY id"))
                .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '4', BIGINT '5'");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + sourceTable);
    }

    /**
     * Parametrized cases cover only types that the materialized-view storage
     * table preserves as a native Iceberg primitive (see
     * {@code AbstractTrinoCatalog#typeForMaterializedViewStorageTable}).
     */
    private static List<IncrementalColumnTypeCase> incrementalColumnTypeCases()
    {
        return List.of(
                new IncrementalColumnTypeCase(
                        "INTEGER",
                        "INTEGER '50'",
                        "INTEGER '100'",
                        "INTEGER '200'",
                        "INTEGER '300'",
                        "INTEGER '400'"),
                new IncrementalColumnTypeCase(
                        "BIGINT",
                        "BIGINT '50'",
                        "BIGINT '100'",
                        "BIGINT '200'",
                        "BIGINT '300'",
                        "BIGINT '400'"),
                new IncrementalColumnTypeCase(
                        "DATE",
                        "DATE '2025-12-31'",
                        "DATE '2026-01-01'",
                        "DATE '2026-02-01'",
                        "DATE '2026-03-01'",
                        "DATE '2026-04-01'"),
                new IncrementalColumnTypeCase(
                        "TIME(6)",
                        "TIME '01:00:00.000000'",
                        "TIME '02:00:00.000000'",
                        "TIME '03:00:00.000000'",
                        "TIME '04:00:00.000000'",
                        "TIME '05:00:00.000000'"),
                new IncrementalColumnTypeCase(
                        "TIMESTAMP(6)",
                        "TIMESTAMP '2025-12-01 00:00:00.000000'",
                        "TIMESTAMP '2026-01-01 00:00:00.000000'",
                        "TIMESTAMP '2026-02-01 00:00:00.000000'",
                        "TIMESTAMP '2026-03-01 00:00:00.000000'",
                        "TIMESTAMP '2026-04-01 00:00:00.000000'"),
                new IncrementalColumnTypeCase(
                        "VARCHAR",
                        "VARCHAR 'a'",
                        "VARCHAR 'b'",
                        "VARCHAR 'c'",
                        "VARCHAR 'd'",
                        "VARCHAR 'e'"),
                new IncrementalColumnTypeCase(
                        "DECIMAL(10, 2)",
                        "DECIMAL '0.50'",
                        "DECIMAL '1.00'",
                        "DECIMAL '2.00'",
                        "DECIMAL '3.00'",
                        "DECIMAL '4.00'"));
    }

    public record IncrementalColumnTypeCase(
            String columnType,
            String belowCheckpoint,
            String initialLow,
            String initialHigh,
            String aboveCheckpointLow,
            String aboveCheckpointHigh)
    {
        @Override
        public String toString()
        {
            return columnType;
        }
    }
}
