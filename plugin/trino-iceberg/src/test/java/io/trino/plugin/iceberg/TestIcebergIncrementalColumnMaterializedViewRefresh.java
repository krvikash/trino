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
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.StoredWatermark;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeId;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getConnectorService;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.parseStoredWatermark;
import static io.trino.spi.RefreshType.INCREMENTAL_COLUMN;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
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
                        "iceberg.materialized-view-incremental-column-refresh.enabled", "true",
                        // Lets testIncrementalColumnWatermarkFallsBackWhenMetricsDisabled turn off column
                        // metrics on a materialized view's storage table via ALTER TABLE.
                        "iceberg.allowed-extra-properties", "write.metadata.metrics.column.event_timestamp"))
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
    public void testRenameOfSourceIncrementalColumnRequiresReplace()
    {
        String source = ICEBERG_TEST_SCHEMA + ".rename_src_" + randomNameSuffix();
        String mvName = "rename_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp') AS SELECT id, event_timestamp FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);

        assertUpdate("ALTER TABLE " + source + " RENAME COLUMN event_timestamp TO event_time");
        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);
        assertQueryFails(
                "REFRESH MATERIALIZED VIEW " + qualifiedMvName,
                ".*Column 'event_timestamp' cannot be resolved.*");

        assertUpdate(
                "CREATE OR REPLACE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_time') AS SELECT id, event_time FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 3);
        assertStoredWatermarkValue(mvName, Optional.of("300"));

        assertUpdate("INSERT INTO " + source + " VALUES (4, 400)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertThat(query("SELECT id, event_time FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300'), (BIGINT '4', BIGINT '400')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testCreateRejectsRowIncrementalColumn()
    {
        String source = ICEBERG_TEST_SCHEMA + ".row_reject_src_" + randomNameSuffix();
        String mvName = ICEBERG_TEST_SCHEMA + ".row_reject_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event ROW(event_timestamp BIGINT, name VARCHAR))");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'event.event_timestamp') AS SELECT id, event FROM " + source,
                ".*incremental_column 'event.event_timestamp' is not part of the materialized view output columns.*");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'event') AS SELECT id, event FROM " + source,
                ".*incremental_column type is not supported: row\\(\"event_timestamp\" bigint, \"name\" varchar\\).*");
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnProjectedFromNestedField()
    {
        String source = ICEBERG_TEST_SCHEMA + ".nested_field_src_" + randomNameSuffix();
        String mvName = "nested_field_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event ROW(event_timestamp BIGINT, name VARCHAR))");
        assertUpdate("INSERT INTO " + source + " VALUES (1, ROW(100, 'a')), (2, ROW(200, 'b'))", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp') AS SELECT id, event.event_timestamp AS event_timestamp FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);
        assertWatermark(mvName, "200");

        assertUpdate("INSERT INTO " + source + " VALUES (3, ROW(300, 'c'))", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");
        String sourceTableName = source.substring(source.lastIndexOf('.') + 1);
        assertThat(plan).containsPattern("ScanFilter\\[table = .*" + sourceTableName + ".*filterPredicate = \\(bigint '200' < event_event_timestamp\\)]");

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertWatermark(mvName, "300");
        assertThat(query("SELECT id, event_timestamp FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
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

    /**
     * Iceberg rounds string upper bounds up, so VARCHAR keeps the correlated subquery — correct, but
     * it does not push down, which shows up as a surviving CrossJoin.
     */
    @Test
    public void testIncrementalRefreshOnVarcharDoesNotPushPredicate()
    {
        String source = ICEBERG_TEST_SCHEMA + ".varchar_fallback_src_" + randomNameSuffix();
        String mvName = "varchar_fallback_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, checkpoint VARCHAR)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 'a'), (2, 'b')", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'checkpoint') AS SELECT id, checkpoint FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);

        // Without a change to the source since the last refresh, the MV is considered fresh and
        // REFRESH short-circuits to a trivial zero-row plan with no CrossJoin at all — insert
        // first so the plan below reflects a real (fallback) refresh, not a skipped one.
        assertUpdate("INSERT INTO " + source + " VALUES (3, 'c')", 1);

        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).contains("CrossJoin");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    /**
     * A file carrying no upper bound must fail closed, rather than read as "no rows" (which re-appends
     * everything) or as a stale value (which drops rows).
     */
    @Test
    public void testIncrementalColumnWatermarkFallsBackWhenMetricsDisabled()
    {
        String source = ICEBERG_TEST_SCHEMA + ".metrics_fallback_src_" + randomNameSuffix();
        String mvName = "metrics_fallback_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);

        // Trino's Parquet writer ignores Iceberg's MetricsConfig (trinodb/trino#9791), so metrics can
        // only be turned off for an ORC-backed storage table.
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp', format = 'ORC') AS SELECT id, event_timestamp FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);

        // The storage table is always built with extra properties disallowed, so they cannot be set at
        // CREATE MATERIALIZED VIEW time.
        assertUpdate("ALTER TABLE " + storageTableName(ICEBERG_TEST_SCHEMA, mvName) +
                " SET PROPERTIES extra_properties = MAP(ARRAY['write.metadata.metrics.column.event_timestamp'], ARRAY['none'])");

        // Iceberg table properties are not retroactive, so this refresh still filters by the stored
        // watermark 200. The row it appends lands in a file written without event_timestamp bounds, so it stores none.
        assertUpdate("INSERT INTO " + source + " VALUES (3, 50), (4, 300)", 2);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);

        // The previous refresh stored no watermark, so this one has nothing to push down.
        assertUpdate("INSERT INTO " + source + " VALUES (5, 400)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).contains("CrossJoin");

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertThat(query("SELECT id, event_timestamp FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '4', BIGINT '300'), (BIGINT '5', BIGINT '400')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    /**
     * An all-null file also carries no upper bound, but holds no row that could exceed the maximum, so it
     * must not make it unprovable. The two are told apart by the file's null and record counts.
     * <p>
     * Partitioning by {@code id} is what makes the all-null file deterministic: one file per partition.
     */
    @Test
    public void testIncrementalColumnWatermarkIgnoresAllNullStorageFile()
    {
        String source = ICEBERG_TEST_SCHEMA + ".all_null_src_" + randomNameSuffix();
        String mvName = "all_null_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, NULL)", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp', partitioning = ARRAY['id']) AS SELECT id, event_timestamp FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);

        assertWatermark(mvName, "100");

        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");
        String sourceTableName = source.substring(source.lastIndexOf('.') + 1);
        assertThat(plan).containsPattern("ScanFilter\\[table = .*" + sourceTableName + ".*filterPredicate = \\(bigint '100' < event_timestamp\\)]");

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertWatermark(mvName, "300");
        assertThat(query("SELECT id, event_timestamp FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', CAST(NULL AS BIGINT)), (BIGINT '3', BIGINT '300')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnWatermarkIsStoredForMixedCaseColumn()
    {
        String source = ICEBERG_TEST_SCHEMA + ".mixed_case_watermark_src_" + randomNameSuffix();
        String mvName = "mixed_case_watermark_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'EventTimestamp') AS SELECT id, event_timestamp AS \"EventTimestamp\" FROM " + source);

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);
        assertWatermark(mvName, "200");

        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertWatermark(mvName, "300");
        assertThat(query("SELECT id, \"EventTimestamp\" FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnWatermarkIsStoredInSnapshotSummary()
    {
        String source = ICEBERG_TEST_SCHEMA + ".stored_watermark_src_" + randomNameSuffix();
        String mvName = "stored_watermark_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp') AS SELECT id, event_timestamp FROM " + source);

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);
        assertWatermark(mvName, "200");

        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");
        String sourceTableName = source.substring(source.lastIndexOf('.') + 1);
        assertThat(plan).containsPattern("ScanFilter\\[table = .*" + sourceTableName + ".*filterPredicate = \\(bigint '200' < event_timestamp\\)]");

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertWatermark(mvName, "300");
        assertThat(query("SELECT id, event_timestamp FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    /**
     * OPTIMIZE preserves every row's value, so it carries the watermark forward. Otherwise routine
     * maintenance would permanently demote the view back to the correlated-subquery plan.
     */
    @Test
    public void testIncrementalColumnWatermarkSurvivesOptimize()
    {
        String source = ICEBERG_TEST_SCHEMA + ".optimize_watermark_src_" + randomNameSuffix();
        String mvName = "optimize_watermark_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp') AS SELECT id, event_timestamp FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);

        // A second refresh, so the storage table holds two data files for OPTIMIZE to actually rewrite.
        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertWatermark(mvName, "300");

        assertUpdate("ALTER MATERIALIZED VIEW " + qualifiedMvName + " EXECUTE optimize");
        assertWatermark(mvName, "300");

        assertUpdate("INSERT INTO " + source + " VALUES (4, 400)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");
        String sourceTableName = source.substring(source.lastIndexOf('.') + 1);
        assertThat(plan).containsPattern("ScanFilter\\[table = .*" + sourceTableName + ".*filterPredicate = \\(bigint '300' < event_timestamp\\)]");

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertWatermark(mvName, "400");
        assertThat(query("SELECT id, event_timestamp FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300'), (BIGINT '4', BIGINT '400')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    /**
     * The other half of that rule: a DELETE lowers the real maximum, so a watermark carried across it would
     * exceed what the table holds and the next refresh would skip source rows. Dropping it costs pushdown for
     * one refresh.
     */
    @Test
    public void testIncrementalColumnWatermarkDroppedByStorageTableDelete()
    {
        String source = ICEBERG_TEST_SCHEMA + ".delete_watermark_src_" + randomNameSuffix();
        String mvName = "delete_watermark_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200), (3, 300)", 3);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp') AS SELECT id, event_timestamp FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 3);
        assertWatermark(mvName, "300");

        assertUpdate("DELETE FROM " + storageTableName(ICEBERG_TEST_SCHEMA, mvName) + " WHERE event_timestamp = 300", 1);
        assertThat(latestStoredWatermark(mvName)).isEmpty();

        assertUpdate("INSERT INTO " + source + " VALUES (4, 250)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).contains("CrossJoin");

        // Had the stale watermark of 300 been carried across the DELETE, both rows would be skipped.
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);
        assertThat(query("SELECT id, event_timestamp FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300'), (BIGINT '4', BIGINT '250')");

        assertWatermark(mvName, "300");
        assertUpdate("INSERT INTO " + source + " VALUES (5, 400)", 1);
        plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertWatermark(mvName, "400");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
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
        String mvName = "typed_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + sourceTable + " (id BIGINT, checkpoint " + testCase.columnType() + ")");
        assertUpdate(
                "INSERT INTO " + sourceTable + " VALUES " +
                        "(1, " + testCase.initialLow() + "), " +
                        "(2, " + testCase.initialHigh() + ")",
                2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'checkpoint') AS SELECT id, checkpoint FROM " + sourceTable);

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);
        assertStoredWatermarkValue(mvName, testCase.storedWatermarkAfterFirstRefresh());

        // Mix rows below and above the current checkpoint.
        assertUpdate(
                "INSERT INTO " + sourceTable + " VALUES " +
                        "(3, " + testCase.belowCheckpoint() + "), " +
                        "(4, " + testCase.aboveCheckpointLow() + "), " +
                        "(5, " + testCase.aboveCheckpointHigh() + ")",
                3);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);

        assertThat(query("SELECT id FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '4', BIGINT '5'");
        assertStoredWatermarkValue(mvName, testCase.storedWatermarkAfterSecondRefresh());

        assertIncrementalRefreshPushdown(testCase, sourceTable, qualifiedMvName);

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + sourceTable);
    }

    /**
     * Asserting per type keeps the connector's exactness gate, the engine's literal rendering and the set
     * of types accepted for {@code incremental_column} from drifting apart.
     */
    private void assertIncrementalRefreshPushdown(IncrementalColumnTypeCase testCase, String sourceTable, String mvName)
    {
        // Without a change to the source since the last refresh, the MV is considered fresh and
        // REFRESH short-circuits to a trivial zero-row plan (LogicalPlanner#planStatement) with no
        // TableScan or join at all — insert first so the plan below reflects a real refresh.
        assertUpdate("INSERT INTO " + sourceTable + " VALUES (6, " + testCase.aboveCheckpointHigh() + ")", 1);

        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + mvName, ExplainType.Type.DISTRIBUTED);
        if (!testCase.watermarkPushedDown()) {
            assertThat(plan).contains("CrossJoin");
            return;
        }
        assertThat(plan).doesNotContain("CrossJoin");
        String sourceTableName = sourceTable.substring(sourceTable.lastIndexOf('.') + 1);
        assertThat(countOccurrences(plan, sourceTableName)).isEqualTo(1);
        assertThat(plan).containsPattern("ScanFilter\\[table = .*" + sourceTableName + ".*filterPredicate = \\(.* < checkpoint\\)]");
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
                        "INTEGER '400'",
                        Optional.of("200"),
                        Optional.of("400")),
                new IncrementalColumnTypeCase(
                        "BIGINT",
                        "BIGINT '50'",
                        "BIGINT '100'",
                        "BIGINT '200'",
                        "BIGINT '300'",
                        "BIGINT '400'",
                        Optional.of("200"),
                        Optional.of("400")),
                // Outside the INTEGER range, so the literal is typed BIGINT rather than widened from INTEGER.
                new IncrementalColumnTypeCase(
                        "BIGINT",
                        "BIGINT '5000000000'",
                        "BIGINT '6000000000'",
                        "BIGINT '7000000000'",
                        "BIGINT '8000000000'",
                        "BIGINT '9000000000'",
                        Optional.of("7000000000"),
                        Optional.of("9000000000")),
                new IncrementalColumnTypeCase(
                        "DATE",
                        "DATE '2025-12-31'",
                        "DATE '2026-01-01'",
                        "DATE '2026-02-01'",
                        "DATE '2026-03-01'",
                        "DATE '2026-04-01'",
                        Optional.of("20485"),
                        Optional.of("20544")),
                // Spans the Unix epoch, so the watermark's epoch-day representation is negative.
                new IncrementalColumnTypeCase(
                        "DATE",
                        "DATE '1965-06-14'",
                        "DATE '1969-06-14'",
                        "DATE '1969-12-31'",
                        "DATE '1970-01-01'",
                        "DATE '1975-06-14'",
                        Optional.of("-1"),
                        Optional.of("1990")),
                new IncrementalColumnTypeCase(
                        "TIME(0)",
                        "TIME '01:00:00'",
                        "TIME '02:00:00'",
                        "TIME '03:00:00'",
                        "TIME '04:00:00'",
                        "TIME '05:00:00'",
                        Optional.of("10800000000000000"),
                        Optional.of("18000000000000000")),
                new IncrementalColumnTypeCase(
                        "TIME(3)",
                        "TIME '01:00:00.100'",
                        "TIME '02:00:00.200'",
                        "TIME '03:00:00.300'",
                        "TIME '04:00:00.400'",
                        "TIME '05:00:00.500'",
                        Optional.of("10800300000000000"),
                        Optional.of("18000500000000000")),
                new IncrementalColumnTypeCase(
                        "TIME(6)",
                        "TIME '01:00:00.000000'",
                        "TIME '02:00:00.000000'",
                        "TIME '03:00:00.000000'",
                        "TIME '04:00:00.000000'",
                        "TIME '05:00:00.000000'",
                        Optional.of("10800000000000000"),
                        Optional.of("18000000000000000")),
                new IncrementalColumnTypeCase(
                        "TIMESTAMP(0)",
                        "TIMESTAMP '2025-12-01 00:00:00'",
                        "TIMESTAMP '2026-01-01 00:00:00'",
                        "TIMESTAMP '2026-02-01 00:00:00'",
                        "TIMESTAMP '2026-03-01 00:00:00'",
                        "TIMESTAMP '2026-04-01 00:00:00'",
                        Optional.of("1769904000000000"),
                        Optional.of("1775001600000000")),
                new IncrementalColumnTypeCase(
                        "TIMESTAMP(3)",
                        "TIMESTAMP '2025-12-01 00:00:00.100'",
                        "TIMESTAMP '2026-01-01 00:00:00.200'",
                        "TIMESTAMP '2026-02-01 00:00:00.300'",
                        "TIMESTAMP '2026-03-01 00:00:00.400'",
                        "TIMESTAMP '2026-04-01 00:00:00.500'",
                        Optional.of("1769904000300000"),
                        Optional.of("1775001600500000")),
                new IncrementalColumnTypeCase(
                        "TIMESTAMP(6)",
                        "TIMESTAMP '2025-12-01 00:00:00.000000'",
                        "TIMESTAMP '2026-01-01 00:00:00.000000'",
                        "TIMESTAMP '2026-02-01 00:00:00.000000'",
                        "TIMESTAMP '2026-03-01 00:00:00.000000'",
                        "TIMESTAMP '2026-04-01 00:00:00.000000'",
                        Optional.of("1769904000000000"),
                        Optional.of("1775001600000000")),
                new IncrementalColumnTypeCase(
                        "VARCHAR",
                        "VARCHAR 'a'",
                        "VARCHAR 'b'",
                        "VARCHAR 'c'",
                        "VARCHAR 'd'",
                        "VARCHAR 'e'",
                        Optional.empty(),
                        Optional.empty()),
                new IncrementalColumnTypeCase(
                        "DECIMAL(10, 2)",
                        "DECIMAL '0.50'",
                        "DECIMAL '1.00'",
                        "DECIMAL '2.00'",
                        "DECIMAL '3.00'",
                        "DECIMAL '4.00'",
                        Optional.of("200"),
                        Optional.of("400")),
                new IncrementalColumnTypeCase(
                        "INTEGER",
                        "INTEGER '-2147483648'",
                        "INTEGER '-1000'",
                        "INTEGER '-500'",
                        "INTEGER '0'",
                        "INTEGER '2147483647'",
                        Optional.of("-500"),
                        Optional.of("2147483647")),
                // Spans the whole BIGINT range, so the watermark hits Long.MIN_VALUE and Long.MAX_VALUE.
                new IncrementalColumnTypeCase(
                        "BIGINT",
                        "BIGINT '-9223372036854775808'",
                        "BIGINT '-5000000000'",
                        "BIGINT '-100'",
                        "BIGINT '0'",
                        "BIGINT '9223372036854775807'",
                        Optional.of("-100"),
                        Optional.of("9223372036854775807")),
                // DECIMAL(18,0) is the widest short decimal, carried as a long.
                new IncrementalColumnTypeCase(
                        "DECIMAL(18, 0)",
                        "DECIMAL '-999999999999999999'",
                        "DECIMAL '-1'",
                        "DECIMAL '0'",
                        "DECIMAL '1'",
                        "DECIMAL '999999999999999999'",
                        Optional.of("0"),
                        Optional.of("999999999999999999")),
                // DECIMAL(19,0) is the narrowest long decimal, carried as an Int128.
                new IncrementalColumnTypeCase(
                        "DECIMAL(19, 0)",
                        "DECIMAL '-9999999999999999999'",
                        "DECIMAL '-1'",
                        "DECIMAL '0'",
                        "DECIMAL '1'",
                        "DECIMAL '9999999999999999999'",
                        Optional.of("0"),
                        Optional.of("9999999999999999999")),
                new IncrementalColumnTypeCase(
                        "DECIMAL(38, 0)",
                        "DECIMAL '-99999999999999999999999999999999999999'",
                        "DECIMAL '-1'",
                        "DECIMAL '0'",
                        "DECIMAL '1'",
                        "DECIMAL '99999999999999999999999999999999999999'",
                        Optional.of("0"),
                        Optional.of("99999999999999999999999999999999999999")),
                // Straddles the epoch, so the watermark's epoch-microsecond representation is negative.
                new IncrementalColumnTypeCase(
                        "TIMESTAMP(6)",
                        "TIMESTAMP '1900-01-01 00:00:00.000000'",
                        "TIMESTAMP '1960-01-01 00:00:00.123456'",
                        "TIMESTAMP '1969-12-31 23:59:59.999999'",
                        "TIMESTAMP '1970-01-01 00:00:00.000000'",
                        "TIMESTAMP '1970-01-01 00:00:00.000001'",
                        Optional.of("-1"),
                        Optional.of("1")),
                new IncrementalColumnTypeCase(
                        "TIME(6)",
                        "TIME '00:00:00.000000'",
                        "TIME '12:00:00.000000'",
                        "TIME '20:00:00.000000'",
                        "TIME '23:59:59.999998'",
                        "TIME '23:59:59.999999'",
                        Optional.of("72000000000000000"),
                        Optional.of("86399999999000000")),
                new IncrementalColumnTypeCase(
                        "DATE",
                        "DATE '0001-01-01'",
                        "DATE '1000-01-01'",
                        "DATE '1969-12-31'",
                        "DATE '1970-01-01'",
                        "DATE '9999-12-31'",
                        Optional.of("-1"),
                        Optional.of("2932896")),
                new IncrementalColumnTypeCase(
                        "DECIMAL(38, 10)",
                        "DECIMAL '0.5000000000'",
                        "DECIMAL '123456789012345678.1000000000'",
                        "DECIMAL '223456789012345678.2000000000'",
                        "DECIMAL '323456789012345678.3000000000'",
                        "DECIMAL '423456789012345678.4000000000'",
                        Optional.of("2234567890123456782000000000"),
                        Optional.of("4234567890123456784000000000")));
    }

    /**
     * ORC pads a timestamp upper bound out to the end of its millisecond and Avro states no bounds at
     * all. Whatever the storage format, a row above the previous maximum must survive.
     */
    @ParameterizedTest
    @ValueSource(strings = {"PARQUET", "ORC", "AVRO"})
    public void testIncrementalColumnWatermarkNeverDropsRows(String storageFormat)
    {
        String source = ICEBERG_TEST_SCHEMA + ".format_src_" + randomNameSuffix();
        String mvName = "format_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp TIMESTAMP(6))");
        assertUpdate("INSERT INTO " + source + " VALUES (1, TIMESTAMP '2026-01-01 00:00:00.000000')", 1);
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp', format = '" + storageFormat + "') AS SELECT id, event_timestamp FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);

        // Strictly above the true maximum, but inside the millisecond an ORC upper bound is padded to.
        assertUpdate("INSERT INTO " + source + " VALUES (2, TIMESTAMP '2026-01-01 00:00:00.000500')", 1);
        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);

        assertThat(query("SELECT id FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES BIGINT '1', BIGINT '2'");
        assertStoredWatermarkValue(mvName, storageFormat.equals("PARQUET") ? Optional.of("1767225600000500") : Optional.empty());

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    private String storageTableName(String schema, String mvName)
    {
        MaterializedRow row = getOnlyElement(computeActual(
                "SELECT storage_catalog, storage_schema, storage_table FROM system.metadata.materialized_views " +
                        "WHERE catalog_name = '" + ICEBERG_CATALOG + "' AND schema_name = '" + schema + "' AND name = '" + mvName + "'")
                .getMaterializedRows());
        // The storage table name contains a literal '$', so only that part may be quoted — quoting
        // the whole catalog.schema.table string makes it a single identifier containing dots.
        return row.getField(0) + "." + row.getField(1) + ".\"" + row.getField(2) + "\"";
    }

    private void assertStoredWatermarkValue(String mvName, Optional<String> expectedValue)
    {
        assertThat(latestStoredWatermark(mvName).map(watermark -> parseStoredWatermark(watermark).value()))
                .isEqualTo(expectedValue);
    }

    private void assertWatermark(String mvName, String expectedValue)
    {
        assertThat(latestStoredWatermark(mvName))
                .hasValueSatisfying(watermark -> assertThat(parseStoredWatermark(watermark))
                        .isEqualTo(new StoredWatermark(2, TypeId.of("bigint"), expectedValue)));
    }

    private Optional<String> latestStoredWatermark(String mvName)
    {
        MaterializedRow row = getOnlyElement(computeActual(
                "SELECT element_at(summary, 'trino-incremental-column-watermark') FROM " + ICEBERG_TEST_SCHEMA + ".\"" + mvName + "$snapshots\" " +
                        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1")
                .getMaterializedRows());
        return Optional.ofNullable((String) row.getField(0));
    }

    private static int countOccurrences(String text, String token)
    {
        return (text.length() - text.replace(token, "").length()) / token.length();
    }

    public record IncrementalColumnTypeCase(
            String columnType,
            String belowCheckpoint,
            String initialLow,
            String initialHigh,
            String aboveCheckpointLow,
            String aboveCheckpointHigh,
            Optional<String> storedWatermarkAfterFirstRefresh,
            Optional<String> storedWatermarkAfterSecondRefresh)
    {
        // A refresh pushes down exactly when the refresh before it managed to store a watermark.
        boolean watermarkPushedDown()
        {
            return storedWatermarkAfterSecondRefresh().isPresent();
        }

        @Override
        public String toString()
        {
            return columnType;
        }
    }

    /**
     * Another refresh commits while this one writes, whether or not the storage table had a snapshot.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testIncrementalColumnRefreshRejectsInputAfterConcurrentRefresh(boolean refreshedBefore)
    {
        String source = ICEBERG_TEST_SCHEMA + ".concurrent_src_" + randomNameSuffix();
        String mvName = "concurrent_mv_" + randomNameSuffix();
        String qualifiedMvName = ICEBERG_TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, source_incremental_column BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 10)", 1);
        assertUpdate("CREATE MATERIALIZED VIEW " + qualifiedMvName + " WITH (incremental_column = 'source_incremental_column') AS SELECT id, source_incremental_column FROM " + source);
        if (refreshedBefore) {
            assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
            assertUpdate("INSERT INTO " + source + " VALUES (2, 20)", 1);
        }
        long refreshedId = refreshedBefore ? 2 : 1;

        IcebergMetadata metadata = getConnectorService(getQueryRunner(), IcebergMetadataFactory.class).create(SESSION.getIdentity());
        SchemaTableName storageTableName = metadata.getMaterializedView(SESSION, new SchemaTableName(ICEBERG_TEST_SCHEMA, mvName))
                .orElseThrow().getStorageTable().orElseThrow().getSchemaTableName();
        ConnectorTableHandle storageTable = metadata.getTableHandle(SESSION, storageTableName, Optional.empty(), Optional.empty());
        assertThat(((IcebergTableHandle) storageTable).getSnapshotId().isPresent()).isEqualTo(refreshedBefore);
        IcebergWritableTableHandle insertHandle = (IcebergWritableTableHandle) metadata.beginRefreshMaterializedView(
                SESSION, storageTable, List.of(), false, NO_RETRIES, INCREMENTAL_COLUMN);
        ConnectorPageSink pageSink = getConnectorService(getQueryRunner(), ConnectorPageSinkProvider.class).createPageSink(
                new HiveTransactionHandle(true), SESSION, (ConnectorInsertTableHandle) insertHandle, metadata.getTableCredentials(SESSION, insertHandle), () -> 0);
        try {
            pageSink.appendPage(new Page(createLongsBlock(refreshedId), createLongsBlock(refreshedId * 10))).join();
            Collection<Slice> fragments = pageSink.finish().join();

            assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);

            assertTrinoExceptionThrownBy(() -> metadata.finishRefreshMaterializedView(
                    SESSION, storageTable, insertHandle, fragments, List.of(), List.of(), false, false, false))
                    .hasErrorCode(ICEBERG_COMMIT_ERROR)
                    .hasMessageContaining("Materialized view storage table changed during incremental refresh");
            assertThat(query("SELECT count(*) FROM " + qualifiedMvName + " WHERE id = " + refreshedId)).matches("VALUES BIGINT '1'");
        }
        finally {
            pageSink.abort();
            metadata.rollback();
            assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
            assertUpdate("DROP TABLE " + source);
        }
    }
}
