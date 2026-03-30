/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.synapse;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.TestingColumnHandle;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.starburstdata.trino.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static com.starburstdata.trino.plugin.synapse.SynapseServer.TEST_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestSynapseTableStatistics
        extends BaseJdbcTableStatisticsTest
{
    private static final Percentage STAT_TOLERANCE_PERCENT = withinPercentage(80.0);
    private static final InstanceOfAssertFactory<Map, MapAssert<ColumnHandle, ColumnStatistics>> COLUMN_STATS_MAP =
            InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class);

    private static final int ERROR_STATISTICS_EXIST = 1927;
    private SynapseServer synapseServer;

    @BeforeAll
    @Override
    public void setUpTables()
    {
        // Prevent the base class from also trying to create tpch tables, since it's not synchronized with SynapseQueryRunner.
        // However, we still need to try gathering stats since some tests assume it's done on these tables.
        gatherStats(NATION.getTableName());
        gatherStats(REGION.getTableName());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(synapseServer, Map.of(), List.of(NATION, REGION));
    }

    @Override
    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed_" + randomNameSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", tableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('nationkey', null, null, null, null, null, null)," +
                            "('name', null, null, null, null, null, null)," +
                            "('regionkey', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 1000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testBasic()
    {
        String tableName = "test_stats_" + randomNameSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", tableName));
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    protected void checkEmptyTableStats(String tableName)
    {
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('orderkey', 0, 0, 1, null, null, null)," +
                        "('custkey', 0, 0, 1, null, null, null)," +
                        "('orderpriority', 0, 0, 1, null, null, null)," +
                        "('comment', 0, 0, 1, null, null, null)," +
                        // TODO: Empty tables should have total row count as 0 (https://starburstdata.atlassian.net/browse/SEP-5963)
                        "(null, null, null, null, 1, null, null)");
    }

    @Override
    @Test
    public void testAllNulls()
    {
        String tableName = "test_stats_table_all_nulls_" + randomNameSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation WHERE false", tableName));
        try {
            computeActual(format("INSERT INTO %s (nationkey) VALUES NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('nationkey', 0, 0, 1, null, null, null)," +
                            "('name', 0, 0, 1, null, null, null)," +
                            "('regionkey', 0, 0, 1, null, null, null)," +
                            "('comment', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 3, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testNullsFraction()
    {
        String tableName = "test_stats_table_with_nulls_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " AS " +
                        "SELECT " +
                        "    if(nationkey % 3 = 0, NULL, nationkey) nationkey, " +
                        "    regionkey " +
                        "FROM tpch.tiny.nation",
                25);
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(16, 0.36, Double.NaN))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testAverageColumnLength()
    {
        String tableName = "test_stats_table_avg_col_len_" + randomNameSuffix();
        computeActual("" +
                "CREATE TABLE " + tableName + " AS SELECT " +
                "  nationkey, " +
                "  'abc' v3_in_3, " +
                "  CAST('abc' AS varchar(42)) v3_in_42, " +
                "  if(nationkey = 1, '0123456789', NULL) single_10v_value, " +
                "  if(nationkey % 2 = 0, '0123456789', NULL) half_10v_value, " +
                "  if(nationkey % 2 = 0, CAST((1000000 - nationkey) * (1000000 - nationkey) AS varchar(20)), NULL) half_distinct_20v_value, " + // 12 chars each
                "  CAST(NULL AS varchar(10)) all_nulls " +
                "FROM tpch.tiny.nation");
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("v3_in_3"), statsCloseTo(1, 0, 150))
                    .hasEntrySatisfying(handle("v3_in_42"), statsCloseTo(1, 0, 150))
                    .hasEntrySatisfying(handle("single_10v_value"), statsCloseTo(1, 0.96, 20))
                    .hasEntrySatisfying(handle("half_10v_value"), statsCloseTo(1, 0.48, 259))
                    .hasEntrySatisfying(handle("half_distinct_20v_value"), statsCloseTo(13, 0.48, 314))
                    .hasEntrySatisfying(handle("all_nulls"), statsCloseTo(0, 1, 0));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testPartitionedTable()
    {
        String tableName = "test_stats_partitioned_table_" + randomNameSuffix();
        synapseServer.execute(format("CREATE TABLE %s WITH " +
                "(DISTRIBUTION = ROUND_ROBIN, " +
                "PARTITION (nationkey RANGE LEFT FOR VALUES (12))) " +
                "AS SELECT * FROM nation", tableName));
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testView()
    {
        String tableName = "test_stats_view_" + randomNameSuffix();
        synapseServer.execute("DROP VIEW IF EXISTS " + tableName);
        synapseServer.execute("CREATE VIEW " + tableName + " AS SELECT * FROM nation");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('nationkey', null, null, null, null, null, null)," +
                            "('name', null, null, null, null, null, null)," +
                            "('regionkey', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to ANALYZE a VIEW in Synapse
        }
        finally {
            synapseServer.execute("DROP VIEW IF EXISTS " + tableName);
        }
    }

    @Override
    @Test
    public void testMaterializedView()
    {
        abort("Synapse does not support statistics on materialized views");
    }

    @Override
    protected void testCaseColumnNames(String tableName)
    {
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        synapseServer.execute("" +
                "SELECT " +
                "  nationkey CASE_UNQUOTED_UPPER, " +
                "  name case_unquoted_lower, " +
                "  regionkey cASe_uNQuoTeD_miXED, " +
                "  comment \"CASE_QUOTED_UPPER\", " +
                "  nationkey \"case_quoted_lower\", " +
                "  name \"CasE_QuoTeD_miXED\" " +
                "INTO " + tableName + " " +
                "FROM nation");
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("case_unquoted_upper"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("case_unquoted_lower"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("case_unquoted_mixed"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("case_quoted_upper"), statsCloseTo(25, 0, 3713))
                    .hasEntrySatisfying(handle("case_quoted_lower"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("case_quoted_mixed"), statsCloseTo(25, 0, 353));
        }
        finally {
            synapseServer.execute("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testCaseColumnNames()
    {
        this.testCaseColumnNames(format("TEST_STATS_MIXED_UNQUOTED_UPPER_%s", randomNameSuffix()));
        this.testCaseColumnNames(format("test_stats_mixed_unquoted_lower_%s", randomNameSuffix()));
        this.testCaseColumnNames(format("test_stats_mixed_uNQuoTeD_miXED_%s", randomNameSuffix()));
        this.testCaseColumnNames(format("\"TEST_STATS_MIXED_QUOTED_UPPER_%s\"", randomNameSuffix()));
        this.testCaseColumnNames(format("\"test_stats_mixed_quoted_lower_%s\"", randomNameSuffix()));
        this.testCaseColumnNames(format("\"test_stats_mixed_QuoTeD_miXED_%s\"", randomNameSuffix()));
    }

    @Override
    @Test
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        .put("long_decimals_big_fraction decimal(38,37)", List.of("-1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"))
                        .put("long_decimals_middle decimal(38,16)", List.of("-1234567890123456.7890123456789012345678", "1234567890123456.7890123456789012345678"))
                        .put("long_decimals_big_integral decimal(38,1)", List.of("-1234567890123456789012345678901234567.8", "1234567890123456789012345678901234567.8"))
                        .buildOrThrow(),
                "null")) {
            gatherStats(table.getName());
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            "('large_doubles', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "(null, null, null, null, 2, null, null)");
        }
    }

    @Test
    public void testShowStatsAfterCreateIndex()
    {
        String tableName = "test_stats_create_index_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", tableName));

        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));

            // CREATE INDEX statement updates sys.partitions table
            synapseServer.execute(format("CREATE INDEX unique_index ON %s (nationkey)", tableName));

            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private ColumnHandle handle(String name)
    {
        return new TestingColumnHandle(name);
    }

    private Estimate asEstimate(Object value)
    {
        if (value == null) {
            return Estimate.unknown();
        }
        return Estimate.of((Double) value);
    }

    @Override
    protected void gatherStats(String tableName)
    {
        List<String> columnNames = stream(computeActual(format("SHOW COLUMNS FROM %s.%s", TEST_SCHEMA, tableName)))
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        for (Object columnName : columnNames) {
            synapseServer.executeIgnoringErrors(
                    format("CREATE STATISTICS %1$s ON %2$s.%3$s (%1$s)", columnName, TEST_SCHEMA, tableName),
                    ERROR_STATISTICS_EXIST);
        }
        synapseServer.execute(format("UPDATE STATISTICS %s.%s", TEST_SCHEMA, tableName));
    }

    private Optional<TableStatistics> showStats(String target)
    {
        return showStats(getSession(), target);
    }

    private Optional<TableStatistics> showStats(Session session, String target)
    {
        List<MaterializedRow> showStatsResult = computeActual(session, "SHOW STATS FOR " + target).getMaterializedRows();
        double rowCount = (double) showStatsResult.get(showStatsResult.size() - 1).getField(4);

        TableStatistics.Builder tableStatistics = TableStatistics.builder();
        tableStatistics.setRowCount(Estimate.of(rowCount));

        for (MaterializedRow materializedRow : showStatsResult) {
            if (materializedRow.getField(0) != null) {
                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setDataSize(asEstimate(materializedRow.getField(1)))
                        .setDistinctValuesCount(asEstimate(materializedRow.getField(2)))
                        .setNullsFraction(asEstimate(materializedRow.getField(3)))
                        .build();

                tableStatistics.setColumnStatistics(
                        handle(String.valueOf(materializedRow.getField(0))),
                        statistics);
            }
        }
        return Optional.of(tableStatistics.build());
    }

    private static Consumer<ColumnStatistics> statsCloseTo(double distinctValues, double nullsFraction)
    {
        return statsCloseTo(distinctValues, nullsFraction, OptionalDouble.empty());
    }

    private static Consumer<ColumnStatistics> statsCloseTo(double distinctValues, double nullsFraction, double dataSize)
    {
        return statsCloseTo(distinctValues, nullsFraction, OptionalDouble.of(dataSize));
    }

    private static Consumer<ColumnStatistics> statsCloseTo(double distinctValues, double nullsFraction, OptionalDouble dataSize)
    {
        return stats -> {
            SoftAssertions softly = new SoftAssertions();

            softly.assertThat(stats.getDistinctValuesCount().getValue())
                    .isCloseTo(distinctValues, STAT_TOLERANCE_PERCENT);

            softly.assertThat(stats.getNullsFraction().getValue())
                    .isCloseTo(nullsFraction, STAT_TOLERANCE_PERCENT);

            dataSize.ifPresent(size ->
                    softly.assertThat(stats.getDataSize().getValue())
                            .isCloseTo(size, STAT_TOLERANCE_PERCENT));

            softly.assertThat(stats.getRange()).isEmpty();
            softly.assertAll();
        };
    }

    @Override
    @Test
    public void testStatsWithPredicatePushdown()
    {
        String query = "SELECT * FROM nation WHERE regionkey = 1";

        assertThat(query(query)).isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(5, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("nationkey"), statsCloseTo(5, 0))
                .hasEntrySatisfying(handle("name"), statsCloseTo(5, 0))
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(1, 0))
                .hasEntrySatisfying(handle("comment"), statsCloseTo(5, 0));
    }

    @Override
    @Test
    public void testStatsWithVarcharPredicatePushdown()
    {
        Optional<TableStatistics> stats = showStats("(SELECT * FROM nation WHERE name = 'PERU')");

        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(1, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("nationkey"), statsCloseTo(1, 0))
                .hasEntrySatisfying(handle("name"), statsCloseTo(1, 0))
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(1, 0))
                .hasEntrySatisfying(handle("comment"), statsCloseTo(1, 0));

        try (TestTable table = newTrinoTable(
                "varchar_duplicates",
                " AS SELECT nationkey, chr(codepoint('A') + nationkey / 5) fl FROM tpch.tiny.nation")) {
            gatherStats(table.getName());

            stats = showStats("(SELECT * FROM " + table.getName() + " WHERE fl = 'B')");
            assertThat(stats)
                    .get()
                    .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                    .isCloseTo(5, STAT_TOLERANCE_PERCENT);
            assertThat(stats)
                    .get()
                    .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(5, 0))
                    .hasEntrySatisfying(handle("fl"), statsCloseTo(1, 0));
        }
    }

    /**
     * Verify that when {@value SystemSessionProperties#STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED} is disabled,
     * the connector still returns reasonable statistics.
     */
    @Override
    @Test
    public void testStatsWithPredicatePushdownWithStatsPrecalculationDisabled()
    {
        String query = "SELECT * FROM nation WHERE regionkey = 1";
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED, "false")
                .build();

        assertThat(query(session, query)).isFullyPushedDown();
        assertThat(showStats(session, "(" + query + ")"))
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0))
                .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0))
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0))
                .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0));
    }

    @Override
    @Test
    public void testStatsWithLimitPushdown()
    {
        String query = "SELECT regionkey, nationkey FROM nation LIMIT 2";

        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(2, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(2, 0))
                .hasEntrySatisfying(handle("nationkey"), statsCloseTo(2, 0));
    }

    @Override
    @Test
    public void testStatsWithTopNPushdown()
    {
        String query = "SELECT regionkey, nationkey FROM nation ORDER BY regionkey LIMIT 2";

        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(2, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(2, 0))
                .hasEntrySatisfying(handle("nationkey"), statsCloseTo(2, 0));
    }

    @Override
    @Test
    public void testStatsWithDistinctPushdown()
    {
        String query = "SELECT DISTINCT regionkey FROM nation";

        assertThat(query(query)).isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(5, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0));
    }

    @Override
    @Test
    public void testStatsWithDistinctLimitPushdown()
    {
        String query = "SELECT DISTINCT regionkey FROM nation LIMIT 3";

        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(3, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(3, 0));
    }

    @Override
    @Test
    public void testStatsWithAggregationPushdown()
    {
        String query = "SELECT regionkey, max(nationkey) max_nationkey, count(*) c FROM nation GROUP BY regionkey";

        assertThat(query(query)).isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(5, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0))
                .hasEntrySatisfying(handle("max_nationkey"), statsCloseTo(Double.NaN, Double.NaN))
                .hasEntrySatisfying(handle("c"), statsCloseTo(Double.NaN, Double.NaN));
    }

    @Override
    @Test
    public void testStatsWithSimpleJoinPushdown()
    {
        String query = "SELECT n.name n_name FROM nation n JOIN region r ON n.nationkey = r.regionkey";

        assertThat(query(query)).isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(5, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("n_name"), statsCloseTo(5, 0));
    }

    @Override
    @Test
    public void testStatsWithJoinPushdown()
    {
        String query = "SELECT r.regionkey regionkey, r.name r_name, n.name n_name FROM region r JOIN nation n ON r.regionkey = n.regionkey WHERE n.nationkey = 5";

        assertThat(query(query)).isFullyPushedDown();

        Optional<TableStatistics> stats = showStats("(" + query + ")");
        assertThat(stats)
                .get()
                .extracting(s -> s.getRowCount().getValue(), InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(1, STAT_TOLERANCE_PERCENT);
        assertThat(stats)
                .get()
                .extracting(TableStatistics::getColumnStatistics, COLUMN_STATS_MAP)
                .hasEntrySatisfying(handle("regionkey"), statsCloseTo(1, 0))
                .hasEntrySatisfying(handle("r_name"), statsCloseTo(1, 0))
                .hasEntrySatisfying(handle("n_name"), statsCloseTo(1, 0));
    }
}
