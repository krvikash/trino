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
package io.trino.plugin.warp.it.nativeengine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupPropertiesData;
import io.trino.plugin.warp.api.warmup.expression.TransformFunctionData;
import io.trino.plugin.warp.api.warmup.expression.WarpExpressionData;
import io.trino.plugin.warp.api.warmup.expression.WarpPrimitiveConstantData;
import io.trino.plugin.warp.di.WarpNativeStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.it.DispatcherAbstractTestQueryFramework;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.spi.QueryId;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.SoftAssertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs every function predicate the warp native engine accepts over edge-value rows and compares the
 * rows it returns with the rows Trino returns for the same predicate through the proxied connector.
 */
// The native library ships Linux builds only.
@EnabledOnOs(OS.LINUX)
public abstract class BaseWarpNativeFunctionPredicateTest
        extends DispatcherAbstractTestQueryFramework
{
    protected static final String CATALOG = "warp";
    protected static final String TABLE = "edge_values";
    protected static final int ROW_COUNT = 16;

    private static final Duration WARMUP_WAIT = Duration.ofMinutes(2);
    private static final String WARP_MATCH_COLUMNS = "warp_match_columns";
    private static final String WARP_COLLECT_COLUMNS = "warp_collect_columns";
    private static final String EXTERNAL_MATCH_COLUMNS = "external_match_columns";
    private static final String EXTERNAL_COLLECT_COLUMNS = "external_collect_columns";

    private static final List<String> TIMESTAMPS = ImmutableList.of(
            "1970-01-01 00:00:00.000000",
            "1969-12-31 23:59:59.999999",
            "1900-01-01 12:00:00.000000",
            "9999-12-31 23:59:59.999999",
            "2000-02-29 00:00:00.000001",
            "2004-12-31 23:59:59.999000",
            "2005-01-01 00:00:00.000000",
            "2008-12-29 13:00:00.000000",
            "2024-02-29 23:00:00.000000",
            "2023-01-01 01:30:00.000000",
            "2023-12-31 23:59:59.000000",
            "2020-01-01 00:00:00.000000",
            "1999-12-31 23:59:59.500000",
            "2021-03-14 15:09:26.000000",
            "2022-07-04 12:00:00.000000");

    private static final List<String> DATES = TIMESTAMPS.stream()
            .map(timestamp -> timestamp.substring(0, 10))
            .collect(toImmutableList());

    /**
     * A column of the test table with one SQL literal per non-null row; the last row of the table is null in every column except id.
     */
    record Column(String name, String type, List<String> values)
    {
        Column
        {
            values = ImmutableList.copyOf(values);
            int expected = ROW_COUNT - 1;
            if (name.equals("id")) {
                expected = ROW_COUNT;
            }
            if (values.size() != expected) {
                throw new IllegalArgumentException(format("column %s has %d values, expected %d", name, values.size(), expected));
            }
        }
    }

    protected abstract Map<String, String> proxiedConnectorConfig();

    protected abstract int timestampPrecision();

    protected abstract String tableProperties();

    protected List<Column> extraColumns()
    {
        return ImmutableList.of();
    }

    protected List<String> extraPredicates()
    {
        return ImmutableList.of();
    }

    protected void addFunctions(QueryRunner queryRunner) {}

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                new WarpNativeStorageEngineModule(true),
                Optional.empty(),
                1,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("http-server.log.enabled", "false")
                        .put(USE_HTTP_SERVER_PORT, "false")
                        .put("node.environment", "warp")
                        .putAll(proxiedConnectorConfig())
                        .buildOrThrow(),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                CATALOG,
                new WarpPlugin(),
                ImmutableMap.of());
        addFunctions(queryRunner);
        return queryRunner;
    }

    @BeforeAll
    public void createAndWarmTable()
            throws Exception
    {
        List<Column> columns = columns();
        String columnDefinitions = columns.stream()
                .map(column -> column.name() + " " + column.type())
                .collect(joining(", "));
        createSchemaAndTable(DEFAULT_SCHEMA, TABLE, "(" + columnDefinitions + ")" + tableProperties());
        String rows = IntStream.range(0, ROW_COUNT)
                .mapToObj(row -> columns.stream()
                        .map(column -> valueAt(column, row))
                        .collect(joining(", ", "(", ")")))
                .collect(joining(", "));
        assertUpdate(format("INSERT INTO %s.%s VALUES %s", DEFAULT_SCHEMA, TABLE, rows), ROW_COUNT);

        createWarmupRules(DEFAULT_SCHEMA, TABLE, warmupRules(columns));
        computeActual(format("SELECT * FROM %s.%s", DEFAULT_SCHEMA, TABLE));
        waitForWarmupToSettle();
    }

    @Test
    public void testNumericPredicates()
    {
        assertPredicates(ImmutableList.of(
                "d = 0.0",
                "d = -0.0",
                "d IN (0.0, 0.5)",
                "r = REAL '0.0'",
                "r = REAL '-0.0'",
                "is_nan(d)",
                "is_nan(r)",
                "ceiling(d) = 97",
                "ceiling(r) = 97",
                "ceiling(d) = 0",
                "ceiling(r) = 0",
                "ceiling(d) = -96",
                "ceiling(d) < 0",
                "ceiling(d) IN (-96, 0, 97)",
                "ceiling(d) = 1.0E300",
                "ceil(r) = 16777216",
                "CAST(d AS real) = REAL '96.5'",
                "CAST(d AS real) = REAL '0.0'",
                "CAST(d AS real) IN (REAL '42.4486', REAL '-0.5')",
                "CAST(r AS double) = 4.244860076904297E1",
                "CAST(r AS double) BETWEEN 0.0 AND 1.0",
                "CAST(i AS bigint) = 42",
                "CAST(i AS bigint) IN (-1, 2147483647)",
                "CAST(i AS bigint) < 0",
                "CAST(i AS double) = 42.0",
                "CAST(i AS real) = REAL '-2.14748365E9'",
                "CAST(b AS double) < 0.0",
                "CAST(b AS real) = REAL '9.223372E18'",
                "CAST(d AS varchar) = '9.65E1'",
                "CAST(d AS varchar) = '1.0E300'",
                "CAST(d AS varchar) = 'Infinity'",
                "CAST(d AS varchar) = '-Infinity'",
                // Warp collects a stored -0.0 as 0.0, so CAST(d AS varchar) = '-0E0' and '0E0' diverge from Trino.
                // Several reals share one varchar form, and warp inverts the literal to a single real, so CAST(r AS varchar) = '1.677722E7' diverges from Trino.
                "CAST(d AS varchar) = '4.9E-324'",
                "CAST(d AS varchar) IN ('5.0E-1', '9.7E1', '-9.65E1')",
                "CAST(r AS varchar) = '4.24486E1'",
                "CAST(i AS varchar) = '-2147483648'",
                "CAST(i AS varchar) IN ('0', '42')",
                "CAST(b AS varchar) = '9223372036854775807'",
                "CAST(b AS varchar) IN ('42', '-9223372036854775808')"));
    }

    @Test
    public void testDatePredicates()
    {
        assertPredicates(ImmutableList.of(
                "day(dt) = 29",
                "day(dt) IN (1, 31)",
                "day_of_month(dt) = 1",
                "day_of_week(dt) = 7",
                "day_of_week(dt) IN (1, 6)",
                "dow(dt) = 1",
                "day_of_year(dt) = 366",
                "day_of_year(dt) < 2",
                "doy(dt) = 1",
                "week(dt) = 53",
                "week(dt) = 1",
                "week_of_year(dt) IN (1, 52, 53)",
                "year_of_week(dt) = 2004",
                "yow(dt) = 2009",
                "CAST(vd AS date) = DATE '2024-02-29'",
                "CAST(vd AS date) IN (DATE '1969-12-31', DATE '9999-12-31')",
                "CAST(vd AS date) < DATE '1970-01-01'",
                "CAST(vd AS date) BETWEEN DATE '2023-01-01' AND DATE '2023-12-31'",
                "date(vd) = DATE '1900-01-01'"));
    }

    @Test
    public void testTimestampPredicates()
    {
        assertPredicates(ImmutableList.of(
                "day(ts) = 29",
                "day(ts) IN (1, 31)",
                "day_of_week(ts) = 7",
                "day_of_year(ts) = 366",
                "day_of_year(ts) = 1",
                "week(ts) = 53",
                "week(ts) = 1",
                "year_of_week(ts) = 2009",
                "year_of_week(ts) = 1970",
                "hour(ts) = 23",
                "hour(ts) = 0",
                "hour(ts) = 1",
                "hour(ts) = 12",
                "hour(ts) = 13",
                "hour(ts) = 15",
                "hour(ts) IN (1, 13)",
                "hour(ts) IN (0, 23)",
                "hour(ts) IN (0, 12)",
                "hour(ts) IN (0, 1, 12, 13, 23)",
                "hour(ts) < 2",
                "hour(ts) <= 1",
                "CAST(ts AS date) = DATE '2000-02-29'",
                "CAST(ts AS date) = DATE '1969-12-31'",
                "CAST(ts AS date) IN (DATE '1969-12-31', DATE '9999-12-31')",
                "CAST(ts AS date) < DATE '1970-01-01'",
                "CAST(ts AS date) BETWEEN DATE '2023-01-01' AND DATE '2023-12-31'",
                "date(ts) = DATE '1970-01-01'"));
    }

    @Test
    public void testVarcharPredicates()
    {
        assertPredicates(ImmutableList.of(
                "lower(v) = 'macbook pro'",
                "lower(v) = 'äpfel'",
                "lower(v) = 'straße'",
                "lower(v) = 'strasse'",
                "lower(v) IN ('a', 'äpfel')",
                "upper(v) = 'MACBOOK PRO'",
                "upper(v) = 'ÄPFEL'",
                "upper(v) = 'STRASSE'",
                "upper(v) = 'STRAßE'",
                "upper(v) IN ('A', 'MAC')",
                "v LIKE 'Mac%'",
                "v LIKE 'Ma_'",
                "starts_with(v, 'Mac')",
                "starts_with(v, 'mac')",
                "starts_with(v, '')",
                "json_extract_scalar(v, '$.a') = 'x'",
                "v = ''",
                "v IS NULL",
                "v IS NOT NULL",
                "v > 'a'",
                "v IN ('', 'Mac', 'straße')"));
    }

    @Test
    public void testExtraPredicates()
    {
        assertPredicates(extraPredicates());
    }

    protected List<Column> columns()
    {
        List<String> ids = IntStream.rangeClosed(1, ROW_COUNT)
                .mapToObj(Integer::toString)
                .collect(toImmutableList());
        return ImmutableList.<Column>builder()
                .add(new Column("id", "integer", ids))
                .add(new Column("i", "integer", ImmutableList.of(
                        "0", "1", "-1", "2147483647", "-2147483648", "42", "7", "8", "9", "10", "11", "12", "13", "14", "15")))
                .add(new Column("b", "bigint", ImmutableList.of(
                        "0", "1", "-1", "9223372036854775807", "-9223372036854775808", "42", "7", "8", "9", "10", "11", "12", "13", "14", "15")))
                .add(new Column("r", "real", ImmutableList.of(
                        "REAL '0.0'",
                        "REAL '-0.0'",
                        "REAL '0.5'",
                        "REAL '96.5'",
                        "REAL '97.0'",
                        "REAL '-96.5'",
                        "CAST(nan() AS real)",
                        "CAST(infinity() AS real)",
                        "CAST(-infinity() AS real)",
                        "REAL '16777216.0'",
                        "REAL '16777217.0'",
                        "REAL '42.4486'",
                        "REAL '-0.5'",
                        "REAL '1.5'",
                        "REAL '3.4028235E38'")))
                .add(new Column("d", "double", ImmutableList.of(
                        "DOUBLE '0.0'",
                        "DOUBLE '-0.0'",
                        "0.5E0",
                        "96.5E0",
                        "97.0E0",
                        "-96.5E0",
                        "nan()",
                        "infinity()",
                        "-infinity()",
                        "1.0E300",
                        "1.0E-300",
                        "42.4486E0",
                        "-0.5E0",
                        "4.9E-324",
                        "1.7976931348623157E308")))
                .add(new Column("v", "varchar", ImmutableList.of(
                        "''",
                        "'a'",
                        "'A'",
                        "'MacBook Pro'",
                        "'macbook pro'",
                        "'MACBOOK PRO'",
                        "'Äpfel'",
                        "'äpfel'",
                        "'straße'",
                        "'STRASSE'",
                        "'STRAßE'",
                        "' spaced '",
                        "'a,b,c'",
                        "'{\"a\":\"x\",\"n\":1}'",
                        "'Mac'")))
                .add(new Column("vd", "varchar", DATES.stream().map(date -> "'" + date + "'").collect(toImmutableList())))
                .add(new Column("dt", "date", DATES.stream().map(date -> "DATE '" + date + "'").collect(toImmutableList())))
                .add(new Column("ts", format("timestamp(%d)", timestampPrecision()), TIMESTAMPS.stream().map(this::timestampLiteral).collect(toImmutableList())))
                .addAll(extraColumns())
                .build();
    }

    protected String timestampLiteral(String timestamp)
    {
        int fractionStart = timestamp.indexOf('.');
        String truncated = timestamp.substring(0, fractionStart + 1 + timestampPrecision());
        return "TIMESTAMP '" + truncated + "'";
    }

    // The last row is NULL in every column except id.
    private static String valueAt(Column column, int row)
    {
        if (row == ROW_COUNT - 1 && !column.name().equals("id")) {
            return "CAST(NULL AS " + column.type() + ")";
        }
        return column.values().get(row);
    }

    // Every column gets data and basic index elements; varchar columns add a Lucene element for the string functions and transformed elements for lower, upper, date and json_extract_scalar.
    private static Map<String, Set<WarmupPropertiesData>> warmupRules(List<Column> columns)
    {
        WarmupPropertiesData basic = new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL);
        WarmupPropertiesData lucene = new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL);
        WarmupPropertiesData lower = transformed(new TransformFunctionData(TransformFunctionData.TransformType.LOWER));
        WarmupPropertiesData upper = transformed(new TransformFunctionData(TransformFunctionData.TransformType.UPPER));
        WarmupPropertiesData date = transformed(new TransformFunctionData(TransformFunctionData.TransformType.DATE));
        WarmupPropertiesData jsonExtractScalar = transformed(new TransformFunctionData(
                TransformFunctionData.TransformType.JSON_EXTRACT_SCALAR,
                ImmutableList.of(new WarpPrimitiveConstantData("$.a", WarpExpressionData.Type.VARCHAR))));
        Map<String, Set<WarmupPropertiesData>> rules = new HashMap<>();
        for (Column column : columns) {
            ImmutableSet.Builder<WarmupPropertiesData> elements = ImmutableSet.builder();
            elements.add(basic);
            if (column.type().startsWith("varchar")) {
                elements.add(lucene, lower, upper);
            }
            if (column.name().equals("v")) {
                elements.add(jsonExtractScalar);
            }
            if (column.name().equals("vd")) {
                elements.add(date);
            }
            rules.put(column.name(), elements.build());
        }
        return ImmutableMap.copyOf(rules);
    }

    private static WarmupPropertiesData transformed(TransformFunctionData transformFunction)
    {
        return new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL, transformFunction);
    }

    private void waitForWarmupToSettle()
    {
        String warmingStatsTable = "%s:catalog=%s,name=%s_%s_*,type=%s".formatted(
                WarmingServiceStats.class.getPackageName(),
                CATALOG,
                WarmingServiceStats.createKey(),
                CATALOG,
                WarmingServiceStats.class.getSimpleName().toLowerCase(Locale.ROOT));
        Session jmxSession = Session.builder(getSession())
                .setCatalog("jmx")
                .setSchema("current")
                .build();
        @Language("SQL") String statsQuery = format(
                "SELECT sum(warm_started), sum(warm_accomplished), sum(warm_failed) FROM \"*%s*\"",
                warmingStatsTable);
        long deadline = System.nanoTime() + WARMUP_WAIT.toNanos();
        while (true) {
            MaterializedRow stats = computeActual(jmxSession, statsQuery).getMaterializedRows().getFirst();
            long started = (Long) stats.getField(0);
            long accomplished = (Long) stats.getField(1);
            long failed = (Long) stats.getField(2);
            if (started > 0 && started == accomplished + failed) {
                assertThat(failed).describedAs("failed warmup elements").isZero();
                return;
            }
            if (System.nanoTime() > deadline) {
                // Zero scheduled tasks means the native engine rejected warming because its init failed; the log then shows the native error, such as a low fs.aio-max-nr.
                long scheduled = (Long) computeActual(jmxSession, format("SELECT sum(task_scheduled) FROM \"*catalog=%s,*type=workertaskexecutorservicestats*\"", CATALOG))
                        .getOnlyValue();
                throw new AssertionError(format("warmup did not settle: started=%d accomplished=%d failed=%d scheduled=%d", started, accomplished, failed, scheduled));
            }
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    protected void assertPredicates(List<String> predicates)
    {
        SoftAssertions softly = new SoftAssertions();
        for (String predicate : predicates) {
            assertPredicate(softly, predicate);
        }
        softly.assertAll();
    }

    private void assertPredicate(SoftAssertions softly, String predicate)
    {
        @Language("SQL") String query = format("SELECT id FROM %s.%s WHERE %s ORDER BY id", DEFAULT_SCHEMA, TABLE, predicate);

        List<Object> expected = computeActual(trinoSession(), query).getOnlyColumn().collect(toImmutableList());
        softly.assertThat(expected.size())
                .describedAs("Trino result for [%s] must select some rows but not all", predicate)
                .isBetween(1, ROW_COUNT - 1);

        QueryRunner.MaterializedResultWithPlan warpResult = getDistributedQueryRunner().executeWithPlan(warpSession(), query);
        List<Object> actual = warpResult.result().getOnlyColumn().collect(toImmutableList());
        Map<String, Long> metrics = DispatcherStubsIntegrationSmokeIT.getCustomMetrics(warpResult.queryId(), getDistributedQueryRunner());

        softly.assertThat(metric(metrics, WARP_MATCH_COLUMNS)).describedAs("warp did not match [%s]", predicate).isPositive();
        softly.assertThat(metric(metrics, WARP_COLLECT_COLUMNS)).describedAs("warp did not collect [%s]", predicate).isPositive();
        long externalMatch = metric(metrics, EXTERNAL_MATCH_COLUMNS);
        softly.assertThat(externalMatch).describedAs("proxied connector matched [%s] %s", predicate, columnMetrics(metrics)).isZero();
        if (externalMatch == 0) {
            // Trino re-applies the filter, so only the row count handed to the scan operator reveals rows warp matched wrongly.
            AbstractLongAssert<?> handedRows = softly.assertThat(scanInputRows(warpResult.queryId())).describedAs("rows warp handed to Trino for [%s] %s", predicate, columnMetrics(metrics));
            if (matchedByLucene(metrics)) {
                // Lucene matches on tokens, so it prefilters and may hand extra rows to Trino.
                handedRows.isGreaterThanOrEqualTo(expected.size());
            }
            else {
                handedRows.isEqualTo(expected.size());
            }
        }
        // Varchar columns are always collected by the proxied connector, every other column must be collected by warp.
        long externalCollect = metric(metrics, EXTERNAL_COLLECT_COLUMNS);
        long varcharExternalCollect = metrics.entrySet().stream()
                .filter(entry -> varcharExternalCollectKeys().contains(entry.getKey()))
                .mapToLong(Map.Entry::getValue)
                .sum();
        if (externalCollect != varcharExternalCollect) {
            String plan = (String) computeActual(warpSession(), "EXPLAIN " + query).getOnlyValue();
            softly.assertThat(externalCollect).describedAs("proxied connector collected [%s] %s%n%s", predicate, columnMetrics(metrics), plan).isEqualTo(varcharExternalCollect);
        }
        softly.assertThat(actual).describedAs("warp result for [%s]", predicate).isEqualTo(expected);
    }

    private static boolean matchedByLucene(Map<String, Long> metrics)
    {
        return metrics.entrySet().stream()
                .anyMatch(entry -> entry.getKey().startsWith("warp-match:") && entry.getKey().endsWith(":WARM_UP_TYPE_LUCENE") && entry.getValue() > 0);
    }

    private long scanInputRows(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(operator -> operator.getOperatorType().contains("Scan"))
                .mapToLong(OperatorStats::getInputPositions)
                .sum();
    }

    private static long metric(Map<String, Long> metrics, String name)
    {
        return metrics.getOrDefault(DispatcherPageSourceFactory.createFixedStatKey(DispatcherPageSourceStats.createKey(), name), 0L);
    }

    // Warp never collects varchar natively, so these columns are read through the proxied connector.
    private Set<String> varcharExternalCollectKeys()
    {
        return columns().stream()
                .filter(column -> column.type().startsWith("varchar"))
                .map(column -> "external-collect:" + column.name())
                .collect(toImmutableSet());
    }

    // Names the columns the match and collect counters were attributed to.
    private static Map<String, Long> columnMetrics(Map<String, Long> metrics)
    {
        return metrics.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .filter(entry -> entry.getKey().contains("match") || entry.getKey().contains("collect"))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // The proxied connector alone answers the query, so the result is Trino's own evaluation of the predicate.
    private Session trinoSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(CATALOG + "." + WarpSessionProperties.ENABLE_BYPASS, "true")
                .build();
    }

    // A zero skip threshold keeps the filtering gate from handing splits back to the proxied connector.
    private Session warpSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(CATALOG + "." + WarpSessionProperties.FILTERING_CHUNK_SKIP_THRESHOLD, "0.0")
                .build();
    }
}
