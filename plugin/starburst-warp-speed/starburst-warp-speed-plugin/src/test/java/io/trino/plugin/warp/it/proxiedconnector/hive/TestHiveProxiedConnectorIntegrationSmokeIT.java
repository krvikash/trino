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

package io.trino.plugin.warp.it.proxiedconnector.hive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.api.health.HealthResult;
import io.trino.plugin.warp.api.warmup.DateSlidingWindowWarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.PartitionValueWarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupColRuleData;
import io.trino.plugin.warp.api.warmup.WarmupColRuleUsageData;
import io.trino.plugin.warp.api.warmup.WarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.WarmupPropertiesData;
import io.trino.plugin.warp.api.warmup.WarmupRulesUsageData;
import io.trino.plugin.warp.api.warmup.column.RegularColumnData;
import io.trino.plugin.warp.api.warmup.column.WildcardColumnData;
import io.trino.plugin.warp.dictionary.DebugDictionaryMetadata;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.model.DictionaryInfo;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions;
import io.trino.plugin.warp.extension.execution.debugtools.FailureGeneratorResource;
import io.trino.plugin.warp.extension.execution.debugtools.PredicateCacheTask;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupCountResult;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask;
import io.trino.plugin.warp.extension.execution.debugtools.dictionary.DictionaryCountResult;
import io.trino.plugin.warp.extension.execution.debugtools.dictionary.DictionaryTask;
import io.trino.plugin.warp.extension.execution.health.ClusterHealthTask;
import io.trino.plugin.warp.extension.execution.health.HealthTask;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.gen.constants.FailureRepetitionMode;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.plugin.warp.juffer.PredicateBufferPoolType;
import io.trino.plugin.warp.storage.write.PageSink;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.HttpMethod;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.PREFILLED;
import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.createFixedStatKey;
import static io.trino.plugin.warp.dispatcher.model.DictionaryState.DICTIONARY_MAX_EXCEPTION;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static io.trino.plugin.warp.extension.execution.dump.RowGroupDataDumpTask.CACHED_ROW_GROUP;
import static io.trino.plugin.warp.extension.execution.dump.RowGroupDataDumpTask.CACHED_SHARED_ROW_GROUP;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestHiveProxiedConnectorIntegrationSmokeIT()
    {
        super(1, "hive");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(storageEngineModule,
                Optional.empty(),
                numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of());
    }

    @Test
    public void testSinglePartitionMultipleSplits()
    {
        String table = "pt";
        createTable(DEFAULT_SCHEMA,
                table,
                "(id integer, a varchar, date_date date) " +
                        "WITH (format='PARQUET', partitioned_by = ARRAY['date_date'])");
        IntStream.range(1, 9)
                .forEach(value -> assertUpdate(format("INSERT INTO %s(id, a, date_date) VALUES(%d, 'a-%d',CAST('2020-04-0%d' AS date))", table, value, value, value), 1));

        MaterializedResult materializedRows = computeActual("SELECT count(a) FROM %s WHERE date_date=CAST('2020-04-01' AS date)".formatted(table));
        assertThat(materializedRows.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
    }

    @Test
    public void testSimpleWithoutWarmReturnHive()
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        MaterializedResult materializedRows = computeActual(format("SELECT %s FROM t WHERE %s = 1", C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testCTAS()
    {
        computeActual("CREATE TABLE t2 AS SELECT * FROM t");
        computeActual("DROP TABLE t2");
    }

    @Test
    public void testWarmBooleanArray()
    {
        String table = "array_test";
        createTable(DEFAULT_SCHEMA, table, "(dummy ARRAY(BOOLEAN))");
        computeActual("INSERT INTO %s values (ARRAY [true, false, true, false])".formatted(table));
        Session session = buildSession(true, false);
        warmAndValidate("select * from %s".formatted(table),
                session,
                1,
                1,
                0);
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "warp_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("select * from %s where contains(dummy, false)".formatted(table),
                session,
                expectedJmxQueryStats);
    }

    @Test
    public void testWarmDateArray()
    {
        String table = "array_test";
        createTable(DEFAULT_SCHEMA, table, "(dummy ARRAY(DATE))");
        computeActual("INSERT INTO %s values (ARRAY [DATE '2022-02-02'])".formatted(table));
        Session session = buildSession(true, false);
        warmAndValidate("select * from %s".formatted(table),
                session,
                1,
                1,
                0);
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "warp_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("select * from %s where contains(dummy, CAST('2002-04-29' as date))".formatted(table),
                session,
                expectedJmxQueryStats);
    }

    @Test
    public void testWarmTimestampArray()
    {
        String table = "array_test";
        createTable(DEFAULT_SCHEMA, table, "(dummy ARRAY(TIMESTAMP))");
        computeActual("INSERT INTO %s values (ARRAY [current_timestamp])".formatted(table));
        Session session = buildSession(true, false);
        warmAndValidate("select * from %s".formatted(table),
                session,
                1,
                1,
                0);
    }

    @Test
    // TODO stuck in endless loop
    public void testRowDereference()
    {
        createTable(DEFAULT_SCHEMA, "evolve_test", "(dummy bigint, a row(b bigint, c varchar), d bigint)");
        computeActual("INSERT INTO evolve_test values (1, row(1, 'abc'), 1)");
        computeActual(getSession(), "select * from evolve_test where a[1] > 1");
    }

    @Test
    public void testUnsupportedArrayOperation()
    {
        String table = "t0";
        createTable(DEFAULT_SCHEMA, table, "(varcharColumn varchar, arrayColumn array(varchar))");
        computeActual("INSERT INTO %s values ('a', ARRAY ['1','2','3','4'])".formatted(table));
        warmAndValidate("SELECT arrayColumn from %s WHERE arrayColumn = ARRAY['a', 'b']".formatted(table),
                true,
                1,
                1); //only DATA
        warmAndValidate("SELECT * from %s WHERE arrayColumn = ARRAY['a', 'b'] OR varcharColumn='a'".formatted(table),
                true,
                1,
                1); //DATA for varcharColumn

        Map<String, Long> expectedQueryStats = Map.of(
                "warp_collect_columns", 1L,
                "warp_prefilled_collect_columns", 1L,
                "external_collect_columns", 0L,
                "warp_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("SELECT * from %s WHERE arrayColumn = ARRAY['a', 'b'] OR varcharColumn='a'".formatted(table),
                getSession(),
                expectedQueryStats);
        validateQueryStats("SELECT * from " + table + " WHERE contains(split('%, NULL', ', '), varcharColumn)", getSession(), expectedQueryStats);
    }

    /**
     * Trino doesn't pushdown element_at predicate for  map[key] = value predicate.
     * currently we don't support composite map expression
     */
    @Test
    public void testCompositeMap()
    {
        String table = "maps_table";
        createTable(DEFAULT_SCHEMA,
                table,
                "(int1 integer, map_column_integer map(integer, map(integer, integer))) " +
                        "WITH (format='PARQUET', partitioned_by = ARRAY[])");
        int rowCount = 2;
        @Language("SQL") String insertSql = "INSERT INTO %s VALUES ".formatted(table) +
                String.join(", ", IntStream.range(0, rowCount)
                        .mapToObj("(%1$d, MAP(ARRAY[%1$d], ARRAY[MAP(ARRAY[(2)], ARRAY[(3)])]))"::formatted).toList());
        assertUpdate(insertSql, rowCount);

        @Language("SQL") String query = "select int1 from %s where element_at(map_column_integer[1], 2) = 3".formatted(table);
        warmAndValidate(query, true, 1, 1);
        Map<String, Long> expectedQueryStats = Map.of(
                "warp_collect_columns", 1L,
                "external_collect_columns", 1L,
                "warp_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
        query = "select int1 from %s where element_at(element_at(map_column_integer,1),2) = 3".formatted(table);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    void testJsonExtractScalarFunction()
    {
        createTable(DEFAULT_SCHEMA,
                "json_test_table",
                "(varchar1 varchar, varchar2 varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual("INSERT INTO json_test_table " +
                "VALUES " +
                "  ( '{ \"first\" : \"John\" , \"middle\" : \"K\", \"last name\" : \"Doe\" }', '{ \"father\": \"John\", \"mother\": \"Mary\", \"children\": [ { \"age\": 12 }, { \"age\": 10 } ] }' ), " +
                "  ( null, '{ \"father\": \"Paul\", \"mother\": \"Laura\", \"children\": [{\"age\": 9},{\"age\": 3}] }' ), " +
                "  ( '{ \"people\": [ { \"name\":\"John Smith\" }, { \"name\":\"Sally Brown\" }, { \"name\":\"John Johnson\" } ] }', null ), " +
                "  ( '{ \"id\": \"1001\", \"type\": \"Regular\", \"flag\": true, \"value\": null }', '{ \"id\": \"5001\", \"type\": \"None\", \"number\": 12.345678 }' )");

        @Language("SQL") String query = "select * from json_test_table";
        warmAndValidate(query, true, 2, 1);

        // expression: Call[functionName=name='$equal', arguments=[Call[functionName=name='json_extract_scalar', arguments=[varchar1::varchar, Call[functionName=name='$cast', arguments=[Slice[hash=1773388407,length=13]::varchar(13)]]]], Slice[hash=-122647857,length=3]::varchar]]
        // Call must contain Variable, and we don't support 2 Variables in a single predicate (GenericRewriter.rewrite)
        query = "select count(*) from json_test_table where json_extract_scalar(varchar1, '$.\"last name\"') = 'Doe'";
        Map<String, Long> expectedQueryStats = Map.of(
                "warp_collect_columns", 1L,
                "external_collect_columns", 0L,
                "warp_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testTransformColumnDate()
    {
        createTable(DEFAULT_SCHEMA,
                "transform_data",
                "(var_date_col varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO transform_data VALUES ('2002-04-29')");
        Session warmSession = Session.builder(getSession()).build();
        warmAndValidate("select var_date_col from transform_data where var_date_col = 'a'", warmSession, 2, 1, 0);

        //now warm with transform column
        warmAndValidate("select var_date_col from transform_data where day_of_week(CAST(var_date_col as date)) = 2012", warmSession, 1, 1, 0);

        String baseQuery = "select var_date_col from transform_data where %s(CAST(var_date_col as date)) = 5";
        Map<String, Long> expectedQueryStats = Map.of(
                "transformed_column", 1L,
                "warp_collect_columns", 1L,
                "warp_match_columns", 1L,
                "external_match_columns", 0L);
        for (FunctionName dateFunction : SupportedFunctions.DATE_FUNCTIONS) {
            @Language("SQL") String dateFunctionQuery = format(baseQuery, dateFunction.getName());
            validateQueryStats(dateFunctionQuery, getSession(), expectedQueryStats);
        }
        baseQuery = "select var_date_col from transform_data where %s(date(var_date_col)) = 5";
        for (FunctionName dateFunction : SupportedFunctions.DATE_FUNCTIONS) {
            @Language("SQL") String dateFunctionQuery = format(baseQuery, dateFunction.getName());
            validateQueryStats(dateFunctionQuery, getSession(), expectedQueryStats);
        }
        List<String> queriesWithoutDateFunction = List.of(
                "select var_date_col from transform_data where date(var_date_col) = date('2002-04-29')",
                "select var_date_col from transform_data where CAST(var_date_col as date) = CAST('2002-04-29' as date)",
                "select var_date_col from transform_data where CAST(var_date_col as date) in (CAST('2002-04-29' as date), CAST('2002-05-29' as date))",
                "select var_date_col from transform_data where CAST(var_date_col as date) = CAST('2002-04-29' as date) or CAST(var_date_col as date) = CAST('2002-04-30' as date)");
        for (@Language("SQL") String query : queriesWithoutDateFunction) {
            validateQueryStats(query, getSession(), expectedQueryStats);
        }
    }

    @Test
    public void testTransformColumnDateInvalidSplit()
    {
        createTable(DEFAULT_SCHEMA,
                "transform_data",
                "(var_date_col varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO transform_data (var_date_col) VALUES ('2002-04-29'), ('2002-04-29bla')");
        Session warmSession = Session.builder(getSession()).build();
        warmAndValidate("select var_date_col from transform_data where var_date_col = 'a'", warmSession, 2, 1, 0);

        //now warm with transform column - expected to fail
        @Language("SQL") String query = "select var_date_col from transform_data where day_of_week(CAST(var_date_col as date)) = 2012";
        warmAndValidate(query, warmSession, 0, 2, 2);
        Map<String, Long> expectedQueryStats = Map.of(
                "transformed_column", 0L,
                "warp_collect_columns", 1L,
                "warp_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testNeverRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL),
                        new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 5, DEFAULT_TTL))));
        warmAndValidate(format("select %s from t", C2), true, 1, 1);

        RowGroupCountResult ret = getRowGroupCount();

        assertThat(ret.warmupColumnCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isFalse();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();

        cleanWarmupRules();

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL))));
        warmAndValidate(format("select %s from t", C2), true, "all_elements_warmed_or_skipped", 1);
        ret = getRowGroupCount();
        assertThat(ret.warmupColumnCount().size()).isEqualTo(1);
    }

    @Test
    public void testWarmDefaultWithLuceneRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select int1, v1 from t where v1 <> 'shlo' and int1 = 1", true, 4, 2);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isFalse();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_LUCENE))).isTrue();
        Session session = Session.builder(getSession()).build();
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "warp_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats("select count(*) from t where v1 not like '%mishlomi%'", session, expectedJmxQueryStats);
    }

    @Test
    public void testWarmDefaultLuceneRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        warmAndValidate("select int1, v1 from t where v1 <> 'shlo' and int1 = 1", true, 4, 1);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).doesNotContain(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_LUCENE));

        //another basic for v1
        warmAndValidate("select int1, v1 from t where v1 = 'shlomi' and int1 = 1", true, 0, 0);
        //another basic for v1
        warmAndValidate("select int1, v1 from t where v1 > 's' and int1 = 1", true, 0, 0);

        //lucene for v1
        warmAndValidate("select int1, v1 from t where v1 like '%shlo%' and int1 = 1", true, 1, 1);
        ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_LUCENE));

        //lucene already exists so nothing added
        warmAndValidate("select int1, v1 from t where starts_with(v1, 's') and int1 = 1", true, 0, 0);
    }

    @Test
    public void testWarmDefaultBasicAfterData()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        warmAndValidate("select int1 from t", true, 1, 1);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.warmupColumnCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().stream().filter((name) -> name.contains(WarmUpType.WARM_UP_TYPE_DATA.name())).findFirst()).isNotEmpty();
        warmAndValidate("select int1 from t where int1 <> 5", true, 1, 1);
        ret = getRowGroupCount();
        assertThat(ret.warmupColumnCount().size()).isEqualTo(2);
        assertThat(ret.warmupColumnNames().stream().filter((name) -> name.contains(WarmUpType.WARM_UP_TYPE_BASIC.name())).findFirst()).isNotEmpty();
    }

    @Test
    public void testWarmDefault()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select int1, v1 from t where v1 ='shlomi' and int1 = 1", true, 3, 2);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isFalse();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();

        WarmupRulesUsageData warmupRulesUsageData = objectMapper.readerFor(new TypeReference<WarmupRulesUsageData>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_GET_USAGE, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));

        assertThat(warmupRulesUsageData.warmupColRuleUsageDataList().size()).isEqualTo(2);
        assertThat(warmupRulesUsageData.warmupDefaultRuleUsageDataList().size()).isEqualTo(1);

        demoteAll();
        validateDemoteAll();
        cleanWarmupRules();
        warmAndValidate("select v1 from t where int1 = 1", true, 3, 1);
        ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isEqualTo(true);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();

        demoteAll();
        validateDemoteAll();
        cleanWarmupRules();
        Session session = buildSession(true, true);
        warmAndValidate("select * from t", session, 4, 1, 0);
        ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
    }

    private void validateDemoteAll()
            throws IOException
    {
        String result = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        List<Object> cachedRowGroupRes = objectMapper.readerFor(new TypeReference<List<Object>>() {})
                .readValue(result);
        assertThat(cachedRowGroupRes).isEmpty();
        RowGroupCountResult rowGroupCountResult = getRowGroupCount();
        assertThat(rowGroupCountResult.warmupColumnCount()).isEmpty();
    }

    @Test
    public void testWarmupAPI()
            throws IOException
    {
        ImmutableSet<WarmupPredicateRule> predicates = ImmutableSet.of(new PartitionValueWarmupPredicateRule(C1, "1"), new PartitionValueWarmupPredicateRule(C2, "shlomi"));
        ImmutableSet<WarmupPredicateRule> predicates2 = ImmutableSet.of(new PartitionValueWarmupPredicateRule(C1, "2"), new PartitionValueWarmupPredicateRule(C2, "shlomi2"));
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                predicates);
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                predicates2);
        assertThat(getWarmupRules().size()).isEqualTo(2);
    }

    @Test
    public void testWarmPredicatePushDownVarchar()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select v1 from t", false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), "select v1 from t where v1 = 'shlomishlomishlomi'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
    }

    @Test
    public void testVarcharMaxFail()
            throws IOException
    {
        int varcharMaxLen = storageEngineModule.getStorageEngineConstants().getVarcharMaxLen();

        createTable(DEFAULT_SCHEMA, "varchar_max_table", "(varchar_max varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), format("INSERT INTO varchar_max_table VALUES ('%s')", "varcharMaxLen"));
        computeActual(getSession(), format("INSERT INTO varchar_max_table VALUES ('%s')", "varcharMaxLen" + StringUtils.randomAlphanumeric(varcharMaxLen)));

        createWarmupRules(DEFAULT_SCHEMA,
                "varchar_max_table",
                Map.of("varchar_max",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from varchar_max_table",
                getSession(),
                3,
                3,
                Optional.of(2));

        @Language("SQL") String query = "select * from varchar_max_table";
        Map<String, Long> expectedQueryStats = Map.of(
                WARP_COLLECT_COLUMNS_STAT, 1L,
                EXTERNAL_COLLECT_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats, OptionalInt.empty(), OptionalInt.of(1));
    }

    @Test
    public void testUTF8CharDataFail()
            throws IOException
    {
        createTable(DEFAULT_SCHEMA, "char_128_table", "(cchar_128 char(1)) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        //each insert cmd is a single parquet file
        computeActual(getSession(), "INSERT INTO char_128_table (cchar_128) VALUES ('G'), ('É')");

        createWarmupRules(DEFAULT_SCHEMA,
                "char_128_table",
                Map.of("cchar_128",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select cchar_128 from char_128_table",
                Session.builder(getSession()).build(),
                0,
                2,
                2);

        MaterializedResult materializedRows = computeActual(getSession(), "select cchar_128 from char_128_table where cchar_128 is not null");
        // fetch from proxy since warmup failed
        assertThat(materializedRows.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testWarmWithAlias()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi'), (2, 'shlomi2')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s as a_int1, %s as a_v1 from t", C1, C2), false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), format("select %s from t where %s = 'shlomi' or %s = 1", C2, C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
    }

    @Test
    public void testWarmAllNullsColumns()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, NULL)");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s as a_int1, %s as a_v1 from t", C1, C2), false, 2, 1);

        @Language("SQL") String query = format("select %s from t where %s is NULL", C2, C2);
        Map<String, Long> expectedQueryStats = Map.of(
                "warp_prefilled_collect_columns", 1L,
                "warp_match_columns", 0L,
                "warp_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testIncrementalWarm()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s from t", C1), false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), format("select %s from t where %s = 1", C1, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
        //second column
        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s from t", C2), false, 2, 1);

        materializedRows = computeActual(getSession(), format("select %s,%s from t where %s = 'shlomi' or %s = 1", C1, C2, C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
    }

    @Test
    public void testSimpleAnalyzeWithColumns()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        computeActual(getSession(), "ANALYZE t WITH (columns = ARRAY['int1'])");
        assertQuery("SHOW STATS FOR t",
                "SELECT * FROM VALUES " +
                        "('int1',  null,    1,    0, null, 1, 1), " +
                        "('v1',  6,    1,    0, null, null, null), " +
                        "(null,  null,    null,   null,    1, null, null)");
    }

    @Test
    public void testSimpleAllPartitionQuery()
    {
        createTable(DEFAULT_SCHEMA, "pt", "(id integer, a varchar, b varchar, ds varchar) WITH (format='PARQUET', partitioned_by = ARRAY['ds'])");
        assertUpdate("INSERT INTO pt(id,a,ds) VALUES(1, 'a1','a1')", 1);
        assertUpdate("INSERT INTO pt(id,a,ds) VALUES(2, 'b1','b1')", 1);
        warmAndValidate("select id, ds from pt", true, 4, 2);

        @Language("SQL") String query = "SELECT COUNT(ds) FROM pt where ds = 'b1'";
        List<String> expectedPositiveQueryStats = List.of(createFixedStatKey(PREFILLED, "ds"));
        validateQueryStats(query, getSession(), Collections.emptyMap(), expectedPositiveQueryStats);

        query = "SELECT COUNT(*) FROM pt where ds = 'b1'";
        Map<String, Long> expectedQueryStats = Map.of("empty_collect_columns", 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testWarmupApi()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();

        WarmupColRuleData warmupColRuleDataLucene = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                0,
                Duration.ofSeconds(0),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule(C2, "2"),
                        new DateSlidingWindowWarmupPredicateRule(C2, 30, "DATE_FORMAT", "")));

        WarmupColRuleData warmupColRuleDataData = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_DATA,
                0,
                Duration.ofSeconds(0),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule(C2, "2"),
                        new DateSlidingWindowWarmupPredicateRule(C2, 30, "DATE_FORMAT", "")));

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleDataLucene, warmupColRuleDataData), HttpMethod.POST, HttpURLConnection.HTTP_OK);

        result = objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupRuleService.TASK_NAME_GET, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));

        assertThat(result).hasSize(2);

        WarmupColRuleData warmupColRuleDataResult = result.stream().findFirst().orElse(null);
        assertThat(warmupColRuleDataResult.getId()).isNotEqualTo(0);
        assertThat(warmupColRuleDataResult.getColumn()).isEqualTo(warmupColRuleDataLucene.getColumn());
        assertThat(warmupColRuleDataResult.getPredicates()).isEqualTo(warmupColRuleDataLucene.getPredicates());

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_DELETE, result.stream().map(WarmupColRuleData::getId).collect(toList()), HttpMethod.DELETE, HttpURLConnection.HTTP_NO_CONTENT);

        result = getWarmupRules();

        assertThat(result).isEmpty();
    }

    @SuppressWarnings("LanguageMismatch")
    @Test
    public void testLuceneQueryPushDown()
    {
        // We're just checking the pushdown - warmup is not required
        String prefixLikePattern = "prefix%";
        String suffixLikePattern = "%suffix";
        String query = format("SELECT %1$s FROM t WHERE %1$s LIKE '%2$s' AND %1$s LIKE '%3$s'", C2, prefixLikePattern, suffixLikePattern);
        DispatcherTableHandle table = executeWithTableHandle(getSession(), query);

        // Validate the translation to WarpExpression
        io.trino.plugin.warp.expression.rewrite.WarpExpression warpExpression = table.getWarpExpression().orElseThrow();
        assertThat(warpExpression.warpExpressionDataLeaves().size()).isEqualTo(2);

        HiveColumnHandle hiveColumnHandle1 = validateLikeExpression(warpExpression.rootExpression().getChildren().get(0), prefixLikePattern);
        HiveColumnHandle hiveColumnHandle2 = validateLikeExpression(warpExpression.rootExpression().getChildren().get(1), suffixLikePattern);
        assertThat(hiveColumnHandle1).isEqualTo(hiveColumnHandle2);

        // Validate the translation to Domain (done by Trino)
        Type expectedColumnType = VarcharType.createVarcharType(20);
        Range range = Range.range(expectedColumnType, Slices.utf8Slice("prefix"), true, Slices.utf8Slice("prefiy"), false);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(expectedColumnType, List.of(range));
        Domain domain = Domain.create(sortedRangeSet, false);
        assertThat(table.getFullPredicate().getDomains()).isEqualTo(Optional.of(Map.of(hiveColumnHandle1, domain)));

        // Validate that we go to Native
        MaterializedResult result = computeActual(getSession(), query);
        assertThat(result.getRowCount()).isEqualTo(0); //return 0 because we don't have native
    }

    private HiveColumnHandle validateLikeExpression(WarpExpression likeExpression, String likePattern)
    {
        assertThat(likeExpression).isInstanceOf(WarpCall.class);
        WarpCall likeCall = (WarpCall) likeExpression;
        assertThat(likeCall.getFunctionName()).isEqualTo(LIKE_FUNCTION_NAME.getName());
        assertThat(likeCall.getArguments().size()).isEqualTo(2);
        assertThat(likeCall.getArguments().get(0)).isInstanceOf(WarpVariable.class);
        assertThat(likeCall.getArguments().get(1)).isInstanceOf(WarpConstant.class);
        WarpVariable warpVariable = (WarpVariable) likeCall.getArguments().get(0);
        WarpConstant likeConstant = (WarpConstant) likeCall.getArguments().get(1);
        HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) warpVariable.getColumnHandle();
        assertThat(hiveColumnHandle.getBaseColumnName()).isEqualTo(C2);
        assertThat(likeConstant).isEqualTo(new WarpSliceConstant(Slices.utf8Slice(likePattern), VarcharType.createVarcharType(likePattern.length())));
        return hiveColumnHandle;
    }

    @Test
    public void testMultiNot()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select int1 from t", false, 2, 1);

        @Language("SQL") String query = "SELECT int1 FROM t WHERE int1 != 3";
        Map<String, Long> expectedQueryStats = Map.of(WARP_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate(format("select %s from t", C2), false, 2, 1);

        query = format("SELECT %s FROM t WHERE %s NOT IN ('shlomi', 'alfasi', 'aaa')", C2, C2);
        expectedQueryStats = Map.of(WARP_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = format("SELECT %s FROM t WHERE %s <> 'shlomi'", C2, C2);
        expectedQueryStats = Map.of(WARP_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testBasicLuceneDataQueryPushDown()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select int1,v1 from t", false, 4, 1);

        MaterializedResult materializedRows = computeActual(getSession(), "select v1 from t where v1 = 'shlomishlomishlomi'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        materializedRows = computeActual(getSession(), "select v1 from t where v1 like '%mishlomi%'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native

        materializedRows = computeActual(getSession(), "select v1 from t where v1 = 'shlomishlomishlomi' AND v1 like '%mishlomi%'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native
    }

    @Test
    public void testExport()
            throws IOException
    {
        final String schemaName = "varada";
        final String tableName = "ext";
        createSchemaAndTable(
                schemaName,
                tableName,
                "(c1 varchar, c2 varchar, c3 varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), format("INSERT INTO %s.%s VALUES ('import', 'export', 'test')", schemaName, tableName));
        createWarmupRules(schemaName,
                tableName,
                Map.of("c1",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "c2",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + ".enable_import_export", "true")
                .build();
        int expectedWarmupElements = 5;
        warmAndValidateWithExport(format("select count(c1), count(c2) from %s.%s", schemaName, tableName),
                session,
                expectedWarmupElements,
                1,
                1);
    }

    @Test
    public void testWarmUnsupportedColTypes()
    {
        createTable("schema",
                "test_table",
                "(intCol integer, rowCol ROW(latitudedeg varchar, longitudedeg double), " +
                        "mapCol MAP(varchar(3), integer), " +
                        "arrayCol ARRAY(integer))");
        computeActual("INSERT INTO schema.test_table select " +
                "7, " +
                "CAST(ROW('x', 4.5) AS ROW(latitudedeg varchar, longitudedeg double)), " +
                "MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]), " +
                "ARRAY[1]");

        Session session = Session.builder(getSession()).build();
        warmAndValidate("select intCol, rowCol, mapCol, arrayCol from schema.test_table where rowCol.latitudedeg = 'x' and rowCol.longitudedeg > 2", session, 6, 1, 0);
        computeActual("select count(rowCol.latitudedeg), count(rowCol.longitudedeg) from schema.test_table where rowCol.latitudedeg = 'x'");
    }

    @Test
    public void testWarmInternalRowFields()
            throws IOException
    {
        createTable("schema", "test_table",
                "(rowCol ROW(latitudedeg varchar, longitudedeg double))");
        computeActual("INSERT INTO schema.test_table select " +
                "CAST(ROW('x', 4.5) AS ROW(latitudedeg varchar, longitudedeg double))");

        createWarmupRules(DEFAULT_SCHEMA,
                "test_table",
                Map.of("rowCol#latitudedeg", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "rowCol#longitudedeg", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        Session session = buildSession(false, false);
        warmAndValidate("select rowCol.latitudedeg, rowCol.longitudedeg from schema.test_table", session, 3, 1, 0);

        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.warmupColumnCount().size()).isEqualTo(3);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#latitudedeg", WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#latitudedeg", WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#longitudedeg", WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#longitudedeg", WarmUpType.WARM_UP_TYPE_BASIC))).isFalse();

        MaterializedResult materializedRows = computeActual(session, "select rowCol.latitudedeg, rowCol.longitudedeg from schema.test_table where rowCol.latitudedeg = 'x'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
    }

    @Test
    public void testNeverRuleShouldNotWarmUnSupportColumnType()
            throws IOException
    {
        createTable(DEFAULT_SCHEMA, "bbb", "(a ARRAY(ROW(b integer, c varchar)), d varchar) WITH (format='PARQUET', partitioned_by = ARRAY['d'])");
        computeActual("INSERT INTO bbb values (array[row(1, 'abc')], 'a')");
        Map<String, Set<WarmupPropertiesData>> rules = new HashMap<>();
        rules.put("a", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, NEVER_PRIORITY, DEFAULT_TTL)));

        createWarmupRules(DEFAULT_SCHEMA, "bbb", rules);

        Session session1 = buildSession(true, false);
        warmAndValidate("select * from bbb", session1, 1, 1, 0);

        RowGroupCountResult result = getRowGroupCount();
        assertThat(result.nodesWarmupElementsCount().size()).isEqualTo(1);

        String str = executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_COUNT_WITH_FILES_TASK_NAME, null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        result = objectMapper.readerFor(RowGroupCountResult.class).readValue(str);
        assertThat(result.warmupColumnNames()).containsExactly("schema.bbb.d.WARM_UP_TYPE_DATA");
    }

    /**
     * 2 splits with same values, will create and export dictionary only once
     */
    @Test
    public void testExportedDictionaries()
    {
        String tableName = "dictionary_exporter_test";
        createTable(DEFAULT_SCHEMA, tableName, "(int1 bigint)");
        computeActual(getSession(), "INSERT INTO %s VALUES (1)".formatted(tableName)); //split1
        computeActual(getSession(), "INSERT INTO %s VALUES (1)".formatted(tableName)); //split2

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + ".enable_import_export", "true")
                .build();
        int createdDictionaries = 1; //same value for all splits
        int expectedExportRowGroupsAccomplished = 2;
        int expectedWarmupElements = 2; // 2 rowGroups, each hold single column
        int expectedWarmFinished = 2;
        Session jmxSession = createJmxSession();
        MaterializedResult dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        long beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        long beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long expectedReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
        warmAndValidateWithExport("select * from %s".formatted(tableName),
                session,
                expectedWarmupElements,
                expectedWarmFinished,
                expectedExportRowGroupsAccomplished);
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount, beforeWriteDictionaryCount + createdDictionaries, expectedReadDictionaryCount);
    }

    @Test
    public void testDictionary()
            throws IOException
    {
        String tableName = "dictionary_test_1";
        createTable(DEFAULT_SCHEMA,
                tableName,
                "(bingint1 bigint, var1 varchar, char1 char(5), int1 integer, shortdecimal decimal(2,1))");

        int rowCount = 2;
        @Language("SQL") String insertSql = "INSERT INTO %s VALUES ".formatted(tableName) +
                String.join(", ", IntStream.range(0, rowCount)
                        .mapToObj("(%1$d, 'a%1$d', 'b%1$d', %1$d,  %1$d)"::formatted).toList());
        assertUpdate(insertSql, rowCount);

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + "enable_import_export", "true")
                .build();
        int createdDictionaries = 5;
        Session jmxSession = createJmxSession();
        MaterializedResult dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        long beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        long beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long expectedReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
//        int beforeWarmDictionaryUsage = getDictionariesUsage();
        warmAndValidateWithExport("select * from %s".formatted(tableName),
                session,
                createdDictionaries,
                1,
                1);
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount, beforeWriteDictionaryCount + createdDictionaries, expectedReadDictionaryCount);
//        int dictionariesUsedPages = getDictionariesUsage();
//        int expectedUsedPages = 5; //each dictionary entry is a page (mock)
//        assertThat(dictionariesUsedPages - beforeWarmDictionaryUsage).isEqualTo(expectedUsedPages);
        String executeRestCommand = executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_COUNT_AGGREGATED_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        DictionaryCountResult dictionaryCountAggregatedResult = objectMapper.readerFor(new TypeReference<DictionaryCountResult>() {})
                .readValue(executeRestCommand);
        List<DebugDictionaryMetadata> dictionaryResult = dictionaryCountAggregatedResult
                .getWorkerDictionaryResultsList()
                .getFirst()
                .getDictionaryMetadataList()
                .stream()
                .filter(x -> x.dictionaryKey().schemaTableName().getTableName().equalsIgnoreCase(tableName))
                .collect(toList());

        assertThat(dictionaryCountAggregatedResult.getWorkerDictionaryResultsList().size()).isEqualTo(1);
        assertThat(dictionaryResult.size()).isEqualTo(createdDictionaries);
        assertThat(dictionaryResult.stream().allMatch(x -> x.dictionarySize() == 2)).isTrue();
        assertThat(dictionaryResult.stream().allMatch(x -> x.failedWriteCount() == 0)).isTrue();
        assertThat(dictionaryCountAggregatedResult.getWorkerDictionaryResultsList().getFirst().getNodeIdentifier()).isNull();
        executeRestCommand = executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_COUNT_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        DictionaryCountResult dictionaryCountResult = objectMapper.readerFor(new TypeReference<DictionaryCountResult>() {})
                .readValue(executeRestCommand);
        dictionaryResult = dictionaryCountResult
                .getWorkerDictionaryResultsList()
                .getFirst()
                .getDictionaryMetadataList()
                .stream()
                .filter(x -> x.dictionaryKey().schemaTableName().getTableName().equalsIgnoreCase(tableName))
                .toList();

        assertThat(dictionaryResult.size()).isEqualTo(createdDictionaries);
        assertThat(dictionaryResult.stream().allMatch(x -> x.dictionarySize() == 2)).isTrue();
        assertThat(dictionaryResult.stream().allMatch(x -> x.failedWriteCount() == 0)).isTrue();
        assertThat(dictionaryCountResult.getWorkerDictionaryResultsList().getFirst().getNodeIdentifier()).isNotNull();

        // expect one cached row group with 5 elements and zero failed warm up elements
        validateWarmupElementsDictionaryId(createdDictionaries, 1, 0, tableName);

        jmxSession = createJmxSession();
        dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long beforeReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
        MaterializedResult materializedRows = computeActual(getSession(), "select * from %s".formatted(tableName));
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount, beforeWriteDictionaryCount, beforeReadDictionaryCount + createdDictionaries);
        assertThat(materializedRows.getRowCount()).isEqualTo(0);
    }

    /**
     * the api define on workers only so run on single.
     */
    @Test
    public void testPredicateCache()
            throws IOException
    {
        String str = executeRestCommand(PredicateCacheTask.PREDICATES_DUMP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        PredicateCacheTask.PredicateCacheDump result = objectMapper.readerFor(PredicateCacheTask.PredicateCacheDump.class).readValue(str);
        assertThat(result.bufferPoolDumpMap().size()).isEqualTo(PredicateBufferPoolType.values().length);
    }

    @Test
    public void testSharedRowGroups()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");
        warmAndValidate("SELECT * FROM T", true, "warm_finished", 1);

        Failsafe.with(RetryPolicy.builder()
                        .handle(AssertionError.class)
                        .withMaxRetries(5)
                        .withDelay(Duration.ofMillis(100))
                        .withMaxDuration(Duration.ofMillis(1000))
                        .build())
                .run(() -> {
                    String cachedSharedRowGroupsStr = executeRestCommand(CACHED_SHARED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
                    List<Object> cachedSharedRowGroupRes = objectMapper.readerFor(new TypeReference<List<Object>>() {})
                            .readValue(cachedSharedRowGroupsStr);
                    assertThat(cachedSharedRowGroupRes.size()).isEqualTo(1);
                    assertThat(((LinkedHashMap<?, ?>) ((LinkedHashMap<?, ?>) cachedSharedRowGroupRes.getFirst()).get("key")).get("schema_name")).isEqualTo(DEFAULT_SCHEMA);
                });
    }

    @Test
    public void testFailuresGenerator()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        List<FailureGeneratorResource.FailureGeneratorData> failureGeneratorDataList =
                List.of(new FailureGeneratorResource.FailureGeneratorData(
                        PageSink.class.getName(),
                        "appendPage",
                        FailureRepetitionMode.REP_MODE_ALWAYS,
                        FailureGeneratorInvocationHandler.FailureType.JAVA_EXCEPTION,
                        1));
//        FailureGeneratorResource.FailureGeneratorData data = new FailureGeneratorResource.FailureGeneratorData(StorageEngine.class.getName(), "txInsertCreate", FailureRepetitionMode.REP_MODE_ONCE, FailureGeneratorInvocationHandler.FailureType.JAVA_EXCEPTION, 1);
//        failureGeneratorDataList.add(data);
        executeRestCommand(FailureGeneratorResource.TASK_NAME, "", failureGeneratorDataList, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
        warmAndValidate("SELECT * FROM T", true, "warm_accomplished", 1);

        Failsafe.with(RetryPolicy.builder()
                        .handle(AssertionError.class)
                        .withMaxRetries(5)
                        .withDelay(Duration.ofSeconds(1))
                        .withMaxDuration(Duration.ofSeconds(10))
                        .build())
                .run(() -> {
                    String cachedRowGroupsStr = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
                    List<RowGroupData> cachedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {}).readValue(cachedRowGroupsStr);
                    assertThat(cachedRowGroupRes.size()).isEqualTo(1);
                    RowGroupData rowGroup = cachedRowGroupRes.getFirst();
                    Collection<WarmUpElement> warmupElements = rowGroup.getWarmUpElements();
                    WarmUpElementState state = warmupElements.iterator().next().getState();
                    assertThat((Integer) state.temporaryFailureCount()).isGreaterThan(0);
                });
        executeRestCommand(FailureGeneratorResource.TASK_NAME, "", Collections.emptyList(), HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
    }

    @Test
    public void testPrefill()
            throws IOException
    {
        createTable(DEFAULT_SCHEMA,
                "table_with_nulls",
                "(c_char varchar, c_null varchar, c_partition varchar) WITH (format='PARQUET', partitioned_by = ARRAY['c_partition'])");

        int rowCount = 2;
        @Language("SQL") String insertSql = "INSERT INTO table_with_nulls VALUES " +
                String.join(", ", IntStream.range(0, rowCount)
                        .mapToObj("('a%d', NULL, 'a_p')"::formatted).toList());
        assertUpdate(insertSql, rowCount);
//        computeActual("INSERT INTO table_with_nulls values ('b', NULL, 'b_p')");

        warmAndValidate("SELECT * FROM table_with_nulls",
                true,
                "warm_finished",
                rowCount);

        //prefill data source
        MaterializedResult materializedRows = computeActual("SELECT c_null FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(rowCount);

        //prefill data source
        materializedRows = computeActual("SELECT c_partition FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(rowCount);

        //native data source
        materializedRows = computeActual("SELECT c_char FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        //prefill data source
        materializedRows = computeActual("SELECT c_null, c_partition FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(rowCount);

        //mix data source
        materializedRows = computeActual("SELECT c_null, c_char, c_partition FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        //prefill data source - count returns 0 since all values are null
        materializedRows = computeActual("SELECT count(c_null) FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
        assertThat((long) materializedRows.getMaterializedRows().getFirst().getField(0)).isEqualTo(0);

        //prefill data source
        materializedRows = computeActual("SELECT count(c_partition) FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
        assertThat((long) materializedRows.getMaterializedRows().getFirst().getField(0)).isEqualTo(rowCount);

        //test prefill data source with specific warmup rule and lucene syntax
        createWarmupRules(DEFAULT_SCHEMA,
                "table_with_nulls",
                Map.of("c_partition",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        //warmup
        warmAndValidate("SELECT * FROM table_with_nulls",
                getSession(),
                1,
                1,
                0);
        @Language("SQL") String query = "SELECT c_partition FROM table_with_nulls WHERE c_partition LIKE 'a_%'";
        Map<String, Long> expectedQueryStats = Map.of(
                "warp_prefilled_collect_columns", 1L,
                "warp_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testTableLevelWarmupRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        WarmupColRuleData tableLevelRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new WildcardColumnData(),
                WarmUpType.WARM_UP_TYPE_DATA,
                DEFAULT_PRIORITY,
                Duration.ofSeconds(DEFAULT_TTL.toSeconds()),
                ImmutableSet.of());

        WarmupColRuleData columnRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_BASIC,
                DEFAULT_PRIORITY,
                Duration.ofSeconds(DEFAULT_TTL.toSeconds()),
                ImmutableSet.of());

        executeRestCommand(WarmupRuleService.WARMUP_PATH,
                WarmupTask.TASK_NAME_SET,
                List.of(tableLevelRule, columnRule),
                HttpMethod.POST,
                HttpURLConnection.HTTP_OK);

        List<WarmupColRuleData> result = objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupRuleService.TASK_NAME_GET,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(result).hasSize(2);

        warmAndValidate("select * from t", false, 3, 1);
        WarmupRulesUsageData warmupRulesUsageData = objectMapper.readerFor(new TypeReference<WarmupRulesUsageData>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupTask.TASK_NAME_GET_USAGE,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(warmupRulesUsageData.warmupColRuleUsageDataList()).hasSize(2);

        WarmupColRuleUsageData tableLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof WildcardColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(tableLevelWarmupColRuleUsageData.getSchema()).isEqualTo(tableLevelRule.getSchema());
        assertThat(tableLevelWarmupColRuleUsageData.getTable()).isEqualTo(tableLevelRule.getTable());
        assertThat(tableLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new WildcardColumnData());
        assertThat(tableLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(tableLevelRule.getWarmUpType());
        assertThat(tableLevelWarmupColRuleUsageData.getTtl()).isEqualTo(tableLevelRule.getTtl());
        assertThat(tableLevelWarmupColRuleUsageData.getPriority()).isEqualTo(tableLevelRule.getPriority());
        assertThat(tableLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(tableLevelRule.getPredicates());

        WarmupColRuleUsageData columnLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof RegularColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(columnLevelWarmupColRuleUsageData.getSchema()).isEqualTo(columnRule.getSchema());
        assertThat(columnLevelWarmupColRuleUsageData.getTable()).isEqualTo(columnRule.getTable());
        assertThat(columnLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new RegularColumnData(C2));
        assertThat(columnLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(columnRule.getWarmUpType());
        assertThat(columnLevelWarmupColRuleUsageData.getTtl()).isEqualTo(columnRule.getTtl());
        assertThat(columnLevelWarmupColRuleUsageData.getPriority()).isEqualTo(columnRule.getPriority());
        assertThat(columnLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(columnRule.getPredicates());

        String cachedSharedRowGroupsStr = executeRestCommand(CACHED_SHARED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);

        List<RowGroupData> cachedSharedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {})
                .readValue(cachedSharedRowGroupsStr);
        assertThat(cachedSharedRowGroupRes.size()).isEqualTo(1);
        Collection<WarmUpElement> warmUpElements = cachedSharedRowGroupRes.getFirst().getWarmUpElements();
        Stream.of(Pair.of(C1, WarmUpType.WARM_UP_TYPE_DATA),
                        Pair.of(C2, WarmUpType.WARM_UP_TYPE_DATA),
                        Pair.of(C2, WarmUpType.WARM_UP_TYPE_BASIC))
                .forEach(warmUpTypePair -> {
                    WarmUpElement warmupElement = warmUpElements.stream()
                            .filter(warmUpElement -> {
                                WarpColumn warpColumn = warmUpElement.getWarpColumn();
                                return warpColumn instanceof RegularColumn &&
                                        warpColumn.getName().equals(warmUpTypePair.getKey()) &&
                                        warmUpElement.getWarmUpType().name().equals(warmUpTypePair.getValue().name());
                            })
                            .findFirst()
                            .orElse(null);
                    assertThat(warmupElement).isNotNull();
                });
        demoteAll();
        validateDemoteAll();
    }

    @Test
    public void testTableLevelWarmupRuleOverriddenByColumnLevelWarmupRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        WarmupColRuleData tableLevelRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new WildcardColumnData(),
                WarmUpType.WARM_UP_TYPE_DATA,
                DEFAULT_PRIORITY,
                Duration.ofSeconds(DEFAULT_TTL.toSeconds()),
                ImmutableSet.of());

        WarmupColRuleData columnRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_DATA,
                tableLevelRule.getPriority() - 1,
                Duration.ofSeconds(DEFAULT_TTL.toSeconds()),
                ImmutableSet.of());

        executeRestCommand(WarmupRuleService.WARMUP_PATH,
                WarmupTask.TASK_NAME_SET,
                List.of(tableLevelRule, columnRule),
                HttpMethod.POST,
                HttpURLConnection.HTTP_OK);

        List<WarmupColRuleData> result = objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupRuleService.TASK_NAME_GET,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(result).hasSize(2);

        warmAndValidate("select * from t", false, 2, 1);
        WarmupRulesUsageData warmupRulesUsageData = objectMapper.readerFor(new TypeReference<WarmupRulesUsageData>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupTask.TASK_NAME_GET_USAGE,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(warmupRulesUsageData.warmupColRuleUsageDataList()).hasSize(2);

        WarmupColRuleUsageData tableLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof WildcardColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(tableLevelWarmupColRuleUsageData.getSchema()).isEqualTo(tableLevelRule.getSchema());
        assertThat(tableLevelWarmupColRuleUsageData.getTable()).isEqualTo(tableLevelRule.getTable());
        assertThat(tableLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new WildcardColumnData());
        assertThat(tableLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(tableLevelRule.getWarmUpType());
        assertThat(tableLevelWarmupColRuleUsageData.getTtl()).isEqualTo(tableLevelRule.getTtl());
        assertThat(tableLevelWarmupColRuleUsageData.getPriority()).isEqualTo(tableLevelRule.getPriority());
        assertThat(tableLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(tableLevelRule.getPredicates());

        WarmupColRuleUsageData columnLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof RegularColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(columnLevelWarmupColRuleUsageData.getSchema()).isEqualTo(columnRule.getSchema());
        assertThat(columnLevelWarmupColRuleUsageData.getTable()).isEqualTo(columnRule.getTable());
        assertThat(columnLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new RegularColumnData(C2));
        assertThat(columnLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(columnRule.getWarmUpType());
        assertThat(columnLevelWarmupColRuleUsageData.getTtl()).isEqualTo(columnRule.getTtl());
        assertThat(columnLevelWarmupColRuleUsageData.getPriority()).isEqualTo(columnRule.getPriority());
        assertThat(columnLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(columnRule.getPredicates());

        String cachedSharedRowGroupsStr = executeRestCommand(CACHED_SHARED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);

        List<RowGroupData> cachedSharedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {})
                .readValue(cachedSharedRowGroupsStr);
        assertThat(cachedSharedRowGroupRes.size()).isEqualTo(1);
        List<WarmUpElement> warmUpElements = (List<WarmUpElement>) cachedSharedRowGroupRes.getFirst().getWarmUpElements();
        Stream.of(Pair.of(C1, WarmUpType.WARM_UP_TYPE_DATA),
                        Pair.of(C2, WarmUpType.WARM_UP_TYPE_DATA))
                .forEach(warmUpTypePair -> {
                    WarmUpElement warmupElement = warmUpElements.stream()
                            .filter(warmUpElement -> {
                                WarpColumn warpColumn = warmUpElement.getWarpColumn();
                                return warpColumn instanceof RegularColumn &&
                                        warpColumn.getName().equals(warmUpTypePair.getKey()) &&
                                        warmUpElement.getWarmUpType().name().equals(warmUpTypePair.getValue().name());
                            })
                            .findFirst()
                            .orElse(null);
                    assertThat(warmupElement).isNotNull();
                });
        demoteAll();
        validateDemoteAll();
    }

    @Test
    public void testExternalCollectMetrics()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        @Language("SQL") String query = format("SELECT %s, %s FROM t WHERE %s=1", C1, C2, C1);
        Map<String, Long> expectedQueryStats = Map.of(
                EXTERNAL_COLLECT_STAT, 2L,
                EXTERNAL_MATCH_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testAlternatives()
    {
        createTable(DEFAULT_SCHEMA, "table1", "(id1 integer)");
        computeActual("INSERT INTO table1 VALUES(2)");

        warmAndValidate("SELECT * from table1 WHERE id1 > 0", true, 2, 1);

        @Language("SQL") String query = "SELECT count(*) FROM table1 WHERE id1 > 5";

        String plan = computeActual("EXPLAIN ANALYZE " + query).getMaterializedRows().getFirst().getFields().getFirst().toString();
        assertThat(plan).containsOnlyOnce("ChooseAlternativeNode[alternativesCount = 2]"); // there are 2 alternatives
        assertThat(plan).containsOnlyOnce("TableScan"); // only 1 alternative was used (analyze shows only the alternatives that were used)
        assertThat(plan).containsOnlyOnce("subsumedPredicates=true"); // assert usage of the alternative in which the filter is subsumed by WarpSpeed

        Map<String, Long> expectedQueryStats = Map.of(
                CACHED_TOTAL_ROWS, 1L, // table1's row
                WARP_MATCH_COLUMNS_STAT, 1L, // id1 > 5
                WARP_COLLECT_COLUMNS_STAT, 0L, // predicate is fully pushed down - no need to collect
                PREFILLED_COLUMNS_STAT, 0L,
                EXTERNAL_MATCH_STAT, 0L,
                EXTERNAL_COLLECT_STAT, 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    private DispatcherTableHandle executeWithTableHandle(Session session, String sql)
    {
        PlanNode plan = executeWithPlan(session, sql).getRoot();
        TableScanNode tableScanNode = PlanNodeSearcher.searchFrom(plan)
                .where(node -> node instanceof TableScanNode || node instanceof ChooseAlternativeNode)
                .findAll().stream()
                .map(node -> (TableScanNode) (node instanceof TableScanNode ? node : ((ChooseAlternativeNode) node).getOriginalTableScan().tableScanNode()))
                .findFirst()
                .orElseThrow();
        return (DispatcherTableHandle) tableScanNode.getTable().connectorHandle();
    }

    @SuppressWarnings("LanguageMismatch")
    private Plan executeWithPlan(Session session, String sql)
    {
        return getQueryRunner().executeWithPlan(session, sql).queryPlan().orElseThrow();
    }

    private void validateWarmupElementsDictionaryId(int numWarmupElements, int numCachedRowGroups, int expectedNumFailedWarmupElements, String tableName)
            throws IOException
    {
        HealthResult healthResult = objectMapper.readerFor(HealthResult.class)
                .readValue(executeRestCommand(HealthTask.HEALTH_PATH, ClusterHealthTask.TASK_NAME, null, HttpMethod.GET, 200));
        assertThat(healthResult.getHealthNodes().size()).isEqualTo(1);
        String nodeIdentifier = healthResult.getHealthNodes().stream().findFirst().orElseThrow().nodeIdentifier();
        String cachedRowGroupsStr = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        List<RowGroupData> cachedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {})
                .readValue(cachedRowGroupsStr);
        List<RowGroupData> tableCachedRowGroup = new ArrayList<>();
        for (RowGroupData cachedRowGroup : cachedRowGroupRes) {
            RowGroupKey rowGroupKey = cachedRowGroup.getRowGroupKey();
            assertThat(cachedRowGroup.getNodeIdentifier()).isEqualTo(nodeIdentifier);
            if (rowGroupKey.table().equalsIgnoreCase(tableName)) {
                tableCachedRowGroup.add(cachedRowGroup);
            }
        }
        assertThat(tableCachedRowGroup.size()).isEqualTo(numCachedRowGroups);

        int numFailed = 0;
        for (int i = 0; i < numCachedRowGroups; i++) {
            Collection<WarmUpElement> warmUpElements = tableCachedRowGroup.get(i).getWarmUpElements();
            assertThat(warmUpElements.size()).isEqualTo(numWarmupElements);
            for (WarmUpElement warmUpElement : warmUpElements) {
                assertThat(warmUpElement.getState().state().name()).isEqualTo("VALID");
                DictionaryInfo dictionaryInfo = warmUpElement.getDictionaryInfo();
                if (dictionaryInfo.toString().contains(DICTIONARY_MAX_EXCEPTION.name())) {
                    numFailed++;
                }
                else {
                    assertThat(dictionaryInfo.toString()).contains(DictionaryState.DICTIONARY_VALID.name());
                }
            }
        }
        assertThat(numFailed).isEqualTo(expectedNumFailedWarmupElements);
    }

    private boolean validateFunctionNameExistInExpression(WarpExpression warpExpression, String functionName)
    {
        boolean result = false;
        if (warpExpression instanceof WarpCall callExpression) {
            if (callExpression.getFunctionName().equals(functionName)) {
                result = true;
            }
            else {
                for (WarpExpression expression : callExpression.getArguments()) {
                    result |= validateFunctionNameExistInExpression(expression, functionName);
                }
            }
        }
        return result;
    }
}
