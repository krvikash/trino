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
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.api.warmup.DateSlidingWindowWarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.PartitionValueWarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupColRuleData;
import io.trino.plugin.warp.api.warmup.WarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.WarmupPropertiesData;
import io.trino.plugin.warp.api.warmup.column.RegularColumnData;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.extension.execution.debugtools.PredicateCacheTask;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.plugin.warp.juffer.PredicateBufferPoolType;
import io.trino.plugin.warp.warmup.WarmupRuleService;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.WarpSessionProperties.ENABLE_OR_PUSHDOWN;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
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
    // TODO stuck in endless loop
    public void testRowDereference()
    {
        createTable(DEFAULT_SCHEMA, "evolve_test", "(dummy bigint, a row(b bigint, c varchar), d bigint)");
        computeActual("INSERT INTO evolve_test values (1, row(1, 'abc'), 1)");
        computeActual(getSession(), "select * from evolve_test where a[1] > 1");
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
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(catalog, ENABLE_OR_PUSHDOWN, "true")
                .build();
        String prefixLikePattern = "prefix%";
        String suffixLikePattern = "%suffix";
        String query = format("SELECT %1$s FROM t WHERE %1$s LIKE '%2$s' AND %1$s LIKE '%3$s'", C2, prefixLikePattern, suffixLikePattern);
        DispatcherTableHandle table = executeWithTableHandle(session, query);

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
        MaterializedResult result = computeActual(session, query);
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
    public void testExternalCollectMetrics()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        @Language("SQL") String query = format("SELECT %s, %s FROM t WHERE %s=1", C1, C2, C1);
        Map<String, Long> expectedQueryStats = Map.of(
                EXTERNAL_COLLECT_STAT, 2L,
                EXTERNAL_MATCH_STAT, 1L);
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
