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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ResolvedFunction;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.tree.SortItem.NullOrdering;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_PROJECTIONS_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalAggregationToColumnId;
import static io.trino.cache.CommonSubqueriesExtractor.aggregationKey;
import static io.trino.cache.CommonSubqueriesExtractor.scanFilterProjectKey;
import static io.trino.cache.CommonSubqueriesExtractor.topNKey;
import static io.trino.cache.CommonSubqueriesExtractor.topNRankingKey;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.SymbolsExtractor.extractOutputSymbols;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.identityProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCommonSubqueriesExtractor
        extends BasePlanTest
{
    private static final CacheTableId CACHE_TABLE_ID = new CacheTableId("cache_table_id");
    private static final CacheColumnId REGIONKEY_ID = new CacheColumnId("[regionkey:bigint]");
    private static final CacheColumnId NATIONKEY_ID = new CacheColumnId("[nationkey:bigint]");
    private static final CacheColumnId NAME_ID = new CacheColumnId("[name:varchar(25)]");
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TEST_SCHEMA)
            .build();
    private static final Session TPCH_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            // prevent CBO from interfering with tests
            .setSystemProperty(JOIN_REORDERING_STRATEGY, "none")
            // simplify tests by disabling small DF waiting
            .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT, "0")
            .build();
    private static final MockConnectorColumnHandle HANDLE_1 = new MockConnectorColumnHandle("column1", BIGINT);
    private static final TupleDomain<ColumnHandle> CONSTRAINT_1 = TupleDomain.withColumnDomains(ImmutableMap.of(
            HANDLE_1, Domain.create(ValueSet.ofRanges(
                    Range.lessThan(BIGINT, 50L),
                    Range.greaterThan(BIGINT, 150L)), false)));
    private static final TupleDomain<ColumnHandle> CONSTRAINT_2 = TupleDomain.withColumnDomains(ImmutableMap.of(
            HANDLE_1, Domain.create(ValueSet.ofRanges(
                    Range.lessThan(BIGINT, 20L),
                    Range.greaterThan(BIGINT, 40L)), false)));

    private static final TupleDomain<ColumnHandle> CONSTRAINT_3 = TupleDomain.withColumnDomains(ImmutableMap.of(
            HANDLE_1, Domain.create(ValueSet.ofRanges(
                    Range.lessThan(BIGINT, 30L),
                    Range.greaterThan(BIGINT, 70L)), false)));
    private static final SchemaTableName TABLE_NAME = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final Expression NATIONKEY_EXPRESSION = new Reference(BIGINT, "[nationkey:bigint]");

    private String tpchCatalogId;

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(TEST_SESSION);
        planTester.createCatalog(
                TEST_CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withGetColumns(_ -> ImmutableList.of(
                                new ColumnMetadata("column1", BIGINT),
                                new ColumnMetadata("column2", BIGINT)))
                        .withGetCacheTableId(_ -> Optional.of(CACHE_TABLE_ID))
                        .withGetCanonicalTableHandle(Function.identity())
                        .withGetCacheColumnId(handle -> {
                            MockConnectorColumnHandle column = (MockConnectorColumnHandle) handle;
                            return Optional.of(new CacheColumnId("cache_" + column.name()));
                        })
                        .withApplyFilter((_, _, constraint) -> {
                            // predicate is fully subsumed
                            if (constraint.getSummary().equals(CONSTRAINT_1)) {
                                return Optional.of(new ConstraintApplicationResult<>(
                                        new MockConnectorTableHandle(TABLE_NAME, CONSTRAINT_1, Optional.of(ImmutableList.of(HANDLE_1))),
                                        TupleDomain.all(),
                                        constraint.getExpression(),
                                        false));
                            }
                            // predicate is rejected
                            else if (constraint.getSummary().equals(CONSTRAINT_2)) {
                                return Optional.of(new ConstraintApplicationResult<>(
                                        new MockConnectorTableHandle(TABLE_NAME, TupleDomain.all(), Optional.empty()),
                                        CONSTRAINT_2,
                                        constraint.getExpression(),
                                        false));
                            }
                            // predicate is subsumed opportunistically
                            else if (constraint.getSummary().equals(CONSTRAINT_3)) {
                                return Optional.of(new ConstraintApplicationResult<>(
                                        new MockConnectorTableHandle(TABLE_NAME, CONSTRAINT_3, Optional.empty()),
                                        CONSTRAINT_3,
                                        constraint.getExpression(),
                                        false));
                            }
                            return Optional.empty();
                        })
                        .withGetTableProperties((_, tableHandle) -> {
                            MockConnectorTableHandle handle = (MockConnectorTableHandle) tableHandle;
                            if (handle.getConstraint().equals(CONSTRAINT_2)) {
                                return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), emptyList());
                            }
                            return new ConnectorTableProperties(handle.getConstraint(), Optional.empty(), Optional.empty(), emptyList());
                        })
                        .build(),
                ImmutableMap.of());
        planTester.createCatalog(
                TPCH_SESSION.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        tpchCatalogId = planTester.getCatalogHandle(TPCH_SESSION.getCatalog().get()).getId();
        return planTester;
    }

    @Test
    public void testTopNRankingRowWithWithNonPullableConjuncts()
    {
        @Language("SQL") String query =
                """
                (SELECT *
                FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY nationkey ORDER BY regionkey DESC) update_rank
                FROM nation WHERE regionkey < 11) AS t
                WHERE t.update_rank = 1)
                UNION ALL (SELECT *
                FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY nationkey ORDER BY regionkey DESC) update_rank
                FROM nation WHERE regionkey < 10) AS t
                WHERE t.update_rank = 1)""";
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries(query, true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).allSatisfy((node, _) ->
                assertThat(node).isInstanceOf(TopNRankingNode.class));
        CommonPlanAdaptation topNRankingA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation topNRankingB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplanA = topNRanking(
                pattern -> pattern.specification(
                                ImmutableList.of("NATIONKEY"),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", DESC_NULLS_LAST))
                        .rankingType(ROW_NUMBER)
                        .maxRankingPerPartition(1)
                        .partial(true),
                filter(
                        comparison(LESS_THAN, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 10L)),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey"))));
        PlanMatchPattern commonSubplanB = topNRanking(
                pattern -> pattern.specification(
                                ImmutableList.of("NATIONKEY"),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", DESC_NULLS_LAST))
                        .rankingType(ROW_NUMBER)
                        .maxRankingPerPartition(1)
                        .partial(true),
                filter(
                        comparison(LESS_THAN, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 11L)),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey"))));
        assertTpchPlan(topNRankingB.getCommonSubplan(), commonSubplanA);
        assertTpchPlan(topNRankingA.getCommonSubplan(), commonSubplanB);
        assertThat(topNRankingA.getCommonSubplanSignature()).isNotEqualTo(topNRankingB.getCommonSubplanSignature());
    }

    @Test
    public void testCacheTopNRankingRank()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries(
                """
                SELECT name, regionkey FROM nation WHERE nationkey > 10 ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES
                """,
                true,
                false,
                false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, _) -> assertThat(node).isInstanceOf(TopNRankingNode.class));
        CommonPlanAdaptation topNRanking = planAdaptations.values().stream().findFirst().get();

        PlanMatchPattern commonSubplan = topNRanking(
                pattern -> pattern.specification(
                                ImmutableList.of(),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", ASC_NULLS_LAST))
                        .rankingType(RANK)
                        .maxRankingPerPartition(6)
                        .partial(true),
                strictProject(ImmutableMap.of(
                                "NAME", PlanMatchPattern.expression(new Reference(createVarcharType(25), "NAME")),
                                "REGIONKEY", PlanMatchPattern.expression(new Reference(BIGINT, "REGIONKEY"))),
                        filter(
                                comparison(GREATER_THAN, new Reference(BIGINT, "NATIONKEY"), new Constant(BIGINT, 10L)),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")))));
        assertTpchPlan(topNRanking.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        // validate no adaptation is required
        assertThat(topNRanking.adaptCommonSubplan(topNRanking.getCommonSubplan(), idAllocator)).isEqualTo(topNRanking.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(createVarcharType(25), BIGINT);
        assertThat(topNRanking.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNRankingKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableList.of(),
                                ImmutableMap.of(REGIONKEY_ID, ASC_NULLS_LAST),
                                RANK,
                                6),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        NATIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testCacheTopNRankingRow()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries(
                """
                SELECT *
                FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY name, nationkey ORDER BY regionkey DESC) update_rank FROM nation) AS t
                WHERE t.update_rank = 1""",
                true,
                false,
                false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, _) -> assertThat(node).isInstanceOf(TopNRankingNode.class));
        CommonPlanAdaptation topNRanking = planAdaptations.values().stream().findFirst().get();
        PlanMatchPattern commonSubplan = topNRanking(
                pattern -> pattern.specification(
                                ImmutableList.of("NAME", "NATIONKEY"),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", DESC_NULLS_LAST))
                        .rankingType(ROW_NUMBER)
                        .maxRankingPerPartition(1)
                        .partial(true),
                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")));
        assertTpchPlan(topNRanking.getCommonSubplan(), commonSubplan);
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();

        // validate no adaptation is required
        assertThat(topNRanking.adaptCommonSubplan(topNRanking.getCommonSubplan(), idAllocator)).isEqualTo(topNRanking.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25), BIGINT);
        assertThat(topNRanking.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNRankingKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableList.of(NAME_ID, NATIONKEY_ID),
                                ImmutableMap.of(REGIONKEY_ID, DESC_NULLS_LAST),
                                ROW_NUMBER,
                                1),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testCacheTopN()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries(
                """
                SELECT nationkey FROM nation
                WHERE regionkey > 10 and nationkey > 2
                ORDER BY name ASC, regionkey DESC OFFSET 5 LIMIT 5""",
                true,
                false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, _) -> assertThat(node).isInstanceOf(TopNNode.class));
        CommonPlanAdaptation topN = planAdaptations.values().stream().findFirst().get();
        PlanMatchPattern commonSubplan = topN(
                10,
                ImmutableList.of(sort("NAME", Ordering.ASCENDING, NullOrdering.LAST),
                        sort("REGIONKEY", Ordering.DESCENDING, NullOrdering.LAST)),
                TopNNode.Step.PARTIAL,
                filter(
                        new Logical(AND, ImmutableList.of(
                                comparison(GREATER_THAN, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 10L)),
                                comparison(GREATER_THAN, new Reference(BIGINT, "NATIONKEY"), new Constant(BIGINT, 2L)))),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey"))));
        assertTpchPlan(topN.getCommonSubplan(), commonSubplan);

        // validate no adaptation is required
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(topN.adaptCommonSubplan(topN.getCommonSubplan(), idAllocator)).isEqualTo(topN.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25), BIGINT);
        assertThat(topN.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableMap.of(NAME_ID, ASC_NULLS_LAST, REGIONKEY_ID, DESC_NULLS_LAST),
                                10),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false),
                        NATIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 2L)), false)))));
    }

    @Test
    public void testCacheSingleAggregation()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries(
                """
                SELECT sum(nationkey) FROM nation
                WHERE regionkey > 10
                GROUP BY name""",
                true,
                true);

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, _) -> assertThat(node).isInstanceOf(AggregationNode.class));

        CommonPlanAdaptation aggregation = Iterables.get(planAdaptations.values(), 0);
        PlanMatchPattern commonSubplan = aggregation(
                singleGroupingSet("NAME"),
                ImmutableMap.of(
                        Optional.of("SUM"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY")))),
                Optional.empty(),
                AggregationNode.Step.PARTIAL,
                identityProject(
                        filter(
                                comparison(GREATER_THAN, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 10L)),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")))));

        // validate common subplan
        assertTpchPlan(aggregation.getCommonSubplan(), commonSubplan);

        // validate no adaptation is required
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(aggregation.adaptCommonSubplan(aggregation.getCommonSubplan(), idAllocator)).isEqualTo(aggregation.getCommonSubplan());

        // validate signature
        CanonicalAggregation sum = canonicalAggregation("sum", NATIONKEY_EXPRESSION);
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NAME_ID, canonicalAggregationToColumnId(sum));
        List<Type> cacheColumnsTypes = ImmutableList.of(createVarcharType(25), BIGINT);
        assertThat(aggregation.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of(NAME_ID)),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testCacheSingleProjection()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries(
                """
                SELECT sum(nationkey) FROM nation
                WHERE regionkey > 10
                GROUP BY name""",
                false,
                true);

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, _) -> assertThat(node).isInstanceOf(ProjectNode.class));

        CommonPlanAdaptation projection = Iterables.get(planAdaptations.values(), 0);
        PlanMatchPattern commonSubplan =
                identityProject(
                        filter(
                                comparison(GREATER_THAN, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 10L)),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey"))));

        // validate common subplan
        assertTpchPlan(projection.getCommonSubplan(), commonSubplan);

        // validate no adaptation is required
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(projection.adaptCommonSubplan(projection.getCommonSubplan(), idAllocator)).isEqualTo(projection.getCommonSubplan());

        // validate signature
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25));
        assertThat(projection.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    private CanonicalAggregation canonicalAggregation(String name, Expression... arguments)
    {
        return canonicalAggregation(name, Optional.empty(), arguments);
    }

    private CanonicalAggregation canonicalAggregation(String name, Optional<Symbol> mask, Expression... arguments)
    {
        ResolvedFunction resolvedFunction = getPlanTester().getPlannerContext().getMetadata().resolveBuiltinFunction(
                name,
                TypeDescriptorProvider.fromTypes(Stream.of(arguments)
                        .map(Expression::type)
                        .collect(toImmutableList())));
        return new CanonicalAggregation(
                resolvedFunction,
                mask,
                ImmutableList.copyOf(arguments));
    }

    private CommonSubqueries extractTpchCommonSubqueries(@Language("SQL") String query, boolean cacheAggregations, boolean cacheProjections)
    {
        return extractTpchCommonSubqueries(query, cacheAggregations, cacheProjections, true);
    }

    private CommonSubqueries extractTpchCommonSubqueries(@Language("SQL") String query, boolean cacheAggregations, boolean cacheProjections, boolean forceSingleNode)
    {
        Session tpchSession = Session.builder(TPCH_SESSION)
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, Boolean.toString(cacheAggregations))
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, Boolean.toString(cacheProjections))
                .build();
        PlanTester planTester = getPlanTester();
        return planTester.inTransaction(tpchSession, session -> {
            Plan plan = planTester.createPlan(session, query, planTester.getPlanOptimizers(), planTester.getAlternativeOptimizers(), OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getPlanTester().getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            SymbolAllocator symbolAllocator = new SymbolAllocator(ImmutableSet.<Symbol>builder()
                    .addAll(extractUnique(plan.getRoot()))
                    .addAll(extractOutputSymbols(plan.getRoot())).build());
            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            return new CommonSubqueries(
                    CommonSubqueriesExtractor.extractCommonSubqueries(
                            new CacheController(),
                            getPlanTester().getPlannerContext(),
                            session,
                            idAllocator,
                            symbolAllocator,
                            plan.getRoot()),
                    symbolAllocator,
                    idAllocator,
                    plan.getRoot());
        });
    }

    record CommonSubqueries(Map<PlanNode, CommonPlanAdaptation> planAdaptations, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, PlanNode plan) {}

    private void assertTpchPlan(PlanNode root, PlanMatchPattern expected)
    {
        assertPlan(TPCH_SESSION, root, expected);
    }

    private void assertPlan(Session customSession, PlanNode root, PlanMatchPattern expected)
    {
        getPlanTester().inTransaction(customSession, session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getPlanTester().getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            Plan plan = new Plan(root, StatsAndCosts.empty());
            PlanAssert.assertPlan(session, getPlanTester().getPlannerContext().getMetadata(), createTestingFunctionManager(), noopStatsCalculator(), plan, expected);
            return null;
        });
    }
}
