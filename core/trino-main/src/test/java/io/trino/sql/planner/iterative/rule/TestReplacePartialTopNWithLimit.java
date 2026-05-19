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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.StaticRuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ApplyPartialTopNResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.AlternativesOptimizer;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.plan.TopNNode.Step.FINAL;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.testing.TestingSession.testSessionBuilder;

final class TestReplacePartialTopNWithLimit
        extends BasePlanTest
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";

    private static final SchemaTableName SORTED_TABLE = new SchemaTableName(TEST_SCHEMA, "sorted_table");
    private static final String COLUMN_A = "col_a";
    private static final ColumnHandle COLUMN_HANDLE_A = new MockConnectorColumnHandle(COLUMN_A, VARCHAR);
    private static final String COLUMN_B = "col_b";
    private static final ColumnHandle COLUMN_HANDLE_B = new MockConnectorColumnHandle(COLUMN_B, VARCHAR);

    private static final SchemaTableName UNSORTED_TABLE = new SchemaTableName(TEST_SCHEMA, "unsorted_table");

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(MOCK_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();
        PlanTester planTester = PlanTester.create(session);
        MockConnectorFactory mockFactory = MockConnectorFactory.builder()
                .withApplyPartialTopN((_, handle, sortProperties, _) -> {
                    MockConnectorTableHandle tableHandle = (MockConnectorTableHandle) handle;
                    if (tableHandle.getTableName().equals(SORTED_TABLE) &&
                            sortProperties.equals(ImmutableList.of(new SortingProperty<>(COLUMN_HANDLE_A, ASC_NULLS_FIRST)))) {
                        return Optional.of(new ApplyPartialTopNResult(true, handle));
                    }
                    return Optional.empty();
                })
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(SORTED_TABLE) || schemaTableName.equals(UNSORTED_TABLE)) {
                        return ImmutableList.of(
                                new ColumnMetadata(COLUMN_A, VARCHAR),
                                new ColumnMetadata(COLUMN_B, VARCHAR));
                    }
                    throw new IllegalArgumentException();
                })
                .build();
        planTester.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());
        return planTester;
    }

    @Test
    void testSortedTableProducesAlternative()
    {
        TableHandle tableHandle = sortedTableHandle();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = newPlanBuilder(idAllocator);
        Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);

        PlanNode plan = planBuilder.topN(
                10,
                ImmutableList.of(colA),
                PARTIAL,
                SortOrder.ASC_NULLS_FIRST,
                planBuilder.tableScan(tableHandle, ImmutableList.of(colA), ImmutableMap.of(colA, COLUMN_HANDLE_A)));

        PlanNode optimized = runOptimizer(plan);

        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(PlanMatchPattern.sort("col_a", ASCENDING, FIRST));
        assertPlan(
                optimized,
                chooseAlternativeNode(
                        topN(10,
                                orderBy,
                                PARTIAL,
                                PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A))),
                        limit(10,
                                ImmutableList.of(),
                                true,
                                ImmutableList.of("col_a"),
                                PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A)))));
    }

    @Test
    void testUnsortedTableNoAlternative()
    {
        TableHandle tableHandle = unsortedTableHandle();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = newPlanBuilder(idAllocator);
        Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);

        TopNNode plan = planBuilder.topN(
                10,
                ImmutableList.of(colA),
                PARTIAL,
                SortOrder.ASC_NULLS_FIRST,
                planBuilder.tableScan(tableHandle, ImmutableList.of(colA), ImmutableMap.of(colA, COLUMN_HANDLE_A)));

        PlanNode optimized = runOptimizer(plan);

        // No ChooseAlternativeNode -- plan remains unchanged
        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(PlanMatchPattern.sort("col_a", ASCENDING, FIRST));
        assertPlan(
                optimized,
                topN(10,
                        orderBy,
                        PARTIAL,
                        PlanMatchPattern.tableScan(UNSORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A))));
    }

    @Test
    void testSortedOnWrongColumn()
    {
        // Table sorted on col_a, but TopN orders by col_b
        TableHandle tableHandle = sortedTableHandle();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = newPlanBuilder(idAllocator);
        Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);
        Symbol colB = planBuilder.symbol(COLUMN_B, VARCHAR);

        TopNNode plan = planBuilder.topN(
                10,
                ImmutableList.of(colB),
                PARTIAL,
                SortOrder.ASC_NULLS_FIRST,
                planBuilder.tableScan(tableHandle, ImmutableList.of(colA, colB), ImmutableMap.of(colA, COLUMN_HANDLE_A, colB, COLUMN_HANDLE_B)));

        PlanNode optimized = runOptimizer(plan);

        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(PlanMatchPattern.sort("col_b", ASCENDING, FIRST));
        assertPlan(
                optimized,
                topN(10,
                        orderBy,
                        PARTIAL,
                        PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A, "col_b", COLUMN_B))));
    }

    @Test
    void testFilterBetweenTopNAndScan()
    {
        TableHandle tableHandle = sortedTableHandle();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = newPlanBuilder(idAllocator);
        Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);

        PlanNode plan = planBuilder.topN(
                10,
                ImmutableList.of(colA),
                PARTIAL,
                SortOrder.ASC_NULLS_FIRST,
                planBuilder.filter(
                        idAllocator.getNextId(),
                        TRUE,
                        planBuilder.tableScan(tableHandle, ImmutableList.of(colA), ImmutableMap.of(colA, COLUMN_HANDLE_A))));

        PlanNode optimized = runOptimizer(plan);

        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(PlanMatchPattern.sort("col_a", ASCENDING, FIRST));
        assertPlan(
                optimized,
                chooseAlternativeNode(
                        topN(10, orderBy, PARTIAL,
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A)))),
                        limit(10, ImmutableList.of(), true, ImmutableList.of("col_a"),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A))))));
    }

    @Test
    void testIdentityProjectBetweenTopNAndScan()
    {
        TableHandle tableHandle = sortedTableHandle();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = newPlanBuilder(idAllocator);
        Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);

        PlanNode plan = planBuilder.topN(
                10,
                ImmutableList.of(colA),
                PARTIAL,
                SortOrder.ASC_NULLS_FIRST,
                planBuilder.project(
                        Assignments.identity(colA),
                        planBuilder.tableScan(tableHandle, ImmutableList.of(colA), ImmutableMap.of(colA, COLUMN_HANDLE_A))));

        PlanNode optimized = runOptimizer(plan);

        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(PlanMatchPattern.sort("col_a", ASCENDING, FIRST));
        assertPlan(
                optimized,
                chooseAlternativeNode(
                        topN(10, orderBy, PARTIAL,
                                strictProject(ImmutableMap.of("col_a", expression(new Reference(VARCHAR, "col_a"))),
                                        PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A)))),
                        limit(10, ImmutableList.of(), true, ImmutableList.of("col_a"),
                                strictProject(ImmutableMap.of("col_a", expression(new Reference(VARCHAR, "col_a"))),
                                        PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A))))));
    }

    @Test
    void testNonIdentityProjectBlocksRule()
    {
        TableHandle tableHandle = sortedTableHandle();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = newPlanBuilder(idAllocator);
        Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);
        Symbol derived = planBuilder.symbol("derived", BIGINT);

        PlanNode plan = planBuilder.topN(
                10,
                ImmutableList.of(derived),
                PARTIAL,
                SortOrder.ASC_NULLS_FIRST,
                planBuilder.project(
                        Assignments.of(derived, new Constant(BIGINT, 1L)),
                        planBuilder.tableScan(tableHandle, ImmutableList.of(colA), ImmutableMap.of(colA, COLUMN_HANDLE_A))));

        PlanNode optimized = runOptimizer(plan);

        // Non-identity project on the ordering column prevents the rule from applying
        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(PlanMatchPattern.sort("derived", ASCENDING, FIRST));
        assertPlan(
                optimized,
                topN(10, orderBy, PARTIAL,
                        strictProject(ImmutableMap.of("derived", expression(new Constant(BIGINT, 1L))),
                                PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A)))));
    }

    @Test
    void testFinalStepNotMatched()
    {
        TableHandle tableHandle = sortedTableHandle();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = newPlanBuilder(idAllocator);
        Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);

        TopNNode plan = planBuilder.topN(
                10,
                ImmutableList.of(colA),
                FINAL,
                SortOrder.ASC_NULLS_FIRST,
                planBuilder.tableScan(tableHandle, ImmutableList.of(colA), ImmutableMap.of(colA, COLUMN_HANDLE_A)));

        PlanNode optimized = runOptimizer(plan);

        // FINAL step TopN should not match the rule pattern
        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(PlanMatchPattern.sort("col_a", ASCENDING, FIRST));
        assertPlan(
                optimized,
                topN(10,
                        orderBy,
                        FINAL,
                        PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A))));
    }

    @Test
    void testRetainOriginalPlanFalse()
    {
        // Override the plan tester with a connector that returns retainOriginalPlan=false
        Session session = testSessionBuilder()
                .setCatalog(MOCK_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();
        try (PlanTester planTester = PlanTester.create(session)) {
            MockConnectorFactory mockFactory = MockConnectorFactory.builder()
                    .withApplyPartialTopN((_, handle, _, _) -> {
                        MockConnectorTableHandle tableHandle = (MockConnectorTableHandle) handle;
                        if (tableHandle.getTableName().equals(SORTED_TABLE)) {
                            return Optional.of(new ApplyPartialTopNResult(false, handle));
                        }
                        return Optional.empty();
                    })
                    .withGetColumns(schemaTableName -> {
                        if (schemaTableName.equals(SORTED_TABLE)) {
                            return ImmutableList.of(
                                    new ColumnMetadata(COLUMN_A, VARCHAR),
                                    new ColumnMetadata(COLUMN_B, VARCHAR));
                        }
                        throw new IllegalArgumentException();
                    })
                    .build();
            planTester.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            TableHandle tableHandle = new TableHandle(
                    planTester.getCatalogHandle(MOCK_CATALOG),
                    new MockConnectorTableHandle(SORTED_TABLE),
                    TestingTransactionHandle.create());
            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            PlanBuilder planBuilder = new PlanBuilder(idAllocator, planTester.getPlannerContext(), planTester.getDefaultSession());
            Symbol colA = planBuilder.symbol(COLUMN_A, VARCHAR);

            PlanNode plan = planBuilder.topN(
                    10,
                    ImmutableList.of(colA),
                    PARTIAL,
                    SortOrder.ASC_NULLS_FIRST,
                    planBuilder.tableScan(tableHandle, ImmutableList.of(colA), ImmutableMap.of(colA, COLUMN_HANDLE_A)));

            PlanNode optimized = runOptimizer(planTester, plan);

            // When retainOriginalPlan=false, the limit directly replaces the TopN (no ChooseAlternativeNode)
            assertPlan(
                    planTester,
                    optimized,
                    limit(10,
                            ImmutableList.of(),
                            true,
                            ImmutableList.of("col_a"),
                            PlanMatchPattern.tableScan(SORTED_TABLE.getTableName(), ImmutableMap.of("col_a", COLUMN_A))));
        }
    }

    private TableHandle sortedTableHandle()
    {
        return new TableHandle(
                getPlanTester().getCatalogHandle(MOCK_CATALOG),
                new MockConnectorTableHandle(SORTED_TABLE),
                TestingTransactionHandle.create());
    }

    private TableHandle unsortedTableHandle()
    {
        return new TableHandle(
                getPlanTester().getCatalogHandle(MOCK_CATALOG),
                new MockConnectorTableHandle(UNSORTED_TABLE),
                TestingTransactionHandle.create());
    }

    private PlanBuilder newPlanBuilder(PlanNodeIdAllocator idAllocator)
    {
        return new PlanBuilder(idAllocator, getPlanTester().getPlannerContext(), getPlanTester().getDefaultSession());
    }

    private PlanNode runOptimizer(PlanNode plan)
    {
        return runOptimizer(getPlanTester(), plan);
    }

    private static PlanNode runOptimizer(PlanTester planTester, PlanNode plan)
    {
        return planTester.inTransaction(session -> {
            session.getCatalog().ifPresent(catalog -> planTester.getPlannerContext().getMetadata().getCatalogHandle(session, catalog));

            PlannerContext plannerContext = planTester.getPlannerContext();
            StatsCalculator statsCalculator = planTester.getStatsCalculator();
            CostCalculator costCalculator = planTester.getCostCalculator();

            AlternativesOptimizer optimizer = new AlternativesOptimizer(
                    plannerContext,
                    new RuleStatsRecorder(),
                    statsCalculator,
                    costCalculator,
                    ImmutableSet.<Rule<?>>of(new ReplacePartialTopNWithLimit(plannerContext)));

            return optimizer.optimize(
                    plan,
                    new PlanOptimizer.Context(
                            session,
                            new SymbolAllocator(),
                            new PlanNodeIdAllocator(),
                            WarningCollector.NOOP,
                            createPlanOptimizersStatsCollector(),
                            new CachingTableStatsProvider(plannerContext.getMetadata(), session, () -> false),
                            new StaticRuntimeInfoProvider(ImmutableMap.of(), ImmutableMap.of())));
        });
    }

    private void assertPlan(PlanNode actual, PlanMatchPattern pattern)
    {
        assertPlan(getPlanTester(), actual, pattern);
    }

    private static void assertPlan(PlanTester planTester, PlanNode actual, PlanMatchPattern pattern)
    {
        planTester.inTransaction(session -> {
            session.getCatalog().ifPresent(catalog -> planTester.getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            PlanAssert.assertPlan(
                    session,
                    planTester.getPlannerContext().getMetadata(),
                    planTester.getPlannerContext().getFunctionManager(),
                    planTester.getStatsCalculator(),
                    new Plan(actual, StatsAndCosts.empty()),
                    pattern);
            return null;
        });
    }
}
