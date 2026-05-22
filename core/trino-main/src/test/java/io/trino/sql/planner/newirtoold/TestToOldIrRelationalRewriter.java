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
package io.trino.sql.planner.newirtoold;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogHandle;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.Context;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.RelationalProgramBuilder;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.GroupingSetDescriptor;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.FrameBoundType.FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.PRECEDING;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

class TestToOldIrRelationalRewriter
{
    private static final Symbol A = new Symbol(BIGINT, "a");
    private static final Symbol B = new Symbol(BOOLEAN, "b");
    private static final ValuesNode VALUES_NODE = new ValuesNode(
            new PlanNodeId("values"),
            ImmutableList.of(A, B),
            ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 42L), new Constant(BOOLEAN, true)))));

    private static final Symbol C = new Symbol(BOOLEAN, "c");
    private static final Symbol D = new Symbol(BIGINT, "d");
    private static final ValuesNode ANOTHER_VALUES_NODE = new ValuesNode(
            new PlanNodeId("values"),
            ImmutableList.of(C, D),
            ImmutableList.of(new Row(ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BIGINT, null)))));

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testAggregation()
    {
        // note: the ToOldIrRelationalRewriter will assign the output symbol names for aggregate functions based on resolved function name
        // this is consistent with symbols created for the old IR in QueryPlanner#planAggregation(), with the difference that
        // we get the name from ResolvedFunction, and QueryPlanner gets the name from FunctionCall, so the result might differ in some cases,
        // e.g. "any_value" vs "arbitrary".
        // this test uses the same symbol names to enable roundtrip.

        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        ResolvedFunction arbitraryFunction = FUNCTION_RESOLUTION.resolveFunction("arbitrary", fromTypes(BOOLEAN));

        AggregationNode aggregationNode = new AggregationNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                ImmutableMap.of(
                        new Symbol(BIGINT, "sum"), new Aggregation(sumFunction, ImmutableList.of(new Reference(BIGINT, "a")), true, Optional.of(B), Optional.empty(), Optional.of(A)),
                        new Symbol(BOOLEAN, "any_value"), new Aggregation(arbitraryFunction, ImmutableList.of(new Reference(BOOLEAN, "b")), false, Optional.empty(), Optional.of(new OrderingScheme(ImmutableList.of(B), ImmutableMap.of(B, DESC_NULLS_FIRST))), Optional.empty())),
                new GroupingSetDescriptor(ImmutableList.of(B), 1, ImmutableSet.of()),
                ImmutableList.of(B),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.of(true));
        assertRoundtrip(aggregationNode);

        // aggregation without aggregate functions
        AggregationNode distinctAggregationNode = new AggregationNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                ImmutableMap.of(),
                new GroupingSetDescriptor(ImmutableList.of(), 1, ImmutableSet.of(0)),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.of(true));
        assertRoundtrip(distinctAggregationNode);
    }

    @Test
    public void testAssignUniqueId()
    {
        AssignUniqueId assignUniqueIdNode = new AssignUniqueId(
                new PlanNodeId("0"),
                VALUES_NODE,
                new Symbol(BIGINT, "unique"));
        assertRoundtrip(assignUniqueIdNode);
    }

    @Test
    public void testDynamicFilterSource()
    {
        DynamicFilterSourceNode dynamicFilterSourceNode = new DynamicFilterSourceNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                ImmutableMap.of(
                        new DynamicFilterId("first_dynamic_filter"), A,
                        new DynamicFilterId("second_dynamic_filter"), B,
                        new DynamicFilterId("third_dynamic_filter"), A));
        assertRoundtrip(dynamicFilterSourceNode);
    }

    @Test
    public void testEnforceSingleRow()
    {
        EnforceSingleRowNode enforceSingleRowNode = new EnforceSingleRowNode(new PlanNodeId("0"), VALUES_NODE);
        assertRoundtrip(enforceSingleRowNode);
    }

    @Test
    public void testExcept()
    {
        ExceptNode exceptNode = new ExceptNode(
                new PlanNodeId("0"),
                ImmutableList.of(VALUES_NODE, ANOTHER_VALUES_NODE),
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .put(A, A)
                        .put(A, D)
                        .put(B, B)
                        .put(B, C)
                        .build(),
                ImmutableList.of(A, B),
                true);
        assertRoundtrip(exceptNode);

        // except of two sources with duplicate input symbols
        Symbol a0 = new Symbol(BIGINT, "a_0");
        ExceptNode exceptNodeWithDuplicate = new ExceptNode(
                new PlanNodeId("0"),
                ImmutableList.of(VALUES_NODE, ANOTHER_VALUES_NODE),
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        // the duplicate input symbols "a" from the first source and "d" from the second source are mapped to two distinct output symbols: "a" and "a_0"
                        .put(A, A)
                        .put(A, D)
                        .put(B, B)
                        .put(B, C)
                        .put(a0, A)
                        .put(a0, D)
                        .build(),
                ImmutableList.of(A, B, a0),
                true);
        assertRoundtrip(exceptNodeWithDuplicate);
    }

    @Test
    public void testExchange()
    {
        // note: the ToOldIrRelationalRewriter will assign the output symbol names for exchange based on the first source
        // this is consistent with symbols created for the old IR in StatementAnalyzer#visitSetOperation()
        // this test uses the same symbol names to enable roundtrip.

        // union of two sources
        ConnectorPartitioningHandle testingPartitioningHandle = new ConnectorPartitioningHandle() {};
        ExchangeNode unionExchangeNode = new ExchangeNode(
                new PlanNodeId("0"),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(
                        Partitioning.create(
                                new PartitioningHandle(
                                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                        testingPartitioningHandle),
                                ImmutableList.of(A)),
                        ImmutableList.of(A, B),
                        false,
                        Optional.of(new int[] {5, 6, 7}),
                        OptionalInt.empty(),
                        OptionalInt.empty()),
                ImmutableList.of(
                        VALUES_NODE,
                        new ValuesNode(
                                new PlanNodeId("another values"),
                                ImmutableList.of(new Symbol(SMALLINT, "c"), new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e")),
                                ImmutableList.of())),
                ImmutableList.of(
                        ImmutableList.of(A, B),
                        ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e"))),
                Optional.empty());
        assertRoundtrip(unionExchangeNode);

        // union of two sources with duplicate input symbols
        ExchangeNode unionExchangeNodeWithDuplicate = new ExchangeNode(
                new PlanNodeId("0"),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(
                        Partitioning.create(
                                new PartitioningHandle(
                                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                        testingPartitioningHandle),
                                ImmutableList.of(A)),
                        // the duplicate input symbol "a" is mapped to two distinct output symbols: "a" and "a_0"
                        ImmutableList.of(A, B, new Symbol(BIGINT, "a_0")),
                        false,
                        Optional.of(new int[] {5, 6, 7}),
                        OptionalInt.empty(),
                        OptionalInt.empty()),
                ImmutableList.of(
                        VALUES_NODE,
                        new ValuesNode(
                                new PlanNodeId("another values"),
                                ImmutableList.of(new Symbol(SMALLINT, "c"), new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e")),
                                ImmutableList.of())),
                ImmutableList.of(
                        // for each source, select some symbol twice
                        ImmutableList.of(A, B, A),
                        ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e"), new Symbol(BIGINT, "d"))),
                Optional.empty());
        assertRoundtrip(unionExchangeNodeWithDuplicate);

        // merge sort
        ExchangeNode mergeExchangeNode = new ExchangeNode(
                new PlanNodeId("0"),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(
                        Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of(A)),
                        ImmutableList.of(B, A),
                        false,
                        Optional.of(new int[] {5, 6, 7}),
                        OptionalInt.empty(),
                        OptionalInt.of(10)),
                ImmutableList.of(VALUES_NODE),
                ImmutableList.of(ImmutableList.of(B, A)),
                Optional.of(new OrderingScheme(ImmutableList.of(A), ImmutableMap.of(A, DESC_NULLS_FIRST))));
        assertRoundtrip(mergeExchangeNode);
    }

    @Test
    public void testExplainAnalyze()
    {
        // note: the ToOldIrRelationalRewriter will create the output symbol passing the name "Query Pan", which gets transformed to "query_plan" by the SymbolAllocator.
        // it is consistent with the output symbol created for the old IR in LogicalPlanner#createExplainAnalyzePlan()
        // this test uses the same symbol name to enable roundtrip.
        ExplainAnalyzeNode explainAnalyzeNode = new ExplainAnalyzeNode(new PlanNodeId("0"), VALUES_NODE, new Symbol(VARCHAR, "query_plan"), ImmutableList.of(B), false);
        assertRoundtrip(explainAnalyzeNode);
    }

    @Test
    public void testFilter()
    {
        FilterNode filterNode = new FilterNode(new PlanNodeId("0"), VALUES_NODE, new Reference(BOOLEAN, "b"));
        assertRoundtrip(filterNode);
    }

    @Test
    public void testGroupId()
    {
        // note: the ToOldIrRelationalRewriter will create the grouping symbols passing the name of the respective input symbol suffixed with "_gid"
        // it is consistent with the grouping symbols created for the old IR in QueryPlanner#planGroupingSets() with the difference that
        // QueryPlanner handles differently the grouping columns derived for complex expressions.
        // for the group id column, ToOldIrRelationalRewriter creates the symbol passing the name "groupId", which gets transformed to "groupid" by the SymbolAllocator.
        // it is consistent with the group id symbol created for the old IR in QueryPlanner#planGroupingSets()
        // this test uses the same symbol names to enable roundtrip.
        GroupIdNode groupIdNode = new GroupIdNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                ImmutableList.of(
                        // Note: both grouping sets use symbol a_gid
                        ImmutableList.of(new Symbol(BOOLEAN, "b_gid_0"), new Symbol(BIGINT, "a_gid")),
                        ImmutableList.of(new Symbol(BOOLEAN, "b_gid"), new Symbol(BIGINT, "a_gid"))),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a_gid"), new Symbol(BIGINT, "a"),
                        // note: symbols b_gid and b_gid_0 refer to the same input symbol b
                        new Symbol(BOOLEAN, "b_gid"), new Symbol(BOOLEAN, "b"),
                        new Symbol(BOOLEAN, "b_gid_0"), new Symbol(BOOLEAN, "b")),
                ImmutableList.of(new Symbol(BIGINT, "a")),
                new Symbol(BIGINT, "groupid"));
        assertRoundtrip(groupIdNode);
    }

    @Test
    public void testIntersect()
    {
        IntersectNode intersectNode = new IntersectNode(
                new PlanNodeId("0"),
                ImmutableList.of(VALUES_NODE, ANOTHER_VALUES_NODE),
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .put(A, A)
                        .put(A, D)
                        .put(B, B)
                        .put(B, C)
                        .build(),
                ImmutableList.of(A, B),
                false);
        assertRoundtrip(intersectNode);

        // intersect of two sources with duplicate input symbols
        Symbol a0 = new Symbol(BIGINT, "a_0");
        IntersectNode intersectNodeWithDuplicate = new IntersectNode(
                new PlanNodeId("0"),
                ImmutableList.of(VALUES_NODE, ANOTHER_VALUES_NODE),
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        // the duplicate input symbols "a" from the first source and "d" from the second source are mapped to two distinct output symbols: "a" and "a_0"
                        .put(A, A)
                        .put(A, D)
                        .put(B, B)
                        .put(B, C)
                        .put(a0, A)
                        .put(a0, D)
                        .build(),
                ImmutableList.of(A, B, a0),
                false);
        assertRoundtrip(intersectNodeWithDuplicate);
    }

    @Test
    public void testJoin()
    {
        JoinNode joinNode = new JoinNode(
                new PlanNodeId("0"),
                LEFT,
                VALUES_NODE,
                ANOTHER_VALUES_NODE,
                ImmutableList.of(new JoinNode.EquiJoinClause(A, D)),
                ImmutableList.of(B),
                ImmutableList.of(C),
                false,
                Optional.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "c"), new Reference(BOOLEAN, "b")))),
                Optional.of(PARTITIONED),
                Optional.of(false),
                ImmutableMap.of(new DynamicFilterId("dynamic_filter_for_A"), D),
                Optional.of(new PlanNodeStatsAndCostSummary(1, 2, 3, 4, 5)));
        assertRoundtrip(joinNode);

        // join with empty filter
        JoinNode noFilterJoinNode = new JoinNode(
                new PlanNodeId("0"),
                LEFT,
                VALUES_NODE,
                ANOTHER_VALUES_NODE,
                ImmutableList.of(),
                ImmutableList.of(B),
                ImmutableList.of(C),
                false,
                Optional.empty(),
                Optional.of(PARTITIONED),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.of(new PlanNodeStatsAndCostSummary(1, 2, 3, 4, 5)));
        assertRoundtrip(noFilterJoinNode);
    }

    @Test
    public void testSemiJoin()
    {
        // note: the ToOldIrRelationalRewriter creates the semi-join output symbol passing the name "semiJoinResult",
        // which gets transformed to "semijoinresult" by the SymbolAllocator.
        // this test uses the same symbol name to enable roundtrip.
        SemiJoinNode semiJoinNode = new SemiJoinNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                ANOTHER_VALUES_NODE,
                A,
                D,
                new Symbol(BOOLEAN, "semijoinresult"),
                Optional.of(SemiJoinNode.DistributionType.PARTITIONED),
                Optional.of(new DynamicFilterId("semi_join_dynamic_filter")));
        assertRoundtrip(semiJoinNode);
    }

    @Test
    public void testLimit()
    {
        LimitNode limitNode = new LimitNode(new PlanNodeId("0"), VALUES_NODE, 5L, true);
        assertRoundtrip(limitNode);

        // limit with ties
        LimitNode limitWithTies = new LimitNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                5L,
                Optional.of(new OrderingScheme(
                        ImmutableList.of(B, A),
                        ImmutableMap.of(
                                B, DESC_NULLS_FIRST,
                                A, ASC_NULLS_FIRST))),
                true,
                ImmutableList.of(B));
        assertRoundtrip(limitWithTies);
    }

    @Test
    public void testOutput()
    {
        OutputNode outputNode = new OutputNode(new PlanNodeId("0"), VALUES_NODE, ImmutableList.of("col_b", "col_a"), ImmutableList.of(B, A));
        assertRoundtrip(outputNode);
    }

    @Test
    public void testProject()
    {
        // note: the ToOldIrRelationalRewriter will assign the output symbol names:
        // - equal to input symbol names for identity assignments
        // - function name for function calls
        // - "expr", "expr_0", ... for other computing assignments
        // this is consistent with symbols created for the old IR in PlanBuilder#appendProjections(), with the difference that
        // we also derive meaningful names for function calls, following SymbolAllocator#newSymbol().
        // this test uses the same symbol names to enable roundtrip.

        // prune some fields
        ProjectNode pruningProjection = new ProjectNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                Assignments.of(B, new Reference(BOOLEAN, "b")));
        assertRoundtrip(pruningProjection);

        // prune all fields
        ProjectNode pruningAllFieldsProjection = new ProjectNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                Assignments.of());
        assertRoundtrip(pruningAllFieldsProjection);

        // compute expressions
        ResolvedFunction signFunction = FUNCTION_RESOLUTION.resolveFunction("sign", fromTypes(BIGINT));
        ProjectNode computingProjection = new ProjectNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                Assignments.builder()
                        .put(new Symbol(BOOLEAN, "expr"), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)))
                        .put(new Symbol(BIGINT, "sign"), new Call(signFunction, ImmutableList.of(new Reference(BIGINT, "a"))))
                        .put(new Symbol(BIGINT, "expr_0"), new Constant(BIGINT, 5L))
                        .build());
        assertRoundtrip(computingProjection);

        // mixed identity and computing assignments
        ProjectNode mixedProjection = new ProjectNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                Assignments.builder()
                        .put(new Symbol(BOOLEAN, "expr"), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)))
                        .put(B, new Reference(BOOLEAN, "b"))
                        .put(new Symbol(BIGINT, "sign"), new Call(signFunction, ImmutableList.of(new Reference(BIGINT, "a"))))
                        .put(A, new Reference(BIGINT, "a"))
                        .put(new Symbol(BIGINT, "expr_0"), new Constant(BIGINT, 5L))
                        .build());
        assertRoundtrip(mixedProjection);
    }

    @Test
    public void testProjectWithDuplicateIdentityReferences()
    {
        ProjectNode duplicateReferenceProjection = new ProjectNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                Assignments.builder()
                        .put(A, new Reference(BIGINT, "a"))
                        .put(new Symbol(BIGINT, "a_0"), new Reference(BIGINT, "a"))
                        .build());

        // using a populated SymbolAllocator to prove that input symbol is passed onto output without rename,
        // even though the allocator already knows this symbol
        assertRoundtrip(duplicateReferenceProjection, new SymbolAllocator(VALUES_NODE.getOutputSymbols()));
    }

    @Test
    public void testSort()
    {
        SortNode sortNode = new SortNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                new OrderingScheme(ImmutableList.of(B, A), ImmutableMap.of(B, DESC_NULLS_FIRST, A, ASC_NULLS_FIRST)),
                false);
        assertRoundtrip(sortNode);
    }

    @Test
    public void testTopN()
    {
        TopNNode topNNode = new TopNNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                5L,
                new OrderingScheme(ImmutableList.of(B), ImmutableMap.of(B, DESC_NULLS_FIRST)),
                TopNNode.Step.SINGLE);
        assertRoundtrip(topNNode);
    }

    @Test
    public void testTopNRanking()
    {
        TopNRankingNode topNRankingNode = new TopNRankingNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                new DataOrganizationSpecification(
                        ImmutableList.of(B),
                        Optional.of(new OrderingScheme(ImmutableList.of(A), ImmutableMap.of(A, DESC_NULLS_FIRST)))),
                TopNRankingNode.RankingType.ROW_NUMBER,
                new Symbol(BIGINT, "row_number"),
                10,
                false);
        assertRoundtrip(topNRankingNode);
    }

    @Test
    public void testUnion()
    {
        UnionNode unionNode = new UnionNode(
                new PlanNodeId("0"),
                ImmutableList.of(VALUES_NODE, ANOTHER_VALUES_NODE),
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .put(A, A)
                        .put(A, D)
                        .put(B, B)
                        .put(B, C)
                        .build(),
                ImmutableList.of(A, B));
        assertRoundtrip(unionNode);

        // union of two sources with duplicate input symbols
        Symbol a0 = new Symbol(BIGINT, "a_0");
        UnionNode unionNodeWithDuplicate = new UnionNode(
                new PlanNodeId("0"),
                ImmutableList.of(VALUES_NODE, ANOTHER_VALUES_NODE),
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        // the duplicate input symbols "a" from the first source and "d" from the second source are mapped to two distinct output symbols: "a" and "a_0"
                        .put(A, A)
                        .put(A, D)
                        .put(B, B)
                        .put(B, C)
                        .put(a0, A)
                        .put(a0, D)
                        .build(),
                ImmutableList.of(A, B, a0));
        assertRoundtrip(unionNodeWithDuplicate);
    }

    @Test
    public void testValues()
    {
        // note: the ToOldIrRelationalRewriter will assign default output symbol names: field, field_0, ...
        // this is consistent with symbols created for the old IR in RelationPlanner.
        // this test uses the same symbol names to enable roundtrip.

        ValuesNode valuesNode = new ValuesNode(
                new PlanNodeId("0"),
                ImmutableList.of(new Symbol(BIGINT, "field"), new Symbol(BOOLEAN, "field_0")),
                ImmutableList.of(
                        new Row(ImmutableList.of(new Constant(BIGINT, 42L), new Constant(BOOLEAN, true))),
                        new Cast(
                                new Row(ImmutableList.of(new Constant(BIGINT, null), new Constant(BOOLEAN, null))),
                                rowType(new RowType.Field(Optional.of("bigint_field"), BIGINT), new RowType.Field(Optional.of("boolean_field"), BOOLEAN)))));
        assertRoundtrip(valuesNode);

        // values without output fields
        ValuesNode noOutputFiledsValuesNode = new ValuesNode(new PlanNodeId("0"), 5);
        assertRoundtrip(noOutputFiledsValuesNode);

        ValuesNode noOutputFiledsEmptyValuesNode = new ValuesNode(new PlanNodeId("0"), 0);
        assertRoundtrip(noOutputFiledsEmptyValuesNode);

        // values without rows
        ValuesNode emptyValuesNode = new ValuesNode(
                new PlanNodeId("0"),
                ImmutableList.of(new Symbol(BIGINT, "field"), new Symbol(BOOLEAN, "field_0")));
        assertRoundtrip(emptyValuesNode);
    }

    @Test
    public void testWindow()
    {
        // note: the ToOldIrRelationalRewriter will assign the output symbol names for window functions based on resolved function name
        // this is consistent with symbols created for the old IR in QueryPlanner#planWindow(), with the difference that
        // we get the name from ResolvedFunction, and QueryPlanner gets the name from FunctionCall, so the result might differ in some cases,
        // when the FunctionCall uses a name alias.
        // this test uses the same symbol names to enable roundtrip.

        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        ResolvedFunction lagFunction = FUNCTION_RESOLUTION.resolveFunction("lag", fromTypes(BOOLEAN));

        WindowNode windowNode = new WindowNode(
                new PlanNodeId("0"),
                VALUES_NODE,
                new DataOrganizationSpecification(
                        ImmutableList.of(B),
                        Optional.of(new OrderingScheme(ImmutableList.of(B, A), ImmutableMap.of(B, ASC_NULLS_LAST, A, DESC_NULLS_FIRST)))),
                ImmutableMap.of(
                        new Symbol(BIGINT, "sum"),
                        new WindowNode.Function(
                                sumFunction,
                                ImmutableList.of(new Reference(BIGINT, "a")),
                                Optional.empty(),
                                DEFAULT_FRAME,
                                true,
                                false),
                        new Symbol(BOOLEAN, "lag"),
                        new WindowNode.Function(
                                lagFunction,
                                ImmutableList.of(new Reference(BOOLEAN, "b")),
                                Optional.of(new OrderingScheme(ImmutableList.of(A, B), ImmutableMap.of(B, ASC_NULLS_FIRST, A, DESC_NULLS_LAST))),
                                new WindowNode.Frame(RANGE, PRECEDING, Optional.empty(), Optional.empty(), FOLLOWING, Optional.of(B), Optional.of(A)),
                                false,
                                true)),
                ImmutableSet.of(B),
                1);
        assertRoundtrip(windowNode);
    }

    private void assertRoundtrip(PlanNode originalPlanNode)
    {
        assertRoundtrip(originalPlanNode, new SymbolAllocator());
    }

    private void assertRoundtrip(PlanNode originalPlanNode, SymbolAllocator symbolAllocator)
    {
        PlanNode roundtripPlanNode = roundtripPlanNode(originalPlanNode, symbolAllocator);
        assertThat(roundtripPlanNode)
                .usingRecursiveComparison()
                .isEqualTo(originalPlanNode);
    }

    private PlanNode roundtripPlanNode(PlanNode planNode, SymbolAllocator symbolAllocator)
    {
        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ProgramBuilder.ValueNameAllocator());
        Block.Builder block = new Block.Builder(Optional.empty(), ImmutableList.of());
        Operation rewrittenOperation = planNode.accept(relationalProgramBuilder, new Context(block)).operation();

        // rewrite of TableScan involves a metadata call to resolve column names. This test uses the test metadata manager, which does not support it, so we don't test TableScan rewrite.
        // TODO test TableScan rewrite
        ToOldIrRelationalRewriter rewriter = new ToOldIrRelationalRewriter(new PlanNodeIdAllocator(), symbolAllocator, new ToOldIrScalarRewriter(symbolAllocator), testSession(), createTestingMetadataManager());
        return ((TrinoOperation) rewrittenOperation).accept(rewriter, planNode.getSources());
    }
}
