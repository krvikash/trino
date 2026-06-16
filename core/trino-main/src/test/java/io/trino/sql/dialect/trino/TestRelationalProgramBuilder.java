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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogHandle;
import io.trino.connector.TestingColumnHandle;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.ProgramBuilder.ValueNameAllocator;
import io.trino.sql.dialect.trino.RelationalProgramBuilder.OperationAndMapping;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.AssignUniqueId;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.CorrelatedJoin;
import io.trino.sql.dialect.trino.operation.DynamicFilterSource;
import io.trino.sql.dialect.trino.operation.EnforceSingleRow;
import io.trino.sql.dialect.trino.operation.Except;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.ExplainAnalyze;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.GroupId;
import io.trino.sql.dialect.trino.operation.Intersect;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Limit;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.SemiJoin;
import io.trino.sql.dialect.trino.operation.Sort;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TopN;
import io.trino.sql.dialect.trino.operation.TopNRanking;
import io.trino.sql.dialect.trino.operation.Union;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operation.Window;
import io.trino.sql.dialect.trino.operation.WindowFunctionCall;
import io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CorrelatedJoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ConstantValues;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.Statistics;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata;
import io.trino.sql.ir.Reference;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.GroupingSetDescriptor;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.FrameBoundType;
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
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowFrameType;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Identifier;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.deriveOutputMapping;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.mapStatistics;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operation.Values.valuesWithoutFields;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.AggregationStep.SINGLE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeScope.REMOTE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeType.GATHER;
import static io.trino.sql.dialect.trino.operationmetadata.ExplainAnalyzeOperationMetadata.VERBOSE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DistributionType.REPLICATED;
import static io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata.DistributionType.PARTITIONED;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.TopNStep.FINAL;
import static io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.RankingType.RANK;
import static io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.RankingType.ROW_NUMBER;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameBoundType.FOLLOWING;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameBoundType.PRECEDING;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameType.RANGE;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static java.lang.Boolean.TRUE;
import static java.lang.Double.NaN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRelationalProgramBuilder
{
    private static final ValuesNode VALUES_NODE = valuesNode();
    private static final Values VALUES_OPERATION = valuesOperation();
    private static final Type VALUES_OPERATION_ROW_TYPE = irType(relationRowType(trinoType(VALUES_OPERATION.result().type())));
    private static final ValuesNode SECOND_VALUES_NODE = secondValuesNode();
    private static final Values SECOND_VALUES_OPERATION = secondValuesOperation();
    private static final Type SECOND_VALUES_OPERATION_ROW_TYPE = irType(relationRowType(trinoType(SECOND_VALUES_OPERATION.result().type())));
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final ResolvedFunction LESS_THAN_BIGINT = FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(BIGINT, BIGINT));

    private static Attributes attributes(Consumer<Attributes.Builder> attributesConsumer)
    {
        Attributes.Builder builder = Attributes.builder();
        attributesConsumer.accept(builder);
        return builder.buildOrThrow();
    }

    @Test
    public void testAggregation()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));

        AggregationNode aggregationNode = new AggregationNode(
                new PlanNodeId("aggregation"),
                VALUES_NODE,
                ImmutableMap.of(
                        new Symbol(BIGINT, "sum_agg"), new AggregationNode.Aggregation(
                                sumFunction,
                                ImmutableList.of(new Reference(BIGINT, "a")),
                                false,
                                Optional.of(new Symbol(BOOLEAN, "b")),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST, new Symbol(BIGINT, "a"), DESC_NULLS_LAST))),
                                Optional.empty())),
                new GroupingSetDescriptor(ImmutableList.of(new Symbol(BOOLEAN, "b")), 1, ImmutableSet.of()),
                ImmutableList.of(new Symbol(BOOLEAN, "b")),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.of(true));

        // aggregate parameter
        Block.Parameter aggregateParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION.result().type());

        // aggregate argument
        Block.Parameter argumentsParameter = new Block.Parameter(
                "%12",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationArgument = new FieldReference("%13", argumentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationArgument = new Row("%14", ImmutableList.of(fieldReferenceOperationArgument.result()), ImmutableList.of(fieldReferenceOperationArgument.attributes()));
        Return returnOperationArgument = new Return("%15", rowOperationArgument.result(), rowOperationArgument.attributes());

        // aggregate filter
        Block.Parameter filterParameter = new Block.Parameter(
                "%16",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationFilter = new FieldReference("%17", filterParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationFilter = new Row("%18", ImmutableList.of(fieldReferenceOperationFilter.result()), ImmutableList.of(fieldReferenceOperationFilter.attributes()));
        Return returnOperationFilter = new Return("%19", rowOperationFilter.result(), rowOperationFilter.attributes());

        // aggregate mask
        Block.Parameter maskParameter = new Block.Parameter(
                "%20",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperationMask = new Constant("%21", EMPTY_ROW, null);
        Return returnOperationMask = new Return("%22", constantOperationMask.result(), constantOperationMask.attributes());

        // aggregate ordering
        Block.Parameter orderingParameter = new Block.Parameter(
                "%23",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingA = new FieldReference("%24", orderingParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%25", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row(
                "%26",
                ImmutableList.of(fieldReferenceOperationOrderingA.result(), fieldReferenceOperationOrderingB.result()),
                ImmutableList.of(fieldReferenceOperationOrderingA.attributes(), fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%27", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        AggregateCall aggregateCallOperation = new AggregateCall(
                "%11",
                aggregateParameter,
                BIGINT,
                new Block(
                        Optional.of("^arguments"),
                        ImmutableList.of(argumentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationArgument,
                                rowOperationArgument,
                                returnOperationArgument)),
                new Block(
                        Optional.of("^filterSelector"),
                        ImmutableList.of(filterParameter),
                        ImmutableList.of(
                                fieldReferenceOperationFilter,
                                rowOperationFilter,
                                returnOperationFilter)),
                new Block(
                        Optional.of("^maskSelector"),
                        ImmutableList.of(maskParameter),
                        ImmutableList.of(
                                constantOperationMask,
                                returnOperationMask)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingA,
                                fieldReferenceOperationOrderingB,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                Optional.of(new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST))),
                sumFunction,
                false,
                AggregateCallOperationMetadata.AggregationStep.SINGLE);

        // collecting aggregate functions in a row
        Row aggregatesRowOperation = new Row("%28", ImmutableList.of(aggregateCallOperation.result()), ImmutableList.of(aggregateCallOperation.attributes()));
        Return aggregatesReturnOperation = new Return("%29", aggregatesRowOperation.result(), aggregatesRowOperation.attributes());

        // grouping keys
        Block.Parameter groupingKeysParameter = new Block.Parameter(
                "%30",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationGroupingKeys = new FieldReference("%31", groupingKeysParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationGroupingKeys = new Row("%32", ImmutableList.of(fieldReferenceOperationGroupingKeys.result()), ImmutableList.of(fieldReferenceOperationGroupingKeys.attributes()));
        Return returnOperationGroupingKeys = new Return("%33", rowOperationGroupingKeys.result(), rowOperationGroupingKeys.attributes());

        Aggregation aggregationOperation = new Aggregation(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^aggregates"),
                        ImmutableList.of(aggregateParameter),
                        ImmutableList.of(
                                aggregateCallOperation,
                                aggregatesRowOperation,
                                aggregatesReturnOperation)),
                new Block(
                        Optional.of("^groupingKeysSelector"),
                        ImmutableList.of(groupingKeysParameter),
                        ImmutableList.of(
                                fieldReferenceOperationGroupingKeys,
                                rowOperationGroupingKeys,
                                returnOperationGroupingKeys)),
                1,
                ImmutableList.of(),
                OptionalInt.empty(),
                ImmutableList.of(0),
                SINGLE,
                true,
                VALUES_OPERATION.attributes());

        assertProgram(
                aggregationNode,
                ImmutableList.of(VALUES_OPERATION, aggregationOperation),
                new MultisetType(anonymousRow(BOOLEAN, BIGINT)),
                ImmutableMap.of(
                        new Symbol(BOOLEAN, "b"), 0,
                        new Symbol(BIGINT, "sum_agg"), 1));

        assertThat(aggregateCallOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    AggregateCallOperationMetadata.SORT_ORDERS.putAttribute(builder, new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)));
                    AggregateCallOperationMetadata.RESOLVED_FUNCTION.putAttribute(builder, sumFunction);
                    AggregateCallOperationMetadata.DISTINCT.putAttribute(builder, false);
                    AggregateCallOperationMetadata.AGGREGATION_STEP.putAttribute(builder, AggregateCallOperationMetadata.AggregationStep.SINGLE);
                    AggregateCallOperationMetadata.RESULT_TYPE.putAttribute(builder, BIGINT);
                }));

        assertThat(aggregationOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    AggregationOperationMetadata.GROUPING_SETS_COUNT.putAttribute(builder, 1);
                    AggregationOperationMetadata.GLOBAL_GROUPING_SETS.putAttribute(builder, ImmutableList.of());
                    AggregationOperationMetadata.PRE_GROUPED_INDEXES.putAttribute(builder, ImmutableList.of(0));
                    AggregationOperationMetadata.AGGREGATION_STEP.putAttribute(builder, SINGLE);
                    AggregationOperationMetadata.INPUT_REDUCING.putAttribute(builder, true);
                }));
    }

    @Test
    public void testAssignUniqueId()
    {
        io.trino.sql.planner.plan.AssignUniqueId assignUniqueIdNode = new io.trino.sql.planner.plan.AssignUniqueId(
                new PlanNodeId("assign_unique_id"),
                VALUES_NODE,
                new Symbol(BIGINT, "unique"));

        AssignUniqueId assignUniqueIdOperation = new AssignUniqueId(
                "%9",
                VALUES_OPERATION.result(),
                VALUES_OPERATION.attributes());

        assertProgram(
                assignUniqueIdNode,
                ImmutableList.of(VALUES_OPERATION, assignUniqueIdOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BIGINT)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BIGINT, "unique"), 2));
    }

    @Test
    public void testCorrelatedJoin()
    {
        CorrelatedJoinNode correlatedJoinNode = new CorrelatedJoinNode(
                new PlanNodeId("correlated_join"),
                VALUES_NODE,
                new ValuesNode(
                        new PlanNodeId("correlated_values"),
                        ImmutableList.of(new Symbol(BOOLEAN, "c")),
                        ImmutableList.of(new io.trino.sql.ir.Row(ImmutableList.of(new Reference(BOOLEAN, "b"))))), // correlated values
                ImmutableList.of(new Symbol(BOOLEAN, "b")),
                LEFT,
                new Reference(BOOLEAN, "b"),
                new Identifier("origin subquery - whatever"));

        // correlation
        Block.Parameter correlationParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationCorrelation = new FieldReference("%11", correlationParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationCorrelation = new Row("%12", ImmutableList.of(fieldReferenceOperationCorrelation.result()), ImmutableList.of(fieldReferenceOperationCorrelation.attributes()));
        Return returnOperationCorrelation = new Return("%13", rowOperationCorrelation.result(), rowOperationCorrelation.attributes());

        // subquery
        Block.Parameter subqueryParameter = new Block.Parameter(
                "%14",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationSubquery = new FieldReference("%16", subqueryParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationSubquery = new Row("%17", ImmutableList.of(fieldReferenceOperationSubquery.result()), ImmutableList.of(fieldReferenceOperationSubquery.attributes()));
        Return returnOperationSubqueryRow = new Return("%18", rowOperationSubquery.result(), rowOperationSubquery.attributes());
        Values valuesOperationSubquery = new Values(
                "%15",
                RowType.anonymous(ImmutableList.of(BOOLEAN)),
                ImmutableList.of(new Block(
                        Optional.of("^row"),
                        ImmutableList.of(),
                        ImmutableList.of(
                                fieldReferenceOperationSubquery,
                                rowOperationSubquery,
                                returnOperationSubqueryRow))));
        Return returnOperationSubquery = new Return("%19", valuesOperationSubquery.result(), valuesOperationSubquery.attributes());

        // filter

        // input row
        Block.Parameter firstFilterParameter = new Block.Parameter(
                "%20",
                VALUES_OPERATION_ROW_TYPE);
        // subquery row
        Block.Parameter secondFilterParameter = new Block.Parameter(
                "%21",
                irType(anonymousRow(BOOLEAN)));
        FieldReference fieldReferenceOperationFilter = new FieldReference("%22", firstFilterParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperationFilter = new Return("%23", fieldReferenceOperationFilter.result(), fieldReferenceOperationFilter.attributes());

        CorrelatedJoin correlatedJoinOperation = new CorrelatedJoin(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^correlationSelector"),
                        ImmutableList.of(correlationParameter),
                        ImmutableList.of(
                                fieldReferenceOperationCorrelation,
                                rowOperationCorrelation,
                                returnOperationCorrelation)),
                new Block(
                        Optional.of("^subquery"),
                        ImmutableList.of(subqueryParameter),
                        ImmutableList.of(
                                valuesOperationSubquery,
                                returnOperationSubquery)),
                new Block(
                        Optional.of("^filter"),
                        ImmutableList.of(firstFilterParameter, secondFilterParameter),
                        ImmutableList.of(
                                fieldReferenceOperationFilter,
                                returnOperationFilter)),
                CorrelatedJoinOperationMetadata.JoinType.LEFT,
                VALUES_OPERATION.attributes());

        assertProgram(
                correlatedJoinNode,
                ImmutableList.of(VALUES_OPERATION, correlatedJoinOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BOOLEAN, "c"), 2));
    }

    @Test
    public void testDynamicFilterSource()
    {
        DynamicFilterSourceNode dynamicFilterSourceNode = new DynamicFilterSourceNode(
                new PlanNodeId("dynamic_filter_source"),
                VALUES_NODE,
                ImmutableMap.of(
                        new DynamicFilterId("first_dynamic_filter"), new Symbol(BOOLEAN, "b"),
                        new DynamicFilterId("second_dynamic_filter"), new Symbol(BIGINT, "a")));

        // dynamic filter targets
        Block.Parameter dynamicFilterTargetsParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", dynamicFilterTargetsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%12", dynamicFilterTargetsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationA.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%14", rowOperation.result(), rowOperation.attributes());

        DynamicFilterSource dynamicFilterSourceOperation = new DynamicFilterSource(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^dynamicFilterTargetSelector"),
                        ImmutableList.of(dynamicFilterTargetsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                fieldReferenceOperationA,
                                rowOperation,
                                returnOperation)),
                ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                VALUES_OPERATION.attributes());

        assertProgram(
                dynamicFilterSourceNode,
                ImmutableList.of(VALUES_OPERATION, dynamicFilterSourceOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testEnforceSingleRow()
    {
        EnforceSingleRowNode enforceSingleRowNode = new EnforceSingleRowNode(
                new PlanNodeId("enforce_single_row"),
                VALUES_NODE);

        EnforceSingleRow enforceSingleRowOperation = new EnforceSingleRow(
                "%9",
                VALUES_OPERATION.result(),
                VALUES_OPERATION.attributes());

        assertProgram(
                enforceSingleRowNode,
                ImmutableList.of(VALUES_OPERATION, enforceSingleRowOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testExcept()
    {
        ExceptNode exceptNode = new ExceptNode(
                new PlanNodeId("except"),
                ImmutableList.of(VALUES_NODE, SECOND_VALUES_NODE),
                ImmutableListMultimap.of(
                        new Symbol(BIGINT, "x"), new Symbol(BIGINT, "a"),
                        new Symbol(BIGINT, "x"), new Symbol(BIGINT, "d"),
                        new Symbol(BOOLEAN, "y"), new Symbol(BOOLEAN, "b"),
                        new Symbol(BOOLEAN, "y"), new Symbol(BOOLEAN, "c")),
                ImmutableList.of(new Symbol(BIGINT, "x"), new Symbol(BOOLEAN, "y")),
                true);

        // except input selector for the first source
        Block.Parameter firstSelectorParameter = new Block.Parameter("%15", VALUES_OPERATION_ROW_TYPE);
        FieldReference firstSelectorFieldReferenceA = new FieldReference("%16", firstSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference firstSelectorFieldReferenceB = new FieldReference("%17", firstSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row firstSelectorRow = new Row(
                "%18",
                ImmutableList.of(firstSelectorFieldReferenceA.result(), firstSelectorFieldReferenceB.result()),
                ImmutableList.of(firstSelectorFieldReferenceA.attributes(), firstSelectorFieldReferenceB.attributes()));
        Return firstSelectorReturn = new Return("%19", firstSelectorRow.result(), firstSelectorRow.attributes());

        // except input selector for the second source
        Block.Parameter secondSelectorParameter = new Block.Parameter("%20", SECOND_VALUES_OPERATION_ROW_TYPE);
        FieldReference secondSelectorFieldReferenceD = new FieldReference("%21", secondSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondSelectorFieldReferenceC = new FieldReference("%22", secondSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row secondSelectorRow = new Row(
                "%23",
                ImmutableList.of(secondSelectorFieldReferenceD.result(), secondSelectorFieldReferenceC.result()),
                ImmutableList.of(secondSelectorFieldReferenceD.attributes(), secondSelectorFieldReferenceC.attributes()));
        Return secondSelectorReturn = new Return("%24", secondSelectorRow.result(), secondSelectorRow.attributes());

        Except exceptOperation = new Except(
                "%14",
                ImmutableList.of(VALUES_OPERATION.result(), SECOND_VALUES_OPERATION.result()),
                ImmutableList.of(
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(firstSelectorParameter),
                                ImmutableList.of(
                                        firstSelectorFieldReferenceA,
                                        firstSelectorFieldReferenceB,
                                        firstSelectorRow,
                                        firstSelectorReturn)),
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(secondSelectorParameter),
                                ImmutableList.of(
                                        secondSelectorFieldReferenceD,
                                        secondSelectorFieldReferenceC,
                                        secondSelectorRow,
                                        secondSelectorReturn))),
                true,
                ImmutableList.of(VALUES_OPERATION.attributes(), SECOND_VALUES_OPERATION.attributes()));

        assertProgram(
                exceptNode,
                ImmutableList.of(VALUES_OPERATION, SECOND_VALUES_OPERATION, exceptOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "x"), 0,
                        new Symbol(BOOLEAN, "y"), 1));
    }

    @Test
    public void testExchange()
    {
        ConnectorPartitioningHandle testingPartitioningHandle = new ConnectorPartitioningHandle() {};

        ExchangeNode exchangeNode = new ExchangeNode(
                new PlanNodeId("exchange"),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(
                        Partitioning.create(
                                new PartitioningHandle(
                                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                        testingPartitioningHandle),
                                ImmutableList.of(new Symbol(BIGINT, "f"))),
                        ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g")),
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
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                        ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e"))),
                Optional.empty());

        // right source -- values with no rows
        Values rightSourceOperation = new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(SMALLINT, BIGINT, BOOLEAN)),
                ImmutableList.of());
        Type rightRowType = irType(relationRowType(trinoType(rightSourceOperation.result().type())));

        // input field selectors for exchange
        Block.Parameter leftInputsParameter = new Block.Parameter(
                "%11",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftInputsA = new FieldReference("%12", leftInputsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationLeftInputsB = new FieldReference("%13", leftInputsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationLeftInputs = new Row(
                "%14",
                ImmutableList.of(fieldReferenceOperationLeftInputsA.result(), fieldReferenceOperationLeftInputsB.result()),
                ImmutableList.of(fieldReferenceOperationLeftInputsA.attributes(), fieldReferenceOperationLeftInputsB.attributes()));
        Return returnOperationLeftInputs = new Return("%15", rowOperationLeftInputs.result(), rowOperationLeftInputs.attributes());

        Block.Parameter rightInputsParameter = new Block.Parameter(
                "%16",
                rightRowType);
        FieldReference fieldReferenceOperationRightInputsD = new FieldReference("%17", rightInputsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationRightInputsE = new FieldReference("%18", rightInputsParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationRightInputs = new Row(
                "%19",
                ImmutableList.of(fieldReferenceOperationRightInputsD.result(), fieldReferenceOperationRightInputsE.result()),
                ImmutableList.of(fieldReferenceOperationRightInputsD.attributes(), fieldReferenceOperationRightInputsE.attributes()));
        Return returnOperationRightInputs = new Return("%20", rowOperationRightInputs.result(), rowOperationRightInputs.attributes());

        RowType exchangeOutputRowType = anonymousRow(BIGINT, BOOLEAN);

        // partitioning bound arguments
        Block.Parameter boundArgumentsParameter = new Block.Parameter(
                "%21",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationF = new FieldReference("%22", boundArgumentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationBoundArguments = new Row("%23", ImmutableList.of(fieldReferenceOperationF.result()), ImmutableList.of(fieldReferenceOperationF.attributes()));
        Return returnOperationBoundArguments = new Return("%24", rowOperationBoundArguments.result(), rowOperationBoundArguments.attributes());

        // order by
        Block.Parameter orderByParameter = new Block.Parameter(
                "%25",
                irType(exchangeOutputRowType));
        Constant constantOperationOrderBy = new Constant("%26", EMPTY_ROW, null);
        Return returnOperationOrderBy = new Return("%27", constantOperationOrderBy.result(), constantOperationOrderBy.attributes());

        ConstantValues constantValues = new ConstantValues(new ConstantValue[] {null});
        Exchange exchangeOperation = new Exchange(
                "%10",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(leftInputsParameter),
                                ImmutableList.of(
                                        fieldReferenceOperationLeftInputsA,
                                        fieldReferenceOperationLeftInputsB,
                                        rowOperationLeftInputs,
                                        returnOperationLeftInputs)),
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(rightInputsParameter),
                                ImmutableList.of(
                                        fieldReferenceOperationRightInputsD,
                                        fieldReferenceOperationRightInputsE,
                                        rowOperationRightInputs,
                                        returnOperationRightInputs))),
                new Block(
                        Optional.of("^boundArguments"),
                        ImmutableList.of(boundArgumentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationF,
                                rowOperationBoundArguments,
                                returnOperationBoundArguments)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderByParameter),
                        ImmutableList.of(
                                constantOperationOrderBy,
                                returnOperationOrderBy)),
                GATHER,
                REMOTE,
                new PartitioningHandle(
                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                        testingPartitioningHandle),
                constantValues,
                false,
                Optional.of(ImmutableList.of(5, 6, 7)),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                ImmutableList.of(VALUES_OPERATION.attributes(), rightSourceOperation.attributes()));

        assertProgram(
                exchangeNode,
                ImmutableList.of(VALUES_OPERATION, rightSourceOperation, exchangeOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "f"), 0,
                        new Symbol(BOOLEAN, "g"), 1));

        assertThat(exchangeOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    ExchangeOperationMetadata.EXCHANGE_TYPE.putAttribute(builder, GATHER);
                    ExchangeOperationMetadata.EXCHANGE_SCOPE.putAttribute(builder, REMOTE);
                    ExchangeOperationMetadata.PARTITIONING_HANDLE.putAttribute(builder, new PartitioningHandle(
                            Optional.of(CatalogHandle.fromId("bla:normal:1")),
                            Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                            testingPartitioningHandle));
                    ExchangeOperationMetadata.CONSTANT_VALUES.putAttribute(builder, constantValues);
                    ExchangeOperationMetadata.REPLICATE_NULLS_AND_ANY.putAttribute(builder, false);
                    ExchangeOperationMetadata.BUCKET_TO_PARTITION.putAttribute(builder, ImmutableList.of(5, 6, 7));
                }));
    }

    @Test
    public void testMergingExchange()
    {
        ExchangeNode exchangeNode = new ExchangeNode(
                new PlanNodeId("exchange"),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(
                        Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of(new Symbol(BIGINT, "f"))),
                        ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g")),
                        false,
                        Optional.of(new int[] {5, 6, 7}),
                        OptionalInt.empty(),
                        OptionalInt.of(10)),
                ImmutableList.of(VALUES_NODE),
                ImmutableList.of(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))),
                Optional.of(new OrderingScheme(
                        ImmutableList.of(new Symbol(BIGINT, "f")),
                        ImmutableMap.of(new Symbol(BIGINT, "f"), DESC_NULLS_FIRST))));

        // input field selector for exchange
        Block.Parameter inputsParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationInputsA = new FieldReference("%11", inputsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationInputsB = new FieldReference("%12", inputsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationInputs = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationInputsA.result(), fieldReferenceOperationInputsB.result()),
                ImmutableList.of(fieldReferenceOperationInputsA.attributes(), fieldReferenceOperationInputsB.attributes()));
        Return returnOperationInputs = new Return("%14", rowOperationInputs.result(), rowOperationInputs.attributes());

        RowType exchangeOutputRowType = anonymousRow(BIGINT, BOOLEAN);

        // partitioning bound arguments
        Block.Parameter boundArgumentsParameter = new Block.Parameter(
                "%15",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationF = new FieldReference("%16", boundArgumentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationBoundArguments = new Row("%17", ImmutableList.of(fieldReferenceOperationF.result()), ImmutableList.of(fieldReferenceOperationF.attributes()));
        Return returnOperationBoundArguments = new Return("%18", rowOperationBoundArguments.result(), rowOperationBoundArguments.attributes());

        // order by
        Block.Parameter orderByParameter = new Block.Parameter(
                "%19",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationOrderBy = new FieldReference("%20", orderByParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrderBy = new Row("%21", ImmutableList.of(fieldReferenceOperationOrderBy.result()), ImmutableList.of(fieldReferenceOperationOrderBy.attributes()));
        Return returnOperationOrderBy = new Return("%22", rowOperationOrderBy.result(), rowOperationOrderBy.attributes());

        ConstantValues constantValues = new ConstantValues(new ConstantValue[] {null});
        Exchange exchangeOperation = new Exchange(
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(inputsParameter),
                                ImmutableList.of(
                                        fieldReferenceOperationInputsA,
                                        fieldReferenceOperationInputsB,
                                        rowOperationInputs,
                                        returnOperationInputs))),
                new Block(
                        Optional.of("^boundArguments"),
                        ImmutableList.of(boundArgumentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationF,
                                rowOperationBoundArguments,
                                returnOperationBoundArguments)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderByParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderBy,
                                rowOperationOrderBy,
                                returnOperationOrderBy)),
                GATHER,
                REMOTE,
                SINGLE_DISTRIBUTION,
                constantValues,
                false,
                Optional.of(ImmutableList.of(5, 6, 7)),
                OptionalInt.of(10),
                OptionalInt.empty(),
                Optional.of(new SortOrderList(ImmutableList.of(DESC_NULLS_FIRST))),
                ImmutableList.of(VALUES_OPERATION.attributes()));

        assertProgram(
                exchangeNode,
                ImmutableList.of(VALUES_OPERATION, exchangeOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "f"), 0,
                        new Symbol(BOOLEAN, "g"), 1));

        assertThat(exchangeOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    ExchangeOperationMetadata.EXCHANGE_TYPE.putAttribute(builder, GATHER);
                    ExchangeOperationMetadata.EXCHANGE_SCOPE.putAttribute(builder, REMOTE);
                    ExchangeOperationMetadata.PARTITIONING_HANDLE.putAttribute(builder, SINGLE_DISTRIBUTION);
                    ExchangeOperationMetadata.CONSTANT_VALUES.putAttribute(builder, constantValues);
                    ExchangeOperationMetadata.REPLICATE_NULLS_AND_ANY.putAttribute(builder, false);
                    ExchangeOperationMetadata.BUCKET_TO_PARTITION.putAttribute(builder, ImmutableList.of(5, 6, 7));
                    ExchangeOperationMetadata.PARTITION_COUNT.putAttribute(builder, 10);
                    ExchangeOperationMetadata.SORT_ORDERS.putAttribute(builder, new SortOrderList(ImmutableList.of(DESC_NULLS_FIRST)));
                }));
    }

    @Test
    public void testExplainAnalyze()
    {
        ExplainAnalyzeNode explainAnalyzeNode = new ExplainAnalyzeNode(
                new PlanNodeId("explain_analyze"),
                VALUES_NODE,
                new Symbol(VARCHAR, "Query Plan"),
                ImmutableList.of(new Symbol(BOOLEAN, "b")),
                true);

        Block.Parameter fieldSelectorParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperation = new FieldReference("%11", fieldSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row("%12", ImmutableList.of(fieldReferenceOperation.result()), ImmutableList.of(fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%13", rowOperation.result(), rowOperation.attributes());
        ExplainAnalyze explainAnalyzeOperation = new ExplainAnalyze(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^inputFieldSelector"),
                        ImmutableList.of(fieldSelectorParameter),
                        ImmutableList.of(
                                fieldReferenceOperation,
                                rowOperation,
                                returnOperation)),
                true,
                VALUES_OPERATION.attributes());

        assertProgram(
                explainAnalyzeNode,
                ImmutableList.of(VALUES_OPERATION, explainAnalyzeOperation),
                new MultisetType(anonymousRow(VARCHAR)),
                ImmutableMap.of(new Symbol(VARCHAR, "Query Plan"), 0));

        assertThat(explainAnalyzeOperation.operationAttributes())
                .isEqualTo(VERBOSE.asAttributes(true));
    }

    @Test
    public void testFilter()
    {
        FilterNode filterNode = new FilterNode(
                new PlanNodeId("filter"),
                VALUES_NODE,
                new io.trino.sql.ir.Call(
                        LESS_THAN_BIGINT,
                        ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 5L), new Reference(BIGINT, "a"))));

        Block.Parameter predicateParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperation = new Constant("%11", BIGINT, 5L);
        FieldReference fieldReferenceOperation = new FieldReference("%12", predicateParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Call comparisonOperation = new Call(
                "%13",
                ImmutableList.of(constantOperation.result(), fieldReferenceOperation.result()),
                LESS_THAN_BIGINT,
                ImmutableList.of(constantOperation.attributes(), fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%14", comparisonOperation.result(), comparisonOperation.attributes());
        Filter filterOperation = new Filter(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^predicate"),
                        ImmutableList.of(predicateParameter),
                        ImmutableList.of(
                                constantOperation,
                                fieldReferenceOperation,
                                comparisonOperation,
                                returnOperation)),
                VALUES_OPERATION.attributes());

        assertProgram(
                filterNode,
                ImmutableList.of(VALUES_OPERATION, filterOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testGroupId()
    {
        GroupIdNode groupIdNode = new GroupIdNode(
                new PlanNodeId("groupId"),
                VALUES_NODE,
                ImmutableList.of(
                        // Note: both grouping sets use symbol a_gid
                        ImmutableList.of(new Symbol(BOOLEAN, "c_gid"), new Symbol(BIGINT, "a_gid")),
                        ImmutableList.of(new Symbol(BOOLEAN, "b_gid"), new Symbol(BIGINT, "a_gid"))),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a_gid"), new Symbol(BIGINT, "a"),
                        // Note: symbols b_gid and c_gid refer to the same input symbol b
                        new Symbol(BOOLEAN, "b_gid"), new Symbol(BOOLEAN, "b"),
                        new Symbol(BOOLEAN, "c_gid"), new Symbol(BOOLEAN, "b")),
                ImmutableList.of(new Symbol(BIGINT, "a")),
                new Symbol(BIGINT, "groupId"));

        // grouping columns selector
        Block.Parameter groupingColumnsSelectorParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceA = new FieldReference("%11", groupingColumnsSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceB = new FieldReference("%12", groupingColumnsSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceBAnother = new FieldReference("%13", groupingColumnsSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationGroupingColumns = new Row(
                "%14",
                ImmutableList.of(fieldReferenceA.result(), fieldReferenceB.result(), fieldReferenceBAnother.result()),
                ImmutableList.of(fieldReferenceA.attributes(), fieldReferenceB.attributes(), fieldReferenceBAnother.attributes()));
        Return returnOperationGroupingColumns = new Return("%15", rowOperationGroupingColumns.result(), rowOperationGroupingColumns.attributes());

        // aggregation arguments selector
        Block.Parameter aggregationArgumentsSelectorParameter = new Block.Parameter(
                "%16",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceAAnother = new FieldReference("%17", aggregationArgumentsSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationAggregationArguments = new Row("%18", ImmutableList.of(fieldReferenceAAnother.result()), ImmutableList.of(fieldReferenceAAnother.attributes()));
        Return returnOperationAggregationArguments = new Return("%19", rowOperationAggregationArguments.result(), rowOperationAggregationArguments.attributes());

        GroupId groupIdOperation = new GroupId(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^groupingColumnsSelector"),
                        ImmutableList.of(groupingColumnsSelectorParameter),
                        ImmutableList.of(
                                fieldReferenceA,
                                fieldReferenceB,
                                fieldReferenceBAnother,
                                rowOperationGroupingColumns,
                                returnOperationGroupingColumns)),
                new Block(
                        Optional.of("^aggregationArgumentsSelector"),
                        ImmutableList.of(aggregationArgumentsSelectorParameter),
                        ImmutableList.of(
                                fieldReferenceAAnother,
                                rowOperationAggregationArguments,
                                returnOperationAggregationArguments)),
                ImmutableList.of(ImmutableList.of(2, 0), ImmutableList.of(1, 0)),
                VALUES_OPERATION.attributes());

        assertProgram(
                groupIdNode,
                ImmutableList.of(VALUES_OPERATION, groupIdOperation),
                new MultisetType(anonymousRow(BOOLEAN, BIGINT, BOOLEAN, BIGINT, BIGINT)),
                ImmutableMap.of(
                        new Symbol(BOOLEAN, "c_gid"), 0,
                        new Symbol(BIGINT, "a_gid"), 1,
                        new Symbol(BOOLEAN, "b_gid"), 2,
                        new Symbol(BIGINT, "a"), 3,
                        new Symbol(BIGINT, "groupId"), 4));
    }

    @Test
    public void testIntersect()
    {
        IntersectNode intersectNode = new IntersectNode(
                new PlanNodeId("intersect"),
                ImmutableList.of(VALUES_NODE, SECOND_VALUES_NODE),
                ImmutableListMultimap.of(
                        new Symbol(BIGINT, "x"), new Symbol(BIGINT, "a"),
                        new Symbol(BIGINT, "x"), new Symbol(BIGINT, "d"),
                        new Symbol(BOOLEAN, "y"), new Symbol(BOOLEAN, "b"),
                        new Symbol(BOOLEAN, "y"), new Symbol(BOOLEAN, "c")),
                ImmutableList.of(new Symbol(BIGINT, "x"), new Symbol(BOOLEAN, "y")),
                false);

        // intersect input selector for the first source
        Block.Parameter firstSelectorParameter = new Block.Parameter("%15", VALUES_OPERATION_ROW_TYPE);
        FieldReference firstSelectorFieldReferenceA = new FieldReference("%16", firstSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference firstSelectorFieldReferenceB = new FieldReference("%17", firstSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row firstSelectorRow = new Row(
                "%18",
                ImmutableList.of(firstSelectorFieldReferenceA.result(), firstSelectorFieldReferenceB.result()),
                ImmutableList.of(firstSelectorFieldReferenceA.attributes(), firstSelectorFieldReferenceB.attributes()));
        Return firstSelectorReturn = new Return("%19", firstSelectorRow.result(), firstSelectorRow.attributes());

        // intersect input selector for the second source
        Block.Parameter secondSelectorParameter = new Block.Parameter("%20", SECOND_VALUES_OPERATION_ROW_TYPE);
        FieldReference secondSelectorFieldReferenceD = new FieldReference("%21", secondSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondSelectorFieldReferenceC = new FieldReference("%22", secondSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row secondSelectorRow = new Row(
                "%23",
                ImmutableList.of(secondSelectorFieldReferenceD.result(), secondSelectorFieldReferenceC.result()),
                ImmutableList.of(secondSelectorFieldReferenceD.attributes(), secondSelectorFieldReferenceC.attributes()));
        Return secondSelectorReturn = new Return("%24", secondSelectorRow.result(), secondSelectorRow.attributes());

        Intersect intersectOperation = new Intersect(
                "%14",
                ImmutableList.of(VALUES_OPERATION.result(), SECOND_VALUES_OPERATION.result()),
                ImmutableList.of(
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(firstSelectorParameter),
                                ImmutableList.of(
                                        firstSelectorFieldReferenceA,
                                        firstSelectorFieldReferenceB,
                                        firstSelectorRow,
                                        firstSelectorReturn)),
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(secondSelectorParameter),
                                ImmutableList.of(
                                        secondSelectorFieldReferenceD,
                                        secondSelectorFieldReferenceC,
                                        secondSelectorRow,
                                        secondSelectorReturn))),
                false,
                ImmutableList.of(VALUES_OPERATION.attributes(), SECOND_VALUES_OPERATION.attributes()));

        assertProgram(
                intersectNode,
                ImmutableList.of(VALUES_OPERATION, SECOND_VALUES_OPERATION, intersectOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "x"), 0,
                        new Symbol(BOOLEAN, "y"), 1));
    }

    @Test
    public void testJoin()
    {
        ValuesNode left = VALUES_NODE;
        ValuesNode right = new ValuesNode(
                new PlanNodeId("values_right"),
                ImmutableList.of(new Symbol(BIGINT, "c")),
                ImmutableList.of(new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 5L)))));

        PlanNodeStatsAndCostSummary statsAndCost = new PlanNodeStatsAndCostSummary(1, 2, 3, 4, 5);

        JoinNode joinNode = new JoinNode(
                new PlanNodeId("join"),
                LEFT,
                left,
                right,
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "c"))),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                ImmutableList.of(new Symbol(BIGINT, "c")),
                false,
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.REPLICATED),
                Optional.of(true),
                ImmutableMap.of(
                        new DynamicFilterId("first_dynamic_filter"), new Symbol(BIGINT, "c"),
                        new DynamicFilterId("second_dynamic_filter"), new Symbol(BIGINT, "c")),
                Optional.of(statsAndCost));

        // right source
        Constant constantOperation = new Constant("%10", BIGINT, 5L);
        Row rowOperation = new Row("%11", ImmutableList.of(constantOperation.result()), ImmutableList.of(constantOperation.attributes()));
        Return returnOperation = new Return("%12", rowOperation.result(), rowOperation.attributes());
        Values rightSourceOperation = new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(BIGINT)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation,
                                        rowOperation,
                                        returnOperation))));
        Type rightRowType = irType(relationRowType(trinoType(rightSourceOperation.result().type())));

        // left join criteria
        Block.Parameter leftCriteriaParameter = new Block.Parameter(
                "%14",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftCriteria = new FieldReference("%15", leftCriteriaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationLeftCriteria = new Row("%16", ImmutableList.of(fieldReferenceOperationLeftCriteria.result()), ImmutableList.of(fieldReferenceOperationLeftCriteria.attributes()));
        Return returnOperationLeftCriteria = new Return("%17", rowOperationLeftCriteria.result(), rowOperationLeftCriteria.attributes());

        // right join criteria
        Block.Parameter rightCriteriaParameter = new Block.Parameter(
                "%18",
                rightRowType);
        FieldReference fieldReferenceOperationRightCriteria = new FieldReference("%19", rightCriteriaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationRightCriteria = new Row("%20", ImmutableList.of(fieldReferenceOperationRightCriteria.result()), ImmutableList.of(fieldReferenceOperationRightCriteria.attributes()));
        Return returnOperationRightCriteria = new Return("%21", rowOperationRightCriteria.result(), rowOperationRightCriteria.attributes());

        // join filter
        Block.Parameter leftFilterParameter = new Block.Parameter(
                "%22",
                VALUES_OPERATION_ROW_TYPE);
        Block.Parameter rightFilterParameter = new Block.Parameter(
                "%23",
                rightRowType);
        // JoinNode has empty filter, so default filter is created: constant true
        Constant constantOperationFilter = new Constant("%24", BOOLEAN, true);
        Return returnOperationFilter = new Return("%25", constantOperationFilter.result(), constantOperationFilter.attributes());

        // left outputs
        Block.Parameter leftOutputsParameter = new Block.Parameter(
                "%26",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftOutputsA = new FieldReference("%27", leftOutputsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationLeftOutputsB = new FieldReference("%28", leftOutputsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationLeftOutputs = new Row(
                "%29",
                ImmutableList.of(fieldReferenceOperationLeftOutputsA.result(), fieldReferenceOperationLeftOutputsB.result()),
                ImmutableList.of(fieldReferenceOperationLeftOutputsA.attributes(), fieldReferenceOperationLeftOutputsB.attributes()));
        Return returnOperationLeftOutputs = new Return("%30", rowOperationLeftOutputs.result(), rowOperationLeftOutputs.attributes());

        // right outputs
        Block.Parameter rightOutputsParameter = new Block.Parameter(
                "%31",
                rightRowType);
        FieldReference fieldReferenceOperationRightOutputsC = new FieldReference("%32", rightOutputsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationRightOutputs = new Row("%33", ImmutableList.of(fieldReferenceOperationRightOutputsC.result()), ImmutableList.of(fieldReferenceOperationRightOutputsC.attributes()));
        Return returnOperationRightOutputs = new Return("%34", rowOperationRightOutputs.result(), rowOperationRightOutputs.attributes());

        // dynamic filter targets
        Block.Parameter dynamicFilterTargetsParameter = new Block.Parameter(
                "%35",
                rightRowType);
        FieldReference fieldReferenceOperationDynamicFilterTargetsC1 = new FieldReference("%36", dynamicFilterTargetsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationDynamicFilterTargetsC2 = new FieldReference("%37", dynamicFilterTargetsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationDynamicFilterTargets = new Row(
                "%38",
                ImmutableList.of(fieldReferenceOperationDynamicFilterTargetsC1.result(), fieldReferenceOperationDynamicFilterTargetsC2.result()),
                ImmutableList.of(fieldReferenceOperationDynamicFilterTargetsC1.attributes(), fieldReferenceOperationDynamicFilterTargetsC2.attributes()));
        Return returnOperationDynamicFilterTargets = new Return("%39", rowOperationDynamicFilterTargets.result(), rowOperationDynamicFilterTargets.attributes());

        Join joinOperation = new Join(
                "%13",
                VALUES_OPERATION.result(),
                rightSourceOperation.result(),
                new Block(
                        Optional.of("^leftCriteriaSelector"),
                        ImmutableList.of(leftCriteriaParameter),
                        ImmutableList.of(
                                fieldReferenceOperationLeftCriteria,
                                rowOperationLeftCriteria,
                                returnOperationLeftCriteria)),
                new Block(
                        Optional.of("^rightCriteriaSelector"),
                        ImmutableList.of(rightCriteriaParameter),
                        ImmutableList.of(
                                fieldReferenceOperationRightCriteria,
                                rowOperationRightCriteria,
                                returnOperationRightCriteria)),
                new Block(
                        Optional.of("^filter"),
                        ImmutableList.of(leftFilterParameter, rightFilterParameter),
                        ImmutableList.of(
                                constantOperationFilter,
                                returnOperationFilter)),
                new Block(
                        Optional.of("^leftOutputSelector"),
                        ImmutableList.of(leftOutputsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationLeftOutputsA,
                                fieldReferenceOperationLeftOutputsB,
                                rowOperationLeftOutputs,
                                returnOperationLeftOutputs)),
                new Block(
                        Optional.of("^rightOutputSelector"),
                        ImmutableList.of(rightOutputsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationRightOutputsC,
                                rowOperationRightOutputs,
                                returnOperationRightOutputs)),
                new Block(
                        Optional.of("^dynamicFilterTargetSelector"),
                        ImmutableList.of(dynamicFilterTargetsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationDynamicFilterTargetsC1,
                                fieldReferenceOperationDynamicFilterTargetsC2,
                                rowOperationDynamicFilterTargets,
                                returnOperationDynamicFilterTargets)),
                JoinOperationMetadata.JoinType.LEFT,
                false,
                Optional.of(REPLICATED),
                Optional.of(true),
                ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                Optional.of(statsAndCost),
                VALUES_OPERATION.attributes(),
                rightSourceOperation.attributes());

        assertProgram(
                joinNode,
                ImmutableList.of(VALUES_OPERATION, rightSourceOperation, joinOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BIGINT)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BIGINT, "c"), 2));

        assertThat(joinOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    JoinOperationMetadata.JOIN_TYPE.putAttribute(builder, JoinOperationMetadata.JoinType.LEFT);
                    JoinOperationMetadata.MAY_SKIP_OUTPUT_DUPLICATES.putAttribute(builder, false);
                    JoinOperationMetadata.DISTRIBUTION_TYPE.putAttribute(builder, REPLICATED);
                    JoinOperationMetadata.SPILLABLE.putAttribute(builder, true);
                    JoinOperationMetadata.DYNAMIC_FILTER_IDS.putAttribute(builder, ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"));
                    JoinOperationMetadata.STATISTICS_AND_COST_SUMMARY.putAttribute(builder, statsAndCost);
                }));
    }

    @Test
    public void testLimit()
    {
        LimitNode limitNode = new LimitNode(new PlanNodeId("limit"), VALUES_NODE, 5L, true);

        Block.Parameter orderingParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantNull = new Constant("%11", EMPTY_ROW, null);
        Return returnOperation = new Return("%12", constantNull.result(), constantNull.attributes());

        Limit limitOperation = new Limit(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                constantNull,
                                returnOperation)),
                Optional.empty(),
                5L,
                true,
                ImmutableList.of(),
                VALUES_OPERATION.attributes());

        assertProgram(
                limitNode,
                ImmutableList.of(VALUES_OPERATION, limitOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        assertThat(limitOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    LimitOperationMetadata.COUNT.putAttribute(builder, 5L);
                    LimitOperationMetadata.PARTIAL.putAttribute(builder, true);
                    LimitOperationMetadata.PRE_SORTED_INDEXES.putAttribute(builder, ImmutableList.of());
                }));
    }

    @Test
    public void testLimitWithTies()
    {
        LimitNode limitNode = new LimitNode(
                new PlanNodeId("limit"),
                VALUES_NODE,
                5L,
                Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST))),
                false,
                ImmutableList.of(new Symbol(BOOLEAN, "b")));

        Block.Parameter orderingParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        Limit limitOperation = new Limit(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingB,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                Optional.of(new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST))),
                5L,
                false,
                ImmutableList.of(0),
                VALUES_OPERATION.attributes());

        assertProgram(
                limitNode,
                ImmutableList.of(VALUES_OPERATION, limitOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        assertThat(limitOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    LimitOperationMetadata.SORT_ORDERS.putAttribute(builder, new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)));
                    LimitOperationMetadata.COUNT.putAttribute(builder, 5L);
                    LimitOperationMetadata.PARTIAL.putAttribute(builder, false);
                    LimitOperationMetadata.PRE_SORTED_INDEXES.putAttribute(builder, ImmutableList.of(0));
                }));
    }

    @Test
    public void testOutput()
    {
        OutputNode outputNode = new OutputNode(
                new PlanNodeId("output"),
                VALUES_NODE,
                ImmutableList.of("col_b", "col_a"),
                ImmutableList.of(new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "a")));

        Block.Parameter fieldReferenceParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", fieldReferenceParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%12", fieldReferenceParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationA.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%14", rowOperation.result(), rowOperation.attributes());

        Output outputOperation = new Output(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^outputFieldSelector"),
                        ImmutableList.of(fieldReferenceParameter),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                fieldReferenceOperationA,
                                rowOperation,
                                returnOperation)),
                ImmutableList.of("col_b", "col_a"),
                VALUES_OPERATION.attributes());

        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of());

        outputNode.accept(relationalProgramBuilder, new Context(blockBuilder));
        Block block = blockBuilder.build();

        assertThat(block.operations()).isEqualTo(ImmutableList.of(VALUES_OPERATION, outputOperation));

        assertThat(trinoType(block.getReturnedType())).isEqualTo(BOOLEAN);
    }

    @Test
    public void testProject()
    {
        ProjectNode projectNode = new ProjectNode(
                new PlanNodeId("project"),
                VALUES_NODE,
                Assignments.copyOf(ImmutableMap.of(
                        new Symbol(BOOLEAN, "b"), new Reference(BOOLEAN, "b"),
                        new Symbol(BOOLEAN, "c"), new io.trino.sql.ir.Call(
                                LESS_THAN_BIGINT,
                                ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 5L), new Reference(BIGINT, "a"))))));

        Block.Parameter assignmentsParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", assignmentsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation = new Constant("%12", BIGINT, 5L);
        FieldReference fieldReferenceOperationA = new FieldReference("%13", assignmentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Call comparisonOperation = new Call(
                "%14",
                ImmutableList.of(constantOperation.result(), fieldReferenceOperationA.result()),
                LESS_THAN_BIGINT,
                ImmutableList.of(constantOperation.attributes(), fieldReferenceOperationA.attributes()));
        Row rowOperation = new Row(
                "%15",
                ImmutableList.of(fieldReferenceOperationB.result(), comparisonOperation.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), comparisonOperation.attributes()));
        Return returnOperation = new Return("%16", rowOperation.result(), rowOperation.attributes());
        Project projectOperation = new Project(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^assignments"),
                        ImmutableList.of(assignmentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                constantOperation,
                                fieldReferenceOperationA,
                                comparisonOperation,
                                rowOperation,
                                returnOperation)),
                VALUES_OPERATION.attributes());

        assertProgram(
                projectNode,
                ImmutableList.of(VALUES_OPERATION, projectOperation),
                new MultisetType(anonymousRow(BOOLEAN, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BOOLEAN, "b"), 0,
                        new Symbol(BOOLEAN, "c"), 1));
    }

    @Test
    public void testEmptyProject()
    {
        ValuesNode valuesNode = new ValuesNode(new PlanNodeId("values_empty_row"), 5);

        ProjectNode projectNode = new ProjectNode(
                new PlanNodeId("project"),
                valuesNode,
                Assignments.of());

        Values valuesOperation = valuesWithoutFields("%0", 5);

        Block.Parameter assignmentsParameter = new Block.Parameter("%2", irType(EMPTY_ROW));
        Constant constantOperation = new Constant("%3", EMPTY_ROW, null);
        Return returnOperation = new Return("%4", constantOperation.result(), constantOperation.attributes());

        Project projectOperation = new Project(
                "%1",
                valuesOperation.result(),
                new Block(
                        Optional.of("^assignments"),
                        ImmutableList.of(assignmentsParameter),
                        ImmutableList.of(
                                constantOperation,
                                returnOperation)),
                valuesOperation.attributes());

        assertProgram(
                projectNode,
                ImmutableList.of(valuesOperation, projectOperation),
                new MultisetType(EMPTY_ROW),
                ImmutableMap.of());
    }

    @Test
    public void testSemiJoin()
    {
        ValuesNode filteringSource = new ValuesNode(
                new PlanNodeId("values_filtering_source"),
                ImmutableList.of(new Symbol(BIGINT, "c")),
                ImmutableList.of(new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 5L)))));

        SemiJoinNode semiJoinNode = new SemiJoinNode(
                new PlanNodeId("semi_join"),
                VALUES_NODE,
                filteringSource,
                new Symbol(BIGINT, "a"),
                new Symbol(BIGINT, "c"),
                new Symbol(BOOLEAN, "match"),
                Optional.of(SemiJoinNode.DistributionType.PARTITIONED),
                Optional.of(new DynamicFilterId("semi_join_dynamic_filter")));

        // filtering source
        Constant constantOperation = new Constant("%10", BIGINT, 5L);
        Row rowOperation = new Row("%11", ImmutableList.of(constantOperation.result()), ImmutableList.of(constantOperation.attributes()));
        Return returnOperation = new Return("%12", rowOperation.result(), rowOperation.attributes());
        Values filteringSourceOperation = new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(BIGINT)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation,
                                        rowOperation,
                                        returnOperation))));
        Type filteringSourceRowType = irType(relationRowType(trinoType(filteringSourceOperation.result().type())));

        // source field selector
        Block.Parameter sourceJoinParameter = new Block.Parameter("%14", VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationSourceJoin = new FieldReference("%15", sourceJoinParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationSourceJoin = new Row("%16", ImmutableList.of(fieldReferenceOperationSourceJoin.result()), ImmutableList.of(fieldReferenceOperationSourceJoin.attributes()));
        Return returnOperationSourceJoin = new Return("%17", rowOperationSourceJoin.result(), rowOperationSourceJoin.attributes());

        // filtering source field selector
        Block.Parameter filteringSourceJoinParameter = new Block.Parameter("%18", filteringSourceRowType);
        FieldReference fieldReferenceOperationFilteringSourceJoin = new FieldReference("%19", filteringSourceJoinParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationFilteringSourceJoin = new Row("%20", ImmutableList.of(fieldReferenceOperationFilteringSourceJoin.result()), ImmutableList.of(fieldReferenceOperationFilteringSourceJoin.attributes()));
        Return returnOperationFilteringSourceJoin = new Return("%21", rowOperationFilteringSourceJoin.result(), rowOperationFilteringSourceJoin.attributes());

        SemiJoin semiJoinOperation = new SemiJoin(
                "%13",
                VALUES_OPERATION.result(),
                filteringSourceOperation.result(),
                new Block(
                        Optional.of("^sourceFieldSelector"),
                        ImmutableList.of(sourceJoinParameter),
                        ImmutableList.of(
                                fieldReferenceOperationSourceJoin,
                                rowOperationSourceJoin,
                                returnOperationSourceJoin)),
                new Block(
                        Optional.of("^filteringSourceFieldSelector"),
                        ImmutableList.of(filteringSourceJoinParameter),
                        ImmutableList.of(
                                fieldReferenceOperationFilteringSourceJoin,
                                rowOperationFilteringSourceJoin,
                                returnOperationFilteringSourceJoin)),
                Optional.of(PARTITIONED),
                Optional.of("semi_join_dynamic_filter"),
                VALUES_OPERATION.attributes(),
                filteringSourceOperation.attributes());

        assertProgram(
                semiJoinNode,
                ImmutableList.of(VALUES_OPERATION, filteringSourceOperation, semiJoinOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BOOLEAN, "match"), 2));

        assertThat(semiJoinOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    SemiJoinOperationMetadata.DISTRIBUTION_TYPE.putAttribute(builder, PARTITIONED);
                    SemiJoinOperationMetadata.DYNAMIC_FILTER_ID.putAttribute(builder, "semi_join_dynamic_filter");
                }));
    }

    @Test
    public void testSort()
    {
        SortNode sortNode = new SortNode(
                new PlanNodeId("sort"),
                VALUES_NODE,
                new OrderingScheme(ImmutableList.of(new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST)),
                false);

        Block.Parameter orderingParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        Sort sortOperation = new Sort(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingB,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                false,
                VALUES_OPERATION.attributes());

        assertProgram(
                sortNode,
                ImmutableList.of(VALUES_OPERATION, sortOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testTableScan()
    {
        ConnectorTableHandle testingConnectorTableHandle = new ConnectorTableHandle() {};

        TableScanNode tableScanNode = new TableScanNode(
                new PlanNodeId("table_scan"),
                new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), new TestingColumnHandle("a_handle"),
                        new Symbol(BOOLEAN, "b"), new TestingColumnHandle("b_handle")),
                TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                Optional.of(PlanNodeStatsEstimate.unknown()),
                false,
                Optional.of(TRUE));

        TableScan tableScanOperation = new TableScan(
                "%0",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")),
                TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                Optional.of(mapStatistics(PlanNodeStatsEstimate.unknown(), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")))),
                false,
                Optional.of(TRUE));

        assertProgram(
                tableScanNode,
                ImmutableList.of(tableScanOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        assertThat(tableScanOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    TableScanOperationMetadata.TABLE_HANDLE.putAttribute(builder, new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE));
                    TableScanOperationMetadata.COLUMN_HANDLES.putAttribute(builder, ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")));
                    TableScanOperationMetadata.CONSTRAINT.putAttribute(builder, TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))));
                    TableScanOperationMetadata.STATISTICS.putAttribute(builder, new Statistics(NaN, ImmutableMap.of()));
                    TableScanOperationMetadata.UPDATE_TARGET.putAttribute(builder, false);
                    TableScanOperationMetadata.USE_CONNECTOR_NODE_PARTITIONING.putAttribute(builder, true);
                    TableScanOperationMetadata.ROW_TYPE.putAttribute(builder, anonymousRow(BIGINT, BOOLEAN));
                }));
    }

    @Test
    public void testTopN()
    {
        TopNNode topNNode = new TopNNode(
                new PlanNodeId("topN"),
                VALUES_NODE,
                10,
                new OrderingScheme(ImmutableList.of(new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST)),
                TopNNode.Step.FINAL);

        Block.Parameter orderingParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        TopN topNOperation = new TopN(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingB,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                10,
                FINAL,
                VALUES_OPERATION.attributes());

        assertProgram(
                topNNode,
                ImmutableList.of(VALUES_OPERATION, topNOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testTopNRanking()
    {
        DataOrganizationSpecification specification = new DataOrganizationSpecification(
                ImmutableList.of(new Symbol(BOOLEAN, "b")),
                Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a")), ImmutableMap.of(new Symbol(BIGINT, "a"), DESC_NULLS_FIRST))));

        TopNRankingNode topNRankingNode = new TopNRankingNode(
                new PlanNodeId("topNRanking"),
                VALUES_NODE,
                specification,
                TopNRankingNode.RankingType.ROW_NUMBER,
                new Symbol(BIGINT, "row_number"),
                10,
                false);

        Block.Parameter partitioningParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference partitioningFieldReference = new FieldReference("%11", partitioningParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row partitioningRow = new Row("%12", ImmutableList.of(partitioningFieldReference.result()), ImmutableList.of(partitioningFieldReference.attributes()));
        Return partitioningReturn = new Return("%13", partitioningRow.result(), partitioningRow.attributes());
        Block partitioningSelector = new Block(
                Optional.of("^partitioningSelector"),
                ImmutableList.of(partitioningParameter),
                ImmutableList.of(
                        partitioningFieldReference,
                        partitioningRow,
                        partitioningReturn));

        Block.Parameter orderingParameter = new Block.Parameter(
                "%14",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference orderingFieldReference = new FieldReference("%15", orderingParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row orderingRow = new Row("%16", ImmutableList.of(orderingFieldReference.result()), ImmutableList.of(orderingFieldReference.attributes()));
        Return orderingReturn = new Return("%17", orderingRow.result(), orderingRow.attributes());
        Block orderingSelector = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderingParameter),
                ImmutableList.of(
                        orderingFieldReference,
                        orderingRow,
                        orderingReturn));

        TopNRanking topNRankingOperation = new TopNRanking(
                "%9",
                VALUES_OPERATION.result(),
                partitioningSelector,
                orderingSelector,
                ROW_NUMBER,
                10,
                false,
                new SortOrderList(ImmutableList.of(DESC_NULLS_FIRST)),
                VALUES_OPERATION.attributes());

        assertProgram(
                topNRankingNode,
                ImmutableList.of(VALUES_OPERATION, topNRankingOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BIGINT)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BIGINT, "row_number"), 2));

        TopNRankingNode partialTopNRankingNode = new TopNRankingNode(
                new PlanNodeId("partialTopNRanking"),
                VALUES_NODE,
                specification,
                TopNRankingNode.RankingType.RANK,
                new Symbol(BIGINT, "rank"),
                5,
                true);

        TopNRanking partialTopNRankingOperation = new TopNRanking(
                "%9",
                VALUES_OPERATION.result(),
                partitioningSelector,
                orderingSelector,
                RANK,
                5,
                true,
                new SortOrderList(ImmutableList.of(DESC_NULLS_FIRST)),
                VALUES_OPERATION.attributes());

        assertProgram(
                partialTopNRankingNode,
                ImmutableList.of(VALUES_OPERATION, partialTopNRankingOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testUnion()
    {
        UnionNode unionNode = new UnionNode(
                new PlanNodeId("union"),
                ImmutableList.of(VALUES_NODE, SECOND_VALUES_NODE),
                ImmutableListMultimap.of(
                        new Symbol(BIGINT, "x"), new Symbol(BIGINT, "a"),
                        new Symbol(BIGINT, "x"), new Symbol(BIGINT, "d"),
                        new Symbol(BOOLEAN, "y"), new Symbol(BOOLEAN, "b"),
                        new Symbol(BOOLEAN, "y"), new Symbol(BOOLEAN, "c")),
                ImmutableList.of(new Symbol(BIGINT, "x"), new Symbol(BOOLEAN, "y")));

        // union input selector for the first source
        Block.Parameter firstSelectorParameter = new Block.Parameter("%15", VALUES_OPERATION_ROW_TYPE);
        FieldReference firstSelectorFieldReferenceA = new FieldReference("%16", firstSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference firstSelectorFieldReferenceB = new FieldReference("%17", firstSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row firstSelectorRow = new Row(
                "%18",
                ImmutableList.of(firstSelectorFieldReferenceA.result(), firstSelectorFieldReferenceB.result()),
                ImmutableList.of(firstSelectorFieldReferenceA.attributes(), firstSelectorFieldReferenceB.attributes()));
        Return firstSelectorReturn = new Return("%19", firstSelectorRow.result(), firstSelectorRow.attributes());

        // union input selector for the second source
        Block.Parameter secondSelectorParameter = new Block.Parameter("%20", SECOND_VALUES_OPERATION_ROW_TYPE);
        FieldReference secondSelectorFieldReferenceD = new FieldReference("%21", secondSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondSelectorFieldReferenceC = new FieldReference("%22", secondSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row secondSelectorRow = new Row(
                "%23",
                ImmutableList.of(secondSelectorFieldReferenceD.result(), secondSelectorFieldReferenceC.result()),
                ImmutableList.of(secondSelectorFieldReferenceD.attributes(), secondSelectorFieldReferenceC.attributes()));
        Return secondSelectorReturn = new Return("%24", secondSelectorRow.result(), secondSelectorRow.attributes());

        Union unionOperation = new Union(
                "%14",
                ImmutableList.of(VALUES_OPERATION.result(), SECOND_VALUES_OPERATION.result()),
                ImmutableList.of(
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(firstSelectorParameter),
                                ImmutableList.of(
                                        firstSelectorFieldReferenceA,
                                        firstSelectorFieldReferenceB,
                                        firstSelectorRow,
                                        firstSelectorReturn)),
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(secondSelectorParameter),
                                ImmutableList.of(
                                        secondSelectorFieldReferenceD,
                                        secondSelectorFieldReferenceC,
                                        secondSelectorRow,
                                        secondSelectorReturn))),
                ImmutableList.of(VALUES_OPERATION.attributes(), SECOND_VALUES_OPERATION.attributes()));

        assertProgram(
                unionNode,
                ImmutableList.of(VALUES_OPERATION, SECOND_VALUES_OPERATION, unionOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "x"), 0,
                        new Symbol(BOOLEAN, "y"), 1));
    }

    @Test
    public void testValues()
    {
        assertProgram(
                VALUES_NODE,
                ImmutableList.of(VALUES_OPERATION),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        assertThat(VALUES_OPERATION.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    ValuesOperationMetadata.CARDINALITY.putAttribute(builder, 2L);
                    ValuesOperationMetadata.ROW_TYPE.putAttribute(builder, anonymousRow(BIGINT, BOOLEAN));
                }));
    }

    @Test
    public void testValuesEmptyRow()
    {
        ValuesNode valuesNode = new ValuesNode(new PlanNodeId("values_empty_row"), 5);

        Values valuesOperation = valuesWithoutFields("%0", 5);

        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of());

        OperationAndMapping operationAndMapping = valuesNode.accept(relationalProgramBuilder, new Context(blockBuilder));
        relationalProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();

        // remove the Return operation for comparison
        assertThat(block.operations().subList(0, block.operations().size() - 1)).isEqualTo(ImmutableList.of(valuesOperation));

        // assert relation type
        assertThat(trinoType(block.getReturnedType())).isEqualTo(new MultisetType(EMPTY_ROW));

        // assert symbol mapping
        assertThat(operationAndMapping.mapping()).isEqualTo(ImmutableMap.of());

        assertThat(valuesOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    ValuesOperationMetadata.CARDINALITY.putAttribute(builder, 5L);
                    ValuesOperationMetadata.ROW_TYPE.putAttribute(builder, EMPTY_ROW);
                }));
    }

    @Test
    public void testWindow()
    {
        ResolvedFunction lagFunction = FUNCTION_RESOLUTION.resolveFunction("lag", fromTypes(BOOLEAN, BIGINT));

        WindowNode windowNode = new WindowNode(
                new PlanNodeId("window"),
                VALUES_NODE,
                new DataOrganizationSpecification(
                        ImmutableList.of(new Symbol(BOOLEAN, "b")),
                        Optional.of(new OrderingScheme(
                                ImmutableList.of(new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "a")),
                                ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_LAST, new Symbol(BIGINT, "a"), DESC_NULLS_FIRST)))),
                ImmutableMap.of(
                        new Symbol(BOOLEAN, "lag_function"), new WindowNode.Function(
                                lagFunction,
                                ImmutableList.of(new Reference(BOOLEAN, "b"), new io.trino.sql.ir.Constant(BIGINT, 5L)),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST, new Symbol(BIGINT, "a"), DESC_NULLS_LAST))),
                                new WindowNode.Frame(
                                        WindowFrameType.RANGE,
                                        FrameBoundType.PRECEDING,
                                        Optional.empty(),
                                        Optional.empty(),
                                        FrameBoundType.FOLLOWING,
                                        Optional.of(new Symbol(BOOLEAN, "b")),
                                        Optional.of(new Symbol(BIGINT, "a"))),
                                true,
                                false)),
                ImmutableSet.of(new Symbol(BOOLEAN, "b")),
                1);

        // window functions parameter
        Block.Parameter windowFunctionsParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION.result().type());

        // window function arguments
        Block.Parameter argumentsParameter = new Block.Parameter(
                "%12",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationArgument = new FieldReference("%13", argumentsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperationArgument = new Constant("%14", BIGINT, 5L);
        Row rowOperationArgument = new Row(
                "%15",
                ImmutableList.of(fieldReferenceOperationArgument.result(), constantOperationArgument.result()),
                ImmutableList.of(fieldReferenceOperationArgument.attributes(), constantOperationArgument.attributes()));
        Return returnOperationArgument = new Return("%16", rowOperationArgument.result(), rowOperationArgument.attributes());

        // window function ordering
        Block.Parameter functionOrderingParameter = new Block.Parameter(
                "%17",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationFunctionOrderingA = new FieldReference("%18", functionOrderingParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationFunctionOrderingB = new FieldReference("%19", functionOrderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationFunctionOrdering = new Row(
                "%20",
                ImmutableList.of(fieldReferenceOperationFunctionOrderingA.result(), fieldReferenceOperationFunctionOrderingB.result()),
                ImmutableList.of(fieldReferenceOperationFunctionOrderingA.attributes(), fieldReferenceOperationFunctionOrderingB.attributes()));
        Return returnOperationFunctionOrdering = new Return("%21", rowOperationFunctionOrdering.result(), rowOperationFunctionOrdering.attributes());

        // frame start field
        Block.Parameter frameStartFieldParameter = new Block.Parameter(
                "%22",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperationFrameStart = new Constant("%23", EMPTY_ROW, null);
        Return returnOperationFrameStart = new Return("%24", constantOperationFrameStart.result(), constantOperationFrameStart.attributes());

        // sort key for frame start
        Block.Parameter sortKeyCoercedForFrameStartComparisonParameter = new Block.Parameter(
                "%25",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperationSortKeyStart = new Constant("%26", EMPTY_ROW, null);
        Return returnOperationSortKeyStart = new Return("%27", constantOperationSortKeyStart.result(), constantOperationSortKeyStart.attributes());

        // frame end field
        Block.Parameter frameEndFieldParameter = new Block.Parameter(
                "%28",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationFrameEnd = new FieldReference("%29", frameEndFieldParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationFrameEnd = new Row("%30", ImmutableList.of(fieldReferenceOperationFrameEnd.result()), ImmutableList.of(fieldReferenceOperationFrameEnd.attributes()));
        Return returnOperationFrameEnd = new Return("%31", rowOperationFrameEnd.result(), rowOperationFrameEnd.attributes());

        // sort key for frame end
        Block.Parameter sortKeyCoercedForFrameEndComparisonParameter = new Block.Parameter(
                "%32",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationSortKeyEnd = new FieldReference("%33", sortKeyCoercedForFrameEndComparisonParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationSortKeyEnd = new Row("%34", ImmutableList.of(fieldReferenceOperationSortKeyEnd.result()), ImmutableList.of(fieldReferenceOperationSortKeyEnd.attributes()));
        Return returnOperationSortKeyEnd = new Return("%35", rowOperationSortKeyEnd.result(), rowOperationSortKeyEnd.attributes());

        WindowFunctionCall windowFunctionCallOperation = new WindowFunctionCall(
                "%11",
                windowFunctionsParameter,
                new Block(
                        Optional.of("^arguments"),
                        ImmutableList.of(argumentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationArgument,
                                constantOperationArgument,
                                rowOperationArgument,
                                returnOperationArgument)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(functionOrderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationFunctionOrderingA,
                                fieldReferenceOperationFunctionOrderingB,
                                rowOperationFunctionOrdering,
                                returnOperationFunctionOrdering)),
                new Block(
                        Optional.of("^frameStartFieldSelector"),
                        ImmutableList.of(frameStartFieldParameter),
                        ImmutableList.of(
                                constantOperationFrameStart,
                                returnOperationFrameStart)),
                new Block(
                        Optional.of("^sortKeyCoercedForFrameStartComparisonSelector"),
                        ImmutableList.of(sortKeyCoercedForFrameStartComparisonParameter),
                        ImmutableList.of(
                                constantOperationSortKeyStart,
                                returnOperationSortKeyStart)),
                new Block(
                        Optional.of("^frameEndFieldSelector"),
                        ImmutableList.of(frameEndFieldParameter),
                        ImmutableList.of(
                                fieldReferenceOperationFrameEnd,
                                rowOperationFrameEnd,
                                returnOperationFrameEnd)),
                new Block(
                        Optional.of("^sortKeyCoercedForFrameEndComparisonSelector"),
                        ImmutableList.of(sortKeyCoercedForFrameEndComparisonParameter),
                        ImmutableList.of(
                                fieldReferenceOperationSortKeyEnd,
                                rowOperationSortKeyEnd,
                                returnOperationSortKeyEnd)),
                lagFunction,
                Optional.of(new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST))),
                RANGE,
                PRECEDING,
                FOLLOWING,
                true,
                false);

        // collecting window functions in a row
        Row windowFunctionsRowOperation = new Row("%36", ImmutableList.of(windowFunctionCallOperation.result()), ImmutableList.of(windowFunctionCallOperation.attributes()));
        Return windowFunctionsReturnOperation = new Return("%37", windowFunctionsRowOperation.result(), windowFunctionsRowOperation.attributes());

        // partitioning
        Block.Parameter partitioningSelectorParameter = new Block.Parameter(
                "%38",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationPartitioning = new FieldReference("%39", partitioningSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationPartitioning = new Row("%40", ImmutableList.of(fieldReferenceOperationPartitioning.result()), ImmutableList.of(fieldReferenceOperationPartitioning.attributes()));
        Return returnOperationPartitioning = new Return("%41", rowOperationPartitioning.result(), rowOperationPartitioning.attributes());

        // ordering
        Block.Parameter orderingParameter = new Block.Parameter(
                "%42",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%43", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationOrderingA = new FieldReference("%44", orderingParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row(
                "%45",
                ImmutableList.of(fieldReferenceOperationOrderingB.result(), fieldReferenceOperationOrderingA.result()),
                ImmutableList.of(fieldReferenceOperationOrderingB.attributes(), fieldReferenceOperationOrderingA.attributes()));
        Return returnOperationOrdering = new Return("%46", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        Window windowOperation = new Window(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^windowFunctions"),
                        ImmutableList.of(windowFunctionsParameter),
                        ImmutableList.of(
                                windowFunctionCallOperation,
                                windowFunctionsRowOperation,
                                windowFunctionsReturnOperation)),
                new Block(
                        Optional.of("^partitioningSelector"),
                        ImmutableList.of(partitioningSelectorParameter),
                        ImmutableList.of(
                                fieldReferenceOperationPartitioning,
                                rowOperationPartitioning,
                                returnOperationPartitioning)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingB,
                                fieldReferenceOperationOrderingA,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                ImmutableList.of(0),
                Optional.of(new SortOrderList(ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_FIRST))),
                1,
                VALUES_OPERATION.attributes());

        assertProgram(
                windowNode,
                ImmutableList.of(VALUES_OPERATION, windowOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BOOLEAN, "lag_function"), 2));

        assertThat(windowFunctionCallOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    WindowFunctionCallOperationMetadata.RESOLVED_FUNCTION.putAttribute(builder, lagFunction);
                    WindowFunctionCallOperationMetadata.SORT_ORDERS.putAttribute(builder, new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)));
                    WindowFunctionCallOperationMetadata.FRAME_TYPE.putAttribute(builder, RANGE);
                    WindowFunctionCallOperationMetadata.FRAME_START_TYPE.putAttribute(builder, PRECEDING);
                    WindowFunctionCallOperationMetadata.FRAME_END_TYPE.putAttribute(builder, FOLLOWING);
                    WindowFunctionCallOperationMetadata.IGNORE_NULLS.putAttribute(builder, true);
                    WindowFunctionCallOperationMetadata.DISTINCT.putAttribute(builder, false);
                }));

        assertThat(windowOperation.operationAttributes())
                .isEqualTo(attributes(builder -> {
                    WindowOperationMetadata.PRE_PARTITIONED_INDEXES.putAttribute(builder, ImmutableList.of(0));
                    WindowOperationMetadata.SORT_ORDERS.putAttribute(builder, new SortOrderList(ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_FIRST)));
                    WindowOperationMetadata.PRE_SORTED_PREFIX.putAttribute(builder, 1);
                }));
    }

    @Test
    public void testRelationRowType()
    {
        assertThat(relationRowType(new MultisetType(EMPTY_ROW))).isEqualTo(EMPTY_ROW);

        RowType rowType = anonymousRow(BIGINT, BOOLEAN);
        assertThat(relationRowType(new MultisetType(rowType))).isEqualTo(rowType);

        assertThatThrownBy(() -> relationRowType(new MultisetType(rowType(
                new RowType.Field(Optional.of("a"), BIGINT),
                new RowType.Field(Optional.of("b"), BOOLEAN)))))
                .hasMessage("not a relation type. expected multiset of row with anonymous fields");

        assertThatThrownBy(() -> relationRowType(rowType))
                .hasMessage("not a relation type. expected multiset of row with anonymous fields");

        assertThatThrownBy(() -> relationRowType(BIGINT))
                .hasMessage("not a relation type. expected multiset of row with anonymous fields");
    }

    @Test
    public void testDeriveOutputMapping()
    {
        assertThatThrownBy(() -> deriveOutputMapping(BIGINT, ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("not a relation row type. expected RowType with anonymous fields or EmptyRowType");

        assertThatThrownBy(() -> deriveOutputMapping(EMPTY_ROW, ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("relation row type mismatch: output symbols present for EmptyRowType");

        assertThat(deriveOutputMapping(EMPTY_ROW, ImmutableList.of())).isEqualTo(ImmutableMap.of());

        assertThatThrownBy(() -> deriveOutputMapping(rowType(new RowType.Field(Optional.of("x"), BIGINT)), ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("not a relation row type. expected RowType with anonymous fields or EmptyRowType");

        assertThatThrownBy(() -> deriveOutputMapping(anonymousRow(BIGINT), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))))
                .hasMessage("relation RowType does not match output symbols");

        assertThatThrownBy(() -> deriveOutputMapping(anonymousRow(BOOLEAN), ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("symbol type does not match field type");

        assertThat(deriveOutputMapping(
                anonymousRow(BIGINT, BOOLEAN),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))))
                .isEqualTo(ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        // duplicate output symbols
        assertThat(deriveOutputMapping(
                anonymousRow(BIGINT, BOOLEAN, BIGINT),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "a"))))
                .isEqualTo(ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0, // mapped to the first field
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testFieldSelectorBlock()
    {
        String name = "^field_selector";

        Block.Parameter firstParameter = new Block.Parameter("%first", irType(EMPTY_ROW));
        Block.Parameter secondParameter = new Block.Parameter("%second", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block.Parameter thirdParameter = new Block.Parameter("%third", irType(anonymousRow(BOOLEAN)));
        List<Block.Parameter> inputRows = ImmutableList.of(firstParameter, secondParameter, thirdParameter);

        List<Map<Symbol, Integer>> symbolMappings = ImmutableList.of(
                ImmutableMap.of(),
                ImmutableMap.of(new Symbol(BIGINT, "X"), 0, new Symbol(BOOLEAN, "Y"), 1),
                ImmutableMap.of(new Symbol(BOOLEAN, "Z"), 0));

        // select symbol X from second parameter and symbol Z from third parameter
        Block selectSomeFields = new RelationalProgramBuilder(new ValueNameAllocator())
                .fieldSelectorBlock(
                        name,
                        inputRows,
                        symbolMappings,
                        ImmutableList.of(
                                ImmutableList.of(),
                                ImmutableList.of(new Symbol(BIGINT, "X")),
                                ImmutableList.of(new Symbol(BOOLEAN, "Z"))));

        FieldReference fieldReferenceOperationX = new FieldReference("%0", secondParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationZ = new FieldReference("%1", thirdParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(fieldReferenceOperationX.result(), fieldReferenceOperationZ.result()),
                ImmutableList.of(fieldReferenceOperationX.attributes(), fieldReferenceOperationZ.attributes()));
        Return returnRow = new Return("%3", rowOperation.result(), rowOperation.attributes());

        assertThat(selectSomeFields)
                .isEqualTo(new Block(
                        Optional.of("^field_selector"),
                        inputRows,
                        ImmutableList.of(
                                fieldReferenceOperationX,
                                fieldReferenceOperationZ,
                                rowOperation,
                                returnRow)));

        // do not select any fields -- return constant null of type EMPTY_ROW
        Block selectNoFields = new RelationalProgramBuilder(new ValueNameAllocator())
                .fieldSelectorBlock(
                        name,
                        inputRows,
                        symbolMappings,
                        ImmutableList.of(
                                ImmutableList.of(),
                                ImmutableList.of(),
                                ImmutableList.of()));

        Constant constantOperation = new Constant("%0", EMPTY_ROW, null);
        Return returnEmptyRow = new Return("%1", constantOperation.result(), constantOperation.attributes());

        assertThat(selectNoFields)
                .isEqualTo(new Block(
                        Optional.of("^field_selector"),
                        inputRows,
                        ImmutableList.of(
                                constantOperation,
                                returnEmptyRow)));

        // failure when selecting a non-existent symbol
        assertThatThrownBy(() -> new RelationalProgramBuilder(new ValueNameAllocator())
                .fieldSelectorBlock(
                        name,
                        inputRows,
                        symbolMappings,
                        ImmutableList.of(
                                ImmutableList.of(new Symbol(BOOLEAN, "non_existent_symbol")),
                                ImmutableList.of(),
                                ImmutableList.of())))
                .hasMessage("fieldIndex is null");

        // failure when input rows do not match input mappings
        assertThatThrownBy(() -> new RelationalProgramBuilder(new ValueNameAllocator())
                .fieldSelectorBlock(
                        name,
                        ImmutableList.of(firstParameter, secondParameter), // two input rows
                        symbolMappings, // three input mappings
                        ImmutableList.of(
                                ImmutableList.of(new Symbol(BOOLEAN, "non_existent_symbol")),
                                ImmutableList.of(),
                                ImmutableList.of())))
                .hasMessage("inputs and input symbol mappings do not match");

        // failure when input rows do not match selected symbol lists
        assertThatThrownBy(() -> new RelationalProgramBuilder(new ValueNameAllocator())
                .fieldSelectorBlock(
                        name,
                        inputRows, // three input rows
                        symbolMappings,
                        ImmutableList.of(
                                // four selected symbols lists
                                ImmutableList.of(new Symbol(BOOLEAN, "non_existent_symbol")),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                ImmutableList.of())))
                .hasMessage("inputs and symbol lists do not match");
    }

    private static ValuesNode valuesNode()
    {
        return new ValuesNode(
                new PlanNodeId("values"),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                ImmutableList.of(
                        new io.trino.sql.ir.Row(ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 1L),
                                new io.trino.sql.ir.Constant(BOOLEAN, true))),
                        new io.trino.sql.ir.Row(ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 2L),
                                new io.trino.sql.ir.Constant(BOOLEAN, false)))));
    }

    private static Values valuesOperation()
    {
        Constant constantOperation1Row1 = new Constant("%1", BIGINT, 1L);
        Constant constantOperation2Row1 = new Constant("%2", BOOLEAN, true);
        Row rowOperation1 = new Row(
                "%3",
                ImmutableList.of(constantOperation1Row1.result(), constantOperation2Row1.result()),
                ImmutableList.of(constantOperation1Row1.attributes(), constantOperation2Row1.attributes()));
        Return returnOperation1 = new Return("%4", rowOperation1.result(), rowOperation1.attributes());
        Constant constantOperation1Row2 = new Constant("%5", BIGINT, 2L);
        Constant constantOperation2Row2 = new Constant("%6", BOOLEAN, false);
        Row rowOperation2 = new Row(
                "%7",
                ImmutableList.of(constantOperation1Row2.result(), constantOperation2Row2.result()),
                ImmutableList.of(constantOperation1Row2.attributes(), constantOperation2Row2.attributes()));
        Return returnOperation2 = new Return("%8", rowOperation2.result(), rowOperation2.attributes());

        return new Values(
                "%0",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row1,
                                        constantOperation2Row1,
                                        rowOperation1,
                                        returnOperation1)),
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row2,
                                        constantOperation2Row2,
                                        rowOperation2,
                                        returnOperation2))));
    }

    private static ValuesNode secondValuesNode()
    {
        return new ValuesNode(
                new PlanNodeId("secondValues"),
                ImmutableList.of(new Symbol(BOOLEAN, "c"), new Symbol(BIGINT, "d")),
                ImmutableList.of(new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BOOLEAN, false), new io.trino.sql.ir.Constant(BIGINT, 2L)))));
    }

    private static Values secondValuesOperation()
    {
        Constant secondConstantOperationC = new Constant("%10", BOOLEAN, false);
        Constant secondConstantOperationD = new Constant("%11", BIGINT, 2L);
        Row secondRowOperation = new Row(
                "%12",
                ImmutableList.of(secondConstantOperationC.result(), secondConstantOperationD.result()),
                ImmutableList.of(secondConstantOperationC.attributes(), secondConstantOperationD.attributes()));
        Return secondReturnOperation = new Return("%13", secondRowOperation.result(), secondRowOperation.attributes());

        return new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(BOOLEAN, BIGINT)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        secondConstantOperationC,
                                        secondConstantOperationD,
                                        secondRowOperation,
                                        secondReturnOperation))));
    }

    private void assertProgram(PlanNode plan, List<Operation> expected, io.trino.spi.type.Type expectedType, Map<Symbol, Integer> expectedMapping)
    {
        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of());

        OperationAndMapping operationAndMapping = plan.accept(relationalProgramBuilder, new Context(blockBuilder));

        // Hack: add a terminal Return operation. It is required to build the Block.
        // Usually, the plan should have OutputNode as root, and that would result in Output terminal operation ending the Block.
        // For testing purpose, we don't apply the Output operation so that we can assert output symbol mapping.
        relationalProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();
        // remove the Return operation
        List<Operation> actual = block.operations().subList(0, block.operations().size() - 1);

        assertThat(actual).isEqualTo(expected);

        // assert relation type
        if (plan.getOutputSymbols().isEmpty()) {
            assertThat(expectedType).isEqualTo(new MultisetType(EMPTY_ROW));
        }
        else {
            assertThat(expectedType)
                    .isEqualTo(new MultisetType(RowType.anonymous(plan.getOutputSymbols().stream()
                            .map(Symbol::type)
                            .collect(toImmutableList()))));
        }
        assertThat(expectedType).isEqualTo(trinoType(actual.getLast().result().type()));

        // assert symbol mapping
        assertThat(expectedMapping).isEqualTo(operationAndMapping.mapping());
    }
}
