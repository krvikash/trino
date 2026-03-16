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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.DynamicFilterSource;
import io.trino.sql.dialect.trino.operation.EnforceSingleRow;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.ExplainAnalyze;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.GroupId;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Limit;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Sort;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TopN;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.dialect.trino.operation.TrinoOperationVisitor;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operation.Window;
import io.trino.sql.dialect.trino.operation.WindowFunctionCall;
import io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.DynamicFilterSourceOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeScope;
import io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeType;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.SortOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.Statistics;
import io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.TopNStep;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameBoundType;
import io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.FrameBoundType;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowFrameType;
import io.trino.sql.planner.plan.WindowNode;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.forEachPair;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.GLOBAL_GROUPING_SETS;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.GROUPING_SETS_COUNT;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.GROUP_ID_INDEX;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.INPUT_REDUCING;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.PRE_GROUPED_INDEXES;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.BUCKET_COUNT;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.BUCKET_TO_PARTITION;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.EXCHANGE_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.PARTITIONING_HANDLE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.PARTITION_COUNT;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.REPLICATE_NULLS_AND_ANY;
import static io.trino.sql.dialect.trino.operationmetadata.ExplainAnalyzeOperationMetadata.VERBOSE;
import static io.trino.sql.dialect.trino.operationmetadata.GroupIdOperationMetadata.GROUPING_SETS;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JOIN_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.SPILLABLE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata.PRE_SORTED_INDEXES;
import static io.trino.sql.dialect.trino.operationmetadata.OutputOperationMetadata.COLUMN_NAMES;
import static io.trino.sql.dialect.trino.operationmetadata.SortOperationMetadata.PARTIAL;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.CONSTRAINT;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.STATISTICS;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.UPDATE_TARGET;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.LIMIT;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.TOP_N_STEP;
import static io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata.CARDINALITY;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.DISTINCT;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.FRAME_END_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.FRAME_START_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.FRAME_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.IGNORE_NULLS;
import static io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata.PRE_PARTITIONED_INDEXES;
import static io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata.PRE_SORTED_PREFIX;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.Partitioning.ArgumentBinding.constantBinding;
import static io.trino.sql.planner.Partitioning.ArgumentBinding.expressionBinding;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyRelationalComputation;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class ToOldIrRelationalRewriter
        extends TrinoOperationVisitor<PlanNode, List<PlanNode>>
{
    private final PlanNodeIdAllocator planNodeIdAllocator;
    private final SymbolAllocator symbolAllocator;
    private final ToOldIrScalarRewriter scalarRewriter;
    private final Session session;
    private final Metadata metadata;

    public ToOldIrRelationalRewriter(PlanNodeIdAllocator planNodeIdAllocator, SymbolAllocator symbolAllocator, ToOldIrScalarRewriter scalarRewriter, Session session, Metadata metadata)
    {
        this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.scalarRewriter = requireNonNull(scalarRewriter, "scalarRewriter is null");
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    protected PlanNode visitOperation(TrinoOperation operation, List<PlanNode> sources)
    {
        throw new UnsupportedOperationException("ToOldIrRelationalRewriter is not implemented for " + operation.name() + ". It must support all relational operations.");
    }

    @Override
    public PlanNode visitAggregation(Aggregation aggregation, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        // build aggregations
        List<AggregationNode.Aggregation> aggregations = getAggregations(aggregation.aggregateCalls(), source.getOutputSymbols());
        List<Type> aggregateTypes = trinoType(aggregation.aggregateCalls().getReturnedType()).getTypeParameters();
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsBuilder = ImmutableMap.builder();
        for (int i = 0; i < aggregations.size(); i++) {
            AggregationNode.Aggregation aggregate = aggregations.get(i);
            aggregationsBuilder.put(symbolAllocator.newSymbol(aggregate.getResolvedFunction().name().getFunctionName(), aggregateTypes.get(i)), aggregate);
        }

        // build grouping sets descriptor
        List<Symbol> groupingKeys = scalarRewriter.getSelectedSymbols(aggregation.groupingKeysSelector(), source.getOutputSymbols());
        AggregationNode.GroupingSetDescriptor groupingSets = new AggregationNode.GroupingSetDescriptor(
                groupingKeys,
                GROUPING_SETS_COUNT.getAttribute(aggregation.attributes()),
                ImmutableSet.copyOf(GLOBAL_GROUPING_SETS.getAttribute(aggregation.attributes())));

        return new AggregationNode(
                planNodeIdAllocator.getNextId(),
                source,
                aggregationsBuilder.buildOrThrow(),
                groupingSets,
                PRE_GROUPED_INDEXES.getAttribute(aggregation.attributes()).stream()
                        .map(groupingKeys::get)
                        .collect(toImmutableList()),
                rewriteAggregationStep(AGGREGATION_STEP.getAttribute(aggregation.attributes())),
                Optional.ofNullable(GROUP_ID_INDEX.getAttribute(aggregation.attributes()))
                        .map(groupingKeys::get),
                Optional.of(INPUT_REDUCING.getAttribute(aggregation.attributes())));
    }

    private List<AggregationNode.Aggregation> getAggregations(Block aggregateCalls, List<Symbol> inputSymbols)
    {
        if (isEmptyRelationalComputation(aggregateCalls)) {
            return ImmutableList.of();
        }

        Map<Value, Operation> operations = aggregateCalls.operations().stream()
                .collect(toImmutableMap(Operation::result, identity()));

        return aggregateCalls.operations().get(aggregateCalls.operations().size() - 2).arguments().stream()
                .map(operations::get)
                .map(AggregateCall.class::cast)
                .map(aggregateCall -> getAggregation(aggregateCall, inputSymbols))
                .collect(toImmutableList());
    }

    private AggregationNode.Aggregation getAggregation(AggregateCall aggregateCall, List<Symbol> inputSymbols)
    {
        return new AggregationNode.Aggregation(
                AggregateCallOperationMetadata.RESOLVED_FUNCTION.getAttribute(aggregateCall.attributes()),
                scalarRewriter.getExpressions(aggregateCall.argumentsBlock(), inputSymbols),
                AggregateCallOperationMetadata.DISTINCT.getAttribute(aggregateCall.attributes()),
                scalarRewriter.getOptionalSelectedSymbol(aggregateCall.filterSelector(), inputSymbols),
                getOptionalOrderingScheme(AggregateCallOperationMetadata.SORT_ORDERS.getAttribute(aggregateCall.attributes()), aggregateCall.orderingSelector(), inputSymbols),
                scalarRewriter.getOptionalSelectedSymbol(aggregateCall.maskSelector(), inputSymbols));
    }

    private Optional<OrderingScheme> getOptionalOrderingScheme(@Nullable SortOrderList sortOrderList, Block orderingSelector, List<Symbol> inputSymbols)
    {
        if (sortOrderList == null) {
            return Optional.empty();
        }

        List<SortOrder> sortOrders = sortOrderList.sortOrders();
        List<Symbol> orderBy = scalarRewriter.getSelectedSymbols(orderingSelector, inputSymbols);
        ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
        for (int i = 0; i < orderBy.size(); i++) {
            orderings.put(orderBy.get(i), sortOrders.get(i));
        }
        return Optional.of(new OrderingScheme(orderBy, orderings.buildOrThrow()));
    }

    private static AggregationNode.Step rewriteAggregationStep(AggregationOperationMetadata.AggregationStep step)
    {
        return switch (step) {
            case PARTIAL -> AggregationNode.Step.PARTIAL;
            case FINAL -> AggregationNode.Step.FINAL;
            case INTERMEDIATE -> AggregationNode.Step.INTERMEDIATE;
            case SINGLE -> AggregationNode.Step.SINGLE;
        };
    }

    @Override
    public PlanNode visitDynamicFilterSource(DynamicFilterSource dynamicFilterSource, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        // build dynamic filter map
        List<DynamicFilterId> dynamicFilterIds = DynamicFilterSourceOperationMetadata.DYNAMIC_FILTER_IDS.getAttribute(dynamicFilterSource.attributes()).stream()
                .map(DynamicFilterId::new)
                .collect(toImmutableList());
        List<Symbol> dynamicFilterSymbols = scalarRewriter.getSelectedSymbols(dynamicFilterSource.dynamicFilterTargetSelector(), source.getOutputSymbols());
        ImmutableMap.Builder<DynamicFilterId, Symbol> dynamicFilters = ImmutableMap.builder();
        forEachPair(dynamicFilterIds.stream(), dynamicFilterSymbols.stream(), dynamicFilters::put);

        return new DynamicFilterSourceNode(
                planNodeIdAllocator.getNextId(),
                source,
                dynamicFilters.buildOrThrow());
    }

    @Override
    public PlanNode visitEnforceSingleRow(EnforceSingleRow enforceSingleRow, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        return new EnforceSingleRowNode(
                planNodeIdAllocator.getNextId(),
                source);
    }

    @Override
    public PlanNode visitExchange(Exchange exchange, List<PlanNode> sources)
    {
        // build input symbols lists
        ImmutableList.Builder<List<Symbol>> inputSymbolsBuilder = ImmutableList.builder();
        for (int i = 0; i < sources.size(); i++) {
            inputSymbolsBuilder.add(scalarRewriter.getSelectedSymbols(exchange.inputFieldSelectors().get(i), sources.get(i).getOutputSymbols()));
        }
        List<List<Symbol>> inputSymbols = inputSymbolsBuilder.build();

        // create the exchange's output symbols based on the first source's symbols.
        // assign new symbols in case there are repetitions in th input symbols list.
        List<Symbol> outputSymbols = inputSymbols.getFirst().stream()
                .map(symbolAllocator::newSymbol)
                .collect(toImmutableList());

        return new ExchangeNode(
                planNodeIdAllocator.getNextId(),
                rewriteExchangeType(EXCHANGE_TYPE.getAttribute(exchange.attributes())),
                rewriteExchangeScope(EXCHANGE_SCOPE.getAttribute(exchange.attributes())),
                new PartitioningScheme(
                        getPartitioning(
                                PARTITIONING_HANDLE.getAttribute(exchange.attributes()),
                                getBoundArguments(exchange.partitioningBoundArguments(), outputSymbols)),
                        outputSymbols,
                        REPLICATE_NULLS_AND_ANY.getAttribute(exchange.attributes()),
                        Optional.ofNullable(BUCKET_TO_PARTITION.getAttribute(exchange.attributes())).map(list -> list.stream().mapToInt(Integer::intValue).toArray()),
                        BUCKET_COUNT.getAttribute(exchange.attributes()) == null ? OptionalInt.empty() : OptionalInt.of(BUCKET_COUNT.getAttribute(exchange.attributes())),
                        PARTITION_COUNT.getAttribute(exchange.attributes()) == null ? OptionalInt.empty() : OptionalInt.of(PARTITION_COUNT.getAttribute(exchange.attributes()))),
                sources,
                inputSymbols,
                getOptionalOrderingScheme(ExchangeOperationMetadata.SORT_ORDERS.getAttribute(exchange.attributes()), exchange.orderingSelector(), outputSymbols));
    }

    private static ExchangeNode.Type rewriteExchangeType(ExchangeType type)
    {
        return switch (type) {
            case GATHER -> ExchangeNode.Type.GATHER;
            case REPARTITION -> ExchangeNode.Type.REPARTITION;
            case REPLICATE -> ExchangeNode.Type.REPLICATE;
        };
    }

    private static ExchangeNode.Scope rewriteExchangeScope(ExchangeScope scope)
    {
        return switch (scope) {
            case LOCAL -> ExchangeNode.Scope.LOCAL;
            case REMOTE -> ExchangeNode.Scope.REMOTE;
        };
    }

    public static Partitioning getPartitioning(PartitioningHandle handle, List<Partitioning.ArgumentBinding> boundArguments)
    {
        return Partitioning.jsonCreate(handle, boundArguments);
    }

    public List<Partitioning.ArgumentBinding> getBoundArguments(Block boundArgumentsBlock, List<Symbol> outputSymbols)
    {
        List<Expression> boundArguments = scalarRewriter.getExpressions(boundArgumentsBlock, outputSymbols);

        return boundArguments.stream()
                .map(item -> {
                    if (item instanceof Constant(io.trino.spi.type.Type type, Object value)) {
                        return constantBinding(new NullableValue(type, value));
                    }
                    return expressionBinding(item);
                })
                .collect(toImmutableList());
    }

    @Override
    public PlanNode visitExplainAnalyze(ExplainAnalyze explainAnalyze, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        return new ExplainAnalyzeNode(
                planNodeIdAllocator.getNextId(),
                source,
                symbolAllocator.newSymbol("Query Plan", VARCHAR),
                scalarRewriter.getSelectedSymbols(explainAnalyze.fieldSelector(), source.getOutputSymbols()),
                VERBOSE.getAttribute(explainAnalyze.attributes()));
    }

    @Override
    public PlanNode visitFilter(Filter filter, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        return new FilterNode(
                planNodeIdAllocator.getNextId(),
                source,
                scalarRewriter.toOldIr(filter.predicate(), ImmutableList.of(source.getOutputSymbols())));
    }

    @Override
    public PlanNode visitGroupId(GroupId groupId, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        List<Symbol> groupingInputSymbols = scalarRewriter.getSelectedSymbols(groupId.groupingColumnsSelector(), source.getOutputSymbols());
        List<Symbol> groupingOutputSymbols = groupingInputSymbols.stream()
                .map(inputSymbol -> symbolAllocator.newSymbol(inputSymbol.name() + "_gid", inputSymbol.type()))
                .collect(toImmutableList());

        List<List<Symbol>> groupingSets = GROUPING_SETS.getAttribute(groupId.attributes()).stream()
                .map(indexList -> indexList.stream()
                        .map(groupingOutputSymbols::get)
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        Map<Symbol, Symbol> groupingColumns = IntStream.range(0, groupingOutputSymbols.size())
                .boxed()
                .collect(toImmutableMap(groupingOutputSymbols::get, groupingInputSymbols::get));

        return new GroupIdNode(
                planNodeIdAllocator.getNextId(),
                source,
                groupingSets,
                groupingColumns,
                scalarRewriter.getSelectedSymbols(groupId.aggregationArgumentsSelector(), source.getOutputSymbols()),
                symbolAllocator.newSymbol("groupId", BIGINT));
    }

    @Override
    public PlanNode visitJoin(Join join, List<PlanNode> sources)
    {
        checkArgument(sources.size() == 2, "Expected two sources for Join operation");
        PlanNode left = sources.get(0);
        PlanNode right = sources.get(1);

        // build equi clauses
        List<Symbol> leftEquiSymbols = scalarRewriter.getSelectedSymbols(join.leftCriteriaSelector(), left.getOutputSymbols());
        List<Symbol> rightEquiSymbols = scalarRewriter.getSelectedSymbols(join.rightCriteriaSelector(), right.getOutputSymbols());
        ImmutableList.Builder<EquiJoinClause> criteria = ImmutableList.builder();
        for (int i = 0; i < leftEquiSymbols.size(); i++) {
            criteria.add(new EquiJoinClause(leftEquiSymbols.get(i), rightEquiSymbols.get(i)));
        }

        // build join filter
        Optional<Expression> filter;
        Expression filterExpression = scalarRewriter.toOldIr(join.filter(), ImmutableList.of(left.getOutputSymbols(), right.getOutputSymbols()));
        filter = filterExpression.equals(TRUE) ? Optional.empty() : Optional.of(filterExpression);

        // build dynamic filter map
        List<DynamicFilterId> dynamicFilterIds = DYNAMIC_FILTER_IDS.getAttribute(join.attributes()).stream()
                .map(DynamicFilterId::new)
                .collect(toImmutableList());
        List<Symbol> dynamicFilterSymbols = scalarRewriter.getSelectedSymbols(join.dynamicFilterTargetSelector(), right.getOutputSymbols());
        ImmutableMap.Builder<DynamicFilterId, Symbol> dynamicFilters = ImmutableMap.builder();
        for (int i = 0; i < dynamicFilterIds.size(); i++) {
            dynamicFilters.put(dynamicFilterIds.get(i), dynamicFilterSymbols.get(i));
        }

        return new JoinNode(
                planNodeIdAllocator.getNextId(),
                rewriteJoinType(JOIN_TYPE.getAttribute(join.attributes())),
                left,
                right,
                criteria.build(),
                scalarRewriter.getSelectedSymbols(join.leftOutputSelector(), left.getOutputSymbols()),
                scalarRewriter.getSelectedSymbols(join.rightOutputSelector(), right.getOutputSymbols()),
                MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(join.attributes()),
                filter,
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(join.attributes())).map(ToOldIrRelationalRewriter::rewriteJoinDistributionType),
                Optional.ofNullable(SPILLABLE.getAttribute(join.attributes())),
                dynamicFilters.buildOrThrow(),
                Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(join.attributes())));
    }

    private static JoinType rewriteJoinType(JoinOperationMetadata.JoinType type)
    {
        return switch (type) {
            case INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT;
            case RIGHT -> JoinType.RIGHT;
            case FULL -> JoinType.FULL;
        };
    }

    private static JoinNode.DistributionType rewriteJoinDistributionType(JoinOperationMetadata.DistributionType type)
    {
        return switch (type) {
            case PARTITIONED -> JoinNode.DistributionType.PARTITIONED;
            case REPLICATED -> JoinNode.DistributionType.REPLICATED;
        };
    }

    @Override
    public PlanNode visitLimit(Limit limit, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        Optional<OrderingScheme> tiesResolvingScheme = getOptionalOrderingScheme(LimitOperationMetadata.SORT_ORDERS.getAttribute(limit.attributes()), limit.orderingSelector(), source.getOutputSymbols());

        // build pre-sorted symbols list
        List<Integer> preSortedIndexes = PRE_SORTED_INDEXES.getAttribute(limit.attributes());
        List<Symbol> preSortedInputs = preSortedIndexes.stream()
                .map(tiesResolvingScheme.map(OrderingScheme::orderBy).orElse(ImmutableList.of())::get)
                .collect(toImmutableList());

        return new LimitNode(
                planNodeIdAllocator.getNextId(),
                source,
                LimitOperationMetadata.COUNT.getAttribute(limit.attributes()),
                tiesResolvingScheme,
                LimitOperationMetadata.PARTIAL.getAttribute(limit.attributes()),
                preSortedInputs);
    }

    @Override
    public PlanNode visitOutput(Output output, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        return new OutputNode(
                planNodeIdAllocator.getNextId(),
                source,
                COLUMN_NAMES.getAttribute(output.attributes()),
                scalarRewriter.getSelectedSymbols(output.outputFieldSelector(), source.getOutputSymbols()));
    }

    @Override
    public PlanNode visitProject(Project project, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        // build project assignments
        List<Expression> assignmentExpressions = scalarRewriter.getExpressions(project.assignments(), source.getOutputSymbols());
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        assignmentExpressions.stream()
                .forEach(expression -> assignmentsBuilder.put(symbolAllocator.newSymbol(expression), expression));

        return new ProjectNode(
                planNodeIdAllocator.getNextId(),
                source,
                assignmentsBuilder.build());
    }

    @Override
    public PlanNode visitSort(Sort sort, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        return new SortNode(
                planNodeIdAllocator.getNextId(),
                source,
                getOptionalOrderingScheme(SortOperationMetadata.SORT_ORDERS.getAttribute(sort.attributes()), sort.orderingSelector(), source.getOutputSymbols()).orElseThrow(),
                PARTIAL.getAttribute(sort.attributes()));
    }

    @Override
    public PlanNode visitTableScan(TableScan tableScan, List<PlanNode> sources)
    {
        checkArgument(sources.isEmpty(), "Expected no arguments for TableScan operation");

        List<Type> outputTypes = relationRowType(trinoType(tableScan.result().type())).getTypeParameters();
        List<ColumnHandle> columnHandles = COLUMN_HANDLES.getAttribute(tableScan.attributes());

        // allocate output symbols
        List<Symbol> outputSymbols = IntStream.range(0, columnHandles.size())
                .boxed()
                .map(columnIndex -> {
                    ColumnHandle columnHandle = columnHandles.get(columnIndex);
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, TABLE_HANDLE.getAttribute(tableScan.attributes()), columnHandle);
                    return symbolAllocator.newSymbol(columnMetadata.getName(), outputTypes.get(columnIndex));
                })
                .collect(toImmutableList());

        // build column handle assignments
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < outputSymbols.size(); i++) {
            assignments.put(outputSymbols.get(i), columnHandles.get(i));
        }

        // build statistics
        Optional<Statistics> statistics = Optional.ofNullable(STATISTICS.getAttribute(tableScan.attributes()));
        Optional<PlanNodeStatsEstimate> statsEstimate = statistics.map(stats -> new PlanNodeStatsEstimate(
                stats.outputRowCount(),
                stats.fieldStatistics().entrySet().stream()
                        .collect(toImmutableMap(entry -> outputSymbols.get(entry.getKey()), Map.Entry::getValue))));

        return new TableScanNode(
                planNodeIdAllocator.getNextId(),
                TABLE_HANDLE.getAttribute(tableScan.attributes()),
                outputSymbols,
                assignments.buildOrThrow(),
                CONSTRAINT.getAttribute(tableScan.attributes()),
                statsEstimate,
                UPDATE_TARGET.getAttribute(tableScan.attributes()),
                Optional.ofNullable(USE_CONNECTOR_NODE_PARTITIONING.getAttribute(tableScan.attributes())));
    }

    @Override
    public PlanNode visitTopN(TopN topN, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        return new TopNNode(
                planNodeIdAllocator.getNextId(),
                source,
                LIMIT.getAttribute(topN.attributes()),
                getOptionalOrderingScheme(TopNOperationMetadata.SORT_ORDERS.getAttribute(topN.attributes()), topN.orderingSelector(), source.getOutputSymbols()).orElseThrow(),
                rewriteTopNStep(TOP_N_STEP.getAttribute(topN.attributes())));
    }

    private static TopNNode.Step rewriteTopNStep(TopNStep step)
    {
        return switch (step) {
            case SINGLE -> TopNNode.Step.SINGLE;
            case PARTIAL -> TopNNode.Step.PARTIAL;
            case FINAL -> TopNNode.Step.FINAL;
        };
    }

    @Override
    public PlanNode visitValues(Values values, List<PlanNode> sources)
    {
        checkArgument(sources.isEmpty(), "Expected no arguments for Values operation");

        // allocate output symbols
        List<Symbol> outputSymbols = relationRowType(trinoType(values.result().type())).getTypeParameters().stream()
                .map(type -> symbolAllocator.newSymbol("field", type))
                .collect(toImmutableList());

        if (outputSymbols.isEmpty()) {
            return new ValuesNode(
                    planNodeIdAllocator.getNextId(),
                    toIntExact(CARDINALITY.getAttribute(values.attributes())));
        }

        return new ValuesNode(
                planNodeIdAllocator.getNextId(),
                outputSymbols,
                values.rows().stream()
                        .map(block -> scalarRewriter.toOldIr(block, ImmutableList.of()))
                        .collect(toImmutableList()));
    }

    @Override
    public PlanNode visitWindow(Window window, List<PlanNode> sources)
    {
        PlanNode source = getOnlyElement(sources);

        // build window functions
        List<WindowNode.Function> windowFunctions = getWindowFunctions(window.windowFunctionCalls(), source.getOutputSymbols());
        List<Type> windowFunctionTypes = trinoType(window.windowFunctionCalls().getReturnedType()).getTypeParameters();
        ImmutableMap.Builder<Symbol, WindowNode.Function> windowFunctionsBuilder = ImmutableMap.builder();
        for (int i = 0; i < windowFunctions.size(); i++) {
            WindowNode.Function windowFunction = windowFunctions.get(i);
            windowFunctionsBuilder.put(symbolAllocator.newSymbol(windowFunction.getResolvedFunction().name().getFunctionName(), windowFunctionTypes.get(i)), windowFunction);
        }

        List<Symbol> partitionBy = scalarRewriter.getSelectedSymbols(window.partitioningSelector(), source.getOutputSymbols());
        Optional<OrderingScheme> orderingScheme = getOptionalOrderingScheme(WindowOperationMetadata.SORT_ORDERS.getAttribute(window.attributes()), window.orderingSelector(), source.getOutputSymbols());

        return new WindowNode(
                planNodeIdAllocator.getNextId(),
                source,
                new DataOrganizationSpecification(partitionBy, orderingScheme),
                windowFunctionsBuilder.buildOrThrow(),
                PRE_PARTITIONED_INDEXES.getAttribute(window.attributes()).stream()
                        .map(partitionBy::get)
                        .collect(toImmutableSet()),
                PRE_SORTED_PREFIX.getAttribute(window.attributes()));
    }

    private List<WindowNode.Function> getWindowFunctions(Block windowFunctionCalls, List<Symbol> inputSymbols)
    {
        if (isEmptyRelationalComputation(windowFunctionCalls)) {
            return ImmutableList.of();
        }

        Map<Value, Operation> operations = windowFunctionCalls.operations().stream()
                .collect(toImmutableMap(Operation::result, identity()));

        return windowFunctionCalls.operations().get(windowFunctionCalls.operations().size() - 2).arguments().stream()
                .map(operations::get)
                .map(WindowFunctionCall.class::cast)
                .map(windowFunctionCall -> getWindowFunction(windowFunctionCall, inputSymbols))
                .collect(toImmutableList());
    }

    private WindowNode.Function getWindowFunction(WindowFunctionCall windowFunctionCall, List<Symbol> inputSymbols)
    {
        return new WindowNode.Function(
                WindowFunctionCallOperationMetadata.RESOLVED_FUNCTION.getAttribute(windowFunctionCall.attributes()),
                scalarRewriter.getExpressions(windowFunctionCall.argumentsBlock(), inputSymbols),
                getOptionalOrderingScheme(WindowFunctionCallOperationMetadata.SORT_ORDERS.getAttribute(windowFunctionCall.attributes()), windowFunctionCall.orderingSelector(), inputSymbols),
                new WindowNode.Frame(
                        rewriteFrameType(FRAME_TYPE.getAttribute(windowFunctionCall.attributes())),
                        rewriteFrameBoundType(FRAME_START_TYPE.getAttribute(windowFunctionCall.attributes())),
                        scalarRewriter.getOptionalSelectedSymbol(windowFunctionCall.frameStartFieldSelector(), inputSymbols),
                        scalarRewriter.getOptionalSelectedSymbol(windowFunctionCall.sortKeyCoercedForFrameStartComparisonSelector(), inputSymbols),
                        rewriteFrameBoundType(FRAME_END_TYPE.getAttribute(windowFunctionCall.attributes())),
                        scalarRewriter.getOptionalSelectedSymbol(windowFunctionCall.frameEndFieldSelector(), inputSymbols),
                        scalarRewriter.getOptionalSelectedSymbol(windowFunctionCall.sortKeyCoercedForFrameEndComparisonSelector(), inputSymbols)),
                IGNORE_NULLS.getAttribute(windowFunctionCall.attributes()),
                DISTINCT.getAttribute(windowFunctionCall.attributes()));
    }

    private static WindowFrameType rewriteFrameType(WindowFunctionCallOperationMetadata.WindowFrameType type)
    {
        return switch (type) {
            case RANGE -> WindowFrameType.RANGE;
            case ROWS -> WindowFrameType.ROWS;
            case GROUPS -> WindowFrameType.GROUPS;
        };
    }

    private static FrameBoundType rewriteFrameBoundType(WindowFrameBoundType type)
    {
        return switch (type) {
            case UNBOUNDED_PRECEDING -> FrameBoundType.UNBOUNDED_PRECEDING;
            case PRECEDING -> FrameBoundType.PRECEDING;
            case CURRENT_ROW -> FrameBoundType.CURRENT_ROW;
            case FOLLOWING -> FrameBoundType.FOLLOWING;
            case UNBOUNDED_FOLLOWING -> FrameBoundType.UNBOUNDED_FOLLOWING;
        };
    }
}
