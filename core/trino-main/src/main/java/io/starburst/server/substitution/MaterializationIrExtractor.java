/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.materialization.ir.Aggregation;
import io.starburst.materialization.ir.Operation;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.Reference;
import io.starburst.materialization.ir.SupportedAggregateFunctions;
import io.starburst.materialization.ir.Symbol;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.ir.TableScan;
import io.trino.Session;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.function.BoundSignature;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Query;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.spi.StandardErrorCode.STACK_OVERFLOW;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Objects.requireNonNull;

public class MaterializationIrExtractor
{
    private final AnalyzerFactory analyzerFactory;

    private final PlannerContext plannerContext;
    private final SubstitutionMetadata substitutionMetadata;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final List<PlanOptimizer> planOptimizers;

    @Inject
    public MaterializationIrExtractor(
            AnalyzerFactory analyzerFactory,
            PlannerContext plannerContext,
            SubstitutionMetadata substitutionMetadata,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            PlanOptimizersFactory planOptimizersFactory)
    {
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        // Use only optimizers before the MvSubstitutionOptimizer to match plan and avoid pushdowns into table scan
        ImmutableList.Builder<PlanOptimizer> substitutionOptimizers = ImmutableList.builder();
        boolean foundMvSubstitutionOptimizer = false;
        for (PlanOptimizer optimizer : planOptimizersFactory.getPlanOptimizers()) {
            if (optimizer instanceof MvSubstitutionOptimizer) {
                foundMvSubstitutionOptimizer = true;
                break;
            }
            substitutionOptimizers.add(optimizer);
        }
        checkArgument(foundMvSubstitutionOptimizer, "MvSubstitutionOptimizer must be present in the optimizer pipeline");

        this.planOptimizers = substitutionOptimizers.build();
    }

    public Optional<Output> extract(
            Session session,
            Query query,
            List<Expression> parameters)
    {
        LogicalPlanner.PlanningResult planningResult = getLogicalPlanningResult(session, query, parameters);
        return planningResult.oldIrPlan().getRoot().accept(new Visitor(session), null)
                .map(root -> (Output) root);
    }

    private LogicalPlanner.PlanningResult getLogicalPlanningResult(
            Session session,
            Query query,
            List<Expression> parameters)
    {
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                session,
                parameters,
                bindParameters(query, parameters),
                WarningCollector.NOOP,
                new PlanOptimizersStatsCollector(MAX_VALUE));
        Analysis analysis;
        try {
            analysis = analyzer.analyze(query);
        }
        catch (StackOverflowError e) {
            throw new TrinoException(STACK_OVERFLOW, "statement is too large (stack overflow during analysis)", e);
        }

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                planOptimizers,
                ImmutableList.of(),
                new PlanNodeIdAllocator(),
                plannerContext,
                statsCalculator,
                costCalculator,
                WarningCollector.NOOP,
                new PlanOptimizersStatsCollector(MAX_VALUE),
                new CachingTableStatsProvider(plannerContext.getMetadata(), session, () -> false),
                FormatOptions.TESTING_FORMAT_OPTIONS,
                MvSubstitutionCollector.NOOP);
        return logicalPlanner.plan(analysis, OPTIMIZED, false, false);
    }

    static Aggregation.Step toIrStep(AggregationNode.Step step)
    {
        return switch (step) {
            case SINGLE -> Aggregation.Step.SINGLE;
            case PARTIAL -> Aggregation.Step.PARTIAL;
            case INTERMEDIATE -> Aggregation.Step.INTERMEDIATE;
            case FINAL -> Aggregation.Step.FINAL;
        };
    }

    private static Optional<Aggregation.AggregateFunctionCall> toAggregation(io.trino.sql.planner.Symbol output, AggregationNode.Aggregation aggregation)
    {
        BoundSignature function = aggregation.getResolvedFunction().signature();
        if (!SupportedAggregateFunctions.contains(function.getName())) {
            return Optional.empty();
        }
        ImmutableList.Builder<io.starburst.materialization.ir.Expression> arguments = ImmutableList.builder();
        for (io.trino.sql.ir.Expression argument : aggregation.getArguments()) {
            // A lambda argument has no persisted form yet
            if (!(argument instanceof io.trino.sql.ir.Reference reference)) {
                return Optional.empty();
            }
            arguments.add(new Reference(toSymbol(io.trino.sql.planner.Symbol.from(reference))));
        }
        return Optional.of(new Aggregation.AggregateFunctionCall(
                toSymbol(output),
                function,
                arguments.build(),
                aggregation.isDistinct(),
                aggregation.getFilter().map(MaterializationIrExtractor::toSymbol),
                aggregation.getOrderingScheme().map(MaterializationIrExtractor::toOrderingScheme),
                aggregation.getMask().map(MaterializationIrExtractor::toSymbol)));
    }

    private static Aggregation.OrderingScheme toOrderingScheme(OrderingScheme orderingScheme)
    {
        return new Aggregation.OrderingScheme(orderingScheme.orderBy().stream()
                .map(symbol -> new Aggregation.SortItem(toSymbol(symbol), orderingScheme.ordering(symbol)))
                .collect(toImmutableList()));
    }

    private static Symbol toSymbol(io.trino.sql.planner.Symbol symbol)
    {
        return new Symbol(symbol.type(), symbol.name());
    }

    private class Visitor
            extends PlanVisitor<Optional<Operation>, Void>
    {
        private final Session session;

        private Visitor(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        protected Optional<Operation> visitPlan(PlanNode node, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<Operation> visitOutput(OutputNode node, Void context)
        {
            return node.getSource()
                    .accept(this, context).map(source -> new Output(
                            node.getColumnNames(),
                            node.getOutputSymbols().stream()
                                    .map(symbol -> new Symbol(symbol.type(), symbol.name()))
                                    .collect(toImmutableList()),
                            source));
        }

        @Override
        public Optional<Operation> visitTableScan(TableScanNode node, Void context)
        {
            Optional<TableId> maybeTableId = substitutionMetadata.getTableId(session, node.getTable());
            if (maybeTableId.isEmpty()) {
                return Optional.empty();
            }
            TableId tableId = maybeTableId.get();
            ImmutableMap.Builder<ConnectorColumnId, Symbol> assignments = ImmutableMap.builder();
            for (Map.Entry<io.trino.sql.planner.Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                Optional<ConnectorColumnId> columnId = substitutionMetadata.getColumnId(session, node.getTable(), entry.getValue());
                if (columnId.isEmpty()) {
                    return Optional.empty();
                }
                assignments.put(columnId.get(), new Symbol(entry.getKey().type(), entry.getKey().name()));
            }
            return Optional.of(new TableScan(tableId, assignments.buildOrThrow()));
        }

        @Override
        public Optional<Operation> visitAggregation(AggregationNode node, Void context)
        {
            Optional<Operation> source = node.getSource().accept(this, context);
            if (source.isEmpty()) {
                return Optional.empty();
            }
            ImmutableList.Builder<Aggregation.AggregateFunctionCall> aggregations = ImmutableList.builder();
            for (Map.Entry<io.trino.sql.planner.Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                Optional<Aggregation.AggregateFunctionCall> aggregation = toAggregation(entry.getKey(), entry.getValue());
                if (aggregation.isEmpty()) {
                    return Optional.empty();
                }
                aggregations.add(aggregation.get());
            }
            return Optional.of(new Aggregation(
                    source.get(),
                    new Aggregation.GroupingSets(
                            node.getGroupingKeys().stream().map(MaterializationIrExtractor::toSymbol).collect(toImmutableList()),
                            node.getGroupingSetCount(),
                            node.getGlobalGroupingSets()),
                    aggregations.build(),
                    toIrStep(node.getStep()),
                    node.getGroupIdSymbol().map(MaterializationIrExtractor::toSymbol)));
        }
    }
}
