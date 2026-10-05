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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.iterative.Rule.Result;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.intersection;
import static io.trino.SystemSessionProperties.isPushPartialAggregationThroughExpandingJoin;
import static io.trino.SystemSessionProperties.isPushPartialAggregationThroughJoin;
import static io.trino.sql.planner.iterative.rule.PushProjectionThroughJoin.pushProjectionThroughJoin;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Double.isNaN;

public class PushPartialAggregationThroughJoin
{
    // Pushing the aggregation adds a hash aggregation, so require the collapsed relation to be an order of
    // magnitude smaller than the join input it replaces.
    private static final double JOIN_INPUT_COLLAPSE_RATIO = 10;

    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations
        if (aggregationNode.isStreamable()) {
            return false;
        }

        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    public Iterable<Rule<?>> rules()
    {
        return ImmutableList.of(
                pushPartialAggregationThroughJoinWithoutProjection(),
                pushPartialAggregationThroughJoinWithProjection());
    }

    @VisibleForTesting
    Rule<?> pushPartialAggregationThroughJoinWithoutProjection()
    {
        return new PushPartialAggregationThroughJoinWithoutProjection();
    }

    @VisibleForTesting
    Rule<?> pushPartialAggregationThroughJoinWithProjection()
    {
        return new PushPartialAggregationThroughJoinWithProjection();
    }

    private class PushPartialAggregationThroughJoinWithoutProjection
            implements Rule<AggregationNode>
    {
        private static final Pattern<AggregationNode> PATTERN_WITHOUT_PROJECTION = aggregation()
                .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
                .with(source().matching(join()));

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN_WITHOUT_PROJECTION;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isPushPartialAggregationThroughJoin(session);
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            return applyPushdown(node, context);
        }
    }

    private class PushPartialAggregationThroughJoinWithProjection
            implements Rule<AggregationNode>
    {
        private static final Pattern<AggregationNode> PATTERN_WITH_PROJECTION = aggregation()
                .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
                .with(source().matching(project().with(source().matching(join()))));

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN_WITH_PROJECTION;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isPushPartialAggregationThroughJoin(session);
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            ProjectNode projectNode = (ProjectNode) context.getLookup().resolve(node.getSource());
            Optional<PlanNode> joinNodeOptional = pushProjectionThroughJoin(projectNode, context.getLookup(), context.getIdAllocator());
            if (joinNodeOptional.isEmpty()) {
                return Result.empty();
            }
            return applyPushdown((AggregationNode) node.replaceChildren(ImmutableList.of(joinNodeOptional.get())), context);
        }
    }

    private Result applyPushdown(AggregationNode aggregationNode, Context context)
    {
        JoinNode joinNode = (JoinNode) context.getLookup().resolve(aggregationNode.getSource());

        if (joinNode.getType() != JoinType.INNER) {
            return Result.empty();
        }

        boolean aggregationsFitLeft = allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputSymbols());
        boolean aggregationsFitRight = allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputSymbols());

        Optional<AggregationNode> pushedToLeft = Optional.empty();
        if (aggregationsFitLeft) {
            pushedToLeft = getPushedAggregation(aggregationNode, joinNode, joinNode.getLeft(), context);
        }
        Optional<AggregationNode> pushedToRight = Optional.empty();
        if (aggregationsFitRight) {
            pushedToRight = getPushedAggregation(aggregationNode, joinNode, joinNode.getRight(), context);
        }

        // An aggregation reading no input symbols, count(*) being the common one, fits either side. Grouping a side
        // on the join key divides the join output by that side's collapse factor, so the larger factor wins.
        boolean pushToRight = pushedToRight.isPresent();
        if (pushedToLeft.isPresent() && pushedToRight.isPresent()) {
            pushToRight = collapsesMoreRows(pushedToRight.get(), pushedToLeft.get(), context);
        }

        if (pushToRight) {
            return Result.ofPlanNode(replaceJoin(aggregationNode, pushedToRight.get(), joinNode, joinNode.getLeft(), pushedToRight.get(), context));
        }
        if (pushedToLeft.isPresent()) {
            return Result.ofPlanNode(replaceJoin(aggregationNode, pushedToLeft.get(), joinNode, pushedToLeft.get(), joinNode.getRight(), context));
        }

        return Result.empty();
    }

    private static boolean collapsesMoreRows(AggregationNode candidate, AggregationNode incumbent, Context context)
    {
        OptionalDouble candidateRatio = estimateCollapseRatio(candidate, context);
        OptionalDouble incumbentRatio = estimateCollapseRatio(incumbent, context);
        if (candidateRatio.isEmpty() || incumbentRatio.isEmpty()) {
            return false;
        }
        return candidateRatio.orElseThrow() > incumbentRatio.orElseThrow();
    }

    // Rows the join reads from a side shrink by this factor once the aggregation groups them. Grouping sets wider
    // than one symbol have no ratio: stats treat grouping symbols as uncorrelated, so multiplying their distinct
    // value counts overstates the group count and understates the collapse.
    private static OptionalDouble estimateCollapseRatio(AggregationNode pushedAggregation, Context context)
    {
        List<Symbol> groupingKeys = pushedAggregation.getGroupingKeys();
        if (groupingKeys.size() > 1) {
            return OptionalDouble.empty();
        }
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(pushedAggregation.getSource());
        double sourceRowCount = sourceStats.getOutputRowCount();
        if (isNaN(sourceRowCount)) {
            return OptionalDouble.empty();
        }
        if (groupingKeys.isEmpty()) {
            return OptionalDouble.of(sourceRowCount);
        }
        double distinctValues = sourceStats.getSymbolStatistics(getOnlyElement(groupingKeys)).getDistinctValuesCount();
        if (isNaN(distinctValues) || distinctValues <= 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(sourceRowCount / distinctValues);
    }

    private static boolean allAggregationsOn(Map<Symbol, Aggregation> aggregations, List<Symbol> symbols)
    {
        Set<Symbol> inputs = aggregations.values().stream()
                .map(SymbolsExtractor::extractAll)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        return symbols.containsAll(inputs);
    }

    private Optional<AggregationNode> getPushedAggregation(AggregationNode node, JoinNode child, PlanNode joinSource, Context context)
    {
        Set<Symbol> joinSourceSymbols = ImmutableSet.copyOf(joinSource.getOutputSymbols());
        List<Symbol> groupingSet = getPushedDownGroupingSet(node, joinSourceSymbols, intersection(getJoinRequiredSymbols(child), joinSourceSymbols));
        AggregationNode pushedAggregation = replaceAggregationSource(node, joinSource, groupingSet);
        if (skipPartialAggregationPushdown(child, node, pushedAggregation, context)) {
            return Optional.empty();
        }
        return Optional.of(pushedAggregation);
    }

    private boolean skipPartialAggregationPushdown(
            JoinNode join,
            AggregationNode originalAggregation,
            AggregationNode pushedAggregation,
            Context context)
    {
        // Only push aggregation down if pushed aggregation consumes similar number of input rows (e.g. join is not expanding).
        // Otherwise, there is a possibility that aggregation above join could be more efficient in reducing total number of rows.
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(pushedAggregation.getSource());
        double sourceRowCount = sourceStats.getOutputRowCount();
        double joinRowCount = context.getStatsProvider().getStats(join).getOutputRowCount();
        // Pushing aggregation through filtering join could mean more work for partial aggregation. However,
        // we allow pushing partial aggregations through filtering join because:
        // 1. dynamic filtering should filter unmatched rows at source
        // 2. partial aggregation will adaptively switch off when it's not reducing input rows
        // 3. join operator is not particularly efficient at filtering rows

        // For an expanding cross join , only reason 2 applies. Adaptive shutdown caps the downside,
        // while pushdown can be a large win when the aggregation reduces rows well below the Cartesian product size.
        if (isNaN(sourceRowCount) || isNaN(joinRowCount)) {
            return true;
        }

        // Grouping an expanding join's input on its own join key leaves one row per key, so the join reads the collapsed
        // relation. A single grouping symbol keeps the group count exact, so wider grouping sets are excluded.
        Set<Symbol> pushedGroupingKeys = ImmutableSet.copyOf(pushedAggregation.getGroupingKeys());
        Set<Symbol> sourceJoinKeys = intersection(
                getJoinRequiredSymbols(join),
                ImmutableSet.copyOf(pushedAggregation.getSource().getOutputSymbols()));
        boolean joinExpandsPushedInput = joinRowCount > 1.1 * sourceRowCount;
        boolean collapsesJoinInputOnJoinKey = false;
        if (joinExpandsPushedInput && pushedGroupingKeys.size() == 1 && pushedGroupingKeys.equals(sourceJoinKeys)) {
            double joinKeyDistinctValues = sourceStats.getSymbolStatistics(getOnlyElement(pushedGroupingKeys)).getDistinctValuesCount();
            collapsesJoinInputOnJoinKey = !isNaN(joinKeyDistinctValues) && joinKeyDistinctValues > 0 && joinKeyDistinctValues * JOIN_INPUT_COLLAPSE_RATIO <= sourceRowCount;
        }

        if (joinExpandsPushedInput
                && !collapsesJoinInputOnJoinKey
                && !(isPushPartialAggregationThroughExpandingJoin(context.getSession()) && join.isCrossJoin())) {
            return true;
        }

        // Only push aggregation down if pushed grouping set is of same size or smaller. This is because
        // we want pushdown to happen for star schema queries like:
        //
        // select sum(sales) from fact, date_dim where fact.date_id = date_dim.date_id group by date_dim.year
        //
        // In such case partial aggregation on date_dim.year can be pushed
        // below join with grouping key of "date_id". This can greatly reduce number
        // of rows before join operator. A grouping set that collapses the join input is exempt, since it shrinks
        // that input whatever its size.
        if (!collapsesJoinInputOnJoinKey
                && ImmutableSet.copyOf(originalAggregation.getGroupingKeys()).size() < ImmutableSet.copyOf(pushedAggregation.getGroupingKeys()).size()) {
            return true;
        }

        // Do not push aggregation down if any pushed grouping symbol has NDV that has the
        // same order of magnitude as number of source rows (because then partial aggregation is ineffective).
        // Ideally we should use estimated aggregation row count. However, we assume lack of correlation
        // between group by columns in stats calculations. Therefore, if we used estimated aggregation row count
        // then we would miss good improvement opportunities. Even if partial aggregation is pushed down incorrectly,
        // it should be adaptively turned off, hence potential performance penalty is not that significant.
        for (Symbol symbol : pushedAggregation.getGroupingKeys()) {
            double ndv = sourceStats.getSymbolStatistics(symbol).getDistinctValuesCount();
            if (isNaN(ndv) || ndv * 2 > sourceRowCount) {
                return true;
            }
        }

        return false;
    }

    private Set<Symbol> getJoinRequiredSymbols(JoinNode node)
    {
        return Streams.concat(
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                        node.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of()).stream())
                .collect(toImmutableSet());
    }

    private List<Symbol> getPushedDownGroupingSet(AggregationNode aggregation, Set<Symbol> availableSymbols, Set<Symbol> requiredJoinSymbols)
    {
        List<Symbol> groupingSet = aggregation.getGroupingKeys();

        // keep symbols that are directly from the join's child (availableSymbols)
        List<Symbol> pushedDownGroupingSet = groupingSet.stream()
                .filter(availableSymbols::contains)
                .collect(Collectors.toList());

        // add missing required join symbols to grouping set
        Set<Symbol> existingSymbols = new HashSet<>(pushedDownGroupingSet);
        requiredJoinSymbols.stream()
                .filter(existingSymbols::add)
                .forEach(pushedDownGroupingSet::add);

        return pushedDownGroupingSet;
    }

    private AggregationNode replaceAggregationSource(
            AggregationNode aggregation,
            PlanNode source,
            List<Symbol> groupingKeys)
    {
        return AggregationNode.builderFrom(aggregation)
                .setSource(source)
                .setGroupingSets(singleGroupingSet(groupingKeys))
                .setPreGroupedSymbols(ImmutableList.of())
                // aggregation below join might not be as effective in reducing rows before exchange
                .setIsInputReducingAggregation(false)
                .build();
    }

    private PlanNode replaceJoin(
            AggregationNode aggregation,
            AggregationNode pushedAggregation,
            JoinNode child,
            PlanNode leftChild,
            PlanNode rightChild,
            Context context)
    {
        JoinNode joinNode = new JoinNode(
                child.getId(),
                child.getType(),
                leftChild,
                rightChild,
                child.getCriteria(),
                leftChild.getOutputSymbols(),
                rightChild.getOutputSymbols(),
                child.isMaySkipOutputDuplicates(),
                child.getFilter(),
                child.getDistributionType(),
                child.isSpillable(),
                child.getDynamicFilters(),
                child.getReorderJoinStatsAndCost());
        PlanNode result = restrictOutputs(context.getIdAllocator(), joinNode, ImmutableSet.copyOf(aggregation.getOutputSymbols())).orElse(joinNode);
        // Keep an intermediate aggregation above the join to reduce rows before any remote exchange.
        // We only do this for input-reducing aggregations. For cross joins this is sufficient on its
        // own, since cross joins can arbitrarily multiply row counts. For non-cross joins,
        // skip it only when the original aggregation grouping keys contain all grouping keys of the pushed
        //  aggregation, which is the heuristic condition used here.
        if (aggregation.isInputReducingAggregation() &&
                (joinNode.isCrossJoin() || !ImmutableSet.copyOf(aggregation.getGroupingKeys()).containsAll(pushedAggregation.getGroupingKeys()))) {
            result = toIntermediateAggregation(aggregation, result, context);
        }
        return result;
    }

    private PlanNode toIntermediateAggregation(AggregationNode partialAggregation, PlanNode source, Context context)
    {
        ImmutableMap.Builder<Symbol, Aggregation> intermediateAggregation = ImmutableMap.builder();
        for (Entry<Symbol, AggregationNode.Aggregation> entry : partialAggregation.getAggregations().entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            ResolvedFunction resolvedFunction = aggregation.getResolvedFunction();

            // rewrite partial aggregation in terms of intermediate function
            intermediateAggregation.put(
                    entry.getKey(),
                    new AggregationNode.Aggregation(
                            resolvedFunction,
                            ImmutableList.<Expression>builder()
                                    .add(entry.getKey().toSymbolReference())
                                    .addAll(aggregation.getArguments().stream()
                                            .filter(Lambda.class::isInstance)
                                            .collect(toImmutableList()))
                                    .build(),
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
        }

        return new AggregationNode(
                context.getIdAllocator().getNextId(),
                source,
                intermediateAggregation.buildOrThrow(),
                partialAggregation.getGroupingSets(),
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the join may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                ImmutableList.of(),
                INTERMEDIATE,
                partialAggregation.getGroupIdSymbol());
    }
}
