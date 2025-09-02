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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Type;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalContext;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStates;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStatesAndCheckpointMapping;
import io.trino.sql.planner.optimizations.ctereuse.SingleGroupMerger.SingleGroupMergeDecomposition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.Attributes.DISTINCT;
import static io.trino.sql.dialect.trino.Attributes.GLOBAL_GROUPING_SETS;
import static io.trino.sql.dialect.trino.Attributes.GROUPING_SETS_COUNT;
import static io.trino.sql.dialect.trino.Attributes.GROUP_ID_INDEX;
import static io.trino.sql.dialect.trino.Attributes.INPUT_REDUCING;
import static io.trino.sql.dialect.trino.Attributes.PRE_GROUPED_INDEXES;
import static io.trino.sql.dialect.trino.Attributes.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.Attributes.SORT_ORDERS;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.composeProjectedItems;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPassthroughMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getSelectedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyRelationalComputation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.rebasePredicateAndPruneUnsupportedConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.filterConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.SemanticEquivalenceUtils.blocksSemanticallyEquivalent;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class AggregationMerger
        implements SingleGroupMerger.SingleGroupProcessor
{
    @Override
    public boolean processes(Operation operation)
    {
        return operation instanceof Aggregation;
    }

    /**
     * Identify groups of Aggregation operations with the same grouping, and potentially different aggregate functions.
     * Only deterministic aggregations are included.
     * Also, any branches are excluded where the residual predicate is based on other fields than the grouping keys.
     */
    @Override
    public SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(UnifiedStates unifiedStates, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<TraversalState> branches = unifiedStates.residualStates();
        Operation unifiedOperation = unifiedStates.unifiedOperation();

        int[] aggregationSubgroups = new int[branches.size()];
        Arrays.fill(aggregationSubgroups, -1);
        Map<Integer, AggregationAndPredicate> subgroupRepresentatives = new HashMap<>();

        // create an empty aggregate calls block to avoid rebasing. Aggregate calls are not compared.
        Block emptyAggregateCallsBlock = emptyAggregateCallsBlock(unifiedOperation.result().type(), nameAllocator);

        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            Operation nextOperation = branch.nextOperation().operation();
            if (nextOperation instanceof Aggregation aggregation && isDeterministic(aggregation)) {
                Block rebasedGroupingKeysSelector = rebaseBlock(aggregation.groupingKeysSelector(), relationRowType(trinoType(unifiedOperation.result().type())), branch.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
                // when we merge this aggregation with other aggregations, we will have to pull the traversal context through the merged operation.
                // It is only possible if the residual predicate is based on the grouping keys.
                // We consider the branches mergeable when the conjuncts in their residual predicates which are _not_ based only on the grouping keys are equal.
                // The conjuncts that are equal for all branches will be output and removed from branches as part of common semantics in CteReuse.outputCommonSemantics()
                // after the group is split recursively.
                Set<Integer> groupingKeys = ImmutableSet.copyOf(getSelectedFields(rebasedGroupingKeysSelector));
                Block nonGroupingPredicateToApply = filterConjuncts(
                        branch.traversalContext().predicateToApply(),
                        conjunct -> !groupingKeys.containsAll(extractReferencedFields(conjunct, getOnlyElement(conjunct.parameters()))),
                        nameAllocator);
                // find a matching Aggregation subgroup
                boolean foundMatchingSubgroup = false;
                for (Map.Entry<Integer, AggregationAndPredicate> subgroupRepresentative : subgroupRepresentatives.entrySet()) {
                    if (subgroupRepresentative.getValue().aggregation().attributes().equals(aggregation.attributes()) &&
                            blocksSemanticallyEquivalent(subgroupRepresentative.getValue().aggregation().groupingKeysSelector(), rebasedGroupingKeysSelector) &&
                            blocksSemanticallyEquivalent(subgroupRepresentative.getValue().nonGroupingPredicateToApply(), nonGroupingPredicateToApply)) {
                        aggregationSubgroups[i] = subgroupRepresentative.getKey();
                        foundMatchingSubgroup = true;
                        break;
                    }
                }
                // if there is no matching subgroup, start a new subgroup
                if (!foundMatchingSubgroup) {
                    aggregationSubgroups[i] = i;
                    Aggregation rebasedRepresentative = new Aggregation(
                            nameAllocator.newName(),
                            unifiedOperation.result(),
                            // insert empty aggregate calls. they are not compared.
                            emptyAggregateCallsBlock,
                            rebasedGroupingKeysSelector,
                            GROUPING_SETS_COUNT.getAttribute(aggregation.attributes()),
                            GLOBAL_GROUPING_SETS.getAttribute(aggregation.attributes()),
                            Optional.ofNullable(GROUP_ID_INDEX.getAttribute(aggregation.attributes())).map(OptionalInt::of).orElse(OptionalInt.empty()),
                            PRE_GROUPED_INDEXES.getAttribute(aggregation.attributes()),
                            AGGREGATION_STEP.getAttribute(aggregation.attributes()),
                            INPUT_REDUCING.getAttribute(aggregation.attributes()),
                            ImmutableMap.of());
                    subgroupRepresentatives.put(i, new AggregationAndPredicate(rebasedRepresentative, nonGroupingPredicateToApply));
                }
            }
        }
        // extract subgroups
        Multimap<Integer, Integer> subgroups = ArrayListMultimap.create();
        for (int i = 0; i < aggregationSubgroups.length; i++) {
            if (aggregationSubgroups[i] != -1) {
                subgroups.put(aggregationSubgroups[i], i);
            }
        }

        // return subgroups with two or more elements
        return new SingleGroupMergeDecomposition(subgroups.asMap().values().stream()
                .filter(indexes -> indexes.size() > 1)
                .map(ImmutableList::copyOf)
                .collect(toImmutableList()));
    }

    private static Block emptyAggregateCallsBlock(Type type, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), type);
        Block.Builder builder = new Block.Builder(Optional.of("^aggregates"), ImmutableList.of(parameter));
        Constant constantNull = new Constant(nameAllocator.newName(), EMPTY_ROW, null);
        builder.addOperation(constantNull);
        Return returnOperation = new Return(nameAllocator.newName(), constantNull.result(), constantNull.attributes());
        builder.addOperation(returnOperation);
        return builder.build();
    }

    /**
     * Unify multiple aggregations.
     * <p>
     * The component aggregations have the same grouping and hash field. The resulting aggregation has the same
     * grouping and hash field as all the component operations, and those fields are laid out first (before the aggregates),
     * the same as in the component operations.
     * <p>
     * The resulting Aggregation operation includes aggregates from all component Aggregation operations.
     * The aggregates are deduplicated across the component aggregations based on semantic equivalence.
     * However, when a component aggregation produces multiple semantically equivalent aggregates, these are not deduplicated.
     * The reason for this is that the FieldMapping used to rebase the next downstream operation from the component aggregation
     * onto the unified aggregation must be a reversible BiMap. Therefore, each field of the original aggregation must be mapped
     * to a distinct field of the unified aggregation.
     * <p>
     * The residual predicates of all merged branches are based on the grouping keys.
     */
    @Override
    public UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
            Operation unifiedOperation,
            List<TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations)
    {
        TraversalState firstBranch = branches.getFirst();
        Aggregation firstAggregation = (Aggregation) firstBranch.nextOperation().operation();

        Block rebasedGroupingKeysSelector = rebaseBlock(firstAggregation.groupingKeysSelector(), relationRowType(trinoType(unifiedOperation.result().type())), firstBranch.traversalContext().fieldMapping(), nameAllocator).orElseThrow();

        // the mapping to rebase the context for each branch: predicate to apply and enforced predicate from the recent unified operation onto the new unified Aggregation operation
        // the predicate to apply is fully supported by the grouping keys per identifySingleGroupMergeCandidates() method
        // the enforced predicate is not guaranteed to be fully supported. The unsupported part will be pruned
        FieldMapping predicateMapping = getPassthroughMapping(rebasedGroupingKeysSelector);

        int groupingKeysCount = trinoType(rebasedGroupingKeysSelector.getReturnedType()).getTypeParameters().size();
        Map<Integer, Integer> groupingKeysFieldMapping = IntStream.range(0, groupingKeysCount)
                .boxed()
                .collect(toImmutableBiMap(identity(), identity()));

        // the unified aggregation must include all aggregate calls from the component aggregation operations
        List<Block> unifiedAggregates = new ArrayList<>();

        // for each branch, the mapping to rebase the next downstream operation from the component Aggregation operation onto the new unified Aggregation operation
        List<FieldMapping> newBranchMappings = new ArrayList<>();

        for (TraversalState branch : branches) {
            Aggregation aggregation = (Aggregation) branch.nextOperation().operation();

            List<Block> currentAggregates = rebaseAndExtractAggregateCalls(aggregation.aggregateCalls(), unifiedOperation.result().type(), branch.traversalContext().fieldMapping(), nameAllocator);

            // each aggregate call of the current aggregation operation must be mapped to a distinct aggregate call in the unified aggregation,
            // even if they are identical (they cannot be deduplicated). It's a result of FieldMapping being a reversible BiMap.
            // therefore, each index in the unified aggregates list can be used only once
            Set<Integer> freeUnifiedIndexes = new LinkedHashSet<>();
            IntStream.range(0, unifiedAggregates.size())
                    .forEach(freeUnifiedIndexes::add);

            // the mapping to rebase the next downstream operation from the component Aggregation operation onto the new unified Aggregation operation
            Map<Integer, Integer> aggregateFieldsMapping = new HashMap<>();

            for (int i = 0; i < currentAggregates.size(); i++) {
                Block currentAggregate = currentAggregates.get(i);
                boolean foundUnifiedAggregate = false;
                for (int j = 0; j < unifiedAggregates.size(); j++) {
                    if (freeUnifiedIndexes.contains(j) && blocksSemanticallyEquivalent(currentAggregate, unifiedAggregates.get(j))) {
                        // aggregates are laid out after the grouping keys and hash symbol -- shift the mapping
                        aggregateFieldsMapping.put(groupingKeysCount + i, groupingKeysCount + j);
                        freeUnifiedIndexes.remove(j);
                        foundUnifiedAggregate = true;
                        break;
                    }
                }
                if (!foundUnifiedAggregate) {
                    unifiedAggregates.add(currentAggregate);
                    // aggregates are laid out after the grouping keys and hash symbol -- shift the mapping
                    aggregateFieldsMapping.put(groupingKeysCount + i, groupingKeysCount + unifiedAggregates.size() - 1);
                }
            }

            FieldMapping newMapping = new FieldMapping(
                    ImmutableMap.<Integer, Integer>builder()
                            .putAll(groupingKeysFieldMapping)
                            .putAll(aggregateFieldsMapping)
                            .buildOrThrow());

            newBranchMappings.add(newMapping);
        }

        Aggregation mergedAggregation = new Aggregation(
                nameAllocator.newName(),
                unifiedOperation.result(),
                unifiedAggregates.isEmpty() ?
                        emptyAggregateCallsBlock(unifiedOperation.result().type(), nameAllocator) :
                        composeProjectedItems(unifiedAggregates, nameAllocator).withLabel("^aggregates"),
                rebasedGroupingKeysSelector,
                GROUPING_SETS_COUNT.getAttribute(firstAggregation.attributes()),
                GLOBAL_GROUPING_SETS.getAttribute(firstAggregation.attributes()),
                Optional.ofNullable(GROUP_ID_INDEX.getAttribute(firstAggregation.attributes())).map(OptionalInt::of).orElse(OptionalInt.empty()),
                PRE_GROUPED_INDEXES.getAttribute(firstAggregation.attributes()),
                AGGREGATION_STEP.getAttribute(firstAggregation.attributes()),
                INPUT_REDUCING.getAttribute(firstAggregation.attributes()),
                ImmutableMap.of());
        newOperations.put(mergedAggregation.result(), mergedAggregation);

        io.trino.spi.type.Type mergedAggregationRowType = relationRowType(trinoType(mergedAggregation.result().type()));
        Set<Integer> unifiedFields = IntStream.range(0, mergedAggregationRowType.getTypeParameters().size()).boxed().collect(toImmutableSet());

        // rebase each branch onto the unified aggregation
        ImmutableList.Builder<TraversalState> newTraversalStates = ImmutableList.builder();
        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            Operation originalAggregation = branch.nextOperation().operation();
            TraversalContext traversalContext = branch.traversalContext();
            FieldMapping newMapping = newBranchMappings.get(i);
            Set<Integer> originalAggregationFieldsMapped = IntStream.range(0, relationRowType(trinoType(originalAggregation.result().type())).getTypeParameters().size())
                    .boxed()
                    .map(newMapping::get)
                    .collect(toImmutableSet());
            TraversalContext rebasedContext = new TraversalContext(
                    newMapping,
                    Sets.difference(unifiedFields, originalAggregationFieldsMapped),
                    rebaseBlock(traversalContext.predicateToApply(), mergedAggregationRowType, predicateMapping, nameAllocator).orElseThrow(),
                    rebasePredicateAndPruneUnsupportedConjuncts(traversalContext.enforcedPredicate(), mergedAggregation, predicateMapping, nameAllocator),
                    // after the aggregation, the cardinality is lower or equal than before, so the enforced limit still holds
                    traversalContext.enforcedLimit());
            newTraversalStates.add(new TraversalState(
                    rebasedContext,
                    getNextOperation(originalAggregation, operationToDownstream)));
        }

        return new UnifiedStatesAndCheckpointMapping(
                new UnifiedStates(mergedAggregation, newTraversalStates.build()),
                checkpoints,
                branchToCheckpoint);
    }

    private static List<Block> rebaseAndExtractAggregateCalls(Block aggregateCalls, Type newInputType, FieldMapping mapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        if (isEmptyRelationalComputation(aggregateCalls)) {
            return ImmutableList.of();
        }

        io.trino.spi.type.Type newInputRowType = relationRowType(trinoType(newInputType));
        Block.Parameter newParameter = new Block.Parameter(nameAllocator.newName(), newInputType);

        return aggregateCalls.operations().subList(0, aggregateCalls.operations().size() - 2).stream()
                .map(AggregateCall.class::cast)
                .map(aggregateCall -> new AggregateCall(
                        aggregateCall.result().name(),
                        newParameter,
                        trinoType(aggregateCall.result().type()),
                        rebaseBlock(aggregateCall.argumentsBlock(), newInputRowType, mapping, nameAllocator).orElseThrow(),
                        rebaseBlock(aggregateCall.filterSelector(), newInputRowType, mapping, nameAllocator).orElseThrow(),
                        rebaseBlock(aggregateCall.maskSelector(), newInputRowType, mapping, nameAllocator).orElseThrow(),
                        rebaseBlock(aggregateCall.orderingSelector(), newInputRowType, mapping, nameAllocator).orElseThrow(),
                        Optional.ofNullable(SORT_ORDERS.getAttribute(aggregateCall.attributes())),
                        RESOLVED_FUNCTION.getAttribute(aggregateCall.attributes()),
                        DISTINCT.getAttribute(aggregateCall.attributes()),
                        AGGREGATION_STEP.getAttribute(aggregateCall.attributes())))
                .map(rebasedAggregateCall ->
                        new Block.Builder(Optional.empty(), ImmutableList.of(newParameter))
                                .addOperation(rebasedAggregateCall)
                                .addOperation(new Return(nameAllocator.newName(), rebasedAggregateCall.result(), rebasedAggregateCall.attributes()))
                                .build())
                .collect(toImmutableList());
    }

    /**
     * A structure to represent a merging group representative.
     * It consists of the Aggregation operation and the part of the predicateToApply from its branch that is _not_ based on the grouping keys.
     * When merging Aggregation operations, we can only pull through predicates based on the grouping keys.
     * If the other conjuncts (not based on the grouping keys) are equal for all merged branches, they will be output and removed from the branches
     * by CteReuse.outputCommonSemantics().
     */
    private record AggregationAndPredicate(Aggregation aggregation, Block nonGroupingPredicateToApply)
    {
        private AggregationAndPredicate
        {
            requireNonNull(aggregation, "aggregation is null");
            requireNonNull(nonGroupingPredicateToApply, "nonGroupingPredicateToApply is null");
        }
    }
}
