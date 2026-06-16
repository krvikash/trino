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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.OperatorType;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.AggregationStep;
import io.trino.sql.newir.Attributes;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.GLOBAL_GROUPING_SETS;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.GROUPING_SETS_COUNT;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.GROUP_ID_INDEX;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.INPUT_REDUCING;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.PRE_GROUPED_INDEXES;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.composeProjectedItems;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.createSingleFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPassthroughMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getSelectedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyRelationalComputation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.rebasePredicateAndPruneUnsupportedConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.filterConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.isTrue;
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
     * Branches that have different residual predicates based on non-grouping keys can be merged,
     * with the predicates applied as masks to the aggregate calls.
     */
    @Override
    public SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(UnifiedStates unifiedStates, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<TraversalState> branches = unifiedStates.residualStates();
        Operation unifiedOperation = unifiedStates.unifiedOperation();

        int[] aggregationSubgroups = new int[branches.size()];
        Arrays.fill(aggregationSubgroups, -1);
        Map<Integer, SubgroupDetails> subgroupIdToDetails = new HashMap<>();

        // create an empty aggregate calls block to avoid rebasing. Aggregate calls are not compared.
        Block emptyAggregateCallsBlock = emptyAggregateCallsBlock(unifiedOperation.result().type(), nameAllocator);

        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            Operation nextOperation = branch.nextOperation().operation();
            if (nextOperation instanceof Aggregation aggregation && isDeterministic(aggregation)) {
                Block rebasedGroupingKeysSelector = rebaseBlock(aggregation.groupingKeysSelector(), relationRowType(trinoType(unifiedOperation.result().type())), branch.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
                // when we merge this aggregation with other aggregations, we will have to pull the traversal context through the merged operation.
                // We allow merging branches that have different residual predicates based on non-grouping keys by applying masks on the aggregate calls.
                // The conjuncts that are equal for all branches will be output and removed from branches as part of common semantics in CteReuse.outputCommonSemantics()
                // after the group is split recursively.
                Set<Integer> groupingKeys = ImmutableSet.copyOf(getSelectedFields(rebasedGroupingKeysSelector));
                Block nonGroupingPredicateToApply = filterConjuncts(
                        branch.traversalContext().predicateToApply(),
                        conjunct -> !groupingKeys.containsAll(extractReferencedFields(conjunct, getOnlyElement(conjunct.parameters()))),
                        nameAllocator);

                // TODO: Support existing mask selectors in the case of non-equivalent non-grouping predicates.
                Block aggregateCalls = aggregation.aggregateCalls();
                boolean hasMasks = aggregateCalls.operations()
                        .subList(0, aggregateCalls.operations().size() - 2).stream() // exclude the final Row and Return operations
                        .map(AggregateCall.class::cast)
                        .anyMatch(aggregateCall -> !isEmptyFieldSelector(aggregateCall.maskSelector()));
                // find a matching Aggregation subgroup
                boolean foundMatchingSubgroup = false;
                for (Map.Entry<Integer, SubgroupDetails> subgroupIdAndDetails : subgroupIdToDetails.entrySet()) {
                    int id = subgroupIdAndDetails.getKey();
                    SubgroupDetails details = subgroupIdAndDetails.getValue();
                    if (details.representativeAggregation().operationAttributes().equals(aggregation.operationAttributes()) &&
                            blocksSemanticallyEquivalent(details.representativeAggregation().groupingKeysSelector(), rebasedGroupingKeysSelector) &&
                            blocksSemanticallyEquivalent(details.nonGroupingPredicateToApply(), nonGroupingPredicateToApply)) {
                        aggregationSubgroups[i] = id;
                        foundMatchingSubgroup = true;
                        if (!details.hasMasks() && hasMasks) {
                            // update the subgroup's details to indicate it contains masks
                            subgroupIdToDetails.put(id, new SubgroupDetails(details.representativeAggregation(), details.nonGroupingPredicateToApply(), true));
                        }
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
                            Attributes.empty());
                    subgroupIdToDetails.put(i, new SubgroupDetails(rebasedRepresentative, nonGroupingPredicateToApply, hasMasks));
                }
            }
        }

        mergeSubgroupsWithoutMasks(subgroupIdToDetails, aggregationSubgroups);

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
     * Merge subgroups that have no masks and have the same aggregation attributes and grouping keys.
     */
    private static void mergeSubgroupsWithoutMasks(Map<Integer, SubgroupDetails> subgroupIdToDetails, int[] aggregationSubgroups)
    {
        List<Map.Entry<Integer, SubgroupDetails>> subgroupsWithoutMasks = subgroupIdToDetails.entrySet().stream()
                .filter(entry -> !entry.getValue().hasMasks())
                .toList();

        // each list contains subgroup IDs that can be merged together
        List<List<Integer>> mergeSubgroups = new ArrayList<>();

        for (Map.Entry<Integer, SubgroupDetails> subgroup : subgroupsWithoutMasks) {
            Integer subgroupId = subgroup.getKey();
            SubgroupDetails details = subgroup.getValue();

            // each subgroup has different non-grouping predicates by definition.
            // When merging non-global aggregations with different non-grouping predicates, we add tracking
            // count(*) aggregates to identify empty groups. This requires creating count(*) calls that match
            // the aggregation step. SINGLE/PARTIAL work (count raw rows), but FINAL/INTERMEDIATE require
            // intermediate state as input. Since we can't create count(*), we don't merge such subgroups.
            boolean canCreateCountAll = true;
            if (!isEmptyFieldSelector(details.representativeAggregation().groupingKeysSelector())) {
                AggregationStep aggregationStep = AggregationStep.valueOf(AGGREGATION_STEP.getAttribute(details.representativeAggregation().attributes()).name());
                canCreateCountAll = aggregationStep == AggregationStep.SINGLE || aggregationStep == AggregationStep.PARTIAL;
            }
            if (!canCreateCountAll) {
                mergeSubgroups.add(ImmutableList.of(subgroupId));
                continue;
            }

            // try to merge into an existing merge set
            boolean merged = false;
            for (List<Integer> mergeSet : mergeSubgroups) {
                SubgroupDetails representativeDetails = subgroupIdToDetails.get(mergeSet.getFirst());
                if (details.representativeAggregation().operationAttributes().equals(representativeDetails.representativeAggregation().operationAttributes()) &&
                        blocksSemanticallyEquivalent(details.representativeAggregation().groupingKeysSelector(), representativeDetails.representativeAggregation().groupingKeysSelector())) {
                    mergeSet.add(subgroupId);
                    merged = true;
                    break;
                }
            }

            if (!merged) {
                // create a new merge set
                List<Integer> newMergeSet = new ArrayList<>();
                newMergeSet.add(subgroupId);
                mergeSubgroups.add(newMergeSet);
            }
        }

        // build a merge mapping - all subgroups in a merge set map to the lowest ID in that set
        Map<Integer, Integer> subgroupMergeMapping = new HashMap<>();
        for (List<Integer> mergeSet : mergeSubgroups) {
            if (mergeSet.size() > 1) {
                Integer mergeTarget = Collections.min(mergeSet);
                mergeSet.stream()
                        .filter(subgroupId -> !subgroupId.equals(mergeTarget))
                        .forEach(subgroupId -> subgroupMergeMapping.put(subgroupId, mergeTarget));
            }
        }

        // nothing to merge
        if (subgroupMergeMapping.isEmpty()) {
            return;
        }

        // apply the merge mapping to aggregationSubgroups
        for (int i = 0; i < aggregationSubgroups.length; i++) {
            if (aggregationSubgroups[i] != -1) {
                Integer mappedSubgroup = subgroupMergeMapping.get(aggregationSubgroups[i]);
                if (mappedSubgroup != null) {
                    aggregationSubgroups[i] = mappedSubgroup;
                }
            }
        }
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
     * When branches have non-trivial predicates on non-grouping keys, a Project operation is inserted before the aggregation
     * to compute boolean mask columns by evaluating each distinct predicate expression.
     * Each aggregate call then uses the appropriate mask column to filter which rows contribute to it.
     * <p>
     * For non-global aggregations (with grouping keys), a count(*) aggregate is added for each distinct non-grouping predicate
     * to track how many rows matched that predicate. Each branch with a mask then gets a count(*) > 0 predicate to filter out
     * groups where no rows matched. This is necessary for two reasons:
     * 1. Aggregations with FILTER clauses behave like global aggregations for empty groups: they produce default output
     *    values (e.g., null for sum, 0 for count) even when no rows match. In contrast, when predicates filter rows before
     *    aggregation (using WHERE), filtered-out groups don't appear in the result at all. The count(*) > 0 check identifies
     *    which groups are truly empty and filters them out to match the WHERE behavior.
     * 2. When an aggregation has no aggregate functions, it essentially computes distinct grouping keys.
     *    Since there are no aggregates to apply the mask to, the count(*) > 0 check is necessary to actually enforce
     *    the predicate by filtering out groups where no rows matched the predicate.
     * <p>
     * For global aggregations (no grouping keys), the count(*) tracking and filtering is not needed because global
     * aggregations always produce exactly one output row regardless of whether any input rows match, and this behavior
     * is the same for both FILTER clauses and WHERE predicates.
     */
    @Override
    public UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
            Operation unifiedOperation,
            List<TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations,
            Metadata metadata)
    {
        TraversalState firstBranch = branches.getFirst();
        Aggregation firstAggregation = (Aggregation) firstBranch.nextOperation().operation();

        io.trino.spi.type.Type unifiedOperationRowType = relationRowType(trinoType(unifiedOperation.result().type()));
        int inputFieldCount = unifiedOperationRowType.getTypeParameters().size();

        Block rebasedGroupingKeysSelector = rebaseBlock(firstAggregation.groupingKeysSelector(), unifiedOperationRowType, firstBranch.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
        int groupingKeysCount = trinoType(rebasedGroupingKeysSelector.getReturnedType()).getTypeParameters().size();
        Set<Integer> groupingKeys = ImmutableSet.copyOf(getSelectedFields(rebasedGroupingKeysSelector));
        Map<Integer, Integer> groupingKeysFieldMapping = IntStream.range(0, groupingKeysCount)
                .boxed()
                .collect(toImmutableBiMap(identity(), identity()));

        List<Block> nonGroupingPredicates = branches.stream()
                .map(branch -> filterConjuncts(
                        branch.traversalContext().predicateToApply(),
                        conjunct -> !groupingKeys.containsAll(extractReferencedFields(conjunct, getOnlyElement(conjunct.parameters()))),
                        nameAllocator))
                .collect(toImmutableList());

        boolean allNonGroupingPredicatesTrue = nonGroupingPredicates.stream().allMatch(PredicateUtils::isTrue);

        Value aggregationInput; // either the unified operation directly or a Project with mask columns
        Map<Integer, Integer> branchToMaskIndex = new HashMap<>();
        Optional<FieldMapping> aggregationInputMapping;
        Block finalGroupingKeysSelector;
        int distinctNonGroupingPredicatesCount; // number of distinct predicates based on non-grouping keys (equals the number of required count(*) aggregates)

        // the unified aggregation must include all aggregate calls from the component aggregation operations
        // aggregates from original branches will be added first, then tracking count(*) aggregates for masks without count(*)
        List<Block> unifiedAggregates = new ArrayList<>();

        if (allNonGroupingPredicatesTrue) {
            // no mask columns needed
            aggregationInput = unifiedOperation.result();
            aggregationInputMapping = Optional.empty();
            finalGroupingKeysSelector = rebasedGroupingKeysSelector;
            distinctNonGroupingPredicatesCount = 0;
        }
        else {
            // need to create a Project with mask columns for distinct predicates based on non-grouping keys
            List<Block> projectedItems = new ArrayList<>();

            // create blocks for each passthrough field
            for (int i = 0; i < inputFieldCount; i++) {
                projectedItems.add(createSingleFieldSelector(unifiedOperationRowType, i, Optional.empty(), false, nameAllocator));
            }

            // collect distinct predicates based on non-grouping keys and map branches to their mask index
            List<Block> distinctNonGroupingPredicates = new ArrayList<>();
            for (int i = 0; i < branches.size(); i++) {
                Block predicate = nonGroupingPredicates.get(i);
                // TRUE predicates don't need a mask
                if (!isTrue(predicate)) {
                    branchToMaskIndex.put(i, inputFieldCount + findOrAddDistinctPredicate(predicate, distinctNonGroupingPredicates));
                }
            }
            projectedItems.addAll(distinctNonGroupingPredicates);
            distinctNonGroupingPredicatesCount = distinctNonGroupingPredicates.size();

            Project maskProject = new Project(
                    nameAllocator.newName(),
                    unifiedOperation.result(),
                    composeProjectedItems(projectedItems, nameAllocator).withLabel("^assignments"),
                    unifiedOperation.attributes());
            newOperations.put(maskProject.result(), maskProject);
            aggregationInput = maskProject.result();

            // rebase grouping keys selector to the mask project's output type
            io.trino.spi.type.Type aggregationInputRowType = relationRowType(trinoType(aggregationInput.type()));
            aggregationInputMapping = Optional.of(FieldMapping.identity(aggregationInputRowType));
            finalGroupingKeysSelector = rebaseBlock(
                    rebasedGroupingKeysSelector,
                    aggregationInputRowType,
                    aggregationInputMapping.get(),
                    nameAllocator).orElseThrow();
        }

        // the mapping to rebase the context for each branch: predicate to apply and enforced predicate from the recent unified operation onto the new unified Aggregation operation
        // the enforced predicate is not guaranteed to be fully supported. The unsupported part will be pruned
        FieldMapping groupingPredicatesMapping = getPassthroughMapping(finalGroupingKeysSelector);

        // for each branch, the mapping to rebase the next downstream operation from the component Aggregation operation onto the new unified Aggregation operation
        List<FieldMapping> newBranchMappings = new ArrayList<>();

        // track which masks have a corresponding count(*) aggregate
        // maps mask field index (in the mask project) to the output field position of the count(*) aggregate in the unified aggregation result
        Map<Integer, Integer> maskToCountAllIndex = new LinkedHashMap<>();

        for (int branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
            TraversalState branch = branches.get(branchIndex);
            FieldMapping branchMapping = branch.traversalContext().fieldMapping();
            FieldMapping composedMapping = aggregationInputMapping.map(branchMapping::composeWith).orElse(branchMapping);
            OptionalInt branchMaskIndex = branchToMaskIndex.containsKey(branchIndex)
                    ? OptionalInt.of(branchToMaskIndex.get(branchIndex))
                    : OptionalInt.empty();

            List<Block> currentAggregates = rebaseAndExtractAggregateCalls(
                    ((Aggregation) branch.nextOperation().operation()).aggregateCalls(),
                    aggregationInput.type(),
                    composedMapping,
                    branchMaskIndex,
                    nameAllocator);

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
                        // aggregates are laid out after the grouping keys
                        aggregateFieldsMapping.put(groupingKeysCount + i, groupingKeysCount + j);
                        freeUnifiedIndexes.remove(j);
                        foundUnifiedAggregate = true;
                        break;
                    }
                }
                if (!foundUnifiedAggregate) {
                    unifiedAggregates.add(currentAggregate);
                    // aggregates are laid out after the grouping keys
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

        // create tracking count(*) aggregates
        if (groupingKeysCount > 0 && !allNonGroupingPredicatesTrue) {
            AggregationStep aggregationStep = AggregationStep.valueOf(AGGREGATION_STEP.getAttribute(firstAggregation.attributes()).name());
            for (int maskFieldIndex = inputFieldCount; maskFieldIndex < inputFieldCount + distinctNonGroupingPredicatesCount; maskFieldIndex++) {
                Block countAggregate = createCountAllAggregateWithMask(
                        aggregationInput.type(),
                        maskFieldIndex,
                        aggregationStep,
                        nameAllocator,
                        metadata);
                unifiedAggregates.add(countAggregate);
                maskToCountAllIndex.put(maskFieldIndex, groupingKeysCount + unifiedAggregates.size() - 1);
            }
        }

        Aggregation mergedAggregation = new Aggregation(
                nameAllocator.newName(),
                aggregationInput,
                unifiedAggregates.isEmpty() ?
                        emptyAggregateCallsBlock(aggregationInput.type(), nameAllocator) :
                        composeProjectedItems(unifiedAggregates, nameAllocator).withLabel("^aggregates"),
                finalGroupingKeysSelector,
                GROUPING_SETS_COUNT.getAttribute(firstAggregation.attributes()),
                GLOBAL_GROUPING_SETS.getAttribute(firstAggregation.attributes()),
                Optional.ofNullable(GROUP_ID_INDEX.getAttribute(firstAggregation.attributes())).map(OptionalInt::of).orElse(OptionalInt.empty()),
                PRE_GROUPED_INDEXES.getAttribute(firstAggregation.attributes()),
                AGGREGATION_STEP.getAttribute(firstAggregation.attributes()),
                INPUT_REDUCING.getAttribute(firstAggregation.attributes()),
                Attributes.empty());
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

            // filter out predicates based on non-grouping keys from predicateToApply since they are now handled as masks
            Block groupingOnlyPredicate = filterConjuncts(
                    traversalContext.predicateToApply(),
                    conjunct -> groupingKeys.containsAll(extractReferencedFields(conjunct, getOnlyElement(conjunct.parameters()))),
                    nameAllocator);

            // rebase the grouping-only predicate (which references pre-aggregation fields)
            Block rebasedGroupingPredicate = rebaseBlock(groupingOnlyPredicate, mergedAggregationRowType, groupingPredicatesMapping, nameAllocator).orElseThrow();

            // when masks are used with non-global aggregations, create a per-branch filter that filters out rows where this branch's count(*) = 0
            Block finalPredicate = rebasedGroupingPredicate;
            if (groupingKeysCount > 0 && !allNonGroupingPredicatesTrue) {
                // if this branch has a mask, add count(*) > 0 predicate
                Integer maskFieldIndex = branchToMaskIndex.get(i);
                if (maskFieldIndex != null) {
                    int countAllIndex = maskToCountAllIndex.get(maskFieldIndex);
                    Block aggregatePredicate = createCountGreaterThanZeroPredicate(
                            mergedAggregationRowType,
                            countAllIndex,
                            nameAllocator,
                            metadata);

                    finalPredicate = isTrue(rebasedGroupingPredicate)
                            ? aggregatePredicate
                            : PredicateUtils.conjunction(ImmutableList.of(rebasedGroupingPredicate, aggregatePredicate), nameAllocator);
                }
            }

            TraversalContext rebasedContext = new TraversalContext(
                    newMapping,
                    Sets.difference(unifiedFields, originalAggregationFieldsMapped),
                    finalPredicate,
                    rebasePredicateAndPruneUnsupportedConjuncts(traversalContext.enforcedPredicate(), mergedAggregation, groupingPredicatesMapping, nameAllocator),
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

    /**
     * Find a semantically equivalent predicate in the list, or add the predicate if not found.
     * Returns the index of the predicate in the list.
     */
    private static int findOrAddDistinctPredicate(Block predicate, List<Block> distinctPredicates)
    {
        for (int i = 0; i < distinctPredicates.size(); i++) {
            if (blocksSemanticallyEquivalent(predicate, distinctPredicates.get(i))) {
                return i;
            }
        }
        distinctPredicates.add(predicate);
        return distinctPredicates.size() - 1;
    }

    /**
     * Creates a count(*) aggregate call with a mask selector.
     *
     * @param inputType the type of the aggregation input (mask project output)
     * @param maskFieldIndex the index of the mask field in the mask project's output
     * @param aggregationStep the aggregation step to use for the count(*) aggregate call
     */
    private static Block createCountAllAggregateWithMask(
            Type inputType,
            int maskFieldIndex,
            AggregationStep aggregationStep,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Metadata metadata)
    {
        io.trino.spi.type.Type inputRowType = relationRowType(trinoType(inputType));
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), inputType);
        Block argumentsBlock = AssignmentsUtils.getEmptyFieldSelector("^arguments", inputRowType, nameAllocator);
        Block filterSelector = AssignmentsUtils.getEmptyFieldSelector("^filterSelector", inputRowType, nameAllocator);
        Block maskSelector = createSingleFieldSelector(inputRowType, maskFieldIndex, Optional.of("^maskSelector"), true, nameAllocator);
        Block orderingSelector = AssignmentsUtils.getEmptyFieldSelector("^orderingSelector", inputRowType, nameAllocator);
        ResolvedFunction countFunction = metadata.resolveBuiltinFunction("count", ImmutableList.of());

        AggregateCall countAggregateCall = new AggregateCall(
                nameAllocator.newName(),
                parameter,
                BIGINT,
                argumentsBlock,
                filterSelector,
                maskSelector,
                orderingSelector,
                Optional.empty(),
                countFunction,
                false,
                aggregationStep);

        Block.Builder builder = new Block.Builder(Optional.empty(), ImmutableList.of(parameter));
        builder.addOperation(countAggregateCall);
        builder.addOperation(new Return(nameAllocator.newName(), countAggregateCall.result(), countAggregateCall.attributes()));
        return builder.build();
    }

    /**
     * Rebase and extract aggregate calls, optionally setting a mask selector for the given mask field index.
     */
    private static List<Block> rebaseAndExtractAggregateCalls(
            Block aggregateCalls,
            Type newInputType,
            FieldMapping mapping,
            OptionalInt maskFieldIndex,
            ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        if (isEmptyRelationalComputation(aggregateCalls)) {
            return ImmutableList.of();
        }

        io.trino.spi.type.Type newInputRowType = relationRowType(trinoType(newInputType));
        Block.Parameter newParameter = new Block.Parameter(nameAllocator.newName(), newInputType);

        return aggregateCalls.operations()
                .subList(0, aggregateCalls.operations().size() - 2).stream() // exclude the final Row and Return operations
                .map(AggregateCall.class::cast)
                .map(aggregateCall -> {
                    Block maskSelector = maskFieldIndex.isEmpty()
                            ? rebaseBlock(aggregateCall.maskSelector(), newInputRowType, mapping, nameAllocator).orElseThrow()
                            : createSingleFieldSelector(newInputRowType, maskFieldIndex.getAsInt(), Optional.of("^maskSelector"), true, nameAllocator);

                    return new AggregateCall(
                            aggregateCall.result().name(),
                            newParameter,
                            trinoType(aggregateCall.result().type()),
                            rebaseBlock(aggregateCall.argumentsBlock(), newInputRowType, mapping, nameAllocator).orElseThrow(),
                            rebaseBlock(aggregateCall.filterSelector(), newInputRowType, mapping, nameAllocator).orElseThrow(),
                            maskSelector,
                            rebaseBlock(aggregateCall.orderingSelector(), newInputRowType, mapping, nameAllocator).orElseThrow(),
                            Optional.ofNullable(AggregateCallOperationMetadata.SORT_ORDERS.getAttribute(aggregateCall.attributes())),
                            AggregateCallOperationMetadata.RESOLVED_FUNCTION.getAttribute(aggregateCall.attributes()),
                            AggregateCallOperationMetadata.DISTINCT.getAttribute(aggregateCall.attributes()),
                            AggregateCallOperationMetadata.AGGREGATION_STEP.getAttribute(aggregateCall.attributes()));
                })
                .map(rebasedAggregateCall ->
                        new Block.Builder(Optional.empty(), ImmutableList.of(newParameter))
                                .addOperation(rebasedAggregateCall)
                                .addOperation(new Return(nameAllocator.newName(), rebasedAggregateCall.result(), rebasedAggregateCall.attributes()))
                                .build())
                .collect(toImmutableList());
    }

    /**
     * Creates a filter predicate that checks if count(*) > 0.
     *
     * @param inputRowType the row type of the filter predicate input (aggregation output)
     * @param countAllIndex field position of the count(*) aggregate to check
     * @param nameAllocator ValueNameAllocator needed for generating unique operation names
     * @return an IR block that evaluates to TRUE if count > 0
     */
    private static Block createCountGreaterThanZeroPredicate(
            io.trino.spi.type.Type inputRowType,
            int countAllIndex,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Metadata metadata)
    {
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(inputRowType));
        Block.Builder builder = new Block.Builder(Optional.of("^predicate"), ImmutableList.of(parameter));

        FieldReference countReference = new FieldReference(
                nameAllocator.newName(),
                parameter,
                countAllIndex,
                DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        builder.addOperation(countReference);

        Constant zeroConstant = new Constant(nameAllocator.newName(), BIGINT, 0L);
        builder.addOperation(zeroConstant);

        // count > 0 in the canonical form of the old IR: $less_than(0, count)
        Call comparison = new Call(
                nameAllocator.newName(),
                ImmutableList.of(zeroConstant.result(), countReference.result()),
                metadata.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(BIGINT, BIGINT)),
                ImmutableList.of(zeroConstant.attributes(), countReference.attributes()));
        builder.addOperation(comparison);

        builder.addOperation(new Return(nameAllocator.newName(), comparison.result(), comparison.attributes()));

        return builder.build();
    }

    private record SubgroupDetails(Aggregation representativeAggregation, Block nonGroupingPredicateToApply, boolean hasMasks)
    {
        private SubgroupDetails
        {
            requireNonNull(representativeAggregation, "representativeAggregation is null");
            requireNonNull(nonGroupingPredicateToApply, "nonGroupingPredicateToApply is null");
        }
    }
}
