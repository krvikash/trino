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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.GroupId;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalContext;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStates;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStatesAndCheckpointMapping;
import io.trino.sql.planner.optimizations.ctereuse.SingleGroupMerger.SingleGroupMergeDecomposition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.GroupIdOperationMetadata.GROUPING_SETS;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPruningAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getSelectedFields;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.rebasePredicateAndPruneUnsupportedConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.SemanticEquivalenceUtils.blocksSemanticallyEquivalent;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class GroupIdMerger
        implements SingleGroupMerger.SingleGroupProcessor
{
    @Override
    public boolean processes(Operation operation)
    {
        return operation instanceof GroupId;
    }

    /**
     * Identify groups of GroupId operations that can be merged. Two GroupId operations are mergeable
     * iff they share the same grouping sets and a semantically equivalent grouping columns selector
     * (after rebasing all onto the unified upstream's row type). Aggregation arguments selectors
     * may differ; they will be unioned by {@link #mergeNextSingleGroupOperation}.
     */
    @Override
    public SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(UnifiedStates unifiedStates, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<TraversalState> branches = unifiedStates.residualStates();
        Type unifiedRowType = relationRowType(trinoType(unifiedStates.unifiedOperation().result().type()));

        int[] subgroupAssignment = new int[branches.size()];
        Arrays.fill(subgroupAssignment, -1);
        Map<Integer, SubgroupKey> subgroupKeys = new HashMap<>();

        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            Operation nextOperation = branch.nextOperation().operation();
            if (nextOperation instanceof GroupId groupId) {
                Block rebasedGroupingColumnsSelector = rebaseBlock(
                        groupId.groupingColumnsSelector(),
                        unifiedRowType,
                        branch.traversalContext().fieldMapping(),
                        nameAllocator).orElseThrow();

                // TODO: Support merging on different grouping sets.
                List<List<Integer>> groupingSets = GROUPING_SETS.getAttribute(groupId.attributes());

                boolean foundMatch = false;
                for (Map.Entry<Integer, SubgroupKey> entry : subgroupKeys.entrySet()) {
                    SubgroupKey existing = entry.getValue();
                    if (existing.groupingSets().equals(groupingSets) &&
                            blocksSemanticallyEquivalent(existing.groupingColumnsSelector(), rebasedGroupingColumnsSelector)) {
                        subgroupAssignment[i] = entry.getKey();
                        foundMatch = true;
                        break;
                    }
                }
                if (!foundMatch) {
                    subgroupAssignment[i] = i;
                    subgroupKeys.put(i, new SubgroupKey(groupingSets, rebasedGroupingColumnsSelector));
                }
            }
        }

        Multimap<Integer, Integer> subgroups = ArrayListMultimap.create();
        for (int i = 0; i < subgroupAssignment.length; i++) {
            if (subgroupAssignment[i] != -1) {
                subgroups.put(subgroupAssignment[i], i);
            }
        }

        return new SingleGroupMergeDecomposition(subgroups.asMap().values().stream()
                .filter(indexes -> indexes.size() > 1)
                .map(ImmutableList::copyOf)
                .collect(toImmutableList()));
    }

    /**
     * Build a unified GroupId that aggregates the union of every branch's aggregation argument fields,
     * plus every input field referenced by any branch's residual predicate. Existing aggregation arguments
     * are never pruned. Each branch's residual predicate is deferred — it will be applied downstream of the
     * unified GroupId after rebasing onto the merged output via the aggregation-arguments slots only,
     * since distinct-grouping-column slots may carry NULL for grouping sets that omit the column.
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
        Type unifiedRowType = relationRowType(trinoType(unifiedOperation.result().type()));
        int unifiedRowFieldCount = unifiedRowType.getTypeParameters().size();

        TraversalState firstBranch = branches.getFirst();
        GroupId firstGroupId = (GroupId) firstBranch.nextOperation().operation();
        // rebase grouping columns selector onto the unified upstream's row type
        Block rebasedGroupingColumnsSelector = rebaseBlock(
                firstGroupId.groupingColumnsSelector(),
                unifiedRowType,
                firstBranch.traversalContext().fieldMapping(),
                nameAllocator).orElseThrow();

        // rebase each branch's selectors onto the unified upstream's row type
        List<Block> rebasedAggregationArgumentsSelectors = branches.stream()
                .map(branch -> rebaseBlock(
                        ((GroupId) branch.nextOperation().operation()).aggregationArgumentsSelector(),
                        unifiedRowType,
                        branch.traversalContext().fieldMapping(),
                        nameAllocator).orElseThrow())
                .collect(toImmutableList());

        // grouping sets are equal across branches
        List<List<Integer>> groupingSets = GROUPING_SETS.getAttribute(firstGroupId.attributes());

        // collect input fields the unified aggregation arguments selector must emit:
        // 1) every branch's existing aggregation arguments
        // 2) every branch's residual-predicate references
        Set<Integer> includedAggregationArgumentFields = new LinkedHashSet<>(); // We need to have a deterministic ordering
        for (int i = 0; i < branches.size(); i++) {
            includedAggregationArgumentFields.addAll(getSelectedFields(rebasedAggregationArgumentsSelectors.get(i)));
            Block predicate = branches.get(i).traversalContext().predicateToApply();
            includedAggregationArgumentFields.addAll(extractReferencedFields(predicate, getOnlyElement(predicate.parameters())));
        }
        List<Integer> aggregationArgumentFields = ImmutableList.copyOf(includedAggregationArgumentFields);

        Set<Integer> allUnifiedFields = IntStream.range(0, unifiedRowFieldCount).boxed().collect(toImmutableSet());
        Set<Integer> aggregationArgumentsFieldsToPrune = ImmutableSet.copyOf(
                Sets.difference(allUnifiedFields, ImmutableSet.copyOf(aggregationArgumentFields)));
        Block unifiedAggregationArgumentsSelector = getPruningAssignments("^aggregationArgumentsSelector", unifiedRowType, aggregationArgumentsFieldsToPrune, nameAllocator);

        GroupId mergedGroupId = new GroupId(
                nameAllocator.newName(),
                unifiedOperation.result(),
                rebasedGroupingColumnsSelector,
                unifiedAggregationArgumentsSelector,
                groupingSets,
                unifiedOperation.attributes());
        newOperations.put(mergedGroupId.result(), mergedGroupId);

        // GroupId output layout :
        //   [GroupingColumns .. AggregationArguments .. GroupId ]
        int distinctGroupingColumnsCount = trinoType(rebasedGroupingColumnsSelector.getReturnedType()).getTypeParameters().size();
        Type mergedRowType = relationRowType(trinoType(mergedGroupId.result().type()));
        int mergedFieldCount = mergedRowType.getTypeParameters().size();
        Set<Integer> allMergedFields = IntStream.range(0, mergedFieldCount).boxed().collect(toImmutableSet());
        int mergedAggregationArgumentsCount = aggregationArgumentFields.size();

        // predicate rebasing mapping is shared across branches: input field -> merged output position via aggregation arguments
        Map<Integer, Integer> aggregationArgumentsMappingBuilder = new HashMap<>();
        for (int i = 0; i < aggregationArgumentFields.size(); i++) {
            aggregationArgumentsMappingBuilder.put(aggregationArgumentFields.get(i), distinctGroupingColumnsCount + i);
        }
        FieldMapping aggregationArgumentsFieldMapping = new FieldMapping(aggregationArgumentsMappingBuilder);

        Map<Integer, Integer> groupingSetMapping = IntStream.range(0, distinctGroupingColumnsCount)
                .boxed()
                .collect(toImmutableMap(identity(), identity()));

        ImmutableList.Builder<TraversalState> newTraversalStates = ImmutableList.builder();
        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            GroupId originalGroupId = (GroupId) branch.nextOperation().operation();
            List<Integer> branchAggregationArgumentFields = getSelectedFields(rebasedAggregationArgumentsSelectors.get(i));
            int branchAggregationArgumentsCount = branchAggregationArgumentFields.size();

            // mapping for downstream rebasing: original GroupId output position -> merged GroupId output position
            Map<Integer, Integer> fieldMapping = new HashMap<>(groupingSetMapping);
            for (int j = 0; j < branchAggregationArgumentsCount; j++) {
                int inputField = branchAggregationArgumentFields.get(j);
                int mergedPosition = aggregationArgumentsFieldMapping.get(inputField);
                fieldMapping.put(distinctGroupingColumnsCount + j, mergedPosition);
            }
            // GroupId
            fieldMapping.put(
                    distinctGroupingColumnsCount + branchAggregationArgumentsCount,
                    distinctGroupingColumnsCount + mergedAggregationArgumentsCount);
            FieldMapping branchFieldMapping = new FieldMapping(fieldMapping);

            Block rebasedPredicateToApply = rebaseBlock(
                    branch.traversalContext().predicateToApply(),
                    mergedRowType,
                    aggregationArgumentsFieldMapping,
                    nameAllocator).orElseThrow();
            Block rebasedEnforcedPredicate = rebasePredicateAndPruneUnsupportedConjuncts(
                    branch.traversalContext().enforcedPredicate(),
                    mergedGroupId,
                    aggregationArgumentsFieldMapping,
                    nameAllocator);

            Set<Integer> branchOutputsInMerged = ImmutableSet.copyOf(fieldMapping.values());
            Set<Integer> branchFieldsToPrune = ImmutableSet.copyOf(Sets.difference(allMergedFields, branchOutputsInMerged));

            OptionalLong enforcedLimit = branch.traversalContext().enforcedLimit();
            if (enforcedLimit.isPresent()) {
                enforcedLimit = OptionalLong.of(enforcedLimit.getAsLong() * groupingSets.size());
            }

            TraversalContext newContext = new TraversalContext(
                    branchFieldMapping,
                    branchFieldsToPrune,
                    rebasedPredicateToApply,
                    rebasedEnforcedPredicate,
                    enforcedLimit);
            newTraversalStates.add(new TraversalState(newContext, getNextOperation(originalGroupId, operationToDownstream)));
        }

        return new UnifiedStatesAndCheckpointMapping(
                new UnifiedStates(mergedGroupId, newTraversalStates.build()),
                checkpoints,
                branchToCheckpoint);
    }

    private record SubgroupKey(List<List<Integer>> groupingSets, Block groupingColumnsSelector)
    {
        private SubgroupKey
        {
            requireNonNull(groupingSets, "groupingSets is null");
            requireNonNull(groupingColumnsSelector, "groupingColumnsSelector is null");
        }
    }
}
