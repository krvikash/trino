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
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DistributionType;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JoinType;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.CheckpointReferences;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JOIN_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JoinType.FULL;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JoinType.LEFT;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JoinType.RIGHT;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.SPILLABLE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.concatenateFieldSelectors;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPassthroughMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPruningAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.CheckpointReferences.concatenateCheckpointReferences;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.rebasePredicateAndPruneUnsupportedConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static io.trino.sql.planner.optimizations.ctereuse.MultiGroupMerger.identifyMultiGroupBranches;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.optimizeLogicalOperations;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.SemanticEquivalenceUtils.blocksSemanticallyEquivalent;
import static java.util.function.Function.identity;

public class JoinMerger
        implements MultiGroupMerger.MultiGroupProcessor
{
    @Override
    public boolean processes(Operation operation)
    {
        return operation instanceof Join;
    }

    @Override
    public List<MultiGroupMerger.MultiGroupMergeCandidate> identifyMultiGroupMergeCandidates(CteReuse.UnifiedStates newGroup, Map<Integer, MultiGroupMerger.HangingGroup> hangingGroups, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // identify multi-group deterministic joins in the new group
        Map<Integer, CteReuse.TraversalState> multiGroupJoins = identifyMultiGroupBranches(newGroup).stream()
                .collect(toImmutableMap(identity(), newGroup.residualStates()::get))
                .entrySet().stream()
                .filter(entry -> entry.getValue().nextOperation().operation() instanceof Join)
                .filter(entry -> isDeterministic(entry.getValue().nextOperation().operation()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        // find subgroups of compatible joins
        int[] joinSubgroups = new int[newGroup.residualStates().size()];
        Arrays.fill(joinSubgroups, -1);
        Map<Integer, CteReuse.OperationAndIndex> subgroupRepresentatives = new HashMap<>();
        Map<Integer, Join> halfRebasedJoins = new HashMap<>();

        for (Map.Entry<Integer, CteReuse.TraversalState> entry : multiGroupJoins.entrySet()) {
            int branchIndex = entry.getKey();
            CteReuse.TraversalState branch = entry.getValue();
            Join join = (Join) branch.nextOperation().operation();
            int sourceIndex = branch.nextOperation().sourceIndex();
            List<Block> halfRebasedBlocks = rebaseJoinBlocks(join, sourceIndex, relationRowType(trinoType(newGroup.unifiedOperation().result().type())), branch.traversalContext().fieldMapping(), nameAllocator);
            Join halfRebasedJoin = new Join(
                    nameAllocator.newName(),
                    sourceIndex == 0 ? newGroup.unifiedOperation().result() : join.arguments().get(0),
                    sourceIndex == 1 ? newGroup.unifiedOperation().result() : join.arguments().get(1),
                    halfRebasedBlocks.get(0),
                    halfRebasedBlocks.get(1),
                    halfRebasedBlocks.get(2),
                    halfRebasedBlocks.get(3),
                    halfRebasedBlocks.get(4),
                    halfRebasedBlocks.get(5),
                    JOIN_TYPE.getAttribute(join.attributes()),
                    MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(join.attributes()),
                    Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(join.attributes())),
                    Optional.ofNullable(SPILLABLE.getAttribute(join.attributes())),
                    DYNAMIC_FILTER_IDS.getAttribute(join.attributes()),
                    Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(join.attributes())),
                    ImmutableMap.of(),
                    ImmutableMap.of());
            halfRebasedJoins.put(branchIndex, halfRebasedJoin);

            boolean foundMatchingSubgroup = false;
            for (Map.Entry<Integer, CteReuse.OperationAndIndex> subgroupRepresentative : subgroupRepresentatives.entrySet()) {
                // compatible branches must be both left join source or both right join source
                if (subgroupRepresentative.getValue().sourceIndex() != sourceIndex) {
                    continue;
                }
                Join subgroupRepresentativeJoin = (Join) subgroupRepresentative.getValue().operation();
                // compare attributes except DYNAMIC_FILTER_IDS, MAY_SKIP_OUTPUT_DUPLICATES, STATISTICS_AND_COST_SUMMARY
                if (!JOIN_TYPE.getAttribute(subgroupRepresentativeJoin.attributes()).equals(JOIN_TYPE.getAttribute(halfRebasedJoin.attributes())) ||
                        !Objects.equals(DISTRIBUTION_TYPE.getAttribute(subgroupRepresentativeJoin.attributes()), DISTRIBUTION_TYPE.getAttribute(halfRebasedJoin.attributes())) ||
                        !Objects.equals(SPILLABLE.getAttribute(subgroupRepresentativeJoin.attributes()), SPILLABLE.getAttribute(halfRebasedJoin.attributes()))) {
                    continue;
                }
                // compare criteria selectors
                if (sourceIndex == 0 && !blocksSemanticallyEquivalent(subgroupRepresentativeJoin.leftCriteriaSelector(), halfRebasedJoin.leftCriteriaSelector())) {
                    continue;
                }
                if (sourceIndex == 1 && !blocksSemanticallyEquivalent(subgroupRepresentativeJoin.rightCriteriaSelector(), halfRebasedJoin.rightCriteriaSelector())) {
                    continue;
                }
                // compare TraversalContext.predicateToApply and consider the join type. The inner side predicate cannot be pulled through join, so it must be equal for all merged joins.
                // Note: after the newGroup is split recursively, the equal predicates will be output as part of common semantics in CteReuse.outputCommonSemantics(), and the residual predicates will be true.
                JoinType joinType = JOIN_TYPE.getAttribute(halfRebasedJoin.attributes());
                if (sourceIndex == 0 &&
                        (joinType.equals(RIGHT) || joinType.equals(FULL)) &&
                        !blocksSemanticallyEquivalent(newGroup.residualStates().get(subgroupRepresentative.getKey()).traversalContext().predicateToApply(), branch.traversalContext().predicateToApply())) {
                    continue;
                }
                if (sourceIndex == 1 &&
                        (joinType.equals(LEFT) || joinType.equals(FULL)) &&
                        !blocksSemanticallyEquivalent(newGroup.residualStates().get(subgroupRepresentative.getKey()).traversalContext().predicateToApply(), branch.traversalContext().predicateToApply())) {
                    continue;
                }

                joinSubgroups[branchIndex] = subgroupRepresentative.getKey();
                foundMatchingSubgroup = true;
                break;
            }
            // if there is no matching subgroup, start a new subgroup
            if (!foundMatchingSubgroup) {
                joinSubgroups[branchIndex] = branchIndex;
                subgroupRepresentatives.put(branchIndex, new CteReuse.OperationAndIndex(halfRebasedJoin, sourceIndex));
            }
        }

        // extract subgroups
        Multimap<Integer, Integer> allSubgroups = ArrayListMultimap.create();
        for (int i = 0; i < joinSubgroups.length; i++) {
            if (joinSubgroups[i] != -1) {
                allSubgroups.put(joinSubgroups[i], i);
            }
        }

        // remove singleton subgroups
        List<List<Integer>> subgroups = allSubgroups.asMap().values().stream()
                .filter(indexes -> indexes.size() > 1)
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());

        // for each subgroup, find matching branches in hanging groups
        ImmutableList.Builder<MultiGroupMerger.MultiGroupMergeCandidate> mergeCandidates = ImmutableList.builder();
        for (List<Integer> subgroup : subgroups) {
            Map<CteReuse.TraversalState, Integer> subgroupBranches = subgroup.stream()
                    .collect(toImmutableMap(newGroup.residualStates()::get, identity()));
            Set<Value> subgroupResults = subgroupBranches.keySet().stream()
                    .map(CteReuse.TraversalState::nextOperation)
                    .map(CteReuse.OperationAndIndex::operation)
                    .map(Operation::result)
                    .collect(toImmutableSet());

            for (Map.Entry<Integer, MultiGroupMerger.HangingGroup> hangingGroupEntry : hangingGroups.entrySet()) {
                int hangingGroupId = hangingGroupEntry.getKey();
                MultiGroupMerger.HangingGroup hangingGroup = hangingGroupEntry.getValue();

                // find all matching branches in the hanging group
                Map<Integer, CteReuse.TraversalState> matchingHangingBranches = IntStream.range(0, hangingGroup.branches().size())
                        .boxed()
                        .collect(toImmutableMap(identity(), hangingGroup.branches()::get))
                        .entrySet().stream()
                        .filter(hangingBranchEntry -> subgroupResults.contains(hangingBranchEntry.getValue().nextOperation().operation().result()))
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                if (matchingHangingBranches.size() < 2) {
                    continue;
                }

                // identify subgroups of the hanging group
                int[] hangingSubgroups = new int[hangingGroup.branches().size()];
                Arrays.fill(hangingSubgroups, -1);
                Map<Integer, Integer> hangingGroupBranchToNewGroupBranch = new HashMap<>();
                Map<Integer, Join> hangingSubgroupRepresentatives = new HashMap<>();

                for (Map.Entry<Integer, CteReuse.TraversalState> hangingBranchEntry : matchingHangingBranches.entrySet()) {
                    int hangingBranchIndex = hangingBranchEntry.getKey();
                    CteReuse.TraversalState hangingBranch = hangingBranchEntry.getValue();
                    Value joinResult = hangingBranch.nextOperation().operation().result();
                    int newGroupIndex = getOnlyElement(subgroupBranches.entrySet().stream()
                            .filter(branchEntry -> branchEntry.getKey().nextOperation().operation().result().equals(joinResult))
                            .map(Map.Entry::getValue)
                            .collect(toImmutableList()));
                    hangingGroupBranchToNewGroupBranch.put(hangingBranchIndex, newGroupIndex);
                    Join halfRebasedJoin = halfRebasedJoins.get(newGroupIndex);
                    // rebase the join onto the other source
                    List<Block> rebasedBlocks = rebaseJoinBlocks(halfRebasedJoin, hangingBranch.nextOperation().sourceIndex(), relationRowType(trinoType(hangingGroup.unifiedOperation().result().type())), hangingBranch.traversalContext().fieldMapping(), nameAllocator);
                    Join rebasedJoin = new Join(
                            nameAllocator.newName(),
                            hangingBranch.nextOperation().sourceIndex() == 0 ? hangingGroup.unifiedOperation().result() : halfRebasedJoin.arguments().get(0),
                            hangingBranch.nextOperation().sourceIndex() == 1 ? hangingGroup.unifiedOperation().result() : halfRebasedJoin.arguments().get(1),
                            rebasedBlocks.get(0),
                            rebasedBlocks.get(1),
                            rebasedBlocks.get(2),
                            rebasedBlocks.get(3),
                            rebasedBlocks.get(4),
                            rebasedBlocks.get(5),
                            JOIN_TYPE.getAttribute(halfRebasedJoin.attributes()),
                            MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(halfRebasedJoin.attributes()),
                            Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(halfRebasedJoin.attributes())),
                            Optional.ofNullable(SPILLABLE.getAttribute(halfRebasedJoin.attributes())),
                            DYNAMIC_FILTER_IDS.getAttribute(halfRebasedJoin.attributes()),
                            Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(halfRebasedJoin.attributes())),
                            ImmutableMap.of(),
                            ImmutableMap.of());

                    boolean foundMatchingSubgroup = false;
                    for (Map.Entry<Integer, Join> subgroupRepresentative : hangingSubgroupRepresentatives.entrySet()) {
                        Join subgroupRepresentativeJoin = subgroupRepresentative.getValue();
                        // compare blocks: criteria selector from the hanging group side, and filter
                        if (hangingBranch.nextOperation().sourceIndex() == 0 && !blocksSemanticallyEquivalent(subgroupRepresentativeJoin.leftCriteriaSelector(), rebasedJoin.leftCriteriaSelector())) {
                            continue;
                        }
                        if (hangingBranch.nextOperation().sourceIndex() == 1 && !blocksSemanticallyEquivalent(subgroupRepresentativeJoin.rightCriteriaSelector(), rebasedJoin.rightCriteriaSelector())) {
                            continue;
                        }
                        if (!blocksSemanticallyEquivalent(subgroupRepresentativeJoin.filter(), rebasedJoin.filter())) {
                            continue;
                        }
                        // compare TraversalContext.predicateToApply and consider the join type. The inner side predicate cannot be pulled through join, so it must be equal for all merged joins.
                        // Note: after the hangingGroup is split recursively, the equal predicates will be output as part of common semantics in CteReuse.outputCommonSemantics(), and the residual predicates will be true.
                        JoinType joinType = JOIN_TYPE.getAttribute(rebasedJoin.attributes());
                        if (hangingBranch.nextOperation().sourceIndex() == 0 &&
                                (joinType.equals(RIGHT) || joinType.equals(FULL)) &&
                                !blocksSemanticallyEquivalent(hangingGroup.branches().get(subgroupRepresentative.getKey()).traversalContext().predicateToApply(), hangingBranch.traversalContext().predicateToApply())) {
                            continue;
                        }
                        if (hangingBranch.nextOperation().sourceIndex() == 1 &&
                                (joinType.equals(LEFT) || joinType.equals(FULL)) &&
                                !blocksSemanticallyEquivalent(hangingGroup.branches().get(subgroupRepresentative.getKey()).traversalContext().predicateToApply(), hangingBranch.traversalContext().predicateToApply())) {
                            continue;
                        }

                        hangingSubgroups[hangingBranchIndex] = subgroupRepresentative.getKey();
                        foundMatchingSubgroup = true;
                        break;
                    }
                    // if there is no matching subgroup, start a new subgroup
                    if (!foundMatchingSubgroup) {
                        hangingSubgroups[hangingBranchIndex] = hangingBranchIndex;
                        hangingSubgroupRepresentatives.put(hangingBranchIndex, rebasedJoin);
                    }
                }

                // extract subgroups of the hanging group
                Multimap<Integer, Integer> allHangingSubgroups = ArrayListMultimap.create();
                for (int i = 0; i < hangingSubgroups.length; i++) {
                    if (hangingSubgroups[i] != -1) {
                        allHangingSubgroups.put(hangingSubgroups[i], i);
                    }
                }

                // remove singleton hanging subgroups
                List<List<Integer>> nonSingletonHangingSubgroups = allHangingSubgroups.asMap().values().stream()
                        .filter(indexes -> indexes.size() > 1)
                        .map(ImmutableList::copyOf)
                        .collect(toImmutableList());

                // return merge candidates
                for (List<Integer> hangingGroupIndexes : nonSingletonHangingSubgroups) {
                    mergeCandidates.add(new MultiGroupMerger.MultiGroupMergeCandidate(
                            hangingGroupIndexes.stream()
                                    .map(hangingGroupBranchToNewGroupBranch::get)
                                    .collect(toImmutableList()),
                            ImmutableMap.of(hangingGroupId, hangingGroupIndexes)));
                }
            }
        }

        return mergeCandidates.build();
    }

    @Override
    public CteReuse.UnifiedStatesAndCheckpointMapping mergeNextMultiGroupOperation(
            Operation unifiedOperation,
            List<CteReuse.TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            List<Integer> hangingGroupsToMerge,
            Map<Integer, MultiGroupMerger.HangingGroup> hangingGroups,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations)
    {
        CteReuse.UnifiedStates newGroup = new CteReuse.UnifiedStates(unifiedOperation, branches);
        int hangingGroupIndex = getOnlyElement(hangingGroupsToMerge);
        CteReuse.UnifiedStates hangingGroup = new CteReuse.UnifiedStates(hangingGroups.get(hangingGroupIndex).unifiedOperation(), hangingGroups.get(hangingGroupIndex).branches());

        CteReuse.UnifiedStates leftSource;
        List<Checkpoint> leftCheckpoints;
        BranchesToCheckpointsMapping leftBranchToCheckpoint;
        CteReuse.UnifiedStates rightSourceUnordered;
        List<Checkpoint> rightCheckpoints;
        BranchesToCheckpointsMapping rightBranchToCheckpointUnordered;
        if (newGroup.residualStates().getFirst().nextOperation().sourceIndex() == 0) {
            leftSource = newGroup;
            leftCheckpoints = checkpoints;
            leftBranchToCheckpoint = branchToCheckpoint;
            rightSourceUnordered = hangingGroup;
            rightCheckpoints = hangingGroups.get(hangingGroupIndex).checkpoints();
            rightBranchToCheckpointUnordered = hangingGroups.get(hangingGroupIndex).branchToCheckpoint();
        }
        else {
            leftSource = hangingGroup;
            leftCheckpoints = hangingGroups.get(hangingGroupIndex).checkpoints();
            leftBranchToCheckpoint = hangingGroups.get(hangingGroupIndex).branchToCheckpoint();
            rightSourceUnordered = newGroup;
            rightCheckpoints = checkpoints;
            rightBranchToCheckpointUnordered = branchToCheckpoint;
        }

        // reorder the right source branches to follow the order of left source branches
        // after reordering, the left and right branches pairwise are children the same Join operation
        CteReuse.UnifiedStates rightSource;
        BranchesToCheckpointsMapping rightBranchToCheckpoint;
        List<Value> leftSourceResults = leftSource.residualStates().stream()
                .map(CteReuse.TraversalState::nextOperation)
                .map(CteReuse.OperationAndIndex::operation)
                .map(Operation::result)
                .collect(toImmutableList());
        List<Value> rightSourceResults = rightSourceUnordered.residualStates().stream()
                .map(CteReuse.TraversalState::nextOperation)
                .map(CteReuse.OperationAndIndex::operation)
                .map(Operation::result)
                .collect(toImmutableList());
        ImmutableList.Builder<CteReuse.TraversalState> rightBranchesBuilder = ImmutableList.builder();
        ImmutableList.Builder<CheckpointReferences> rightBranchToCheckpointBuilder = ImmutableList.builder();
        leftSourceResults.stream()
                .map(rightSourceResults::indexOf)
                .forEach(index -> {
                    rightBranchesBuilder.add(rightSourceUnordered.residualStates().get(index));
                    rightBranchToCheckpointBuilder.add(rightBranchToCheckpointUnordered.getMappingForBranch(index));
                });
        rightSource = new CteReuse.UnifiedStates(rightSourceUnordered.unifiedOperation(), rightBranchesBuilder.build());
        rightBranchToCheckpoint = BranchesToCheckpointsMapping.fromBranchMappings(rightBranchToCheckpointBuilder.build());

        // verify that there is no unsupported TraversalContext.predicateToApply on inner side of the join
        JoinType joinType = JOIN_TYPE.getAttribute(leftSource.residualStates().getFirst().nextOperation().operation().attributes());
        if (joinType.equals(RIGHT) || joinType.equals(FULL)) {
            checkState(
                    leftSource.residualStates().stream()
                            .map(CteReuse.TraversalState::traversalContext)
                            .map(CteReuse.TraversalContext::predicateToApply)
                            .allMatch(PredicateUtils::isTrue),
                    "left source of %s join has predicate left",
                    joinType);
        }
        if (joinType.equals(LEFT) || joinType.equals(FULL)) {
            checkState(
                    rightSource.residualStates().stream()
                            .map(CteReuse.TraversalState::traversalContext)
                            .map(CteReuse.TraversalContext::predicateToApply)
                            .allMatch(PredicateUtils::isTrue),
                    "right source of %s join has predicate left",
                    joinType);
        }

        CteReuse.TraversalState firstLeftBranch = leftSource.residualStates().getFirst();
        CteReuse.TraversalState firstRightBranch = rightSource.residualStates().getFirst();
        Join firstJoin = (Join) firstLeftBranch.nextOperation().operation();

        // get attributes for the unified Join
        boolean unifiedMaySkipOutputDuplicates = leftSource.residualStates().stream()
                .allMatch(branch -> MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(branch.nextOperation().operation().attributes()));
        Optional<DistributionType> unifiedDistributionType = Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(firstJoin.attributes()));
        Optional<Boolean> unifiedSpillable = Optional.ofNullable(SPILLABLE.getAttribute(firstJoin.attributes()));
        List<String> unifiedDynamicFilterIds = leftSource.residualStates().stream()
                .map(branch -> DYNAMIC_FILTER_IDS.getAttribute(branch.nextOperation().operation().attributes()))
                .flatMap(List::stream)
                .collect(toImmutableList());
        Optional<PlanNodeStatsAndCostSummary> unifiedReorderJoinStatsAndCost = Optional.empty();

        // get blocks for the unified Join
        // leftCriteriaSelector, rightCriteriaSelector, filter -- are equivalent for all merged joins
        // compute them by rebasing blocks of the first join
        List<Block> halfRebasedBlocks = rebaseJoinBlocks(
                firstJoin,
                0,
                relationRowType(trinoType(leftSource.unifiedOperation().result().type())),
                firstLeftBranch.traversalContext().fieldMapping(),
                nameAllocator);
        List<Block> rebasedBlocks = rebaseJoinBlocks(
                halfRebasedBlocks,
                1,
                relationRowType(trinoType(rightSource.unifiedOperation().result().type())),
                firstRightBranch.traversalContext().fieldMapping(),
                nameAllocator);
        List<Block> unifiedEquivalentBlocks = rebasedBlocks.subList(0, 3);

        // leftOutputSelector, rightOutputSelector should select the union of left and right outputs from all merged joins
        // additionally, they should select all fields necessary to support residual predicates
        Set<Integer> leftOutputFields = leftSource.residualStates().stream()
                .map(branch -> rebaseBlock(
                        ((Join) branch.nextOperation().operation()).leftOutputSelector(),
                        relationRowType(trinoType(leftSource.unifiedOperation().result().type())),
                        branch.traversalContext().fieldMapping(),
                        nameAllocator)
                        .orElseThrow())
                .map(block -> extractReferencedFields(block, getOnlyElement(block.parameters())))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        Set<Integer> leftResidualPredicateFields = leftSource.residualStates().stream()
                .map(CteReuse.TraversalState::traversalContext)
                .map(CteReuse.TraversalContext::predicateToApply)
                .map(predicate -> extractReferencedFields(predicate, getOnlyElement(predicate.parameters())))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        Set<Integer> leftFieldsToRetain = Sets.union(leftOutputFields, leftResidualPredicateFields);
        Set<Integer> leftFieldsToPrune = Sets.difference(
                IntStream.range(0, relationRowType(trinoType(leftSource.unifiedOperation().result().type())).getTypeParameters().size())
                        .boxed()
                        .collect(toImmutableSet()),
                leftFieldsToRetain);
        Block unifiedLeftOutputSelector = getPruningAssignments("^leftOutputSelector", relationRowType(trinoType(leftSource.unifiedOperation().result().type())), leftFieldsToPrune, nameAllocator);

        Set<Integer> rightOutputFields = rightSource.residualStates().stream()
                .map(branch -> rebaseBlock(
                        ((Join) branch.nextOperation().operation()).rightOutputSelector(),
                        relationRowType(trinoType(rightSource.unifiedOperation().result().type())),
                        branch.traversalContext().fieldMapping(),
                        nameAllocator)
                        .orElseThrow())
                .map(block -> extractReferencedFields(block, getOnlyElement(block.parameters())))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        Set<Integer> rightResidualPredicateFields = rightSource.residualStates().stream()
                .map(CteReuse.TraversalState::traversalContext)
                .map(CteReuse.TraversalContext::predicateToApply)
                .map(predicate -> extractReferencedFields(predicate, getOnlyElement(predicate.parameters())))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        Set<Integer> rightFieldsToRetain = Sets.union(rightOutputFields, rightResidualPredicateFields);
        Set<Integer> rightFieldsToPrune = Sets.difference(
                IntStream.range(0, relationRowType(trinoType(rightSource.unifiedOperation().result().type())).getTypeParameters().size())
                        .boxed()
                        .collect(toImmutableSet()),
                rightFieldsToRetain);
        Block unifiedRightOutputSelector = getPruningAssignments("^rightOutputSelector", relationRowType(trinoType(rightSource.unifiedOperation().result().type())), rightFieldsToPrune, nameAllocator);

        // dynamicFilterTargetSelector selects the union of dynamic filter targets from all merged joins
        Block unifiedDynamicFilterTargetSelector = concatenateFieldSelectors(
                rightSource.residualStates().stream()
                        .map(branch -> rebaseBlock(
                                ((Join) branch.nextOperation().operation()).dynamicFilterTargetSelector(),
                                relationRowType(trinoType(rightSource.unifiedOperation().result().type())),
                                branch.traversalContext().fieldMapping(),
                                nameAllocator)
                                .orElseThrow())
                        .collect(toImmutableList()),
                nameAllocator);

        Join unifiedJoin = new Join(
                nameAllocator.newName(),
                leftSource.unifiedOperation().result(),
                rightSource.unifiedOperation().result(),
                unifiedEquivalentBlocks.get(0),
                unifiedEquivalentBlocks.get(1),
                unifiedEquivalentBlocks.get(2),
                unifiedLeftOutputSelector,
                unifiedRightOutputSelector,
                unifiedDynamicFilterTargetSelector,
                joinType,
                unifiedMaySkipOutputDuplicates,
                unifiedDistributionType,
                unifiedSpillable,
                unifiedDynamicFilterIds,
                unifiedReorderJoinStatsAndCost,
                leftSource.unifiedOperation().attributes(),
                rightSource.unifiedOperation().attributes());
        newOperations.put(unifiedJoin.result(), unifiedJoin);

        // pull the TraversalContexts through the unified Join
        ImmutableList.Builder<CteReuse.TraversalState> traversalStates = ImmutableList.builder();

        // compute mapping from left and right unified types to unified Join type. Right output fields are laid out after left output fields
        FieldMapping unifiedLeftOutputMapping = getPassthroughMapping(unifiedLeftOutputSelector);
        int unifiedLeftOutputFieldsCount = trinoType(unifiedLeftOutputSelector.getReturnedType()).getTypeParameters().size();
        FieldMapping unifiedRightOutputMapping = new FieldMapping(getPassthroughMapping(unifiedRightOutputSelector).fieldIndexMapping().entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue() + unifiedLeftOutputFieldsCount))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

        // compute TraversalContext for each resulting branch -- after the unified Join operation
        // it should be derived from TraversalContexts of the corresponding left and right branch, and the unified Join operation
        for (int i = 0; i < leftSource.residualStates().size(); i++) {
            CteReuse.TraversalState leftBranch = leftSource.residualStates().get(i);
            CteReuse.TraversalState rightBranch = rightSource.residualStates().get(i);
            // get the original Join operation
            Join originalJoin = (Join) leftBranch.nextOperation().operation();

            // compute mapping for the next operation in the branch
            Block originalLeftOutputSelector = originalJoin.leftOutputSelector();
            int originalLeftOutputFieldsCount = trinoType(originalLeftOutputSelector.getReturnedType()).getTypeParameters().size();
            Block originalRightOutputSelector = originalJoin.rightOutputSelector();
            FieldMapping unifiedLeftMapping = getPassthroughMapping(originalLeftOutputSelector)
                    .inverse()
                    .composeWith(leftBranch.traversalContext().fieldMapping())
                    .composeWith(unifiedLeftOutputMapping);
            FieldMapping unifiedRightMapping = new FieldMapping(getPassthroughMapping(originalRightOutputSelector)
                    .inverse()
                    .composeWith(rightBranch.traversalContext().fieldMapping())
                    .composeWith(unifiedRightOutputMapping).fieldIndexMapping().entrySet().stream()
                    .map(entry -> Map.entry(entry.getKey() + originalLeftOutputFieldsCount, entry.getValue()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
            FieldMapping unifiedMapping = new FieldMapping(ImmutableMap.<Integer, Integer>builder()
                    .putAll(unifiedLeftMapping.fieldIndexMapping())
                    .putAll(unifiedRightMapping.fieldIndexMapping())
                    .buildOrThrow());

            // compute fields to prune -- all fields that were not originally output
            Set<Integer> fieldsToRetain = IntStream.range(0, relationRowType(trinoType(originalJoin.result().type())).getTypeParameters().size())
                    .boxed()
                    .map(unifiedMapping::get)
                    .collect(toImmutableSet());
            Set<Integer> fieldsToPrune = Sets.difference(
                    IntStream.range(0, relationRowType(trinoType(unifiedJoin.result().type())).getTypeParameters().size())
                            .boxed()
                            .collect(toImmutableSet()),
                    fieldsToRetain);

            // pull predicates to apply from both sources through the unified Join
            Block predicateToApply = optimizeLogicalOperations(conjunction(
                    ImmutableList.of(
                            rebaseBlock(
                                    leftBranch.traversalContext().predicateToApply(),
                                    relationRowType(trinoType(unifiedJoin.result().type())),
                                    unifiedLeftOutputMapping,
                                    nameAllocator)
                                    .orElseThrow(),
                            rebaseBlock(
                                    rightBranch.traversalContext().predicateToApply(),
                                    relationRowType(trinoType(unifiedJoin.result().type())),
                                    unifiedRightOutputMapping,
                                    nameAllocator)
                                    .orElseThrow()),
                    nameAllocator));

            // derive enforced predicate. It is composed of TraversalContext.enforcedPredicate, join criteria, and join filter.
            // predicates from left side of the left join, right side of the right join, and both sides of the inner join are effective after join.
            // TODO derive enforced predicate from join criteria and join filter
            Block pulledLeftEnforcedPredicate = rebasePredicateAndPruneUnsupportedConjuncts(leftBranch.traversalContext().enforcedPredicate(), unifiedJoin, unifiedLeftOutputMapping, nameAllocator);
            Block pulledRightEnforcedPredicate = rebasePredicateAndPruneUnsupportedConjuncts(rightBranch.traversalContext().enforcedPredicate(), unifiedJoin, unifiedRightOutputMapping, nameAllocator);
            Block enforcedPredicate = switch (joinType) {
                case INNER -> optimizeLogicalOperations(conjunction(ImmutableList.of(pulledLeftEnforcedPredicate, pulledRightEnforcedPredicate), nameAllocator));
                case LEFT -> optimizeLogicalOperations(pulledLeftEnforcedPredicate);
                case RIGHT -> optimizeLogicalOperations(pulledRightEnforcedPredicate);
                case FULL ->
                        truePredicate(Optional.empty(), ImmutableList.of(new Block.Parameter(nameAllocator.newName(), irType(relationRowType(trinoType(unifiedJoin.result().type()))))), nameAllocator);
            };

            traversalStates.add(new CteReuse.TraversalState(
                    new CteReuse.TraversalContext(unifiedMapping, fieldsToPrune, predicateToApply, enforcedPredicate, OptionalLong.empty()),
                    getNextOperation(originalJoin, operationToDownstream)));
        }

        // concatenate checkpoints lists from left and right sources
        List<Checkpoint> unifiedCheckpoints = ImmutableList.<Checkpoint>builder()
                .addAll(leftCheckpoints)
                .addAll(rightCheckpoints)
                .build();

        // combine branch-to-checkpoint mapping. For each branch concatenate mappings from the corresponding left and right branches
        BranchesToCheckpointsMapping unifiedBranchToCheckpoint = BranchesToCheckpointsMapping.fromBranchMappings(IntStream.range(0, leftSource.residualStates().size())
                .boxed()
                .map(index -> concatenateCheckpointReferences(ImmutableList.of(leftBranchToCheckpoint.getMappingForBranch(index), rightBranchToCheckpoint.getMappingForBranch(index))))
                .collect(toImmutableList()));

        return new CteReuse.UnifiedStatesAndCheckpointMapping(
                new CteReuse.UnifiedStates(unifiedJoin, traversalStates.build()),
                unifiedCheckpoints,
                unifiedBranchToCheckpoint);
    }

    private static List<Block> rebaseJoinBlocks(Join join, int sourceIndex, Type inputRowType, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return rebaseJoinBlocks(
                join.regions().stream()
                        .map(Region::getOnlyBlock)
                        .collect(toImmutableList()),
                sourceIndex,
                inputRowType,
                fieldMapping,
                nameAllocator);
    }

    private static List<Block> rebaseJoinBlocks(List<Block> blocks, int sourceIndex, Type inputRowType, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        ImmutableList.Builder<Block> rebasedBlocks = ImmutableList.builder();

        if (sourceIndex == 0) {
            // leftCriteriaSelector
            rebasedBlocks.add(rebaseBlock(blocks.get(0), inputRowType, fieldMapping, nameAllocator).orElseThrow());
            // rightCriteriaSelector
            rebasedBlocks.add(blocks.get(1));
            // filter
            rebasedBlocks.add(rebaseBlock(blocks.get(2), 0, inputRowType, fieldMapping, nameAllocator).orElseThrow());
            // leftOutputSelector
            rebasedBlocks.add(rebaseBlock(blocks.get(3), inputRowType, fieldMapping, nameAllocator).orElseThrow());
            // rightOutputSelector
            rebasedBlocks.add(blocks.get(4));
            // dynamicFilterTargetSelector
            rebasedBlocks.add(blocks.get(5));
        }
        else {
            // leftCriteriaSelector
            rebasedBlocks.add(blocks.get(0));
            // rightCriteriaSelector
            rebasedBlocks.add(rebaseBlock(blocks.get(1), inputRowType, fieldMapping, nameAllocator).orElseThrow());
            // filter
            rebasedBlocks.add(rebaseBlock(blocks.get(2), 1, inputRowType, fieldMapping, nameAllocator).orElseThrow());
            // leftOutputSelector
            rebasedBlocks.add(blocks.get(3));
            // rightOutputSelector
            rebasedBlocks.add(rebaseBlock(blocks.get(4), inputRowType, fieldMapping, nameAllocator).orElseThrow());
            // dynamicFilterTargetSelector
            rebasedBlocks.add(rebaseBlock(blocks.get(5), inputRowType, fieldMapping, nameAllocator).orElseThrow());
        }

        return rebasedBlocks.build();
    }
}
