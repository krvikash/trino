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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.CheckpointReferences;
import io.trino.sql.planner.optimizations.ctereuse.Checkpoint.IntermediateCheckpoint;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.identityBranchToCheckpoint;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.compensateAndWire;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.mergeGroupRecursively;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;

public class MultiGroupMerger
{
    private static final List<MultiGroupProcessor> MULTI_GROUP_PROCESSORS = ImmutableList.of(new JoinMerger(), new ExchangeMerger());

    private int hangingGroupId;
    private final Map<Integer, HangingGroup> hangingGroups = new LinkedHashMap<>();

    /**
     * Cross-check the new group with the hanging groups. Find the operations that can be merged across the new group and the hanging groups.
     * <p>
     * Branches of the new group can be categorized into single-group and multi-group.
     * The single-group branches, i.e. branches having all dependencies within the current group, are not processed in MultiGroupMerger.
     * The multi-group branches, i.e. branches having dependencies within the current group as well as in one or more other groups, are analyzed.
     * MultiGroupMerger finds potential merges of the branches from the new group with other groups.
     * <p>
     * Example:
     * There is hanging group A:
     * a0 -> Join0 (as left source)
     * a1 -> Join1 (as felt source)
     * a2 -> Join2 (as left source),
     * where Join0 and Join1 are identical, and Join2 differs.
     * The new group B has the following branches:
     * b0 -> Join0 (as right source)
     * b1 -> Join1 (as right source)
     * b2 -> Join2 (as right source)
     * b3 -> Join3 (as left source).
     * The Join operations Join0, Join1, and Join2 now have both sources known.
     * Join0 and Join1 can be merged as they are identical. Join2 cannot be merged.
     * Join3 has the right source still unknown.
     * We want to merge Join0 and Join1, so we must split the hanging group A so that the branches to merge constitute a separate group.
     * After the split, we have a new group C, ready to merge with the branches of group B:
     * a0 -> Join0 (as left source)
     * a1 -> Join1 (as felt source).
     * This method should return the following decomposition of the new group B:
     * - multiGroupMerges -> (b0, b1) can be merged with group C
     * - hangingBranches -> (b2, b3)
     */
    public MultiGroupMergeDecomposition identifyMultiGroupSubgroupsToMerge(CteReuse.UnifiedStates newGroup, Map<Operation, Operation> operationToDownstream, ProgramBuilder.ValueNameAllocator nameAllocator, Map<Value, Operation> newOperations, PlannerContext plannerContext, Session session)
    {
        // find merging candidates across the new group and the hanging groups
        List<MultiGroupMergeCandidate> candidates = MULTI_GROUP_PROCESSORS.stream()
                .map(processor -> processor.identifyMultiGroupMergeCandidates(newGroup, hangingGroups, nameAllocator))
                .flatMap(List::stream)
                .collect(toImmutableList());

        // we can only report merges if they involve whole hanging groups
        // if a merge involves part of a hanging group, we must first split the group
        ImmutableListMultimap.Builder<Integer, List<Integer>> hangingSubgroupsToMerge = ImmutableListMultimap.builder();
        candidates.stream()
                .map(MultiGroupMergeCandidate::hangingGroupBranches)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .forEach(hangingSubgroupsToMerge::put);
        boolean splitGroups = false;
        for (Map.Entry<Integer, Collection<List<Integer>>> hangingGroupDecomposition : hangingSubgroupsToMerge.build().asMap().entrySet()) {
            int groupId = hangingGroupDecomposition.getKey();
            Collection<List<Integer>> decomposition = hangingGroupDecomposition.getValue();
            if (!isSingleFullSubgroup(groupId, decomposition)) {
                HangingGroup groupToSplit = hangingGroups.remove(groupId);
                splitGroupRecursively(groupToSplit, decomposition, operationToDownstream, nameAllocator, newOperations, plannerContext, session);
                splitGroups = true;
            }
        }
        // we expect that the split groups re-appeared in MultiGroupMerger as multiple hanging groups. Now they should be granular enough to support merging multi-group operations
        // once again, find merging candidates across the new group and the hanging groups
        if (splitGroups) {
            candidates = MULTI_GROUP_PROCESSORS.stream()
                    .map(processor -> processor.identifyMultiGroupMergeCandidates(newGroup, hangingGroups, nameAllocator))
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }

        // filter out any merges where the hanging groups are not fully used
        List<MultiGroupMerge> multiGroupMerges = candidates.stream()
                .filter(multiGroupMergeCandidate ->
                        multiGroupMergeCandidate.hangingGroupBranches().entrySet().stream()
                                .allMatch(entry -> isSingleFullSubgroup(entry.getKey(), ImmutableList.of(entry.getValue()))))
                .map(multiGroupMergeCandidate -> new MultiGroupMerge(multiGroupMergeCandidate.newGroupBranches(), ImmutableList.copyOf(multiGroupMergeCandidate.hangingGroupBranches().keySet())))
                .collect(toImmutableList());

        // collect branches to be merged from the new group
        Set<Integer> mergedBranches = multiGroupMerges.stream()
                .map(MultiGroupMerge::newGroupBranches)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        // find all multi-group branches from the new group that will not be merged -- report them as hanging branches
        List<Integer> multiGroupBranches = identifyMultiGroupBranches(newGroup);
        List<Integer> hangingBranches = multiGroupBranches.stream()
                .filter(index -> !mergedBranches.contains(index))
                .collect(toImmutableList());

        return new MultiGroupMergeDecomposition(multiGroupMerges, hangingBranches);
    }

    public CteReuse.UnifiedStatesAndCheckpointMapping mergeNextMultiGroupOperation(
            Operation unifiedOperation,
            List<CteReuse.TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            List<Integer> hangingGroupsToMerge,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations)
    {
        MultiGroupProcessor processor = MULTI_GROUP_PROCESSORS.stream()
                .filter(multiGroupProcessor -> multiGroupProcessor.processes(branches.getFirst().nextOperation().operation()))
                .findFirst()
                .orElseThrow();

        CteReuse.UnifiedStatesAndCheckpointMapping mergeResult = processor.mergeNextMultiGroupOperation(unifiedOperation, branches, checkpoints, branchToCheckpoint, hangingGroupsToMerge, hangingGroups, operationToDownstream, nameAllocator, newOperations);

        for (int groupId : hangingGroupsToMerge) {
            hangingGroups.remove(groupId);
        }

        return mergeResult;
    }

    public boolean isCheckpointRequired(List<Integer> hangingGroupIds)
    {
        return hangingGroupIds.stream()
                .map(hangingGroups::get)
                .anyMatch(HangingGroup::setCheckpoint);
    }

    public void registerHangingGroup(
            Operation unifiedOperation,
            List<CteReuse.TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            boolean setCheckpoint)
    {
        hangingGroups.put(
                hangingGroupId++,
                new HangingGroup(unifiedOperation, branches, checkpoints, branchToCheckpoint, setCheckpoint));
    }

    public void flush(ProgramBuilder.ValueNameAllocator nameAllocator, Map<Value, Operation> newOperations)
    {
        for (int groupId : ImmutableSet.copyOf(hangingGroups.keySet())) {
            HangingGroup hangingGroup = hangingGroups.remove(groupId);
            for (int branch = 0; branch < hangingGroup.branches().size(); branch++) {
                CheckpointReferences checkpointReferences = hangingGroup.branchToCheckpoint().getMappingForBranch(branch);
                for (int i = 0; i < hangingGroup.checkpoints().size(); i++) {
                    // only compensate and wire branches from intermediate checkpoints. Skip the bottom branches -- the algorithm will find them when building the final plan.
                    if (hangingGroup.checkpoints().get(i) instanceof IntermediateCheckpoint(CteReuse.UnifiedStates unifiedStates)) {
                        List<Integer> references = checkpointReferences.getReferencesForCheckpoint(i);
                        for (int reference : references) {
                            compensateAndWire(unifiedStates.residualStates().get(reference), unifiedStates.unifiedOperation(), nameAllocator, newOperations);
                        }
                    }
                }
            }
        }
    }

    private boolean isSingleFullSubgroup(int groupId, Collection<List<Integer>> decomposition)
    {
        return decomposition.size() == 1 &&
                range(0, hangingGroups.get(groupId).branches().size())
                        .boxed()
                        .collect(toImmutableSet())
                        .equals(ImmutableSet.copyOf(getOnlyElement(decomposition)));
    }

    private void splitGroupRecursively(
            HangingGroup groupToSplit,
            Collection<List<Integer>> decomposition,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations,
            PlannerContext plannerContext,
            Session session)
    {
        // extract unreferenced branches of the hanging group
        Set<Integer> groupIndexes = IntStream.range(0, groupToSplit.branches().size())
                .boxed()
                .collect(toImmutableSet());
        Set<Integer> referencedIndexes = decomposition.stream()
                .flatMap(List::stream)
                .collect(toImmutableSet());
        List<Integer> unreferencedIndexes = ImmutableList.copyOf(Sets.difference(groupIndexes, referencedIndexes));

        // collect all subgroups to extract
        ImmutableList.Builder<List<Integer>> allSubgroups = ImmutableList.builder();
        allSubgroups.addAll(decomposition);
        // if there are multiple unreferenced branches, extract them as another subgroup
        if (unreferencedIndexes.size() > 1) {
            allSubgroups.add(unreferencedIndexes);
        }

        // for all subgroups, select and merge recursively subgroups from all corresponding checkpoints
        for (List<Integer> subgroupIndexes : allSubgroups.build()) {
            checkArgument(subgroupIndexes.size() > 1, "subgroupIndexes size must be greater than 1");
            for (int i = 0; i < groupToSplit.checkpoints().size(); i++) {
                Checkpoint checkpoint = groupToSplit.checkpoints().get(i);
                ImmutableList.Builder<Integer> checkpointReferences = ImmutableList.builder();
                for (int branch : subgroupIndexes) {
                    checkpointReferences.addAll(groupToSplit.branchToCheckpoint().getMappingForBranch(branch).getReferencesForCheckpoint(i));
                }
                CteReuse.UnifiedStates backtrackSubgroup = checkpoint.extractSubgroup(checkpointReferences.build(), operationToDownstream, nameAllocator, newOperations, plannerContext, session);
                Checkpoint backtrackCheckpoint = checkpoint.extractSubgroupCheckpoint(checkpointReferences.build());
                mergeGroupRecursively(
                        backtrackSubgroup,
                        ImmutableList.of(backtrackCheckpoint),
                        identityBranchToCheckpoint(backtrackCheckpoint.branchesCount()),
                        checkpoint instanceof IntermediateCheckpoint,
                        operationToDownstream,
                        nameAllocator,
                        newOperations,
                        this,
                        plannerContext,
                        session);
            }
        }
        // if there is one remaining branch, handle it
        if (unreferencedIndexes.size() == 1) {
            CheckpointReferences checkpointReferences = groupToSplit.branchToCheckpoint().getMappingForBranch(getOnlyElement(unreferencedIndexes));
            for (int i = 0; i < groupToSplit.checkpoints().size(); i++) {
                // only compensate and wire branches from intermediate checkpoints. Skip the bottom branches -- the algorithm will find them when building the final plan.
                if (groupToSplit.checkpoints().get(i) instanceof IntermediateCheckpoint(CteReuse.UnifiedStates unifiedStates)) {
                    List<Integer> references = checkpointReferences.getReferencesForCheckpoint(i);
                    for (int reference : references) {
                        compensateAndWire(unifiedStates.residualStates().get(reference), unifiedStates.unifiedOperation(), nameAllocator, newOperations);
                    }
                }
            }
        }
    }

    /**
     * Find all branches in the group that have dependencies outside the group.
     * For example, a branch where the next operation is Join, and only the left source of the Join is in the group.
     */
    public static List<Integer> identifyMultiGroupBranches(CteReuse.UnifiedStates unifiedStates)
    {
        Multiset<Value> nextOperationReferences = unifiedStates.residualStates().stream()
                .map(CteReuse.TraversalState::nextOperation)
                .map(CteReuse.OperationAndIndex::operation)
                .map(Operation::result)
                .collect(toImmutableMultiset());

        ImmutableList.Builder<Integer> multigroupBranches = ImmutableList.builder();
        for (int i = 0; i < unifiedStates.residualStates().size(); i++) {
            Operation operation = unifiedStates.residualStates().get(i).nextOperation().operation();
            if (nextOperationReferences.count(operation.result()) < operation.arguments().size()) {
                multigroupBranches.add(i);
            }
        }
        return multigroupBranches.build();
    }

    /**
     * A collection of branches from different groups that can be merged together.
     *
     * @param newGroupBranches -- branches from the current group
     * @param hangingGroupBranches -- branches from hanging groups, identified by hanging group id
     */
    public record MultiGroupMergeCandidate(List<Integer> newGroupBranches, Map<Integer, List<Integer>> hangingGroupBranches)
    {
        public MultiGroupMergeCandidate
        {
            requireNonNull(newGroupBranches, "newGroupBranches is null");
            requireNonNull(hangingGroupBranches, "hangingGroupBranches is null");
            checkArgument(newGroupBranches.size() > 1, "must have at least 2 new group branches");
            newGroupBranches = ImmutableList.copyOf(newGroupBranches);
            hangingGroupBranches = hangingGroupBranches.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
        }
    }

    /**
     * A collection of branches from different groups that will be merged together. It consists of a collection of branches from the current group and hanging groups that can be fully merged.
     */
    public record MultiGroupMerge(List<Integer> newGroupBranches, List<Integer> hangingGroups)
    {
        public MultiGroupMerge
        {
            requireNonNull(newGroupBranches, "newGroupBranches is null");
            requireNonNull(hangingGroups, "hangingGroups is null");
            checkArgument(newGroupBranches.size() > 1, "must have at least 2 new group branches");
            checkArgument(!hangingGroups.isEmpty(), "must have at least 1 hanging group");
            newGroupBranches = ImmutableList.copyOf(newGroupBranches);
            hangingGroups = ImmutableList.copyOf(hangingGroups);
        }
    }

    /**
     * The result of decomposing a group according to the merging capacity across multiple groups.
     *
     * @param multiGroupMerges -- subgroups of branches that can be merged together with other groups.
     * each merge consists of multiple branches from the new group, and one or more hanging groups
     * @param hangingBranches -- branches that cannot be merged currently
     */
    public record MultiGroupMergeDecomposition(List<MultiGroupMerge> multiGroupMerges, List<Integer> hangingBranches)
    {
        public MultiGroupMergeDecomposition
        {
            requireNonNull(multiGroupMerges, "merges is null");
            requireNonNull(hangingBranches, "hangingBranches is null");
            multiGroupMerges = ImmutableList.copyOf(multiGroupMerges);
            hangingBranches = ImmutableList.copyOf(hangingBranches);
        }

        public Set<Integer> getIndexes()
        {
            return Sets.union(
                    multiGroupMerges.stream()
                            .map(MultiGroupMerge::newGroupBranches)
                            .flatMap(List::stream)
                            .collect(toImmutableSet()),
                    ImmutableSet.copyOf(hangingBranches));
        }
    }

    /**
     * A group containing multi-group branches. Potentially, it might be later merged with other groups.
     */
    public record HangingGroup(
            Operation unifiedOperation,
            List<CteReuse.TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            boolean setCheckpoint)
    {
        public HangingGroup
        {
            requireNonNull(unifiedOperation, "unifiedOperation is null");
            requireNonNull(branches, "branches is null");
            requireNonNull(checkpoints, "checkpoints is null");
            requireNonNull(branchToCheckpoint, "branchToCheckpoint is null");
            checkArgument(branches.size() > 1, "must have at least 2 branches");
            branches = ImmutableList.copyOf(branches);
            checkpoints = ImmutableList.copyOf(checkpoints);
        }
    }

    public interface MultiGroupProcessor
    {
        boolean processes(Operation operation);

        /**
         * Find subgroups of branches to merge. Each returned subgroup might be further merged with the mergeNextMultiGroupOperation() method.
         * All branches chosen for merging must be multi-group. It means that their nextOperation must have one or more sources from this group
         * and one or more sources in hangingGroups.
         * <p>
         * Note that merged operations must be deterministic and free of side effects.
         */
        List<MultiGroupMergeCandidate> identifyMultiGroupMergeCandidates(CteReuse.UnifiedStates newGroup, Map<Integer, HangingGroup> hangingGroups, ProgramBuilder.ValueNameAllocator nameAllocator);

        /**
         * Merge the next operations for all branches, combining them with other groups (hangingGroupsToMerge). Update the branchToCheckpoint mapping.
         * <p>
         * Note: every newly created relational operation must be recorded in newOperations.
         * <p>
         * Note on updating the branch to checkpoint mapping.
         * Each resulting branch is a combination of source branches from multiple groups.
         * Each of the source branches has its checkpoints. The resulting checkpoint is a combination
         * of the source checkpoints.
         */
        CteReuse.UnifiedStatesAndCheckpointMapping mergeNextMultiGroupOperation(
                Operation unifiedOperation,
                List<CteReuse.TraversalState> branches,
                List<Checkpoint> checkpoints,
                BranchesToCheckpointsMapping branchToCheckpoint,
                List<Integer> hangingGroupsToMerge,
                Map<Integer, HangingGroup> hangingGroups,
                Map<Operation, Operation> operationToDownstream,
                ProgramBuilder.ValueNameAllocator nameAllocator,
                Map<Value, Operation> newOperations);
    }
}
