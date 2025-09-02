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
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class SingleGroupMerger
{
    private static final List<SingleGroupProcessor> SINGLE_GROUP_PROCESSORS = ImmutableList.of(new ExchangeMerger(), new ProjectMerger(), new AggregationMerger(), new DynamicFilterSourceMerger());

    private SingleGroupMerger()
    {}

    public static SingleGroupMergeDecomposition identifySingleGroupSubgroupsToMerge(CteReuse.UnifiedStates group, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return new SingleGroupMergeDecomposition(SINGLE_GROUP_PROCESSORS.stream()
                .map(processor -> processor.identifySingleGroupMergeCandidates(group, nameAllocator))
                .map(SingleGroupMergeDecomposition::singleGroupMerges)
                .flatMap(List::stream)
                .collect(toImmutableList()));
    }

    public static CteReuse.UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
            Operation unifiedOperation,
            List<CteReuse.TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations)
    {
        SingleGroupProcessor processor = SINGLE_GROUP_PROCESSORS.stream()
                .filter(singleGroupProcessor -> singleGroupProcessor.processes(branches.getFirst().nextOperation().operation()))
                .findFirst()
                .orElseThrow();

        return processor.mergeNextSingleGroupOperation(unifiedOperation, branches, checkpoints, branchToCheckpoint, operationToDownstream, nameAllocator, newOperations);
    }

    /**
     * The result of decomposing a group according to the merging capacity within the group.
     * Each subgroup must have multiple branches.
     */
    public record SingleGroupMergeDecomposition(List<List<Integer>> singleGroupMerges)
    {
        public SingleGroupMergeDecomposition
        {
            requireNonNull(singleGroupMerges, "merges is null");
            checkArgument(
                    singleGroupMerges.stream()
                            .allMatch(indexes -> indexes.size() > 1),
                    "each subgroup must have at least 2 branches");
            singleGroupMerges = singleGroupMerges.stream()
                    .map(ImmutableList::copyOf)
                    .collect(toImmutableList());
        }

        public Set<Integer> getIndexes()
        {
            return singleGroupMerges.stream()
                    .flatMap(List::stream)
                    .collect(toImmutableSet());
        }
    }

    public interface SingleGroupProcessor
    {
        boolean processes(Operation operation);

        /**
         * Find subgroups of branches to merge. Each returned subgroup might be further merged with the mergeNextSingleGroupOperation() method.
         * All branches chosen for merging must be single-group. It means that all sources of their nextOperation must belong to the group.
         * <p>
         * Note that merged operations must be deterministic and free of side effects.
         */
        SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(CteReuse.UnifiedStates group, ProgramBuilder.ValueNameAllocator nameAllocator);

        /**
         * Merge the next operations for all branches. Update the branchToCheckpoint mapping.
         * <p>
         * Note: every newly created relational operation must be recorded in newOperations.
         * <p>
         * Note on updating the branch to checkpoint mapping.
         * Usually, when merging linear branches, the branchToCheckpoint mapping does not need an update.
         * For example, when we merge TopN operations, the existing branchToCheckpoint mapping remains valid.
         * The situation is different when we merge multi-source operations, thus reducing the number of branches in the group.
         * For example, when we transform a self-Join into Window, we have one resulting branch for two input branches.
         * The resulting branch has a checkpoint consisting of the composed checkpoints for the two input branches.
         */
        CteReuse.UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
                Operation unifiedOperation,
                List<CteReuse.TraversalState> branches,
                List<Checkpoint> checkpoints,
                BranchesToCheckpointsMapping branchToCheckpoint,
                Map<Operation, Operation> operationToDownstream,
                ProgramBuilder.ValueNameAllocator nameAllocator,
                Map<Value, Operation> newOperations);
    }
}
