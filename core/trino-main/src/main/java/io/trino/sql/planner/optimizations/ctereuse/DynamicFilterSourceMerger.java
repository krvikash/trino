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
import io.trino.sql.dialect.trino.operation.DynamicFilterSource;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStates;
import io.trino.sql.planner.optimizations.ctereuse.SingleGroupMerger.SingleGroupMergeDecomposition;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.trino.Attributes.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.concatenateFieldSelectors;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;

public class DynamicFilterSourceMerger
        implements SingleGroupMerger.SingleGroupProcessor
{
    @Override
    public boolean processes(Operation operation)
    {
        return operation instanceof DynamicFilterSource;
    }

    /**
     * Identify all DynamicFilterSource operations.
     */
    @Override
    public SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(UnifiedStates unifiedStates, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<TraversalState> branches = unifiedStates.residualStates();
        List<Integer> dynamicFilterSourceBranches = IntStream.range(0, branches.size())
                .filter(index -> branches.get(index).nextOperation().operation() instanceof DynamicFilterSource)
                .boxed()
                .collect(toImmutableList());

        return new SingleGroupMergeDecomposition(dynamicFilterSourceBranches.size() > 1 ? ImmutableList.of(dynamicFilterSourceBranches) : ImmutableList.of());
    }

    @Override
    public CteReuse.UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
            Operation unifiedOperation,
            List<TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            Map<Operation, Operation> usesMap,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations)
    {
        // concatenate dynamic filter Ids lists from all branches
        List<String> unifiedDynamicFilterIds = branches.stream()
                .map(branch -> DYNAMIC_FILTER_IDS.getAttribute(branch.nextOperation().operation().attributes()))
                .flatMap(List::stream)
                .collect(toImmutableList());

        // concatenate dynamic filter target field selectors from all branches
        Block unifiedDynamicFilterTargetSelector = concatenateFieldSelectors(
                branches.stream()
                        .map(branch -> rebaseBlock(
                                ((DynamicFilterSource) branch.nextOperation().operation()).dynamicFilterTargetSelector(),
                                relationRowType(trinoType(unifiedOperation.result().type())),
                                branch.traversalContext().fieldMapping(),
                                nameAllocator)
                                .orElseThrow())
                        .collect(toImmutableList()),
                nameAllocator);

        DynamicFilterSource unifiedDynamicFilterSource = new DynamicFilterSource(
                nameAllocator.newName(),
                unifiedOperation.result(),
                unifiedDynamicFilterTargetSelector,
                unifiedDynamicFilterIds,
                unifiedOperation.attributes());
        newOperations.put(unifiedDynamicFilterSource.result(), unifiedDynamicFilterSource);

        List<TraversalState> newTraversalStates = branches.stream()
                .map(branch -> new TraversalState(
                        // The traversal context can be pulled through the unified operation without modifications.
                        // The unified DynamicFilterSource has the same output type as the recent unified operation,
                        // and the next operation's input type is the same as the component DynamicFilterSource input type.
                        branch.traversalContext(),
                        getNextOperation(branch.nextOperation().operation(), usesMap)))
                .collect(toImmutableList());

        return new CteReuse.UnifiedStatesAndCheckpointMapping(
                new UnifiedStates(unifiedDynamicFilterSource, newTraversalStates),
                checkpoints,
                branchToCheckpoint);
    }
}
