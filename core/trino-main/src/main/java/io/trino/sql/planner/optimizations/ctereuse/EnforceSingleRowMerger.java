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
import com.google.common.collect.Multimap;
import io.trino.metadata.Metadata;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.EnforceSingleRow;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStates;
import io.trino.sql.planner.optimizations.ctereuse.SingleGroupMerger.SingleGroupMergeDecomposition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.disjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.isTrue;
import static io.trino.sql.planner.optimizations.ctereuse.SemanticEquivalenceUtils.blocksSemanticallyEquivalent;

public class EnforceSingleRowMerger
        implements SingleGroupMerger.SingleGroupProcessor
{
    @Override
    public boolean processes(Operation operation)
    {
        return operation instanceof EnforceSingleRow;
    }

    /**
     * Identify all EnforceSingleRow operations and merge them only if their residual predicates are semantically equivalent.
     */
    @Override
    public SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(UnifiedStates unifiedStates, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<TraversalState> branches = unifiedStates.residualStates();

        int[] enforceSingleRowSubGroups = new int[branches.size()];
        Arrays.fill(enforceSingleRowSubGroups, -1);
        Map<Integer, Block> subgroupIdToDetails = new HashMap<>();

        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            Operation nextOperation = branch.nextOperation().operation();
            if (nextOperation instanceof EnforceSingleRow) {
                boolean foundMatchingSubgroup = false;
                for (Map.Entry<Integer, Block> subgroupIdAndDetails : subgroupIdToDetails.entrySet()) {
                    int id = subgroupIdAndDetails.getKey();
                    Block predicateToApply = subgroupIdAndDetails.getValue();
                    if (blocksSemanticallyEquivalent(branch.traversalContext().predicateToApply(), predicateToApply)) {
                        enforceSingleRowSubGroups[i] = id;
                        foundMatchingSubgroup = true;
                        break;
                    }
                }
                // if there is no matching subgroup, start a new subgroup
                if (!foundMatchingSubgroup) {
                    enforceSingleRowSubGroups[i] = i;
                    subgroupIdToDetails.put(i, branch.traversalContext().predicateToApply());
                }
            }
        }

        // extract subgroups
        Multimap<Integer, Integer> subgroups = ArrayListMultimap.create();
        for (int i = 0; i < enforceSingleRowSubGroups.length; i++) {
            if (enforceSingleRowSubGroups[i] != -1) {
                subgroups.put(enforceSingleRowSubGroups[i], i);
            }
        }

        // return subgroups with two or more elements
        return new SingleGroupMergeDecomposition(subgroups.asMap().values().stream()
                .filter(indexes -> indexes.size() > 1)
                .map(ImmutableList::copyOf)
                .collect(toImmutableList()));
    }

    @Override
    public CteReuse.UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
            Operation unifiedOperation,
            List<TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations,
            Metadata metadata)
    {
        checkArgument(branches.stream().allMatch(branch -> isTrue(branch.traversalContext().predicateToApply())), "All branches must have a TRUE residual predicate");
        EnforceSingleRow unifiedEnforceSingleRow = new EnforceSingleRow(
                nameAllocator.newName(),
                unifiedOperation.result(),
                unifiedOperation.attributes());

        io.trino.spi.type.Type unifiedOperationRowType = relationRowType(trinoType(unifiedOperation.result().type()));
        int inputFieldCount = unifiedOperationRowType.getTypeParameters().size();
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(unifiedOperationRowType));

        ImmutableList.Builder<Block> fieldIsNullPredicate = ImmutableList.builder();
        for (int i = 0; i < inputFieldCount; i++) {
            Block.Builder builder = new Block.Builder(Optional.of("^predicate_for_null"), ImmutableList.of(parameter));
            FieldReference fieldReference = new FieldReference(nameAllocator.newName(), parameter, i, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);

            builder.addOperation(fieldReference);

            IsNull isNull = new IsNull(
                    nameAllocator.newName(),
                    fieldReference.result(),
                    fieldReference.attributes());
            builder.addOperation(isNull);

            builder.addOperation(new Return(nameAllocator.newName(), isNull.result(), isNull.attributes()));
            fieldIsNullPredicate.add(builder.build());
        }

        newOperations.put(unifiedEnforceSingleRow.result(), unifiedEnforceSingleRow);

        List<TraversalState> newTraversalStates = branches.stream()
                .map(branch -> new TraversalState(
                        new CteReuse.TraversalContext(
                                branch.traversalContext().fieldMapping(),
                                branch.traversalContext().fieldsToPrune(),
                                branch.traversalContext().predicateToApply(),
                                disjunction(
                                        ImmutableList.of(
                                                branch.traversalContext().enforcedPredicate(),
                                                conjunction(fieldIsNullPredicate.build(), nameAllocator)),
                                        nameAllocator),
                                OptionalLong.of(1)),
                        getNextOperation(branch.nextOperation().operation(), operationToDownstream)))
                .collect(toImmutableList());

        return new CteReuse.UnifiedStatesAndCheckpointMapping(
                new UnifiedStates(unifiedEnforceSingleRow, newTraversalStates),
                checkpoints,
                branchToCheckpoint);
    }
}
