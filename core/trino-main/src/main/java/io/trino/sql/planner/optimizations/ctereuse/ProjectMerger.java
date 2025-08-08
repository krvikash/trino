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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalContext;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStates;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStatesAndCheckpointMapping;
import io.trino.sql.planner.optimizations.ctereuse.SingleGroupMerger.SingleGroupMergeDecomposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.composeProjectedItems;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getIdentityMappings;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getProjectedItems;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.rebasePredicateAndPruneUnsupportedConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.SemanticEquivalenceUtils.blocksSemanticallyEquivalent;

public class ProjectMerger
        implements SingleGroupMerger.SingleGroupProcessor
{
    @Override
    public boolean processes(Operation operation)
    {
        return operation instanceof Project;
    }

    /**
     * Identify all deterministic Project operations.
     * Note: these are computing projections. Identity projections are ingested into the traversal context.
     */
    @Override
    public SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(UnifiedStates unifiedStates, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<TraversalState> branches = unifiedStates.residualStates();
        List<Integer> projectBranches = IntStream.range(0, branches.size())
                .filter(index -> branches.get(index).nextOperation().operation() instanceof Project)
                .filter(index -> isDeterministic(branches.get(index).nextOperation().operation()))
                .boxed()
                .collect(toImmutableList());

        return new SingleGroupMergeDecomposition(projectBranches.size() > 1 ? ImmutableList.of(projectBranches) : ImmutableList.of());
    }

    /**
     * Unify multiple computing projections.
     * <p>
     * The resulting Project operation includes projected expressions from all component Project operations.
     * Additionally, it projects all fields necessary to support the residual predicates for all component branches.
     * <p>
     * The projected expressions are deduplicated across the component projections based on semantic equivalence.
     * However, when a component projection produces multiple semantically equivalent expressions, these are not deduplicated.
     * The reason for this is that the FieldMapping used to rebase the next downstream operation from the component projection
     * onto the unified projection must be a reversible BiMap. Therefore, each field of the original projection must be mapped
     * to a distinct field of the unified projection.
     * <p>
     * Example projection merge:
     * The recent unified operation has output type (f0, f1, f2, f3)
     * Branch1: Projection [f2, "const", f3+f1, f2]. Residual predicate: f0&gt;f1
     * Branch2: Projection ["const", f3+f1, f2, f2, f2]. Residual predicate: f0&lt;0
     * Building the unified projection assignments. Start with empty list [].
     * - After adding the first projection's expressions: [f2, "const", f3+f1, f2]
     * Note that the duplicate expression f2 is added twice.
     * Mapping for the first branch: {0→0, 1→1, 2→2, 3→3}
     * Adding projection for fields f0 and f1, needed for the residual predicate f0&gt;f1: [f2, "const", f3+f1, f2, f0, f1]
     * - After adding the second projection's expressions: [f2, "const", f3+f1, f2, f0, f1, f2]
     * Mapping for the second branch: {0→1, 1→2, 2→0, 3→3, 4→4}
     * Note that the initial four expressions were already present in the unified projection, including two occurrences of f2.
     * The third occurrence of f2 had to be added to the unified projection.
     * The residual predicate for the second branch involves field f0, which is already projected.
     * <p>
     * To rebase the residual predicates (and enforced predicates) on top of the unified projection,
     * we build a mapping from the recent unified operation fields to the new unified projection fields:
     * {0→4, 1→5, 2→0}
     * Note that field f2 is projected three times. For this mapping, we use the first occurrence.
     * Note that field f3 is not projected, because neither of the component projections projected it,
     * and it was not used by any of the residual predicates. If the enforced predicate of either branch
     * uses field f3, such predicate will be pruned to contain only the projected fields.
     */
    @Override
    public UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
            Operation unifiedOperation,
            List<TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            Map<Operation, Operation> usesMap,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations)
    {
        // the unified projection must include all project assignments from the component projections
        List<Block> unifiedAssignments = new ArrayList<>();

        // for each branch, the mapping to rebase the next downstream operation from the component Project operation onto the new unified Project operation
        List<FieldMapping> newBranchMappings = new ArrayList<>();

        // the mapping to rebase the context: predicate to apply and enforced predicate from the recent unified operation onto the new unified Project operation
        Map<Integer, Integer> predicateIdentities = new HashMap<>();

        // iterate over all branches to build the unified projection
        for (TraversalState branch : branches) {
            Project project = (Project) branch.nextOperation().operation();
            // rebase project assignments onto the unified operation
            Block rebased = rebaseBlock(project.assignments(), relationRowType(trinoType(unifiedOperation.result().type())), branch.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
            // break project assignments into individual projected expressions
            List<Block> currentAssignments = getProjectedItems(rebased, nameAllocator);

            // each assignment of the current projection must be mapped to a distinct assignment in the unified projection,
            // even if they are identical (they cannot be deduplicated). It's a result of FieldMapping being a reversible BiMap.
            // therefore, each index in the unified assignments list can be used only once
            Set<Integer> freeUnifiedIndexes = new LinkedHashSet<>();
            IntStream.range(0, unifiedAssignments.size())
                    .forEach(freeUnifiedIndexes::add);

            // the mapping to rebase the next downstream operation from the component Project operation onto the new unified Project operation
            Map<Integer, Integer> fieldMapping = new HashMap<>();

            for (int i = 0; i < currentAssignments.size(); i++) {
                Block currentAssignment = currentAssignments.get(i);
                boolean foundUnifiedAssignment = false;
                for (int j = 0; j < unifiedAssignments.size(); j++) {
                    if (freeUnifiedIndexes.contains(j) && blocksSemanticallyEquivalent(currentAssignment, unifiedAssignments.get(j))) {
                        fieldMapping.put(i, j);
                        freeUnifiedIndexes.remove(j);
                        foundUnifiedAssignment = true;
                        break;
                    }
                }
                if (!foundUnifiedAssignment) {
                    unifiedAssignments.add(currentAssignment);
                    fieldMapping.put(i, unifiedAssignments.size() - 1);
                }
            }
            FieldMapping newMapping = new FieldMapping(fieldMapping);
            newBranchMappings.add(newMapping);

            FieldMapping projectIdentities = getIdentityMappings(rebased);
            predicateIdentities.putAll(projectIdentities.composeWith(newMapping).fieldIndexMapping());

            // the predicate to apply must be fully supported on top of the unified Project operation. project any missing fields
            Block residualPredicate = branch.traversalContext().predicateToApply();
            Set<Integer> residualPredicateFields = extractReferencedFields(residualPredicate, getOnlyElement(residualPredicate.parameters()));
            Set<Integer> additionalFieldsToProject = Sets.difference(residualPredicateFields, predicateIdentities.keySet());
            for (int field : additionalFieldsToProject) {
                FieldReference fieldReference = new FieldReference(nameAllocator.newName(), getOnlyElement(rebased.parameters()), field, ImmutableMap.of());
                Return returnOperation = new Return(nameAllocator.newName(), fieldReference.result(), fieldReference.attributes());
                unifiedAssignments.add(new Block.Builder(rebased.name(), rebased.parameters())
                        .addOperation(fieldReference)
                        .addOperation(returnOperation)
                        .build());
                predicateIdentities.put(field, unifiedAssignments.size() - 1);
            }
        }

        Project mergedProject = new Project(
                nameAllocator.newName(),
                unifiedOperation.result(),
                unifiedAssignments.isEmpty() ?
                        getEmptyFieldSelector("^assignments", relationRowType(trinoType(unifiedOperation.result().type())), nameAllocator) :
                        composeProjectedItems(unifiedAssignments, nameAllocator).withLabel("^assignments"),
                unifiedOperation.attributes());
        newOperations.put(mergedProject.result(), mergedProject);

        Type mergedProjectRowType = relationRowType(trinoType(mergedProject.result().type()));
        Set<Integer> unifiedFields = IntStream.range(0, mergedProjectRowType.getTypeParameters().size()).boxed().collect(toImmutableSet());
        FieldMapping unifiedPredicateMapping = new FieldMapping(predicateIdentities);

        // rebase each branch onto the unified projection
        ImmutableList.Builder<TraversalState> newTraversalStates = ImmutableList.builder();
        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            Operation originalProject = branch.nextOperation().operation();
            TraversalContext traversalContext = branch.traversalContext();
            FieldMapping newMapping = newBranchMappings.get(i);
            Set<Integer> originalProjectFieldsMapped = IntStream.range(0, relationRowType(trinoType(originalProject.result().type())).getTypeParameters().size())
                    .boxed()
                    .map(newMapping::get)
                    .collect(toImmutableSet());
            Block rebasedPredicateToApply = rebaseBlock(traversalContext.predicateToApply(), mergedProjectRowType, unifiedPredicateMapping, nameAllocator).orElseThrow();
            Block rebasedEffectivePredicate = rebasePredicateAndPruneUnsupportedConjuncts(traversalContext.enforcedPredicate(), mergedProject, unifiedPredicateMapping, nameAllocator);
            TraversalContext rebasedContext = new TraversalContext(
                    newMapping,
                    Sets.difference(unifiedFields, originalProjectFieldsMapped),
                    rebasedPredicateToApply,
                    rebasedEffectivePredicate,
                    traversalContext.enforcedLimit());
            newTraversalStates.add(new TraversalState(
                    rebasedContext,
                    getNextOperation(originalProject, usesMap)));
        }

        return new UnifiedStatesAndCheckpointMapping(
                new UnifiedStates(mergedProject, newTraversalStates.build()),
                checkpoints,
                branchToCheckpoint);
    }
}
