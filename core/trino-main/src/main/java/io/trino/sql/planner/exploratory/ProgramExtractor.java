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
package io.trino.sql.planner.exploratory;

import io.trino.spi.TrinoException;
import io.trino.sql.dialect.trino.ProgramBuilder.ValueNameAllocator;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.exploratory.GroupOptimizationState.StateKey;
import io.trino.sql.planner.exploratory.Memo.IdAllocator;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.transform;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.XO_ERROR;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

/**
 * Extracts the optimized Program from Memo and OptimizationStore.
 * <p>
 * It assumes that the optimization and costing phase is completed,
 * and for each reachable group and incoming requirement, there is at least
 * one winner candidate. The extraction starts from the root group
 * and recursively extracts operations according to the winning expressions.
 * <p>
 * The Memo structure can involve cycles, but Program extraction must produce
 * a finite acyclic program. When cycle handling is complete, extraction should
 * backtrack and pick another cost-tied candidate. It assumes that there always
 * exists a finite acyclic solution with minimal cost.
 * <p>
 * The Program extraction can be performed for implemented physical plans
 * as well as for logical plans, depending on the state reached by the preceding
 * optimization process.
 * <p>
 * Operation reuse in Memo is explicit. If a group result should be computed once and reused,
 * a special REUSE group is created that points to the original group.
 * Without the REUSE marker, the group is extracted each time it is referenced.
 */
public class ProgramExtractor<C extends Cost, R extends Requirement, P extends Property>
{
    // TODO The current reuse extraction is known to be conceptually wrong and should be remodeled, likely using an LCA based approach.
    // TODO Clarify how Memo-to-Program extraction treats Exchange and Sort: they are normal Program operations, but only some can be valid Enforcers.

    private final Memo memo; // TODO do we need Memo, or just OptimizationStore and root group id?
    private final R rootRequirement;
    private final OptimizationStore<C, R, P> optimizationStore;
    private final OptimizationBudget budget;
    // Intentionally retained across extractions so generated value names share one namespace.
    private final ValueNameAllocator nameAllocator = new ValueNameAllocator();

    ProgramExtractor(Memo memo, R rootRequirement, OptimizationStore<C, R, P> optimizationStore, OptimizationBudget budget)
    {
        this.memo = requireNonNull(memo, "memo is null");
        this.rootRequirement = requireNonNull(rootRequirement, "rootRequirement is null");
        this.optimizationStore = requireNonNull(optimizationStore, "optimizationStore is null");
        this.budget = requireNonNull(budget, "budget is null");
    }

    public Program extractProgram()
    {
        IdAllocator blockIdAllocator = new IdAllocator();
        Block.Builder rootBuilder = new Block.Builder(Optional.empty(), List.of());
        extractOperation(
                memo.rootGroup(),
                rootRequirement,
                rootBuilder,
                List.of(),
                -1,
                blockIdAllocator,
                new HashMap<>(),
                new HashSet<>());
        List<Operation> rootOperations = rootBuilder.build().operations();
        checkState(
                rootOperations.size() == 1,
                "Root extraction produced %s operations",
                rootOperations.size());
        return new Program(rootOperations.getFirst());
    }

    /**
     * Extracts the winning operation for the given group and requirement, and adds it to the given block builder.
     *
     * @param groupId the group to extract.
     * @param requirement the requirement to extract for. It is used to find the winning operation in the group.
     * @param blockBuilder the block builder to add the extracted operation to.
     * @param parameters parameters available in the block.
     * @param blockId the id of the block being extracted, used for validating reuse of operations within the same block.
     *         For the root operation of the program, blockId is set to -1 since it is not located in any block.
     *         Block ids for other operations are assigned following the traversal order of the program.
     *         They are used to track declarations of reused operations and validate that they are reused only within the same block.
     * @param blockIdAllocator allocator for block ids within this extraction.
     * @param reusedGroupImplementations a map to track extracted operations for reused groups.
     * @param activeStates states on the current extraction path.
     */
    private Operation extractOperation(
            int groupId,
            R requirement,
            Block.Builder blockBuilder,
            List<Parameter> parameters,
            int blockId,
            IdAllocator blockIdAllocator,
            Map<Integer, ReusedGroupImplementation> reusedGroupImplementations,
            Set<StateKey<R>> activeStates)
    {
        budget.consumeExtractionAttempt();
        boolean isReuseGroup = memo.isReuse(groupId);
        if (isReuseGroup && reusedGroupImplementations.containsKey(groupId)) {
            // the reused subprogram is already extracted, and we should reuse its result.
            // for now, we require that the reused subprogram is in the same Block so that its result is visible.
            ReusedGroupImplementation reusedGroupImplementation = reusedGroupImplementations.get(groupId);
            // TODO Put the reused subprogram in a common ancestor block of all users.
            if (reusedGroupImplementation.blockId != blockId) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot reuse code across different blocks");
            }
            return reusedGroupImplementation.operation();
        }

        int originalGroupId = groupId;
        if (isReuseGroup) {
            // the reused subprogram is not yet extracted. Extract the underlying group.
            // Note: we will apply the requirement from the FIRST ENCOUNTERED reference to the reused group. It must satisfy the requirement of all other references.
            // TODO implement it so that the applied requirement is universally good for all uses, or each reference gets an EnforcerPipeline for its requirement.
            groupId = memo.reusedGroup(groupId);
        }
        GroupOptimizationState<C, R, P> state = optimizationStore.getState(groupId, requirement);
        // TODO make better accessors for candidates (first, next...)
        List<Candidate<C, R, P>> winners = state.winners().inIncreasingCostOrder();
        // TODO When extraction backtracking is implemented, select the first finite winner and fail with XO_ERROR if none exists.
        if (winners.isEmpty()) {
            throw new TrinoException(XO_ERROR, "No winner for state %s".formatted(state.key()));
        }
        // TODO for now we only take the first candidate. We can't yet backtrack to resolve cycles
        checkState(activeStates.add(state.key()), "Cyclic winner for state %s", state.key());
        Candidate<C, R, P> candidate = winners.getFirst();
        MemoOperation memoOperation = candidate.implementation().operation();
        List<R> childRequirements = candidate.implementation().childRequirements();
        Attributes groupAttributes = memo.getGroup(groupId).attributes();
        EnforcerPipeline<C, R, P> enforcerPipeline = candidate.enforcerPipeline();

        checkState(memoOperation.groupParameterTypes().equals(transform(parameters, Parameter::type)), "Mismatched parameters for extracted operation");

        int argumentCount = memoOperation.operationId().argumentTypes().size();
        int regionCount = memoOperation.operationId().regionTypes().size();

        // Extract operation arguments.
        // They can be either parameters passed from an enclosing operation, or results of child operations.
        // Child operations are laid out in the same block as the parent operation, before the parent operation.
        List<Value> arguments = new ArrayList<>();
        for (int i = 0; i < argumentCount; i++) {
            switch (memoOperation.children().get(i)) {
                case ParameterChild(int groupParameterIndex) -> arguments.add(parameters.get(groupParameterIndex));
                case GroupChild(int childGroupId, ParameterLineage(List<Integer> passedGroupParameters, List<Integer> passedProperParameters)) -> {
                    checkState(passedProperParameters.isEmpty(), "Proper parameters cannot be passed to a non-region child");
                    List<Parameter> childParameters = passedGroupParameters.stream()
                            .map(parameters::get)
                            .toList();
                    Operation childOperation = extractOperation(childGroupId, childRequirements.get(i), blockBuilder, childParameters, blockId, blockIdAllocator, reusedGroupImplementations, activeStates);
                    arguments.add(childOperation.result());
                }
            }
        }

        // Allocate operation name here so that it has a lower name than the names used in nested regions. Just a matter of esthetics.
        String operationName = nameAllocator.newName();

        // Extract nested regions.
        // They are not laid out in the same block as the parent operation, but in their own blocks, and included in the parent operation.
        List<Region> regions = new ArrayList<>();
        int properParameterOffset = 0;
        for (int i = argumentCount; i < argumentCount + regionCount; i++) {
            // Allocate proper parameters for the region according to the region type in the operation id.
            List<Parameter> properParameters = memoOperation.operationId().getRegionType(i - argumentCount).argumentTypes().stream()
                    .map(type -> new Parameter(nameAllocator.newName(), type))
                    .toList();
            switch (memoOperation.children().get(i)) {
                case ParameterChild(int groupParameterIndex) -> {
                    Block.Builder childBlockBuilder = new Block.Builder(Optional.empty(), properParameters);
                    addOperation(childBlockBuilder, new Return(nameAllocator.newName(), parameters.get(groupParameterIndex), DEFAULT_BLOCK_PARAMETER_ATTRIBUTES));
                    regions.add(singleBlockRegion(childBlockBuilder.build()));
                }
                case GroupChild(int childGroupId, ParameterLineage(List<Integer> passedGroupParameters, List<Integer> passedProperParameters)) -> {
                    // Extract parameters passed to the child group: both group parameters and proper parameters, according to the ParameterLineage.
                    List<Parameter> childParameters = new ArrayList<>();
                    passedGroupParameters.stream()
                            .map(parameters::get)
                            .forEach(childParameters::add);
                    for (int passedProperParameter : passedProperParameters) {
                        int properParameterIndex = passedProperParameter - properParameterOffset;
                        childParameters.add(properParameters.get(properParameterIndex));
                    }

                    // Passed proper parameters might have been pruned. The created Block must get unpruned parameters
                    // since a certain signature is required to resolve and build the Operation.
                    Block.Builder childBlockBuilder = new Block.Builder(Optional.empty(), properParameters);
                    int nestedBlockId = blockIdAllocator.newId();
                    extractOperation(childGroupId, childRequirements.get(i), childBlockBuilder, childParameters, nestedBlockId, blockIdAllocator, reusedGroupImplementations, activeStates);
                    regions.add(singleBlockRegion(childBlockBuilder.build()));
                }
            }
            properParameterOffset += properParameters.size();
        }

        Operation operation = memoOperation.toOperation(operationName, arguments, regions, groupAttributes, memo.dialectRegistry());
        addOperation(blockBuilder, operation);
        operation = applyEnforcers(enforcerPipeline, operation, blockBuilder);

        if (isReuseGroup) {
            // record the extracted operation for reuse
            reusedGroupImplementations.put(originalGroupId, new ReusedGroupImplementation(blockId, operation));
        }

        activeStates.remove(state.key());
        return operation;
    }

    private Operation applyEnforcers(EnforcerPipeline<C, R, P> enforcerPipeline, Operation input, Block.Builder blockBuilder)
    {
        Operation current = input;
        for (Enforcer step : enforcerPipeline.steps()) {
            Operation enforcer = step.operation();
            checkState(enforcer.arguments().size() == 1, "Enforcer operation must have exactly one argument");

            // TODO we could use the Enforcer operation as-is or recreate it through createOperation()
            //  but must remap Values in nested regions, remap enforcer's result and swap the enforcer's argument to match the previous operation in the pipeline
            current = memo.dialectRegistry().dialect(enforcer.dialect()).createOperation(
                    enforcer.name(),
                    nameAllocator.newName(),
                    List.of(current.result()),
                    enforcer.regions(), // TODO need to reallocate Values to avoid name clashes
                    enforcer.attributes());
            addOperation(blockBuilder, current);
        }
        return current;
    }

    private void addOperation(Block.Builder blockBuilder, Operation operation)
    {
        budget.consumeOutputOperation();
        blockBuilder.addOperation(operation);
    }

    private record ReusedGroupImplementation(int blockId, Operation operation)
    {
        public ReusedGroupImplementation
        {
            requireNonNull(operation, "operation is null");
        }
    }
}
