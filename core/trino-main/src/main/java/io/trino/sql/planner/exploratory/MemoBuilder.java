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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Operation.Result;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.ReuseUtils.BlockAndValue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.passIrLevelAttributes;
import static io.trino.sql.dialect.memo.MemoDialect.MEMO;
import static io.trino.sql.dialect.memo.MemoDialect.REUSE;
import static io.trino.sql.dialect.memo.MemoDialect.REUSE_ID;
import static io.trino.sql.planner.exploratory.MemoGroup.singletonGroup;
import static io.trino.sql.planner.exploratory.ReuseUtils.getReusedOperations;
import static java.util.Objects.requireNonNull;

class MemoBuilder
{
    private int rootGroup;
    private final Map<Integer, MemoGroup> groups = new LinkedHashMap<>();
    private final Map<MemoOperation, Integer> operationToGroup = new LinkedHashMap<>();
    private final Multimap<Integer, Integer> groupToParents = LinkedHashMultimap.create();

    private final DialectRegistry dialectRegistry;
    private final Memo.IdAllocator groupIdAllocator = new Memo.IdAllocator();
    private final Memo.IdAllocator reuseIdAllocator = new Memo.IdAllocator();

    MemoBuilder(DialectRegistry dialectRegistry)
    {
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
    }

    Memo build()
    {
        return new Memo(rootGroup, groups, operationToGroup, groupToParents, dialectRegistry, groupIdAllocator);
    }

    void insertProgramRecursively(Program program)
    {
        rootGroup = insertOperationRecursively(program.root(), ImmutableList.of(), new HashMap<>(), -1, new Memo.IdAllocator(), getReusedOperations(program));
    }

    /**
     * Insert the given operation and all its children recursively into the Memo, and return the group id of the inserted operation.
     *
     * @param operation the operation to insert
     * @param parameters the list of parameters in scope for the operation to insert. The operation can only reference parameters from this list, and the parameter index is used to create ParameterChild references.
     * @param insertedOperations mapping from already inserted operation results to their corresponding group ids. It is used to create GroupChild references.
     * @param blockId the id of the block where the operation is located, used to identify reused operations in the program. For the root operation of the program, blockId is set to -1 since it is not located in any block.
     *         Block ids for other operations are assigned following the traversal order of the program, consistent with getReusedOperations method.
     * @param blockIdAllocator the allocator for block ids
     * @param reusedOperations the set of operations that are reused in the program, identified by (block id, operation result) through getReusedOperations method.
     *         If the inserted operation is in this set, the Reuse operation will be created to capture the reuse of this operation in the program. All usages of the operation will point to the group of the Reuse operation
     *         instead of the original operation's group.
     */
    private int insertOperationRecursively(Operation operation, List<Parameter> parameters, Map<Result, Integer> insertedOperations, int blockId, Memo.IdAllocator blockIdAllocator, Set<BlockAndValue> reusedOperations)
    {
        checkArgument(!insertedOperations.containsKey(operation.result()), "Duplicate operation result in scope detected during insertion to Memo: %s", operation.result());

        ImmutableList.Builder<Child> children = ImmutableList.builder();

        for (Value argument : operation.arguments()) {
            switch (argument) {
                case Parameter parameter -> children.add(new ParameterChild(parameters.indexOf(parameter)));
                case Result result -> {
                    // argument is an operation result. the corresponding operation must have been inserted already
                    int childGroupId = insertedOperations.get(result);
                    children.add(new GroupChild(childGroupId, ParameterLineage.identity(parameters.size())));
                }
            }
        }
        int properParameterOffset = 0;
        for (Region region : operation.regions()) {
            // insert region recursively. clone the inserted operations map for each nested region to keep scope isolation
            int childGroupId = insertRegionRecursively(region, parameters, new HashMap<>(insertedOperations), blockIdAllocator, reusedOperations);
            children.add(new GroupChild(childGroupId, ParameterLineage.identityRecursive(parameters.size(), properParameterOffset, region.getOnlyBlock().parameters().size())));
            properParameterOffset += region.getOnlyBlock().parameters().size();
        }

        MemoOperation memoOperation = MemoOperation.create(
                operation.dialect(),
                operation.id(),
                operation.result().type(),
                parameters.stream().map(Parameter::type).collect(toImmutableList()),
                children.build(),
                operation.attributes(),
                dialectRegistry);

        Integer groupId = operationToGroup.get(memoOperation);
        if (groupId == null) {
            groupId = groupIdAllocator.newId();
            MemoGroup group = singletonGroup(memoOperation, dialectRegistry);
            groups.put(groupId, group);
            operationToGroup.put(memoOperation, groupId);
            for (int childGroupId : group.getChildGroups()) {
                groupToParents.put(childGroupId, groupId);
            }
        }
        else {
            MemoGroup group = groups.get(groupId);
            group.addOperation(memoOperation, dialectRegistry);
        }

        if (reusedOperations.contains(new BlockAndValue(blockId, operation.result()))) {
            MemoOperation reuseOperation = MemoOperation.create(
                    MEMO,
                    new OperationId(REUSE, ImmutableList.of(operation.result().type()), ImmutableList.of()),
                    operation.result().type(),
                    parameters.stream().map(Parameter::type).collect(toImmutableList()),
                    List.of(new GroupChild(groupId, ParameterLineage.identity(parameters.size()))),
                    Attributes.builder()
                            .putAll(passIrLevelAttributes(operation.attributes()))
                            .putUnchecked(new AttributeKey(MEMO, REUSE_ID), reuseIdAllocator.newId())
                            .buildOrThrow(),
                    dialectRegistry);
            int reuseGroupId = groupIdAllocator.newId();
            MemoGroup reuseGroup = singletonGroup(reuseOperation, dialectRegistry);
            groups.put(reuseGroupId, reuseGroup);
            operationToGroup.put(reuseOperation, reuseGroupId);
            groupToParents.put(groupId, reuseGroupId);
            insertedOperations.put(operation.result(), reuseGroupId);
            return reuseGroupId;
        }

        insertedOperations.put(operation.result(), groupId);
        return groupId;
    }

    private int insertRegionRecursively(Region region, List<Parameter> outerParameters, Map<Result, Integer> insertedOperations, Memo.IdAllocator blockIdAllocator, Set<BlockAndValue> reusedOperations)
    {
        int blockId = blockIdAllocator.newId();
        Block block = region.getOnlyBlock();
        List<Parameter> parameters = ImmutableList.<Parameter>builder()
                .addAll(outerParameters)
                .addAll(block.parameters())
                .build();
        Integer recentOperationGroupId = null;
        for (Operation operation : block.operations()) {
            recentOperationGroupId = insertOperationRecursively(operation, parameters, insertedOperations, blockId, blockIdAllocator, reusedOperations);
        }
        return recentOperationGroupId;
    }
}
