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
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.Result;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.exploratory.MemoGroup.singletonGroup;
import static java.util.Objects.requireNonNull;

class MemoBuilder
{
    private int rootGroup;
    private final Map<Integer, MemoGroup> groups = new LinkedHashMap<>();
    private final Map<MemoOperation, Integer> operationToGroup = new LinkedHashMap<>();
    private final Multimap<Integer, Integer> groupToParents = LinkedHashMultimap.create();

    private final DialectRegistry dialectRegistry;
    private final Memo.IdAllocator groupIdAllocator;

    MemoBuilder(DialectRegistry dialectRegistry, Memo.IdAllocator groupIdAllocator)
    {
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
        this.groupIdAllocator = requireNonNull(groupIdAllocator, "groupIdAllocator is null");
    }

    Memo build()
    {
        return new Memo(rootGroup, groups, operationToGroup, groupToParents, dialectRegistry, groupIdAllocator);
    }

    void insertProgramRecursively(Program program)
    {
        rootGroup = insertOperationRecursively(program.root(), ImmutableList.of(), new HashMap<>());
    }

    private int insertOperationRecursively(Operation operation, List<Parameter> parameters, Map<Result, Integer> insertedOperations)
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
            int childGroupId = insertRegionRecursively(region, parameters, new HashMap<>(insertedOperations));
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

        insertedOperations.put(operation.result(), groupId);
        return groupId;
    }

    private int insertRegionRecursively(Region region, List<Parameter> outerParameters, Map<Result, Integer> insertedOperations)
    {
        Block block = region.getOnlyBlock();
        List<Parameter> parameters = ImmutableList.<Parameter>builder()
                .addAll(outerParameters)
                .addAll(block.parameters())
                .build();
        Integer recentOperationGroupId = null;
        for (Operation operation : block.operations()) {
            recentOperationGroupId = insertOperationRecursively(operation, parameters, insertedOperations);
        }
        return recentOperationGroupId;
    }
}
