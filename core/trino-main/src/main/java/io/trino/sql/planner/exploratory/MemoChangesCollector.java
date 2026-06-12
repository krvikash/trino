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

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MemoChangesCollector
{
    private final List<MemoChange> changes = new ArrayList<>();

    public void recordSingletonGroupCreate(MemoOperation operation, int groupId)
    {
        changes.add(new SingletonGroupCreate(operation, groupId));
    }

    public void recordGroupMerge(int survivingGroupId, int removedGroupId)
    {
        changes.add(new GroupMerge(survivingGroupId, removedGroupId));
    }

    public void recordGroupPrune(int groupId, List<MemoOperation> operations)
    {
        changes.add(new GroupPrune(groupId, operations));
    }

    public void recordGroupAttributesChange(int groupId)
    {
        changes.add(new GroupAttributesChange(groupId));
    }

    public void recordOperationInsert(MemoOperation operation, int groupId)
    {
        changes.add(new OperationInsert(operation, groupId));
    }

    public void recordOperationSuccession(MemoOperation oldOperation, MemoOperation newOperation, int groupId)
    {
        changes.add(new OperationSuccession(oldOperation, newOperation, groupId));
    }

    public void recordOperationAttributesChange(MemoOperation oldOperation, MemoOperation newOperation, int groupId)
    {
        changes.add(new OperationAttributesChange(oldOperation, newOperation, groupId));
    }

    public void recordOperationRemove(MemoOperation operation, int groupId)
    {
        changes.add(new OperationRemove(operation, groupId));
    }

    public List<MemoChange> getChanges()
    {
        return ImmutableList.copyOf(changes);
    }

    public sealed interface MemoChange
            permits GroupAttributesChange,
                    GroupMerge,
                    GroupPrune,
                    OperationAttributesChange,
                    OperationInsert,
                    OperationRemove,
                    OperationSuccession,
                    SingletonGroupCreate {}

    record SingletonGroupCreate(MemoOperation operation, int groupId)
            implements MemoChange
    {
        SingletonGroupCreate
        {
            requireNonNull(operation, "operation is null");
        }
    }

    record GroupMerge(int survivingGroupId, int removedGroupId)
            implements MemoChange {}

    record GroupPrune(int groupId, List<MemoOperation> operations)
            implements MemoChange
    {
        GroupPrune
        {
            operations = ImmutableList.copyOf(requireNonNull(operations, "operations is null"));
        }
    }

    record GroupAttributesChange(int groupId)
            implements MemoChange {}

    record OperationInsert(MemoOperation operation, int groupId)
            implements MemoChange
    {
        OperationInsert
        {
            requireNonNull(operation, "operation is null");
        }
    }

    record OperationSuccession(MemoOperation oldOperation, MemoOperation newOperation, int groupId)
            implements MemoChange
    {
        OperationSuccession
        {
            requireNonNull(oldOperation, "oldOperation is null");
            requireNonNull(newOperation, "newOperation is null");
        }
    }

    record OperationAttributesChange(MemoOperation oldOperation, MemoOperation newOperation, int groupId)
            implements MemoChange
    {
        OperationAttributesChange
        {
            requireNonNull(oldOperation, "oldOperation is null");
            requireNonNull(newOperation, "newOperation is null");
            checkArgument(oldOperation.equals(newOperation), "oldOperation and newOperation should be equal except for their attributes");
        }
    }

    record OperationRemove(MemoOperation operation, int groupId)
            implements MemoChange
    {
        OperationRemove
        {
            requireNonNull(operation, "operation is null");
        }
    }
}
