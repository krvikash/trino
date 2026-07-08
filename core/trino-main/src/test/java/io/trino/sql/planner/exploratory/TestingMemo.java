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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.GroupHandle;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationHandle;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A {@link Memo} built by {@link TestingMemoBuilder}, together with the metadata that maps the handles
 * declared while building back to the live Memo.
 * <p>
 * Resolution always follows merges: group handles resolve through {@link Memo#getRecentId(int)} and
 * operation handles through {@link Memo#getRecentVersion(MemoOperation)}, so a handle keeps pointing at
 * the right thing after a rule has been applied.
 */
public final class TestingMemo
{
    private final Memo memo;
    private final GroupHandle root;
    private final Map<String, MemoOperation> operationsByHandle;

    TestingMemo(Memo memo, GroupHandle root, Map<String, MemoOperation> operationsByHandle)
    {
        this.memo = requireNonNull(memo, "memo is null");
        this.root = requireNonNull(root, "root is null");
        this.operationsByHandle = ImmutableMap.copyOf(requireNonNull(operationsByHandle, "operationsByHandle is null"));
    }

    public Memo memo()
    {
        return memo;
    }

    public GroupHandle root()
    {
        return root;
    }

    /**
     * Current id of the group originally declared as {@code handle}, following any merges.
     */
    public int groupId(GroupHandle handle)
    {
        return memo.getRecentId(handle.reservedId());
    }

    public MemoGroup group(GroupHandle handle)
    {
        return memo.getGroup(groupId(handle));
    }

    /**
     * The most recent version of the operation originally added under {@code handle}.
     * The operation may have been removed (e.g. by a source-replacing rule); callers that need to
     * tolerate that should check {@link Memo#getGroupId(MemoOperation)}.
     */
    public MemoOperation operation(OperationHandle handle)
    {
        MemoOperation original = operationsByHandle.get(handle.name());
        checkArgument(original != null, "Unknown operation handle: %s", handle.name());
        return memo.getRecentVersion(original);
    }
}
