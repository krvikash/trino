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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * GroupByHash that can switch `BigintGroupByHashBatched` to `BigintGroupByHash` if
 * the `BigintGroupByHashBatched` cannot grow anymore but `BigintGroupByHash` can.
 * `BigintGroupByHashBatched` can only store a bit more than 10^9 elements vs. 1.6 * 10^9 for `BigintGroupByHash`.
 */
public class SwitchingGroupByHash
        implements GroupByHash
{
    private GroupByHash current;

    public SwitchingGroupByHash(GroupByHash groupByHash)
    {
        this.current = requireNonNull(groupByHash, "groupByHash is null");
    }

    @Override
    public Work<?> addPage(Page page)
    {
        if (current instanceof BigintGroupByHashBatched batched && batched.shouldFallback(page.getPositionCount())) {
            return new Work<>()
            {
                private Work<?> actualWork;

                @Override
                public boolean process()
                {
                    if (actualWork != null) {
                        return actualWork.process();
                    }
                    Optional<GroupByHash> newGroupByHash = batched.fallbackToBigintGroupByHash();
                    if (newGroupByHash.isEmpty()) {
                        // memory for switch not available
                        return false;
                    }
                    current = newGroupByHash.get();
                    actualWork = current.addPage(page);
                    return actualWork.process();
                }

                @Override
                public Object getResult()
                {
                    return actualWork.getResult();
                }
            };
        }
        return current.addPage(page);
    }

    // Methods below are simple delegation to current
    @Override
    public long getEstimatedSize()
    {
        return current.getEstimatedSize();
    }

    @Override
    public int getGroupCount()
    {
        return current.getGroupCount();
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        current.appendValuesTo(groupId, pageBuilder);
    }

    @Override
    public void startReleasingOutput()
    {
        current.startReleasingOutput();
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        return current.getGroupIds(page);
    }

    @Override
    public long getRawHash(int groupId)
    {
        return current.getRawHash(groupId);
    }

    @Override
    public int getCapacity()
    {
        return current.getCapacity();
    }

    @Override
    public GroupByHash copy()
    {
        return current.copy();
    }
}
