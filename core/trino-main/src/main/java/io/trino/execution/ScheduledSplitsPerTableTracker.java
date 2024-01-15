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
package io.trino.execution;

import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class ScheduledSplitsPerTableTracker
{
    private final Map<SourceTableId, AtomicLong> totalScheduledSplitCount = new ConcurrentHashMap<>();

    public void prefill(List<SourceTableId> tables)
    {
        for (SourceTableId key : tables) {
            totalScheduledSplitCount.put(key, new AtomicLong(0L));
        }
    }

    public void recordScheduledSplitCount(QualifiedObjectName table, PlanNodeId planNodeId, long count)
    {
        SourceTableId key = new SourceTableId(planNodeId, table);
        requireNonNull(totalScheduledSplitCount.get(key), "totalScheduledSplitCount has no key %s".formatted(key)).addAndGet(count);
    }

    public Map<SourceTableId, AtomicLong> getTotalScheduledSplitCount()
    {
        return totalScheduledSplitCount;
    }

    public long getTotalScheduledSplitCount(PlanNodeId planNodeId, QualifiedObjectName tableName)
    {
        return requireNonNull(totalScheduledSplitCount.get(new SourceTableId(planNodeId, tableName))).get();
    }

    public record SourceTableId(PlanNodeId planNodeId, QualifiedObjectName tableName) {}
}
