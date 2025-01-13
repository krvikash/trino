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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AtomicDouble;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;

import java.util.HashSet;
import java.util.Set;

public record DemoteContext(
        double maxUsageThresholdPercentage,
        double cleanupUsageThresholdPercentage,
        int batchSize,
        long maxElementsToDemote,
        double epsilon,
        boolean isDeleteEmptyRowGroups,
        boolean isForceDeleteFailedObjects,
        boolean isResetHighestPriority,
        TupleRankResult tupleRankResult,
        WarmupDemoterStats statsWarmupDemoter,
        Set<RowGroupKey> failedRowGropDataSet,
        AtomicDouble highestPriorityDemoted)
{
    public DemoteContext(
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemote,
            double epsilon,
            boolean deleteEmptyRowGroups,
            boolean forceDeleteFailedObjects,
            boolean resetHighestPriority,
            TupleRankResult tupleRankResult)
    {
        this(maxUsageThresholdPercentage,
                cleanupUsageThresholdPercentage,
                batchSize,
                maxElementsToDemote,
                epsilon,
                deleteEmptyRowGroups,
                forceDeleteFailedObjects,
                resetHighestPriority,
                tupleRankResult,
                WarmupDemoterStats.create(),
                new HashSet<>(),
                new AtomicDouble());
    }

    @Override
    public String toString()
    {
        String statsWarmupDemoterJson;
        try {
            statsWarmupDemoterJson = new ObjectMapper().writeValueAsString(statsWarmupDemoter);
        }
        catch (JsonProcessingException e) {
            statsWarmupDemoterJson = "{}";
        }
        return "DemoteContext{" +
                "maxUsageThresholdPercentage=" + maxUsageThresholdPercentage +
                ", cleanupUsageThresholdPercentage=" + cleanupUsageThresholdPercentage +
                ", batchSize=" + batchSize +
                ", maxElementsToDemote=" + maxElementsToDemote +
                ", epsilon=" + epsilon +
                ", isDeleteEmptyRowGroups=" + isDeleteEmptyRowGroups +
                ", isForceDeleteFailedObjects=" + isForceDeleteFailedObjects +
                ", isResetHighestPriority=" + isResetHighestPriority +
                ", tupleRankList=" + (tupleRankResult != null ? tupleRankResult.toShortString() : null) +
                ", statsWarmupDemoter=" + statsWarmupDemoterJson +
                ", failedRowGropDataSet=" + failedRowGropDataSet +
                ", highestPriorityDemoted=" + highestPriorityDemoted +
                '}';
    }
}
