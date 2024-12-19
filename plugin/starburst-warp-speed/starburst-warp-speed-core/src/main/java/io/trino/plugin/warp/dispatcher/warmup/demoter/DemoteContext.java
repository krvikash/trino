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
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DemoteContext
{
    private final WarmupDemoterStats statsWarmupDemoter;
    private final boolean deleteEmptyRowGroups;

    private List<TupleRank> tupleRankList = new ArrayList<>();
    private double maxUsageThresholdPercentage = -1;
    private double cleanupUsageThresholdPercentage = -1;
    private int batchSize = 1;
    private long maxElementsToDemote = 1000;
    private double epsilon = -1;

    private final Set<RowGroupKey> failedRowGropDataSet;

    public DemoteContext(
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemote,
            double epsilon,
            boolean deleteEmptyRowGroups)
    {
        this.maxUsageThresholdPercentage = maxUsageThresholdPercentage;
        this.cleanupUsageThresholdPercentage = cleanupUsageThresholdPercentage;
        this.batchSize = batchSize;
        this.maxElementsToDemote = maxElementsToDemote;
        this.epsilon = epsilon;
        this.deleteEmptyRowGroups = deleteEmptyRowGroups;
        statsWarmupDemoter = WarmupDemoterStats.create();
        failedRowGropDataSet = new HashSet<>();
    }

    public List<TupleRank> getTupleRankList()
    {
        return tupleRankList;
    }

    public void setTupleRankList(List<TupleRank> tupleRankList)
    {
        this.tupleRankList = tupleRankList;
    }

    public double getCleanupUsageThresholdPercentage()
    {
        return cleanupUsageThresholdPercentage;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    public WarmupDemoterStats getStatsWarmupDemoter()
    {
        return statsWarmupDemoter;
    }

    public long getMaxElementsToDemote()
    {
        return maxElementsToDemote;
    }

    public Set<RowGroupKey> getFailedRowGropDataSet()
    {
        return failedRowGropDataSet;
    }

    public double getLowestPriority()
    {
        return tupleRankList.isEmpty() ? Double.MIN_VALUE : tupleRankList.getFirst().warmupProperties().priority();
    }

    public void addFailedRowGropData(RowGroupKey failedRowGroup)
    {
        this.failedRowGropDataSet.add(failedRowGroup);
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
                ", tupleRankList.size=" + tupleRankList.size() +
                ", maxUsageThresholdPercentage=" + maxUsageThresholdPercentage +
                ", cleanupUsageThresholdPercentage=" + cleanupUsageThresholdPercentage +
                ", batchSize=" + batchSize +
                ", statsWarmupDemoter=" + statsWarmupDemoterJson +
                ", maxElementsToDemote=" + maxElementsToDemote +
                ", epsilon=" + epsilon +
                ", deleteEmptyRowGroups=" + deleteEmptyRowGroups +
                '}';
    }
}
