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
import io.trino.plugin.warp.tools.util.StopWatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService.FAILED_DEMOTE_SQUENCE;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;

public class DemoteContext
{
    private StopWatch stopWatch;
    private int demoterSequence = FAILED_DEMOTE_SQUENCE;
    private List<TupleRank> tupleRankList = new ArrayList<>();
    private double maxUsageThresholdPercentage = -1;
    private double cleanupUsageThresholdPercentage = -1;
    private int batchSize = 1;
    private WarmupDemoterStats statsWarmupDemoter;
    private long flowId = -1;
    private long maxElementsToDemote = 1000;
    private double epsilon = -1;
    private int numberOfCycles;
    private boolean deleteEmptyRowGroups;

    public DemoteContext(int demoterSequence,
                         double maxUsageThresholdPercentage,
                         double cleanupUsageThresholdPercentage,
                         int batchSize,
                         long maxElementsToDemote,
                         double epsilon,
                         boolean deleteEmptyRowGroups)
    {
        this.demoterSequence = demoterSequence;
        this.maxUsageThresholdPercentage = maxUsageThresholdPercentage;
        this.cleanupUsageThresholdPercentage = cleanupUsageThresholdPercentage;
        this.batchSize = batchSize;
        this.maxElementsToDemote = maxElementsToDemote;
        this.epsilon = epsilon;
        this.deleteEmptyRowGroups = deleteEmptyRowGroups;
        this.statsWarmupDemoter = WarmupDemoterStats.create(WARMUP_DEMOTER_STAT_GROUP);
        stopWatch = new StopWatch();
        stopWatch.start();
    }

    public StopWatch getStopWatch()
    {
        return stopWatch;
    }

    public int getDemoterSequence()
    {
        return demoterSequence;
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

    public long getFlowId()
    {
        return flowId;
    }

    public void setFlowId(long flowId)
    {
        this.flowId = flowId;
    }

    public long getMaxElementsToDemote()
    {
        return maxElementsToDemote;
    }

    public double getEpsilon()
    {
        return epsilon;
    }

    public int getNumberOfCycles()
    {
        return numberOfCycles;
    }

    public Set<RowGroupKey> getFailedRowGropDataSet()
    {
        return failedRowGropDataSet;
    }

    Set<RowGroupKey> failedRowGropDataSet = new HashSet<>();

    public double getLowestPriority()
    {
        return tupleRankList.isEmpty() ? 0 : tupleRankList.get(0).warmupProperties().priority();
    }

    public void increaseNumberOfCycles()
    {
        numberOfCycles++;
    }

    public void addFailedRowGropData(RowGroupKey failedRowGroup)
    {
        this.failedRowGropDataSet.add(failedRowGroup);
    }

    @Override
    public String toString()
    {
        String statsWarmupDemoterJson = null;
        try {
            statsWarmupDemoterJson = new ObjectMapper().writeValueAsString(statsWarmupDemoter);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return "DemoteContext{" +
                "stopWatch=" + stopWatch +
                ", demoterSequence=" + demoterSequence +
                ", tupleRankList=" + tupleRankList.size() +
                ", maxUsageThresholdPercentage=" + maxUsageThresholdPercentage +
                ", cleanupUsageThresholdPercentage=" + cleanupUsageThresholdPercentage +
                ", batchSize=" + batchSize +
                ", statsWarmupDemoter=" + statsWarmupDemoterJson +
                ", flowId=" + flowId +
                ", maxElementsToDemote=" + maxElementsToDemote +
                ", epsilon=" + epsilon +
                ", deleteEmptyRowGroups=" + deleteEmptyRowGroups +
                '}';
    }
}
