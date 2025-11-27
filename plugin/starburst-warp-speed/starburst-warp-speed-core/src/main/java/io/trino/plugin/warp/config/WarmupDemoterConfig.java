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
package io.trino.plugin.warp.config;

import io.airlift.configuration.Config;

import java.time.Duration;

public class WarmupDemoterConfig
{
    public static final String DEFAULT_RULE_TTL_IN_SECONDS = "warp-speed.warmup-demoter.default-rule-ttl-in-seconds";

    public static final int MAX_SUPPORTED_BATCH_SIZE = 100;

    private double maxUsageThresholdPercentage = 92;
    private double cleanupUsageThresholdPercentage = 90;
    private int batchSize = MAX_SUPPORTED_BATCH_SIZE;
    private int defaultRulePriority;
    private int defaultRuleTtlInSeconds = 1200;
    private int warmingPriorityAllowThreshold = 2;
    private long maxElementsToDemoteInIteration = 100;
    private double epsilon = 1;
    private Duration delayAcquireThread = Duration.ofMillis(100);
    private Duration maxDurationAcquireThread = Duration.ofSeconds(200);
    private int maxRetriesAcquireThread = 300;
    private int tasksExecutorQueueSize = 200_000;
    private boolean isEnableDemote = true;
    private boolean isForceDeleteDeadObjects;
    private boolean isForceDeleteFailedObjects;
    private boolean isResetHighestPriority;
    private boolean isDeleteEmptyRowGroups;

    public double getMaxUsageThresholdPercentage()
    {
        return maxUsageThresholdPercentage;
    }

    @Config("warp-speed.warmup-demoter.max-usage-threshold-percentage")
    public void setMaxUsageThresholdPercentage(double maxUsageThresholdPercentage)
    {
        this.maxUsageThresholdPercentage = maxUsageThresholdPercentage;
    }

    public double getCleanupUsageThresholdPercentage()
    {
        return cleanupUsageThresholdPercentage;
    }

    @Config("warp-speed.warmup-demoter.max-cleanup-threshold-percentage")
    public void setCleanupUsageThresholdPercentage(double cleanupUsageThresholdPercentage)
    {
        this.cleanupUsageThresholdPercentage = cleanupUsageThresholdPercentage;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    @Config("warp-speed.warmup-demoter.batch-size")
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public int getWarmingPriorityAllowThreshold()
    {
        return warmingPriorityAllowThreshold;
    }

    @Config("warp-speed.warmup-demoter.priority-allowed-threshold")
    public void setWarmingPriorityAllowThreshold(int warmingPriorityAllowThreshold)
    {
        this.warmingPriorityAllowThreshold = warmingPriorityAllowThreshold;
    }

    public int getDefaultRulePriority()
    {
        return defaultRulePriority;
    }

    @Config("warp-speed.warmup-demoter.default-rule-priority")
    public void setDefaultRulePriority(int defaultRulePriority)
    {
        this.defaultRulePriority = defaultRulePriority;
    }

    public int getDefaultRuleTtlInSeconds()
    {
        return defaultRuleTtlInSeconds;
    }

    @Config(DEFAULT_RULE_TTL_IN_SECONDS)
    public void setDefaultRuleTtlInSeconds(int defaultRuleTtlInSeconds)
    {
        this.defaultRuleTtlInSeconds = defaultRuleTtlInSeconds;
    }

    public long getMaxElementsToDemoteInIteration()
    {
        return maxElementsToDemoteInIteration;
    }

    @Config("warp-speed.warmup-demoter.max-elements-to-demote-in-iteration")
    public void setMaxElementsToDemoteInIteration(long maxElementsToDemoteInIteration)
    {
        this.maxElementsToDemoteInIteration = maxElementsToDemoteInIteration;
    }

    public double getEpsilon()
    {
        return epsilon;
    }

    @Config("warp-speed.warmup-demoter.epsilon")
    public void setEpsilon(double epsilon)
    {
        this.epsilon = epsilon;
    }

    public Duration getDelayAcquireThread()
    {
        return delayAcquireThread;
    }

    @Config("warp-speed.warmup-demoter.delay-duration-acquire-thread")
    public void setDelayAcquireThread(io.airlift.units.Duration delayAcquireThread)
    {
        this.delayAcquireThread = delayAcquireThread.toJavaTime();
    }

    public Duration getMaxDurationAcquireThread()
    {
        return maxDurationAcquireThread;
    }

    @Config("warp-speed.warmup-demoter.max-duration-acquire-thread")
    public void setMaxDurationAcquireThread(io.airlift.units.Duration maxDurationAcquireThread)
    {
        this.maxDurationAcquireThread = maxDurationAcquireThread.toJavaTime();
    }

    public int getMaxRetriesAcquireThread()
    {
        return maxRetriesAcquireThread;
    }

    @Config("warp-speed.warmup-demoter.max-retries-acquire-thread")
    public void setMaxRetriesAcquireThread(int maxRetriesAcquireThread)
    {
        this.maxRetriesAcquireThread = maxRetriesAcquireThread;
    }

    public int getTasksExecutorQueueSize()
    {
        return tasksExecutorQueueSize;
    }

    @Config("warp-speed.config.task.executor-queue-size")
    public void setTasksExecutorQueueSize(int tasksExecutorQueueSize)
    {
        this.tasksExecutorQueueSize = tasksExecutorQueueSize;
    }

    public boolean isEnableDemote()
    {
        return isEnableDemote;
    }

    @Config("warp-speed.enable.demote")
    public void setEnableDemote(boolean enableDemote)
    {
        this.isEnableDemote = enableDemote;
    }

    public boolean isForceDeleteDeadObjects()
    {
        return isForceDeleteDeadObjects;
    }

    public void setForceDeleteDeadObjects(boolean forceDeleteDeadObjects)
    {
        isForceDeleteDeadObjects = forceDeleteDeadObjects;
    }

    public boolean isForceDeleteFailedObjects()
    {
        return isForceDeleteFailedObjects;
    }

    public void setForceDeleteFailedObjects(boolean forceDeleteFailedObjects)
    {
        isForceDeleteFailedObjects = forceDeleteFailedObjects;
    }

    public boolean isResetHighestPriority()
    {
        return isResetHighestPriority;
    }

    public void setResetHighestPriority(boolean resetHighestPriority)
    {
        isResetHighestPriority = resetHighestPriority;
    }

    public boolean isDeleteEmptyRowGroups()
    {
        return isDeleteEmptyRowGroups;
    }

    public void setDeleteEmptyRowGroups(boolean deleteEmptyRowGroups)
    {
        isDeleteEmptyRowGroups = deleteEmptyRowGroups;
    }

    @Override
    public String toString()
    {
        return "WarmupDemoterConfig{" +
                "maxUsageThresholdPercentage=" + maxUsageThresholdPercentage +
                ", cleanupUsageThresholdPercentage=" + cleanupUsageThresholdPercentage +
                ", batchSize=" + batchSize +
                ", defaultRulePriority=" + defaultRulePriority +
                ", defaultRuleTtlInSeconds=" + defaultRuleTtlInSeconds +
                ", warmingPriorityAllowThreshold=" + warmingPriorityAllowThreshold +
                ", maxElementsToDemoteInIteration=" + maxElementsToDemoteInIteration +
                ", epsilon=" + epsilon +
                ", delayAcquireThread=" + delayAcquireThread +
                ", maxDurationAcquireThread=" + maxDurationAcquireThread +
                ", maxRetriesAcquireThread=" + maxRetriesAcquireThread +
                ", tasksExecutorQueueSize=" + tasksExecutorQueueSize +
                ", isEnableDemote=" + isEnableDemote +
                ", isForceDeleteDeadObjects=" + isForceDeleteDeadObjects +
                ", isForceDeleteFailedObjects=" + isForceDeleteFailedObjects +
                ", isResetHighestPriority=" + isResetHighestPriority +
                ", isDeleteEmptyRowGroups=" + isDeleteEmptyRowGroups +
                '}';
    }
}
