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
    private double maxUsageThresholdPercentage = 90;
    private double cleanupUsageThresholdPercentage = 85;
    private int batchSize = 100;
    private int defaultRulePriority;
    private int warmingPriorityAllowThreshold = 2;
    private long maxElementsToDemoteInIteration = 100;
    private double epsilon = 1;
    private Duration delayAcquireThread = Duration.ofMillis(100);
    private Duration maxDurationAcquireThread = Duration.ofSeconds(200);
    private int maxRetriesAcquireThread = 300;
    private int tasksExecutorQueueSize = 2_000_000;
    private boolean enableDemote = true;

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
        return enableDemote;
    }

    @Config("warp-speed.enable.demote")
    public void setEnableDemote(boolean enableDemote)
    {
        this.enableDemote = enableDemote;
    }

    @Override
    public String toString()
    {
        return "WarmupDemoterConfig{" +
                "maxUsageThresholdPercentage=" + maxUsageThresholdPercentage +
                ", cleanupUsageThresholdPercentage=" + cleanupUsageThresholdPercentage +
                ", batchSize=" + batchSize +
                ", defaultRulePriority=" + defaultRulePriority +
                ", warmupHysteresis=" + warmingPriorityAllowThreshold +
                ", maxElementsToDemoteInIteration=" + maxElementsToDemoteInIteration +
                ", epsilon=" + epsilon +
                ", tasksExecutorQueueSize=" + tasksExecutorQueueSize +
                ", enableDemote=" + enableDemote +
                '}';
    }
}
