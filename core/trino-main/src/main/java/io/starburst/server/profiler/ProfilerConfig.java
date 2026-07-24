/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ProfilerConfig
{
    private boolean isProfilerEnabled;
    private Duration minQueryDuration = succinctDuration(1, SECONDS);
    private int threadPoolSize = 2;
    private int maxQueueSize = 1000;

    public boolean isProfilerEnabled()
    {
        return isProfilerEnabled;
    }

    @ConfigHidden
    @Config("profiler.enabled")
    @ConfigDescription("Enable per-query profiler")
    public ProfilerConfig setProfilerEnabled(boolean isProfilerEnabled)
    {
        this.isProfilerEnabled = isProfilerEnabled;
        return this;
    }

    public Duration getMinQueryDuration()
    {
        return minQueryDuration;
    }

    @Config("profiler.min-query-duration")
    @ConfigDescription("Minimal duration of the query that we can analyze")
    public ProfilerConfig setMinQueryDuration(Duration minQueryDuration)
    {
        this.minQueryDuration = minQueryDuration;
        return this;
    }

    @Min(1)
    public int getThreadPoolSize()
    {
        return threadPoolSize;
    }

    @Config("profiler.thread-pool-size")
    @ConfigDescription("Number of threads used to process completed queries for profiler")
    public ProfilerConfig setThreadPoolSize(int threadPoolSize)
    {
        this.threadPoolSize = threadPoolSize;
        return this;
    }

    @Min(1)
    public int getMaxQueueSize()
    {
        return maxQueueSize;
    }

    @Config("profiler.max-queue-size")
    @ConfigDescription("Maximum number of completed queries awaiting profiler; matches query.max-queued-queries by default")
    public ProfilerConfig setMaxQueueSize(int maxQueueSize)
    {
        this.maxQueueSize = maxQueueSize;
        return this;
    }
}
