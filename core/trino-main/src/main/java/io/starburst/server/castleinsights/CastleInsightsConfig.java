/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.trino.plugin.base.configuration.ThreadCountParser;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CastleInsightsConfig
{
    private boolean enabled;
    private Duration minQueryDuration = succinctDuration(0, SECONDS);
    private int threadPoolSize = Runtime.getRuntime().availableProcessors();
    private int maxQueueSize = 1000;
    private int maxResultsPerMinute = 10;
    private int burstFactor = 5;
    private RateLimitStage rateLimitStage = RateLimitStage.AFTER_EXTRACTION;

    public enum RateLimitStage
    {
        /**
         * Cheaper: a query over the limit is never extracted.
         */
        BEFORE_EXTRACTION,
        /**
         * Spends the budget only on results that are actually sent, at the cost of extracting every query.
         */
        AFTER_EXTRACTION,
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("castle-insights.enabled")
    @ConfigDescription("Extract Castle insights, such as joins and metrics, from completed queries")
    public CastleInsightsConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public Duration getMinQueryDuration()
    {
        return minQueryDuration;
    }

    @Config("castle-insights.min-query-duration")
    @ConfigDescription("Skip queries that ran for less than this duration; every query is inspected by default")
    public CastleInsightsConfig setMinQueryDuration(Duration minQueryDuration)
    {
        this.minQueryDuration = minQueryDuration;
        return this;
    }

    @Min(1)
    public int getThreadPoolSize()
    {
        return threadPoolSize;
    }

    @Config("castle-insights.thread-pool-size")
    @ConfigDescription("Number of threads used to extract insights from completed queries; may be given as a per-core multiplier, e.g. 2C")
    public CastleInsightsConfig setThreadPoolSize(String threadPoolSize)
    {
        this.threadPoolSize = ThreadCountParser.DEFAULT.parse(threadPoolSize);
        return this;
    }

    @Min(1)
    public int getMaxQueueSize()
    {
        return maxQueueSize;
    }

    @Config("castle-insights.max-queue-size")
    @ConfigDescription("Maximum number of completed queries awaiting insight extraction; queries beyond it are dropped")
    public CastleInsightsConfig setMaxQueueSize(int maxQueueSize)
    {
        this.maxQueueSize = maxQueueSize;
        return this;
    }

    @Min(1)
    public int getMaxResultsPerMinute()
    {
        return maxResultsPerMinute;
    }

    @Config("castle-insights.max-results-per-minute")
    @ConfigDescription("Sustained number of completed-query insight results sent per minute; results beyond it are dropped")
    public CastleInsightsConfig setMaxResultsPerMinute(int maxResultsPerMinute)
    {
        this.maxResultsPerMinute = maxResultsPerMinute;
        return this;
    }

    @Min(1)
    public int getBurstFactor()
    {
        return burstFactor;
    }

    @Config("castle-insights.burst-factor")
    @ConfigDescription("Unused budget accumulates up to this many minutes' worth, so after a quiet period up to burst-factor times max-results-per-minute results can be sent at once")
    public CastleInsightsConfig setBurstFactor(int burstFactor)
    {
        this.burstFactor = burstFactor;
        return this;
    }

    @NotNull
    public RateLimitStage getRateLimitStage()
    {
        return rateLimitStage;
    }

    @Config("castle-insights.rate-limit-stage")
    @ConfigDescription("Whether the rate limit is applied before insight extraction, saving the work for dropped queries, or after it, spending the budget only on results that are sent")
    public CastleInsightsConfig setRateLimitStage(RateLimitStage rateLimitStage)
    {
        this.rateLimitStage = rateLimitStage;
        return this;
    }
}
