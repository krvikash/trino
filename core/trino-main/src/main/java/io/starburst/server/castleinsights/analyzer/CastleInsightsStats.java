/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.analyzer;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class CastleInsightsStats
{
    private final CounterStat analyzedQueries = new CounterStat();
    private final CounterStat successfulAnalyzers = new CounterStat();
    private final CounterStat failedAnalyzers = new CounterStat();
    private final CounterStat emittedInsights = new CounterStat();
    private final CounterStat droppedQueries = new CounterStat();
    private final CounterStat rateLimitedQueries = new CounterStat();

    public void queryAnalyzed()
    {
        analyzedQueries.update(1);
    }

    public void analyzerSucceeded()
    {
        successfulAnalyzers.update(1);
    }

    public void analyzerFailed()
    {
        failedAnalyzers.update(1);
    }

    public void insightsEmitted(long count)
    {
        emittedInsights.update(count);
    }

    public void queryDropped()
    {
        droppedQueries.update(1);
    }

    public void queryRateLimited()
    {
        rateLimitedQueries.update(1);
    }

    @Managed
    @Nested
    public CounterStat getAnalyzedQueries()
    {
        return analyzedQueries;
    }

    @Managed
    @Nested
    public CounterStat getSuccessfulAnalyzers()
    {
        return successfulAnalyzers;
    }

    @Managed
    @Nested
    public CounterStat getFailedAnalyzers()
    {
        return failedAnalyzers;
    }

    @Managed
    @Nested
    public CounterStat getEmittedInsights()
    {
        return emittedInsights;
    }

    @Managed
    @Nested
    public CounterStat getDroppedQueries()
    {
        return droppedQueries;
    }

    @Managed
    @Nested
    public CounterStat getRateLimitedQueries()
    {
        return rateLimitedQueries;
    }
}
