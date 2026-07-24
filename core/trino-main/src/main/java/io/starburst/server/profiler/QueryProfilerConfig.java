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
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;

public class QueryProfilerConfig
{
    private double topOperatorsPercentage = 0.8;
    private int maxTopOperators = 10;
    private double topStagesPercentage = 0.8;
    private int maxTopStages = 10;

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getTopOperatorsPercentage()
    {
        return topOperatorsPercentage;
    }

    @Config("query.profiler.top-operators-percentage")
    @ConfigDescription("Cumulative fraction of query CPU time used to select the top operators reported in the analysis")
    public QueryProfilerConfig setTopOperatorsPercentage(double topOperatorsPercentage)
    {
        this.topOperatorsPercentage = topOperatorsPercentage;
        return this;
    }

    @Min(1)
    public int getMaxTopOperators()
    {
        return maxTopOperators;
    }

    @Config("query.profiler.max-top-operators")
    @ConfigDescription("Maximum number of top operators reported in the analysis")
    public QueryProfilerConfig setMaxTopOperators(int maxTopOperators)
    {
        this.maxTopOperators = maxTopOperators;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getTopStagesPercentage()
    {
        return topStagesPercentage;
    }

    @Config("query.profiler.top-stages-percentage")
    @ConfigDescription("Cumulative fraction of query resources used to select the top stages reported in the analysis")
    public QueryProfilerConfig setTopStagesPercentage(double topStagesPercentage)
    {
        this.topStagesPercentage = topStagesPercentage;
        return this;
    }

    @Min(1)
    public int getMaxTopStages()
    {
        return maxTopStages;
    }

    @Config("query.profiler.max-top-stages")
    @ConfigDescription("Maximum number of top stages reported in the analysis")
    public QueryProfilerConfig setMaxTopStages(int maxTopStages)
    {
        this.maxTopStages = maxTopStages;
        return this;
    }
}
