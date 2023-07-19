/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig("troubleshooting.max-capture-duration")
public class TroubleshootingConfig
{
    private int maxActiveQueries = 128;
    private Duration maxAccessDuration = succinctDuration(5, MINUTES);

    private boolean anonymizedPlan = true;

    public int getMaxActiveQueries()
    {
        return maxActiveQueries;
    }

    @Config("troubleshooting.max-queries")
    @ConfigDescription("Maximum number of queries for which troubleshooting information will be collected in a single session")
    public TroubleshootingConfig setMaxActiveQueries(int maxActiveQueries)
    {
        this.maxActiveQueries = maxActiveQueries;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getMaxAccessDuration()
    {
        return maxAccessDuration;
    }

    @Config("troubleshooting.max-access-duration")
    @ConfigDescription("Time span for which troubleshooting information will be accessible after it's finished")
    public TroubleshootingConfig setMaxAccessDuration(Duration maxAccessDuration)
    {
        this.maxAccessDuration = maxAccessDuration;
        return this;
    }

    public boolean isAnonymizedPlan()
    {
        return anonymizedPlan;
    }

    @Config("troubleshooting.anonymize-query-plan")
    @ConfigDescription("Remove any sensitive data from the query plan")
    public TroubleshootingConfig setAnonymizedPlan(boolean anonymizedPlan)
    {
        this.anonymizedPlan = anonymizedPlan;
        return this;
    }
}
