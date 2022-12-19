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
import io.airlift.units.Duration;

import static java.util.concurrent.TimeUnit.MINUTES;

public class TroubleshootingConfig
{
    private int maxActiveQueries = 32;
    private Duration maxAccessDuration = Duration.succinctDuration(15, MINUTES);
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

    public Duration getMaxAccessDuration()
    {
        return maxAccessDuration;
    }

    @Config("troubleshooting.max-access-duration")
    @ConfigDescription("Duration for which troubleshooting information will be available after query finishes")
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
