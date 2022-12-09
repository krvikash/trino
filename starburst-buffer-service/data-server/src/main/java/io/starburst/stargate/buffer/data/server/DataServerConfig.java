/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigHidden;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DataServerConfig
{
    private boolean includeChecksumInDataResponse = true;
    private boolean testingDropUploadedPages;
    private int httpResponseThreads = 100;
    private boolean testingEnableStatsLogging = true; // true for now
    private Duration broadcastInterval = Duration.succinctDuration(5, SECONDS);
    private Duration testingDrainDelay = Duration.succinctDuration(0, SECONDS);
    private int drainingMaxAttempts = 4;

    public boolean getIncludeChecksumInDataResponse()
    {
        return includeChecksumInDataResponse;
    }

    @Config("include-checksum-in-data-response")
    public DataServerConfig setIncludeChecksumInDataResponse(boolean includeChecksumInDataResponse)
    {
        this.includeChecksumInDataResponse = includeChecksumInDataResponse;
        return this;
    }

    public boolean isTestingDropUploadedPages()
    {
        return testingDropUploadedPages;
    }

    @ConfigHidden
    @Config("testing.drop-uploaded-pages")
    public DataServerConfig setTestingDropUploadedPages(boolean testingDropUploadedPages)
    {
        this.testingDropUploadedPages = testingDropUploadedPages;
        return this;
    }

    @Min(1)
    public int getHttpResponseThreads()
    {
        return httpResponseThreads;
    }

    @Config("http-response-threads")
    public DataServerConfig setHttpResponseThreads(int httpResponseThreads)
    {
        this.httpResponseThreads = httpResponseThreads;
        return this;
    }

    public boolean isTestingEnableStatsLogging()
    {
        return testingEnableStatsLogging;
    }

    @ConfigHidden
    @Config("testing.enable-stats-logging")
    public DataServerConfig setTestingEnableStatsLogging(boolean testingEnableStatsLogging)
    {
        this.testingEnableStatsLogging = testingEnableStatsLogging;
        return this;
    }

    @NotNull
    public Duration getBroadcastInterval()
    {
        return broadcastInterval;
    }

    @Config("discovery-broadcast-interval")
    public DataServerConfig setBroadcastInterval(Duration broadcastInterval)
    {
        this.broadcastInterval = broadcastInterval;
        return this;
    }

    @NotNull
    @MaxDuration("5s")
    public Duration getTestingDrainDelay()
    {
        return testingDrainDelay;
    }

    @ConfigHidden
    @Config("testing.drain-delay")
    public DataServerConfig setTestingDrainDelay(Duration testingDrainDelay)
    {
        this.testingDrainDelay = testingDrainDelay;
        return this;
    }

    public int getDrainingMaxAttempts()
    {
        return drainingMaxAttempts;
    }

    @Config("draining.max-attempts")
    public DataServerConfig setDrainingMaxAttempts(int drainingMaxAttempts)
    {
        this.drainingMaxAttempts = drainingMaxAttempts;
        return this;
    }
}
