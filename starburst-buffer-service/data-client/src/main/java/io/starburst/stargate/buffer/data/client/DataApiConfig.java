/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DataApiConfig
{
    private boolean dataIntegrityVerificationEnabled = true;
    private int dataClientMaxRetries = 5;
    private Duration dataClientRetryBackoffInitial = succinctDuration(1.0, SECONDS);
    private Duration dataClientRetryBackoffMax = succinctDuration(10.0, SECONDS);
    private double dataClientRetryBackoffFactor = 2.0;
    private double dataClientRetryBackoffJitter = 0.5;

    public int getDataClientMaxRetries()
    {
        return dataClientMaxRetries;
    }

    @Config("data-integrity-verification-enabled")
    public DataApiConfig setDataIntegrityVerificationEnabled(boolean dataIntegrityVerificationEnabled)
    {
        this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        return this;
    }

    public boolean isDataIntegrityVerificationEnabled()
    {
        return dataIntegrityVerificationEnabled;
    }

    @Config("max-retries")
    public DataApiConfig setDataClientMaxRetries(int dataClientMaxRetries)
    {
        this.dataClientMaxRetries = dataClientMaxRetries;
        return this;
    }

    public Duration getDataClientRetryBackoffInitial()
    {
        return dataClientRetryBackoffInitial;
    }

    @Config("retry-backoff-initial")
    public DataApiConfig setDataClientRetryBackoffInitial(Duration dataClientRetryBackoffInitial)
    {
        this.dataClientRetryBackoffInitial = dataClientRetryBackoffInitial;
        return this;
    }

    public Duration getDataClientRetryBackoffMax()
    {
        return dataClientRetryBackoffMax;
    }

    @Config("retry-backoff-max")
    public DataApiConfig setDataClientRetryBackoffMax(Duration dataClientRetryBackoffMax)
    {
        this.dataClientRetryBackoffMax = dataClientRetryBackoffMax;
        return this;
    }

    public double getDataClientRetryBackoffFactor()
    {
        return dataClientRetryBackoffFactor;
    }

    @Config("retry-backoff-factor")
    public DataApiConfig setDataClientRetryBackoffFactor(double dataClientRetryBackoffFactor)
    {
        this.dataClientRetryBackoffFactor = dataClientRetryBackoffFactor;
        return this;
    }

    public double getDataClientRetryBackoffJitter()
    {
        return dataClientRetryBackoffJitter;
    }

    @Config("retry-backoff-jitter")
    public DataApiConfig setDataClientRetryBackoffJitter(double dataClientRetryBackoffJitter)
    {
        this.dataClientRetryBackoffJitter = dataClientRetryBackoffJitter;
        return this;
    }
}
