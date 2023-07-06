/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public final class DataApiFacadeStats
{
    private final AddDataPagesOperationStats addDataPagesOperationStats = new AddDataPagesOperationStats();

    @Managed
    @Nested
    public AddDataPagesOperationStats getAddDataPagesOperationStats()
    {
        return addDataPagesOperationStats;
    }

    public static final class AddDataPagesOperationStats
    {
        private final TimeStat successfulRequestTime = new TimeStat();
        private final TimeStat failedRequestTime = new TimeStat();
        private final CounterStat overloadedRequestErrorCount = new CounterStat();
        private final CounterStat circuitBreakerOpenRequestErrorCount = new CounterStat();
        private final CounterStat anyRequestErrorCount = new CounterStat();
        private final CounterStat requestRetryCount = new CounterStat();
        private final CounterStat successOperationCount = new CounterStat();
        private final CounterStat failedOperationCount = new CounterStat();

        @Managed
        @Nested
        public TimeStat getSuccessfulRequestTime()
        {
            return successfulRequestTime;
        }

        @Managed
        @Nested
        public TimeStat getFailedRequestTime()
        {
            return failedRequestTime;
        }

        @Managed
        @Nested
        public CounterStat getOverloadedRequestErrorCount()
        {
            return overloadedRequestErrorCount;
        }

        @Managed
        @Nested
        public CounterStat getCircuitBreakerOpenRequestErrorCount()
        {
            return circuitBreakerOpenRequestErrorCount;
        }

        @Managed
        @Nested
        public CounterStat getAnyRequestErrorCount()
        {
            return anyRequestErrorCount;
        }

        @Managed
        @Nested
        public CounterStat getRequestRetryCount()
        {
            return requestRetryCount;
        }

        @Managed
        @Nested
        public CounterStat getSuccessOperationCount()
        {
            return successOperationCount;
        }

        @Managed
        @Nested
        public CounterStat getFailedOperationCount()
        {
            return failedOperationCount;
        }
    }
}
