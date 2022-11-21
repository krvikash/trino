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

import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class DataServerStats
{
    private final DistributionStat totalMemoryInBytes = new DistributionStat();
    private final DistributionStat freeMemoryInBytes = new DistributionStat();
    private final DistributionStat trackedExchanges = new DistributionStat();
    private final DistributionStat openChunks = new DistributionStat();
    private final DistributionStat closedChunks = new DistributionStat();
    private final DistributionStat spooledChunks = new DistributionStat();

    @Managed
    @Nested
    public DistributionStat getTotalMemoryInBytes()
    {
        return totalMemoryInBytes;
    }

    @Managed
    @Nested
    public DistributionStat getFreeMemoryInBytes()
    {
        return freeMemoryInBytes;
    }

    @Managed
    @Nested
    public DistributionStat getTrackedExchanges()
    {
        return trackedExchanges;
    }

    @Managed
    @Nested
    public DistributionStat getOpenChunks()
    {
        return openChunks;
    }

    @Managed
    @Nested
    public DistributionStat getClosedChunks()
    {
        return closedChunks;
    }

    @Managed
    @Nested
    public DistributionStat getSpooledChunks()
    {
        return spooledChunks;
    }
}
