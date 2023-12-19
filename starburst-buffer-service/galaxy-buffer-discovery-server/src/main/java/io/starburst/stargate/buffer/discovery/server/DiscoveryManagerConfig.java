/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DiscoveryManagerConfig
{
    @VisibleForTesting
    static final Duration DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD = succinctDuration(20, SECONDS);
    @VisibleForTesting
    static final Duration DEFAULT_START_GRACE_PERIOD = succinctDuration(20, SECONDS);

    private Duration bufferNodeDiscoveryStalenessThreshold = DEFAULT_BUFFER_NODE_DISCOVERY_STALENESS_THRESHOLD;
    private Duration startGracePeriod = DEFAULT_START_GRACE_PERIOD;

    public Duration getBufferNodeDiscoveryStalenessThreshold()
    {
        return bufferNodeDiscoveryStalenessThreshold;
    }

    @Config("buffer.discovery.buffer-node-info-staleness-threshold")
    @ConfigDescription("The threshold doesn't apply to drained nodes; drained nodes will be kept up to the lifetime of a query")
    public DiscoveryManagerConfig setBufferNodeDiscoveryStalenessThreshold(Duration bufferNodeDiscoveryStalenessThreshold)
    {
        this.bufferNodeDiscoveryStalenessThreshold = bufferNodeDiscoveryStalenessThreshold;
        return this;
    }

    public Duration getStartGracePeriod()
    {
        return startGracePeriod;
    }

    @Config("buffer.discovery.start-grace-period")
    public DiscoveryManagerConfig setStartGracePeriod(Duration startGracePeriod)
    {
        this.startGracePeriod = startGracePeriod;
        return this;
    }
}
