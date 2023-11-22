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

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class DiscoveryStats
{
    private volatile NodeStateStats nodeStateStats = new NodeStateStats();

    @Managed
    @Nested
    public NodeStateStats getNodeStateStats()
    {
        return nodeStateStats;
    }

    public void setNodeStateStats(NodeStateStats nodeStateStats)
    {
        this.nodeStateStats = nodeStateStats;
    }
}
