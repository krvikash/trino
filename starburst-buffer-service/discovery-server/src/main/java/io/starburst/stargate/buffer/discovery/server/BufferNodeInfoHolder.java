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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.starburst.stargate.buffer.BufferNodeInfo;

import static java.util.Objects.requireNonNull;

public class BufferNodeInfoHolder
{
    @GuardedBy("this")
    private long lastUpdateTime;
    @GuardedBy("this")
    private BufferNodeInfo lastNodeInfo;
    @GuardedBy("this")
    private boolean stale;

    public BufferNodeInfoHolder(BufferNodeInfo nodeInfo, long updateTime)
    {
        updateNodeInfo(nodeInfo, updateTime);
    }

    public synchronized long getLastUpdateTime()
    {
        return lastUpdateTime;
    }

    public synchronized BufferNodeInfo getLastNodeInfo()
    {
        return lastNodeInfo;
    }

    public synchronized void updateNodeInfo(BufferNodeInfo nodeInfo, long updateTime)
    {
        this.lastNodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.lastUpdateTime = updateTime;
        this.stale = false;
    }

    public synchronized void markStale()
    {
        this.stale = true;
    }

    public synchronized boolean isStale()
    {
        return stale;
    }
}
