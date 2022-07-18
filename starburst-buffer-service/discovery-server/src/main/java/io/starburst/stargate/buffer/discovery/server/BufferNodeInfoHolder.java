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

import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;

import javax.annotation.concurrent.GuardedBy;

import static java.util.Objects.requireNonNull;

public class BufferNodeInfoHolder
{
    @GuardedBy("this")
    private long lastUpdateTime;
    @GuardedBy("this")
    private BufferNodeInfo lastNodeInfo;

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
    }
}
