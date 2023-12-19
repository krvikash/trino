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

import io.starburst.stargate.buffer.BufferNodeState;
import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class NodeStateStats
{
    private final AtomicLong starting = new AtomicLong();
    private final AtomicLong started = new AtomicLong();
    private final AtomicLong active = new AtomicLong();
    private final AtomicLong draining = new AtomicLong();
    private final AtomicLong drained = new AtomicLong();

    @Managed
    public long getStarting()
    {
        return starting.get();
    }

    public void updateStarting(long starting)
    {
        this.starting.set(starting);
    }

    @Managed
    public long getStarted()
    {
        return started.get();
    }

    public void updateStarted(long started)
    {
        this.started.set(started);
    }

    @Managed
    public long getActive()
    {
        return active.get();
    }

    public void updateActive(long active)
    {
        this.active.set(active);
    }

    @Managed
    public long getDraining()
    {
        return draining.get();
    }

    public void updateDraining(long draining)
    {
        this.draining.set(draining);
    }

    @Managed
    public long getDrained()
    {
        return drained.get();
    }

    public void updateDrained(long drained)
    {
        this.drained.set(drained);
    }

    public void increment(BufferNodeState state)
    {
        switch (state) {
            case STARTING -> starting.incrementAndGet();
            case STARTED -> started.incrementAndGet();
            case ACTIVE -> active.incrementAndGet();
            case DRAINING -> draining.incrementAndGet();
            case DRAINED -> drained.incrementAndGet();
        }
    }
}
