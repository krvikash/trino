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

    @Managed
    public long getStarted()
    {
        return started.get();
    }

    @Managed
    public long getActive()
    {
        return active.get();
    }

    @Managed
    public long getDraining()
    {
        return draining.get();
    }

    @Managed
    public long getDrained()
    {
        return drained.get();
    }

    public Updater update()
    {
        return new Updater();
    }

    public class Updater
    {
        private long starting;
        private long started;
        private long active;
        private long draining;
        private long drained;

        public void increment(BufferNodeState state)
        {
            switch (state) {
                case STARTING -> starting++;
                case STARTED -> started++;
                case ACTIVE -> active++;
                case DRAINING -> draining++;
                case DRAINED -> drained++;
            }
        }

        public void commit()
        {
            NodeStateStats.this.starting.set(starting);
            NodeStateStats.this.started.set(started);
            NodeStateStats.this.active.set(active);
            NodeStateStats.this.draining.set(draining);
            NodeStateStats.this.drained.set(drained);
        }
    }
}
