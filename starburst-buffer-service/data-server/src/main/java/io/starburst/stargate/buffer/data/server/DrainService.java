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

import io.starburst.stargate.buffer.discovery.client.BufferNodeState;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

/**
 * The service responsible for handling the DRAINING of the Server Node.
 * Upon finishing the draining process moves the server to DRAINED state.
 * Server in Drained state is ready for shutdown.
 */
public class DrainService
{
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, daemonThreadsNamed("data-server-drain-service"));
    private final BufferNodeStateManager statusManager;
    private final int drainDurationLimit;

    private volatile boolean draining;

    @Inject
    public DrainService(BufferNodeStateManager statusManager, DataServerConfig dataServerConfig)
    {
        this.statusManager = requireNonNull(statusManager, "statusManager is null");
        this.drainDurationLimit = dataServerConfig.getTestingDrainDurationLimit();
    }

    @PreDestroy
    public void stop()
    {
        scheduler.shutdownNow();
    }

    public synchronized void drain()
    {
        if (!draining) {
            startDraining();
        }
    }

    private void startDraining()
    {
        statusManager.transitionState(BufferNodeState.DRAINING);
        scheduler.schedule(this::drained, getDrainDelay(), TimeUnit.SECONDS);
        draining = true;
    }

    private void drained()
    {
        statusManager.transitionState(BufferNodeState.DRAINED);
        draining = false;
    }

    private int getDrainDelay()
    {
        // arbitrarily chosen range (0 -- drainDurationLimit s) for tests
        return drainDurationLimit <= 0 ? 0 : ThreadLocalRandom.current().nextInt(drainDurationLimit);
    }
}
