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

import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeState;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The service responsible for handling the DRAINING of the Server Node.
 * Upon finishing the draining process moves the server to DRAINED state.
 * Server in Drained state is ready for shutdown.
 */
public class DrainService
{
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("data-server-drain-service"));
    private final BufferNodeStateManager statusManager;
    private final Duration drainDurationLimit;

    @Inject
    public DrainService(BufferNodeStateManager statusManager, DataServerConfig dataServerConfig)
    {
        this.statusManager = requireNonNull(statusManager, "statusManager is null");
        this.drainDurationLimit = requireNonNull(dataServerConfig.getTestingDrainDurationLimit(), "drainDuration is null");
    }

    @PreDestroy
    public void stop()
    {
        scheduler.shutdownNow();
    }

    public synchronized void drain()
    {
        if (!BufferNodeState.DRAINING.equals(statusManager.getState())) {
            startDraining();
        }
    }

    private void startDraining()
    {
        statusManager.transitionState(BufferNodeState.DRAINING);
        scheduler.schedule(this::drained, getDrainDelay(), SECONDS);
    }

    private void drained()
    {
        statusManager.transitionState(BufferNodeState.DRAINED);
    }

    private long getDrainDelay()
    {
        // arbitrarily chosen range (0 -- drainDurationLimit s) for tests
        long limitSeconds = drainDurationLimit.roundTo(SECONDS);
        return limitSeconds <= 0 ? 0 : Math.max(5, ThreadLocalRandom.current().nextLong(limitSeconds));
    }
}
