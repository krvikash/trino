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

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeState;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Objects.requireNonNull;

public class BufferNodeStateManager
{
    private static final Logger LOG = Logger.get(BufferNodeStateManager.class);

    private final LifeCycleManager lifeCycleManager;
    private final Duration minDrainingDuration;

    @GuardedBy("this")
    private BufferNodeState state = BufferNodeState.STARTING;
    @GuardedBy("this")
    private Optional<Instant> drainingStart = Optional.empty();

    @Inject
    public BufferNodeStateManager(LifeCycleManager lifeCycleManager, DataServerConfig config)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.minDrainingDuration = config.getMinDrainingDuration();
    }

    public synchronized void transitionState(BufferNodeState targetState)
    {
        if (state == targetState) {
            // ignore no-op transition
            return;
        }
        checkState(targetState.canTransitionFrom(state), "can't transition from %s to %s".formatted(state, targetState));
        LOG.info("Transition node state from %s to %s", state, targetState);
        state = targetState;
        if (drainingStart.isEmpty() && state == BufferNodeState.DRAINED || state == BufferNodeState.DRAINING) {
            drainingStart = Optional.of(Instant.now());
        }
    }

    public synchronized BufferNodeState getState()
    {
        return state;
    }

    public void preShutdownCleanup()
    {
        long remainingDrainingWaitMillis;
        synchronized (this) {
            BufferNodeState currentState = getState();
            checkState(currentState == BufferNodeState.DRAINED, "can't cleanup when in %s state".formatted(currentState));
            remainingDrainingWaitMillis = Instant.now().toEpochMilli() - drainingStart.orElseThrow().toEpochMilli();
        }
        if (remainingDrainingWaitMillis > 0) {
            LOG.info("Sleeping for %s so buffer node is kept in DRAINING state for at least %s", remainingDrainingWaitMillis, minDrainingDuration);
            sleepUninterruptibly(remainingDrainingWaitMillis, TimeUnit.MILLISECONDS);
        }
        lifeCycleManager.stop();
    }

    public boolean isDrainingStarted()
    {
        BufferNodeState bufferNodeState = getState();
        return bufferNodeState == BufferNodeState.DRAINING || bufferNodeState == BufferNodeState.DRAINED;
    }
}
