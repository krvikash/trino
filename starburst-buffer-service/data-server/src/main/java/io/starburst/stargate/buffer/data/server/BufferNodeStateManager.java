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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.BufferNodeState;

import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class BufferNodeStateManager
{
    private static final Logger LOG = Logger.get(BufferNodeStateManager.class);

    private final ExecutorService lifeCycleStopper = newSingleThreadExecutor(daemonThreadsNamed("lifecycle-stopper-%s"));
    private final LifeCycleManager lifeCycleManager;

    @GuardedBy("this")
    private BufferNodeState state = BufferNodeState.STARTING;

    @Inject
    public BufferNodeStateManager(LifeCycleManager lifeCycleManager)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
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
    }

    public synchronized BufferNodeState getState()
    {
        return state;
    }

    public void preShutdownCleanup()
    {
        synchronized (this) {
            BufferNodeState currentState = getState();
            checkState(currentState == BufferNodeState.DRAINED, "can't cleanup when in %s state".formatted(currentState));
        }
        lifeCycleStopper.submit(lifeCycleManager::stop);
    }

    public boolean isDrainingStarted()
    {
        BufferNodeState bufferNodeState = getState();
        return bufferNodeState == BufferNodeState.DRAINING || bufferNodeState == BufferNodeState.DRAINED;
    }
}
