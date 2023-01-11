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
import io.starburst.stargate.buffer.BufferNodeState;

import javax.inject.Inject;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class BufferNodeStateManager
{
    private static final Logger LOG = Logger.get(BufferNodeStateManager.class);

    private final AtomicReference<BufferNodeState> state = new AtomicReference<>(BufferNodeState.STARTING);
    private final LifeCycleManager lifeCycleManager;

    @Inject
    public BufferNodeStateManager(LifeCycleManager lifeCycleManager)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    }

    public void transitionState(BufferNodeState targetState)
    {
        state.getAndUpdate(currentState -> {
            if (currentState == targetState) {
                // ignore no-op transition
                return currentState;
            }
            checkState(targetState.canTransitionFrom(currentState), "can't transition from %s to %s".formatted(currentState, targetState));
            LOG.info("Transition node state from %s to %s", currentState, targetState);
            return targetState;
        });
    }

    public BufferNodeState getState()
    {
        return state.get();
    }

    public void preShutdownCleanup()
    {
        BufferNodeState currentState = getState();
        checkState(currentState == BufferNodeState.DRAINED, "can't cleanup when in %s state".formatted(currentState));
        lifeCycleManager.stop();
    }

    public boolean isDrainingStarted()
    {
        BufferNodeState bufferNodeState = state.get();
        return bufferNodeState == BufferNodeState.DRAINING || bufferNodeState == BufferNodeState.DRAINED;
    }
}
