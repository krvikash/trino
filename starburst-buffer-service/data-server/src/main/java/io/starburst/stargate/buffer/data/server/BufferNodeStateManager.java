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

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

public class BufferNodeStateManager
{
    private final AtomicReference<BufferNodeState> state = new AtomicReference<>(BufferNodeState.STARTING);

    public void transitionState(BufferNodeState targetState)
    {
        state.getAndUpdate(currentState -> {
            checkState(targetState.canTransitionFrom(currentState), "can't transition from %s to %s".formatted(currentState, targetState));
            return targetState;
        });
    }

    public BufferNodeState getState()
    {
        return state.get();
    }
}
