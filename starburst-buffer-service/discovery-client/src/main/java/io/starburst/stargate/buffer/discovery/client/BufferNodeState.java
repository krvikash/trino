/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.stargate.buffer.discovery.client;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public enum BufferNodeState
{
    STARTING(ImmutableSet.of()),
    STARTED(ImmutableSet.of(STARTING)),
    ACTIVE(ImmutableSet.of(STARTED)),
    DRAINING(ImmutableSet.of(ACTIVE)),
    DRAINED(ImmutableSet.of(DRAINING));

    private final Set<BufferNodeState> canTransitionFrom;

    BufferNodeState(Set<BufferNodeState> canTransitionFrom)
    {
        this.canTransitionFrom = requireNonNull(canTransitionFrom, "canTransitionFrom is null");
    }

    public boolean canTransitionFrom(BufferNodeState state)
    {
        return this.canTransitionFrom.contains(state);
    }
}
