/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public enum ExchangeState
{
    /**
     * Exchange created.
     */
    CREATED(false, false),
    /**
     * Receiving data from source.
     */
    SOURCE_STREAMING(false, false),
    /**
     * Done receiving data from source but not yet sending data to sink.
     */
    SOURCE_FINISHED(false, false),
    /**
     * Receiving data from source and sending data to sink. This state only occurs
     * when the prior state was SOURCE_STREAMING and won't be transitioned to if the
     * prior state was SOURCE_AND_SINK_STREAMING.
     */
    SOURCE_AND_SINK_STREAMING(false, false),
    /**
     * Sending data to sink after source finished.
     */
    SINK_STREAMING(false, false),
    /**
     * SOURCE AND SINK streaming are complete.
     */
    REMOVED(true, false),
    /**
     * A failure occurred.
     */
    FAILED(true, true);

    public static final Set<ExchangeState> TERMINAL_EXCHANGE_STATES = Stream.of(ExchangeState.values()).filter(ExchangeState::isDone).collect(toImmutableSet());

    private final boolean doneState;
    private final boolean failureState;

    ExchangeState(boolean doneState, boolean failureState)
    {
        checkArgument(!failureState || doneState, "%s is a non-done failure state", name());
        this.doneState = doneState;
        this.failureState = failureState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }

    /**
     * Is this a non-success terminal state.
     */
    public boolean isFailure()
    {
        return failureState;
    }
}
