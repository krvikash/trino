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

import com.google.errorprone.annotations.ThreadSafe;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.CREATED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.FAILED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.REMOVED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SINK_STREAMING;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SOURCE_AND_SINK_STREAMING;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SOURCE_FINISHED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SOURCE_STREAMING;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.TERMINAL_EXCHANGE_STATES;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ExchangeStateMachine
{
    private final StateMachine<ExchangeState> exchangeState;

    private final AtomicBoolean firstSourceDataReceived = new AtomicBoolean();
    private final AtomicBoolean firstSinkDataRequested = new AtomicBoolean();
    // keep value of EVENT_STATE consistent with attribute defined in TrinoAttributes in trino-main
    private static final AttributeKey<String> EVENT_STATE = stringKey("state");

    public ExchangeStateMachine(String exchangeId, ExchangeState initialState, Executor executor)
    {
        requireNonNull(exchangeId, "exchangeId is null");
        requireNonNull(initialState, "initialState is null");
        requireNonNull(executor, "executor is null");
        checkArgument(initialState == CREATED || initialState == SOURCE_STREAMING,
                "unexpected initial exchange state %s", initialState);
        this.exchangeState = new StateMachine<>("exchange " + exchangeId, executor, initialState, TERMINAL_EXCHANGE_STATES);
    }

    public ExchangeState getState()
    {
        return exchangeState.get();
    }

    public void attachSpan(Span exchangeSpan)
    {
        exchangeState.addStateChangeListener(newState -> {
            exchangeSpan.addEvent("exchange_state", Attributes.of(
                    EVENT_STATE, newState.toString()));
        });
    }

    public void sourceStreaming()
    {
        if (firstSourceDataReceived.compareAndSet(false, true)) {
            // Since it is possible for workers to send slices through addDataPages() before
            // the coordinator calls createExchange(), the state may already be SOURCE_STREAMING.
            exchangeState.compareAndSet(CREATED, SOURCE_STREAMING);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    public void sinkStreaming()
    {
        if (firstSinkDataRequested.compareAndSet(false, true)) {
            checkState(firstSourceDataReceived.get(), "sink data requested before source data has been received");
            // This is not atomic, so it can race with transitionToSourceFinished()
            while (true) {
                ExchangeState currentState = exchangeState.get();
                if (currentState != SOURCE_STREAMING && currentState != SOURCE_FINISHED) {
                    return;
                }
                // If the state changes between the get and the compareAndSet, retry the loop
                if (currentState == SOURCE_STREAMING && exchangeState.compareAndSet(SOURCE_STREAMING, SOURCE_AND_SINK_STREAMING)) {
                    return;
                }
                // If the state changes between the get and the compareAndSet, retry the loop
                if (currentState == SOURCE_FINISHED && exchangeState.compareAndSet(SOURCE_FINISHED, SINK_STREAMING)) {
                    return;
                }
            }
        }
    }

    @SuppressWarnings("DuplicatedCode")
    public void transitionToSourceFinished()
    {
        // This is not atomic, so it can race with sinkStreaming()
        while (true) {
            ExchangeState currentState = exchangeState.get();
            if (currentState != SOURCE_STREAMING && currentState != SOURCE_AND_SINK_STREAMING) {
                return;
            }
            // If the state changes between the get and the compareAndSet, retry the loop
            if (currentState == SOURCE_STREAMING && exchangeState.compareAndSet(SOURCE_STREAMING, SOURCE_FINISHED)) {
                return;
            }
            // If the state changes between the get and the compareAndSet, retry the loop
            if (currentState == SOURCE_AND_SINK_STREAMING && exchangeState.compareAndSet(SOURCE_AND_SINK_STREAMING, SINK_STREAMING)) {
                return;
            }
        }
    }

    public boolean transitionToRemoved()
    {
        // This should not be called twice.
        return exchangeState.trySet(REMOVED) != REMOVED;
    }

    public boolean transitionToFailed()
    {
        // This will report success if the state has already been set to FAILED.
        return exchangeState.trySet(FAILED) != REMOVED;
    }
}
