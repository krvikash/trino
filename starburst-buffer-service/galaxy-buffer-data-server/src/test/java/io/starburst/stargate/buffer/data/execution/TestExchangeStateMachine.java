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

import io.opentelemetry.api.common.Attributes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.starburst.stargate.buffer.data.execution.ExchangeState.CREATED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.FAILED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.REMOVED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SINK_STREAMING;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SOURCE_FINISHED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SOURCE_STREAMING;
import static io.starburst.stargate.buffer.data.execution.ExchangeStateMachine.EVENT_STATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestExchangeStateMachine
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private static final Attributes REMOVED_EXCHANGE_ATTRIBUTES = Attributes.builder().put(EVENT_STATE, "REMOVED").build();

    @Test
    public void testHappyPath()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);
        assertThat(state.getState()).isEqualTo(CREATED);

        state.sourceStreaming();
        assertThat(state.getState()).isEqualTo(SOURCE_STREAMING);

        state.transitionToSourceFinished();
        assertThat(state.getState()).isEqualTo(SOURCE_FINISHED);

        state.sinkStreaming();
        assertThat(state.getState()).isEqualTo(SINK_STREAMING);

        state.transitionToRemoved(REMOVED_EXCHANGE_ATTRIBUTES);
        assertThat(state.getState()).isEqualTo(REMOVED);
    }

    @Test
    public void testSourceBeforeCreate()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", SOURCE_STREAMING, executor);
        assertThat(state.getState()).isEqualTo(SOURCE_STREAMING);

        // Should not affect state
        state.sourceStreaming();
        assertThat(state.getState()).isEqualTo(SOURCE_STREAMING);
    }

    @Test
    public void testSinkBeforeSource()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);

        assertThatThrownBy(state::sinkStreaming)
                .as("Expected sinkStreaming() to throw when a source has not been initialized, but it didn't")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRemoveAfterCreate()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);

        state.transitionToRemoved(REMOVED_EXCHANGE_ATTRIBUTES);
        assertThat(state.getState()).isEqualTo(REMOVED);

        // Should not affect state
        assertThat(state.transitionToFailed(REMOVED_EXCHANGE_ATTRIBUTES)).isFalse();
        assertThat(state.getState()).isEqualTo(REMOVED);

        state.sourceStreaming();
        state.sinkStreaming();
        state.transitionToSourceFinished();
        assertThat(state.getState()).isEqualTo(REMOVED);
    }

    @Test
    public void testFailedIsPermanent()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);
        state.transitionToFailed(REMOVED_EXCHANGE_ATTRIBUTES);
        assertThat(state.getState()).isEqualTo(FAILED);

        assertThat(state.transitionToRemoved(REMOVED_EXCHANGE_ATTRIBUTES)).isTrue();
        assertThat(state.getState()).isEqualTo(FAILED);

        state.sourceStreaming();
        state.sinkStreaming();
        state.transitionToSourceFinished();
        assertThat(state.getState()).isEqualTo(FAILED);
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdown();
    }
}
