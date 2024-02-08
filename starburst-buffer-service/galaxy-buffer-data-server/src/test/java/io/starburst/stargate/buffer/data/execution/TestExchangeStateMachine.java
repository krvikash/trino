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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestExchangeStateMachine
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private static final Attributes REMOVED_EXCHANGE_ATTRIBUTES = Attributes.builder().put(EVENT_STATE, "REMOVED").build();

    @Test
    public void testHappyPath()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);
        assertEquals(CREATED, state.getState());

        state.sourceStreaming();
        assertEquals(SOURCE_STREAMING, state.getState());

        state.transitionToSourceFinished();
        assertEquals(SOURCE_FINISHED, state.getState());

        state.sinkStreaming();
        assertEquals(SINK_STREAMING, state.getState());

        state.transitionToRemoved(REMOVED_EXCHANGE_ATTRIBUTES);
        assertEquals(REMOVED, state.getState());
    }

    @Test
    public void testSourceBeforeCreate()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", SOURCE_STREAMING, executor);
        assertEquals(SOURCE_STREAMING, state.getState());

        // Should not affect state
        state.sourceStreaming();
        assertEquals(SOURCE_STREAMING, state.getState());
    }

    @Test
    public void testSinkBeforeSource()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);

        assertThrows(IllegalStateException.class, () -> state.sinkStreaming(),
                "Expected sinkStreaming() to throw when a source has not been initialized, but it didn't");
    }

    @Test
    public void testRemoveAfterCreate()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);

        state.transitionToRemoved(REMOVED_EXCHANGE_ATTRIBUTES);
        assertEquals(REMOVED, state.getState());

        // Should not affect state
        assertFalse(state.transitionToFailed(REMOVED_EXCHANGE_ATTRIBUTES));
        assertEquals(REMOVED, state.getState());

        state.sourceStreaming();
        state.sinkStreaming();
        state.transitionToSourceFinished();
        assertEquals(REMOVED, state.getState());
    }

    @Test
    public void testFailedIsPermanent()
    {
        ExchangeStateMachine state = new ExchangeStateMachine("1", CREATED, executor);
        state.transitionToFailed(REMOVED_EXCHANGE_ATTRIBUTES);
        assertEquals(FAILED, state.getState());

        assertTrue(state.transitionToRemoved(REMOVED_EXCHANGE_ATTRIBUTES));
        assertEquals(FAILED, state.getState());

        state.sourceStreaming();
        state.sinkStreaming();
        state.transitionToSourceFinished();
        assertEquals(FAILED, state.getState());
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdown();
    }
}
