/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import io.airlift.units.Duration;
import io.trino.execution.StateMachine;
import io.trino.spi.QueryId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static io.starburst.server.troubleshooting.TroubleshootingContext.State.FINISHED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.INITIALIZED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.REMOVED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.STARTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TroubleshootingContext
{
    private final QueryId queryId;
    private final StateMachine<State> state;

    private final Map<String, Object> values = new ConcurrentHashMap<>();

    public TroubleshootingContext(QueryId queryId, ExecutorService executorService)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.state = new StateMachine<>("troubleshooting-" + queryId.getId(), executorService, INITIALIZED, Set.of(REMOVED));
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public <T> void set(Class<T> clazz, T object)
    {
        values.put(clazz.getSimpleName(), object);
    }

    public <T> Optional<T> get(Class<T> clazz)
    {
        return Optional.ofNullable(values.get(clazz.getSimpleName()))
                .map(clazz::cast);
    }

    public void awaitTermination(Duration duration)
    {
        try {
            state.getStateChange(STARTED).get(duration.toMillis(), MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> boolean has(Class<T> clazz)
    {
        return values.containsKey(clazz.getSimpleName());
    }

    public <T> T getOrThrow(Class<T> clazz)
    {
        return get(clazz).orElseThrow();
    }

    public boolean start()
    {
        return state.compareAndSet(INITIALIZED, STARTED);
    }

    public boolean finish()
    {
        return state.compareAndSet(STARTED, FINISHED);
    }

    public boolean remove()
    {
        return state.setIf(State.REMOVED, oldState -> oldState == STARTED || oldState == FINISHED);
    }

    public State getState()
    {
        return state.get();
    }

    public boolean is(State expectedState)
    {
        return state.get() == expectedState;
    }

    public enum State
    {
        INITIALIZED,
        STARTED,
        FINISHED,
        REMOVED;
    }
}
