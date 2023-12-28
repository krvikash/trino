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

import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.log.Logger;
import io.trino.execution.StateMachine;
import io.trino.spi.QueryId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.FINISHED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.INITIALIZED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.REMOVED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.STARTED;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;

public class TroubleshootingContext
{
    private static final Logger log = Logger.get(TroubleshootingContext.class);

    private final String contextId;
    private final QueryId queryId;
    private final StateMachine<State> state;

    private final Map<String, Object> values = new ConcurrentHashMap<>();

    public TroubleshootingContext(QueryId queryId, ExecutorService executorService)
    {
        this.contextId = randomUUID().toString().replace("-", "");
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

    public ListenableFuture<State> getStartedStateChange()
    {
        return state.getStateChange(STARTED);
    }

    public <T> boolean has(Class<T> clazz)
    {
        return values.containsKey(clazz.getSimpleName());
    }

    public <T> T getOrThrow(Class<T> clazz)
    {
        return get(clazz).orElseThrow();
    }

    public boolean start(Set<TroubleshootingProvider> dataProviders)
    {
        if (is(STARTED)) {
            return false;
        }
        dataProviders.forEach(provider -> {
            try {
                provider.onContextStarted(this);
            }
            catch (Throwable t) {
                log.warn(t, provider.getClass().getName() + ".onContextStarted() failed for query with id: " + queryId.getId());
            }
        });
        return state.compareAndSet(INITIALIZED, STARTED);
    }

    public boolean finish(Set<TroubleshootingProvider> dataProviders)
    {
        if (is(FINISHED)) {
            return false;
        }
        dataProviders.forEach(provider -> {
            try {
                provider.onContextFinished(this);
            }
            catch (Throwable t) {
                log.warn(t, provider.getClass().getName() + ".onContextFinished() failed for query with id: " + queryId.getId());
            }
        });
        return state.compareAndSet(STARTED, FINISHED);
    }

    public boolean remove(Set<TroubleshootingProvider> dataProviders)
    {
        if (is(REMOVED)) {
            return false;
        }
        dataProviders.forEach(provider -> provider.onContextRemoved(this));
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", contextId)
                .add("queryId", queryId)
                .add("currentState", state.get())
                .add("values", values.entrySet().stream()
                        .map(entry -> "%s@%s".formatted(entry.getKey(), entry.getValue().hashCode()))
                        .collect(joining(", ")))
                .toString();
    }
}
