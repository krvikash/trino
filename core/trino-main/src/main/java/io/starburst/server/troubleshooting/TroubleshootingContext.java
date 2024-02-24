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

import io.trino.execution.StateMachine;
import io.trino.spi.QueryId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;

public class TroubleshootingContext
{
    private final String contextId;
    private final QueryId queryId;
    private final StateMachine<State> state;
    private final Map<String, Object> values = new ConcurrentHashMap<>();
    // available only once the query is finished
    private Optional<Set<String>> jfrCollectedNodes = Optional.empty();
    // available only once the query is finished
    private Optional<Set<String>> traceCollectedNodes = Optional.empty();
    private final List<Exception> topLevelErrors = new ArrayList<>();
    private final Map<String, Exception> errors = new HashMap<>();

    public TroubleshootingContext(QueryId queryId, StateMachine<State> state)
    {
        this.contextId = randomUUID().toString().replace("-", "");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.state = state;
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

    public <T> boolean has(Class<T> clazz)
    {
        return values.containsKey(clazz.getSimpleName());
    }

    public <T> T getOrThrow(Class<T> clazz)
    {
        return get(clazz).orElseThrow();
    }

    public StateMachine<State> getState()
    {
        return state;
    }

    public void addGeneralError(Exception e)
    {
        topLevelErrors.add(e);
    }

    public void addError(String filename, Exception e)
    {
        errors.put(filename, e);
    }

    public List<Exception> getTopLevelErrors()
    {
        return topLevelErrors;
    }

    public Map<String, Exception> getErrors()
    {
        return errors;
    }

    public void setCollectedNodes(Set<String> jfrCollectedNodes, Set<String> traceCollectedNodes)
    {
        checkArgument(jfrCollectedNodes.containsAll(traceCollectedNodes) || traceCollectedNodes.containsAll(jfrCollectedNodes));
        this.jfrCollectedNodes = Optional.of(jfrCollectedNodes);
        this.traceCollectedNodes = Optional.of(traceCollectedNodes);
    }

    public Set<String> getTraceCollectedNodes()
    {
        return traceCollectedNodes.orElseThrow(() -> new NoSuchElementException("traceCollectedNodes not available"));
    }

    public Set<String> getJfrCollectedNodes()
    {
        return jfrCollectedNodes.orElseThrow(() -> new NoSuchElementException("jfrCollectedNodes not available"));
    }

    public enum State
    {
        INITIALIZED,
        STARTED,
        FINISHED,
        REMOVED,
        INVALID;
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
