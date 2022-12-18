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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;

import javax.inject.Inject;

import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.FINISHED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.REMOVED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.STARTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TroubleshootingManager
{
    private static final Logger log = Logger.get(TroubleshootingManager.class);

    private final QueryManager queryManager;
    private final Cache<QueryId, TroubleshootingContext> contexts;
    private final Set<TroubleshootingProvider> dataProviders;
    private final ScheduledExecutorService executorService;
    private final Duration destroyAfterFinishDelay;

    @Inject
    public TroubleshootingManager(TroubleshootingConfig config, QueryManager queryManager, Set<TroubleshootingProvider> dataProviders, @ForTroubleshooting ScheduledExecutorService executorService)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.contexts = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getMaxActiveQueries())
                .shareNothingWhenDisabled()
                .build();
        this.dataProviders = ImmutableSet.copyOf(requireNonNull(dataProviders, "dataProviders is null"));
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.destroyAfterFinishDelay = requireNonNull(config, "config is null").getMaxAccessDuration();
    }

    public void start(QueryCreatedEvent event)
    {
        QueryId queryId = QueryId.valueOf(event.getMetadata().getQueryId());
        try {
            log.info("Starting new troubleshooting context %s", queryId);
            contexts.get(queryId, () -> createNewContext(executorService, queryId, event));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Could not start new troubleshooting context", e);
        }
    }

    public void finish(QueryCompletedEvent event)
    {
        QueryId queryId = QueryId.valueOf(event.getMetadata().getQueryId());
        getContext(queryId).ifPresent(context -> {
            context.set(QueryCompletedEvent.class, event);
            try {
                context.set(QueryInfo.class, queryManager.getFullQueryInfo(queryId));
            }
            catch (NoSuchElementException e) {
                log.warn(e, "Could not fetch query %s full info, was query executed?", queryId);
            }

            verify(transitionContextTo(context, FINISHED), "Troubleshooting context is already finished or destroyed");
            log.info("Troubleshooting context for %s has finished", queryId);
            executorService.schedule(() -> remove(queryId), destroyAfterFinishDelay.toMillis(), MILLISECONDS);
        });
    }

    public void remove(QueryId queryId)
    {
        getContext(queryId).ifPresent(context -> {
            verify(transitionContextTo(context, REMOVED), "Context for " + queryId + " was already removed");
            contexts.invalidate(queryId);
        });
    }

    private TroubleshootingContext createNewContext(ExecutorService executorService, QueryId queryId, QueryCreatedEvent event)
    {
        TroubleshootingContext context = new TroubleshootingContext(queryId, executorService);
        context.set(QueryCreatedEvent.class, event);

        verify(transitionContextTo(context, STARTED), "Context for " + queryId + " was already started");
        return context;
    }

    private boolean transitionContextTo(TroubleshootingContext context, TroubleshootingContext.State nextState)
    {
        TroubleshootingContext.State currentState = context.getState();

        if (currentState == nextState) {
            throw new IllegalStateException("Context for %s was going to transition to %s but it's already in that state".formatted(context.getQueryId(), currentState));
        }

        boolean transitioned = false;

        // Events are fired before the actual transition so the state is not observed first
        switch (nextState) {
            case STARTED -> {
                transitioned = context.start();
            }
            case FINISHED -> {
                transitioned = context.finish();
            }
            case REMOVED -> {
                transitioned = context.remove();
            }
        }

        return transitioned;
    }

    public ListenableFuture<Map<String, InputStream>> getInputStreams(QueryId queryId)
    {
        Optional<TroubleshootingContext> context = getContext(queryId);
        if (context.isEmpty()) {
            return immediateFailedFuture(new NoSuchElementException("Troubleshooting context for query " + queryId + " does not exist"));
        }

        return transform(context.get().getStartedStateChange(), state -> {
            verify(state == FINISHED, "Troubleshooting context for %s is not in the %s state but %s", queryId, FINISHED, state);
            ImmutableMap.Builder<String, InputStream> builder = ImmutableMap.builder();
            for (TroubleshootingProvider dataProvider : dataProviders) {
                builder.putAll(dataProvider.getInputStreams(context.get()));
            }
            return builder.buildOrThrow();
        }, executorService);
    }

    private <T> Optional<T> runWithContext(QueryId queryId, Function<TroubleshootingContext, T> callable)
    {
        return getContext(queryId).map(callable);
    }

    private Optional<TroubleshootingContext> getContext(QueryId queryId)
    {
        return Optional.ofNullable(contexts.getIfPresent(queryId));
    }
}
