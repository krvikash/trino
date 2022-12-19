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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.REMOVED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TroubleshootingManager
{
    private static final Logger log = Logger.get(TroubleshootingManager.class);
    private static final Duration TERMINATION_DELAY = Duration.valueOf("1s");
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

            verify(context.finish(), "Troubleshooting context is already finished or destroyed");
            log.info("Troubleshooting context for %s has finished", queryId);
            executorService.schedule(() -> remove(queryId), destroyAfterFinishDelay.toMillis(), MILLISECONDS);
        });
    }

    public void remove(QueryId queryId)
    {
        getContext(queryId).ifPresent(context -> {
            verify(context.remove(), "Context for " + queryId + " was already removed");
            contexts.invalidate(queryId);
        });
    }

    private TroubleshootingContext createNewContext(ExecutorService executorService, QueryId queryId, QueryCreatedEvent event)
    {
        TroubleshootingContext context = new TroubleshootingContext(queryId, executorService);
        context.set(QueryCreatedEvent.class, event);
        return context;
    }

    public Optional<Map<String, InputStream>> getInputStreams(QueryId queryId)
    {
        return runWithContext(queryId, context -> {
            context.awaitTermination(TERMINATION_DELAY);
            checkState(!context.is(REMOVED), "Context for " + context.getQueryId() + " has been removed");
            ImmutableMap.Builder<String, InputStream> builder = ImmutableMap.builder();
            for (TroubleshootingProvider dataProvider : dataProviders) {
                builder.putAll(dataProvider.getInputStreams(context));
            }
            return builder.buildOrThrow();
        });
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
