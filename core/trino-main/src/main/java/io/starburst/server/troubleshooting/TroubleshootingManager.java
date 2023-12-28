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
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;

import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

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

    private final FullQueryInfoProvider fullQueryInfoProvider;
    private final Cache<QueryId, TroubleshootingContext> contexts;
    private final Set<TroubleshootingProvider> dataProviders;
    private final ScheduledExecutorService executorService;
    private final Duration destroyAfterFinishDelay;

    @Inject
    public TroubleshootingManager(TroubleshootingConfig config, FullQueryInfoProvider fullQueryInfoProvider, Set<TroubleshootingProvider> dataProviders, @ForTroubleshooting ScheduledExecutorService executorService)
    {
        this.fullQueryInfoProvider = requireNonNull(fullQueryInfoProvider, "dispatchManager is null");
        this.contexts = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getMaxActiveQueries())
                .shareNothingWhenDisabled()
                .build();
        this.dataProviders = ImmutableSet.copyOf(requireNonNull(dataProviders, "dataProviders is null"));
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.destroyAfterFinishDelay = requireNonNull(config, "config is null").getMaxAccessDuration();
    }

    public void start(QueryId queryId)
    {
        try {
            TroubleshootingContext context = contexts.get(queryId, () -> createNewContext(executorService, queryId));
            log.info("Started new %s", context);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Could not start new troubleshooting context", e);
        }
    }

    public void finish(QueryId queryId)
    {
        getContext(queryId).ifPresent(context -> {
            if (context.is(FINISHED)) {
                return;
            }
            waitForQueryInfoIsGathered();
            fullQueryInfoProvider.getFullQueryInfo(queryId)
                    .ifPresent(value -> context.set(QueryInfo.class, value));
            if (transitionContextTo(context, FINISHED)) {
                log.info("%s has finished", context);
                executorService.schedule(() -> {
                    remove(queryId);
                    log.info("Removed %s after timeout %s", context, destroyAfterFinishDelay);
                }, destroyAfterFinishDelay.toMillis(), MILLISECONDS);
            }
        });
    }

    public void remove(QueryId queryId)
    {
        getContext(queryId).ifPresent(context -> {
            verify(transitionContextTo(context, REMOVED), "%s was already removed", context);
            contexts.invalidate(queryId);
        });
    }

    private TroubleshootingContext createNewContext(ExecutorService executorService, QueryId queryId)
    {
        TroubleshootingContext context = new TroubleshootingContext(queryId, executorService);
        verify(transitionContextTo(context, STARTED), "%s was already started", context);
        return context;
    }

    private boolean transitionContextTo(TroubleshootingContext context, TroubleshootingContext.State nextState)
    {
        TroubleshootingContext.State currentState = context.getState();
        if (currentState == nextState) {
            return false;
        }

        // Events are fired before the actual transition so the state change is not observed first
        return switch (nextState) {
            case STARTED -> context.start(dataProviders);
            case FINISHED -> context.finish(dataProviders);
            case REMOVED -> context.remove(dataProviders);
            default -> throw new IllegalArgumentException("Cannot transition to %s state".formatted(nextState));
        };
    }

    public ListenableFuture<Map<String, InputStream>> getInputStreams(QueryId queryId)
    {
        Optional<TroubleshootingContext> context = getContext(queryId);
        if (context.isEmpty()) {
            return immediateFailedFuture(new NoSuchElementException("Troubleshooting context for " + queryId + " does not exist"));
        }

        return transform(context.get().getStartedStateChange(), state -> {
            verify(state == FINISHED, "%s is not in the %s state but %s", context.get(), FINISHED, state);
            ImmutableMap.Builder<String, InputStream> builder = ImmutableMap.builder();
            for (TroubleshootingProvider dataProvider : dataProviders) {
                try {
                    builder.putAll(dataProvider.getInputStreams(context.get()));
                }
                catch (Throwable t) {
                    log.warn(t, dataProvider.getClass().getName() + ".getInputStreams() failed for query with id: " + queryId.getId());
                }
            }
            return builder.buildOrThrow();
        }, executorService);
    }

    private Optional<TroubleshootingContext> getContext(QueryId queryId)
    {
        return Optional.ofNullable(contexts.getIfPresent(queryId));
    }

    private static void waitForQueryInfoIsGathered()
    {
        // QueryLoggerEventListener#queryCompleted has such comment:
        // failed dispatch queries are reported to the listener before being added to query tracker
        // magically it works here too.
        try {
            Thread.sleep(100);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
