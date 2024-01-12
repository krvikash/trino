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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;

import java.io.InputStream;
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
    private final TroubleshootingArchiver troubleshootingArchiver;

    @Inject
    public TroubleshootingManager(TroubleshootingConfig config,
            FullQueryInfoProvider fullQueryInfoProvider,
            Set<TroubleshootingProvider> dataProviders,
            @ForTroubleshooting ScheduledExecutorService executorService,
            TroubleshootingArchiver troubleshootingArchiver)
    {
        this.fullQueryInfoProvider = requireNonNull(fullQueryInfoProvider, "dispatchManager is null");
        this.contexts = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getMaxActiveQueries())
                .shareNothingWhenDisabled()
                .build();
        this.dataProviders = requireNonNull(dataProviders, "dataProviders is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.destroyAfterFinishDelay = requireNonNull(config, "config is null").getMaxAccessDuration();
        this.troubleshootingArchiver = requireNonNull(troubleshootingArchiver, "troubleshootingArchiver is null");
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
        TroubleshootingContext context = new TroubleshootingContext(queryId, executorService, dataProviders);
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
            case STARTED -> context.start();
            case FINISHED -> context.finish();
            case REMOVED -> context.remove();
            default -> throw new IllegalArgumentException("Cannot transition to %s state".formatted(nextState));
        };
    }

    public ListenableFuture<InputStream> getArchive(QueryId queryId)
    {
        //entry of this method means that the web UI queried for troubleshooting info
        //but the query might have even not started
        //it might take hours to complete before we even start gathering troubleshooting info
        //therefore we return a ListenableFuture that has the following properties
        // - it will NOT contain a computed value until the query has finished processing
        // - it WILL contain a computed value once we start gathering troubleshooting info,
        //   the result will be an asynchronous stream that will be populated by a separate thread
        //   once we have troubleshooting files
        return getContext(queryId)
                .map(context -> transform(context.getStartedStateChange(), state -> {
                    verify(state == FINISHED, "%s is not in the %s state but %s", context, FINISHED, state);
                    return troubleshootingArchiver.execute(context);
                }, executorService))
                .orElse(immediateFailedFuture(new NoSuchElementException("Troubleshooting context for " + queryId + " does not exist")));
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
