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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.spi.QueryId;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.FINISHED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.INITIALIZED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.INVALID;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.REMOVED;
import static io.starburst.server.troubleshooting.TroubleshootingContext.State.STARTED;
import static io.trino.execution.StageInfo.getAllStages;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TroubleshootingContextManager
{
    private static final Logger log = Logger.get(TroubleshootingContextManager.class);

    private final FullQueryInfoProvider fullQueryInfoProvider;
    private final Cache<QueryId, TroubleshootingContext> contexts;
    private final Set<TroubleshootingProvider> dataProviders;
    private final ScheduledExecutorService executorService;
    private final Duration destroyAfterFinishDelay;
    private final TroubleshootingArchiver troubleshootingArchiver;

    @Inject
    public TroubleshootingContextManager(
            TroubleshootingConfig config,
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
        TroubleshootingContext context;
        try {
            context = contexts.get(queryId, () -> new TroubleshootingContext(queryId,
                    new StateMachine<>("troubleshooting-" + queryId.getId(), executorService, INITIALIZED, Set.of(REMOVED))));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Could not create new troubleshooting context for queryid: " + queryId.getId(), e);
        }

        try {
            verify(transitionContextTo(context, STARTED), "%s was already started", context);
            log.info("Started new %s", context);
        }
        catch (Exception e) {
            context.addGeneralError(e);
            context.getState().set(INVALID);
            log.error(e, "Could not start new troubleshooting context for queryId: %s", queryId.getId());
        }
    }

    public void finish(QueryId queryId)
    {
        TroubleshootingContext context = contexts.getIfPresent(queryId);
        if (null == context || isInExpectedState(context, FINISHED) || isInExpectedState(context, INVALID)) {
            return;
        }
        try {
            waitForQueryInfoIsGathered();
            fullQueryInfoProvider.getFullQueryInfo(queryId)
                    .ifPresent(value -> {
                        context.set(QueryInfo.class, value);
                        context.setProcessingNodeIds(getProcessingNodesForQuery(value));
                    });
            if (transitionContextTo(context, FINISHED)) {
                log.info("%s has finished", context);
                executorService.schedule(() -> {
                    remove(queryId);
                    log.info("Removed %s after timeout %s", context, destroyAfterFinishDelay);
                }, destroyAfterFinishDelay.toMillis(), MILLISECONDS);
            }
        }
        catch (Exception e) {
            context.addGeneralError(e);
            context.getState().set(INVALID);
            log.error(e, "Could not finish troubleshooting context for queryId: %s", queryId.getId());
        }
    }

    public void remove(QueryId queryId)
    {
        TroubleshootingContext context = contexts.getIfPresent(queryId);
        if (null == context) {
            return;
        }
        verify(transitionContextTo(context, REMOVED), "%s was already removed", context);
        contexts.invalidate(queryId);
    }

    private boolean transitionContextTo(TroubleshootingContext context, TroubleshootingContext.State nextState)
    {
        TroubleshootingContext.State currentState = context.getState().get();
        if (currentState == nextState) {
            return false;
        }

        // Events are fired before the actual transition so the state change is not observed first
        return switch (nextState) {
            case STARTED -> startContext(context);
            case FINISHED -> finishContext(context);
            case REMOVED -> removeContext(context);
            default -> throw new IllegalArgumentException("Cannot transition to %s state".formatted(nextState));
        };
    }

    public ListenableFuture<InputStream> getArchive(QueryId queryId)
    {
        // entry of this method means that the web UI queried for troubleshooting info
        // but the query might have even not started
        // it might take hours to complete before we even start gathering troubleshooting info
        // therefore we return a ListenableFuture that has the following properties
        //  - it will NOT contain a computed value until the query has finished processing
        //  - it WILL contain a computed value once we start gathering troubleshooting info,
        //    the result will be an asynchronous stream that will be populated by a separate thread
        //    once we have troubleshooting files
        final TroubleshootingContext context = contexts.getIfPresent(queryId);
        if (null == context) {
            TroubleshootingContext immediateContext = new TroubleshootingContext(queryId, new StateMachine<>("troubleshooting-" + queryId.getId(), executorService, INVALID, Set.of(INVALID)));
            immediateContext.addGeneralError(new NoSuchElementException("TroubleshootingContext is null"));
            return immediateFuture(troubleshootingArchiver.execute(immediateContext));
        }

        return transform(context.getState().getStateChange(STARTED), state -> {
            verify(state == FINISHED, "%s is not in the %s state but %s", context, FINISHED, state);
            return troubleshootingArchiver.execute(context);
        }, executorService);
    }

    private boolean startContext(TroubleshootingContext context)
    {
        if (isInExpectedState(context, STARTED)) {
            return false;
        }
        dataProviders.forEach(provider -> {
            try {
                provider.onContextStarted(context);
            }
            catch (Throwable t) {
                log.warn(t, "%s.onContextStarted() failed for query with id: %s", provider.getClass().getName(), context.getQueryId().getId());
            }
        });
        return context.getState().compareAndSet(INITIALIZED, STARTED);
    }

    public boolean finishContext(TroubleshootingContext context)
    {
        if (isInExpectedState(context, FINISHED)) {
            return false;
        }
        dataProviders.forEach(provider -> {
            try {
                provider.onContextFinished(context);
            }
            catch (Throwable t) {
                log.warn(t, "%s.onContextFinished() failed for query with id: %s", provider.getClass().getName(), context.getQueryId().getId());
            }
        });
        return context.getState().compareAndSet(STARTED, FINISHED);
    }

    public boolean removeContext(TroubleshootingContext context)
    {
        if (isInExpectedState(context, REMOVED)) {
            return false;
        }
        dataProviders.forEach(provider -> provider.onContextRemoved(context));
        return context.getState().setIf(TroubleshootingContext.State.REMOVED, oldState -> oldState == STARTED || oldState == FINISHED);
    }

    private boolean isInExpectedState(TroubleshootingContext context, TroubleshootingContext.State expectedState)
    {
        return context.getState().get() == expectedState;
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

    private static Set<String> getProcessingNodesForQuery(QueryInfo queryInfo)
    {
        return queryInfo.getOutputStage().map(TroubleshootingContextManager::getNodeIdsProcessingQuery).orElse(ImmutableSet.of());
    }

    private static Set<String> getNodeIdsProcessingQuery(StageInfo outputStage)
    {
        List<TaskInfo> tasks = getAllStages(Optional.of(outputStage)).stream()
                .map(StageInfo::getTasks)
                .flatMap(Collection::stream)
                .collect(toImmutableList());

        return tasks.stream()
                .map(TaskInfo::getTaskStatus)
                .map(TaskStatus::getNodeId)
                .collect(toImmutableSet());
    }
}
