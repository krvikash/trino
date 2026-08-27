/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.server.castleinsights.analyzer.CastleInsightsExtractor;
import io.starburst.server.castleinsights.analyzer.CastleInsightsStats;
import io.starburst.server.castleinsights.model.CastleInsightsResult;
import io.trino.dispatcher.DispatchManager;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.resourcegroups.QueryType;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.spi.resourcegroups.QueryType.DATA_DEFINITION;
import static io.trino.spi.resourcegroups.QueryType.DESCRIBE;
import static io.trino.spi.resourcegroups.QueryType.EXPLAIN;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Extracts insights from queries as they complete, off the query's critical path.
 */
public class CastleInsightsListener
        implements EventListener
{
    private static final Logger log = Logger.get(CastleInsightsListener.class);

    /**
     * Query shapes that never carry the plan insights are drawn from.
     */
    private static final Set<QueryType> NON_ANALYZABLE_QUERY_TYPES = immutableEnumSet(EXPLAIN, DESCRIBE, DATA_DEFINITION);

    private final CastleInsightsExtractor extractor;
    private final DispatchManager dispatchManager;
    private final CastleInsightsResultSink resultSink;
    private final CastleInsightsStats stats;
    private final ExecutorService executorService;
    private final Duration minQueryDuration;

    @Inject
    public CastleInsightsListener(
            EventListenerManager listenerManager,
            CastleInsightsExtractor extractor,
            DispatchManager dispatchManager,
            CastleInsightsConfig config,
            CastleInsightsResultSink resultSink,
            CastleInsightsStats stats)
    {
        this.extractor = requireNonNull(extractor, "extractor is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.resultSink = requireNonNull(resultSink, "resultSink is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.minQueryDuration = config.getMinQueryDuration();
        this.executorService = new ThreadPoolExecutor(
                config.getThreadPoolSize(),
                config.getThreadPoolSize(),
                0L,
                SECONDS,
                new LinkedBlockingQueue<>(config.getMaxQueueSize()),
                daemonThreadsNamed("castle-insights-%s"));
        listenerManager.addEventListener(this);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        QueryId queryId = QueryId.valueOf(event.getMetadata().getQueryId());
        Optional<QueryInfo> fullQueryInfo = dispatchManager.getFullQueryInfo(queryId);
        if (fullQueryInfo.isEmpty()) {
            log.debug("Full query info not available for query %s; skipping insight extraction", queryId);
            return;
        }

        QueryInfo queryInfo = fullQueryInfo.get();
        if (!isAnalyzable(queryInfo)) {
            return;
        }

        try {
            executorService.execute(() -> extractInsights(queryInfo));
        }
        catch (RejectedExecutionException e) {
            stats.queryDropped();
            log.warn("Castle insights queue is full; dropping insight extraction for query %s", queryId);
        }
    }

    @Override
    public void shutdown()
    {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, SECONDS)) {
                log.warn("Castle insights executor did not terminate in time");
                executorService.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            currentThread().interrupt();
            throw new RuntimeException("Interrupted while shutting down Castle insights listener", e);
        }
    }

    private boolean isAnalyzable(QueryInfo queryInfo)
    {
        QueryId queryId = queryInfo.getQueryId();
        if (queryInfo.getState() != FINISHED) {
            log.debug("Query %s ended in state %s; skipping insight extraction", queryId, queryInfo.getState());
            return false;
        }
        if (queryInfo.getQueryType().filter(NON_ANALYZABLE_QUERY_TYPES::contains).isPresent()) {
            log.debug("Query %s is a %s; skipping insight extraction", queryId, queryInfo.getQueryType().orElseThrow());
            return false;
        }
        if (queryInfo.getQueryStats().getElapsedTime().compareTo(minQueryDuration) < 0) {
            log.debug("Query %s ran for less than %s; skipping insight extraction", queryId, minQueryDuration);
            return false;
        }
        return true;
    }

    private void extractInsights(QueryInfo queryInfo)
    {
        try {
            CastleInsightsResult result = extractor.process(queryInfo);
            resultSink.accept(result);
        }
        catch (Throwable e) {
            log.warn(e, "Castle insight extraction failed for query %s", queryInfo.getQueryId());
        }
    }
}
