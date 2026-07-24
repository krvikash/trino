/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.server.profiler.results.QueryProfilerResult;
import io.trino.dispatcher.DispatchManager;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.QueryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ProfilerListener
        implements EventListener
{
    private static final Logger log = Logger.get(ProfilerListener.class);

    private final QueryProfiler profiler;
    private final DispatchManager dispatchManager;
    private final ProfilerResultSink resultSink;
    private final ExecutorService executorService;
    private final Duration minQueryDuration;

    @Inject
    public ProfilerListener(
            EventListenerManager listenerManager,
            QueryProfiler profiler,
            DispatchManager dispatchManager,
            ProfilerConfig profilerConfig,
            ProfilerResultSink resultSink)
    {
        requireNonNull(listenerManager, "listenerManager is null");
        this.profiler = requireNonNull(profiler, "profiler is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        requireNonNull(profilerConfig, "profilerConfig is null");
        this.minQueryDuration = profilerConfig.getMinQueryDuration();
        this.executorService = new ThreadPoolExecutor(
                profilerConfig.getThreadPoolSize(),
                profilerConfig.getThreadPoolSize(),
                0L,
                SECONDS,
                new LinkedBlockingQueue<>(profilerConfig.getMaxQueueSize()),
                daemonThreadsNamed("query-profiler-%s"));
        this.resultSink = requireNonNull(resultSink, "resultSink is null");
        listenerManager.addEventListener(this);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        QueryId queryId = QueryId.valueOf(event.getMetadata().getQueryId());
        Optional<QueryInfo> dispatchQueryInfo = dispatchManager.getFullQueryInfo(queryId);
        if (dispatchQueryInfo.isEmpty()) {
            log.debug("Full query info not available for query %s; skipping profiling", queryId);
            return;
        }
        QueryInfo queryInfo = dispatchQueryInfo.get();
        Duration queryDuration = queryInfo.getQueryStats().getElapsedTime();
        if (queryDuration.toMillis() < minQueryDuration.toMillis()) {
            log.debug("Query duration time is smaller than minimal query duration time for query %s; skipping profiling", queryId);
            return;
        }
        try {
            executorService.execute(() -> processQuery(queryInfo));
        }
        catch (RejectedExecutionException e) {
            log.warn("Query profiler queue is full; dropping profiler for query %s", queryId);
        }
    }

    @Override
    public void shutdown()
    {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, SECONDS)) {
                log.warn("Query profiling executor did not terminate in time");
                executorService.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            currentThread().interrupt();
            throw new RuntimeException("Interrupted while shutting down profiling listener", e);
        }
    }

    private void processQuery(QueryInfo queryInfo)
    {
        QueryId queryId = queryInfo.getQueryId();
        try {
            QueryProfilerResult result = profiler.analyze(queryInfo);
            resultSink.accept(result);
        }
        catch (Throwable e) {
            log.warn(e, "Query profiling failed for query %s", queryId);
        }
    }
}
