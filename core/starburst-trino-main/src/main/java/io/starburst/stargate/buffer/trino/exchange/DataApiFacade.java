/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import dev.failsafe.CircuitBreaker;
import dev.failsafe.CircuitBreakerOpenException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.DistributionStat;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.BufferNodeExchangeMetrics;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.RateLimitInfo;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.units.Duration.succinctDuration;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class DataApiFacade
{
    private static final Logger log = Logger.get(DataApiFacade.class);
    private static final boolean ENABLE_LOG_RATE_LIMITING = true;
    private static final RateLimitingLogger rateLimitingLogger = new RateLimitingLogger(log, ENABLE_LOG_RATE_LIMITING);

    private static final Duration CLEANUP_DELAY = succinctDuration(5, TimeUnit.SECONDS);
    private static final Duration STATS_UPDATE_INTERVAL = succinctDuration(5, TimeUnit.SECONDS);
    private static final Duration PROCESS_PENDING_REQUESTS_INTERVAL = succinctDuration(10, TimeUnit.SECONDS);

    private final BufferNodeDiscoveryManager discoveryManager;
    private final ApiFactory apiFactory;
    private final DataApiFacadeStats stats;
    private final Map<Long, DataApi> dataApiClients = new ConcurrentHashMap<>();
    private final Map<Long, FailsafeExecutor<Object>> defaultRetryExecutors = new ConcurrentHashMap<>();
    private final Map<Long, FailsafeExecutor<Object>> addDataPagesRetryExecutors = new ConcurrentHashMap<>();
    private final RetryExecutorConfig defaultRetryExecutorConfig;
    private final RetryExecutorConfig addDataPagesRetryExecutorConfig;
    private final ScheduledExecutorService executor;
    private final ListeningScheduledExecutorService listeningScheduledExecutor;
    private final RateMonitor rateMonitor;
    private final Closer destroyCloser = Closer.create();
    private final boolean useOldRateLimit;
    private final Stopwatch stopwatch = Stopwatch.createStarted();

    private final int maxConcurrentAddDataPagesPerNode;
    private final AddDataPagesStatsUpdater addDataPagesStatsUpdater;
    private final Map<Long, AddDataPagesProcessingState> addDataPagesProcessingStates = new ConcurrentHashMap<>();

    private final DistributionStat pendingRequestsPerNodeDistribution = new DistributionStat();
    private final DistributionStat inFlightRequestsPerNodeDistribution = new DistributionStat();
    private final DistributionStat backoffRequestsPerNodeDistribution = new DistributionStat();

    record RetryExecutorConfig(
            int maxRetries,
            Duration backoffInitial,
            Duration backoffMax,
            double backoffFactor,
            double backoffJitter,
            int circuitBreakerFailureThreshold,
            int circuitBreakerSuccessThreshold,
            Duration circuitBreakerDelay) {}

    @Inject
    public DataApiFacade(
            BufferNodeDiscoveryManager discoveryManager,
            ApiFactory apiFactory,
            DataApiFacadeStats stats,
            BufferExchangeConfig config,
            ScheduledExecutorService executor)
    {
        this(
                discoveryManager,
                apiFactory,
                stats,
                new RetryExecutorConfig(
                        config.getDataClientMaxRetries(),
                        config.getDataClientRetryBackoffInitial(),
                        config.getDataClientRetryBackoffMax(),
                        config.getDataClientRetryBackoffFactor(),
                        config.getDataClientRetryBackoffJitter(),
                        config.getDataClientCircuitBreakerFailureThreshold(),
                        config.getDataClientCircuitBreakerSuccessThreshold(),
                        config.getDataClientCircuitBreakerDelay()),
                new RetryExecutorConfig(
                        config.getDataClientAddDataPagesMaxRetries(),
                        config.getDataClientAddDataPagesRetryBackoffInitial(),
                        config.getDataClientAddDataPagesRetryBackoffMax(),
                        config.getDataClientAddDataPagesRetryBackoffFactor(),
                        config.getDataClientAddDataPagesRetryBackoffJitter(),
                        config.getDataClientAddDataPagesCircuitBreakerFailureThreshold(),
                        config.getDataClientAddDataPagesCircuitBreakerSuccessThreshold(),
                        config.getDataClientAddDataPagesCircuitBreakerDelay()),
                config.isUseOldRateLimiting(),
                config.getMaxConcurrentAddDataPagesPerNode(),
                executor);
    }

    @VisibleForTesting
    DataApiFacade(
            BufferNodeDiscoveryManager discoveryManager,
            ApiFactory apiFactory,
            DataApiFacadeStats stats,
            RetryExecutorConfig defaultRetryExecutorConfig,
            RetryExecutorConfig addDataPagesRetryExecutorConfig,
            boolean useOldRateLimit,
            int maxConcurrentAddDataPagesPerNode,
            ScheduledExecutorService executor)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.defaultRetryExecutorConfig = requireNonNull(defaultRetryExecutorConfig, "defaultRetryExecutorConfig is null");
        this.addDataPagesRetryExecutorConfig = requireNonNull(addDataPagesRetryExecutorConfig, "addDataPagesRetryExecutorConfig is null");
        this.useOldRateLimit = useOldRateLimit;
        this.maxConcurrentAddDataPagesPerNode = maxConcurrentAddDataPagesPerNode;
        this.executor = requireNonNull(executor, "executor is null");
        this.listeningScheduledExecutor = listeningDecorator(executor);
        this.rateMonitor = new RateMonitor(Ticker.systemTicker());
        this.addDataPagesStatsUpdater = new AddDataPagesStatsUpdater(stats.getAddDataPagesOperationStats());
    }

    @PostConstruct
    @VisibleForTesting
    void init()
    {
        ScheduledFuture<?> cleanupFuture = this.executor.scheduleWithFixedDelay(() -> {
            try {
                cleanUp();
            }
            catch (Exception e) {
                // catch all so we are not unscheduled
                log.error(e, "Unexpected error caught in cleanUp");
            }
        }, CLEANUP_DELAY.toMillis(), CLEANUP_DELAY.toMillis(), MILLISECONDS);
        destroyCloser.register(() -> cleanupFuture.cancel(true));

        ScheduledFuture<?> statsFuture = this.executor.scheduleWithFixedDelay(() -> {
            try {
                updateDistributionStats();
            }
            catch (Exception e) {
                log.error(e, "Unexpected error caught in updateDistributionStats");
            }
        }, STATS_UPDATE_INTERVAL.toMillis(), STATS_UPDATE_INTERVAL.toMillis(), MILLISECONDS);
        destroyCloser.register(() -> statsFuture.cancel(true));

        ScheduledFuture<?> processPendingFuture = this.executor.scheduleWithFixedDelay(() -> {
            try {
                scheduleProcessAddDataPages();
            }
            catch (Exception e) {
                log.error(e, "Unexpected error caught in processPendingAddDataPagesRequests");
            }
        }, PROCESS_PENDING_REQUESTS_INTERVAL.toMillis(), PROCESS_PENDING_REQUESTS_INTERVAL.toMillis(), MILLISECONDS);
        destroyCloser.register(() -> processPendingFuture.cancel(true));

        destroyCloser.register(() -> addDataPagesProcessingStates.values().forEach(state -> {
            // flip draining so an onFailure backoff racing the schedule()/backoffRequests.put() window
            // is rejected by enqueueAddDataPagesRequest instead of resurrecting a request after destroy
            state.draining.set(true);
            state.backoffRequests.values().forEach(future -> future.cancel(false));
            state.backoffRequests.keySet().forEach(request -> request.resultFuture.cancel(false));
            state.activeRequests.forEach(request -> request.resultFuture.cancel(false));
        }));
    }

    @PreDestroy
    @VisibleForTesting
    void destroy()
    {
        try {
            destroyCloser.close();
        }
        catch (IOException e) {
            log.error(e, "Unexpected error in destroy");
        }
    }

    @Managed
    public int getActiveDataNodeCount()
    {
        return (int) addDataPagesProcessingStates.values().stream()
                .filter(state -> !state.draining.get())
                .count();
    }

    @Managed
    public int getDrainingDataNodeCount()
    {
        return (int) addDataPagesProcessingStates.values().stream()
                .filter(state -> state.draining.get())
                .count();
    }

    @Managed
    public int getTotalPendingRequests()
    {
        return addDataPagesProcessingStates.values().stream()
                .mapToInt(state -> state.activeRequests.size())
                .sum();
    }

    @Managed
    public int getTotalInFlightRequests()
    {
        return addDataPagesProcessingStates.values().stream()
                .mapToInt(state -> state.inFlightRequests.get())
                .sum();
    }

    @Managed
    public int getTotalBackoffRequests()
    {
        return addDataPagesProcessingStates.values().stream()
                .mapToInt(state -> state.backoffRequests.size())
                .sum();
    }

    @Managed
    @Nested
    public DistributionStat getPendingRequestsPerNodeDistribution()
    {
        return pendingRequestsPerNodeDistribution;
    }

    @Managed
    @Nested
    public DistributionStat getInFlightRequestsPerNodeDistribution()
    {
        return inFlightRequestsPerNodeDistribution;
    }

    @Managed
    @Nested
    public DistributionStat getBackoffRequestsPerNodeDistribution()
    {
        return backoffRequestsPerNodeDistribution;
    }

    private void updateDistributionStats()
    {
        addDataPagesProcessingStates.forEach((_, state) -> {
            pendingRequestsPerNodeDistribution.add(state.activeRequests.size());
            inFlightRequestsPerNodeDistribution.add(state.inFlightRequests.get());
            backoffRequestsPerNodeDistribution.add(state.backoffRequests.size());
        });
    }

    @VisibleForTesting
    void cleanUp()
    {
        BufferNodeDiscoveryManager.BufferNodesState bufferNodes = discoveryManager.getBufferNodes();

        Predicate<Long> isBufferNodeStale = bufferNodeId -> {
            BufferNodeInfo bufferNodeInfo = bufferNodes.getAllBufferNodes().get(bufferNodeId);
            return bufferNodeInfo == null || bufferNodeInfo.state() == BufferNodeState.DRAINED;
        };

        Set<Long> staleDataApiBufferNodeIds = dataApiClients.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.debug("cleaning up stale dataApi clients for buffer nodes %s", staleDataApiBufferNodeIds);
        staleDataApiBufferNodeIds.forEach(dataApiClients::remove);

        Set<Long> staleDefaultRetryExecutors = defaultRetryExecutors.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.debug("cleaning up stale retry executors for buffer nodes %s", staleDefaultRetryExecutors);
        staleDefaultRetryExecutors.forEach(defaultRetryExecutors::remove);

        Set<Long> staleAddDataPagesRetryExecutors = addDataPagesRetryExecutors.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.debug("cleaning up stale add data pages retry executors for buffer nodes %s", staleAddDataPagesRetryExecutors);
        staleAddDataPagesRetryExecutors.forEach(addDataPagesRetryExecutors::remove);

        Set<Long> staleAddDataPagesProcessingStates = addDataPagesProcessingStates.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.debug("cleaning up stale add data pages processing states for buffer nodes %s", staleAddDataPagesProcessingStates);
        staleAddDataPagesProcessingStates.forEach(this::drainAddDataPagesState);

        rateMonitor.cleanUp(isBufferNodeStale);
    }

    private void drainAddDataPagesState(long bufferNodeId)
    {
        AddDataPagesProcessingState drainingState = addDataPagesProcessingStates.compute(bufferNodeId, (ignored, state) -> {
            if (state == null) {
                return null;
            }

            // mark for draining
            state.draining.set(true);

            // if already drained delete
            if (state.backoffRequests.isEmpty() && state.activeRequests.isEmpty() && state.inFlightRequests.get() == 0) {
                return null;
            }
            return state;
        });

        if (drainingState != null) {
            checkState(drainingState.draining.get(), "Expected draining to be true");
            scheduleProcessAddDataPages(bufferNodeId, drainingState);
        }
    }

    private static void completeWithDrainedException(PendingAddDataPagesRequest request)
    {
        ErrorCode errorCode = (request.tryCount.get() > 0 && request.requestPossiblyDelivered.get())
                ? ErrorCode.DRAINING_ON_RETRY
                : ErrorCode.DRAINED;
        request.resultFuture.setException(new DataApiException(errorCode, "Buffer node is no longer available"));
    }

    // Safety net to unstick pending requests that might have been missed due to race conditions
    // in the callback-driven processing of tryHandlePendingAddDataPages
    private void scheduleProcessAddDataPages()
    {
        for (Map.Entry<Long, AddDataPagesProcessingState> entry : addDataPagesProcessingStates.entrySet()) {
            // Remove completed requests that are stuck in the middle of the queue where
            // doHandlePendingAddDataPages (which only peeks/polls from the head) won't reach them.
            // ConcurrentLinkedQueue.removeIf is safe for concurrent access.
            entry.getValue().activeRequests.removeIf(request -> request.resultFuture.isDone());
            scheduleProcessAddDataPages(entry.getKey(), entry.getValue());
        }
    }

    @VisibleForTesting
    DataApiFacadeStats getStats()
    {
        return stats;
    }

    public ListenableFuture<ChunkList> listClosedChunks(long bufferNodeId, String exchangeId, OptionalLong pagingId)
    {
        return runWithRetry(bufferNodeId, () -> internalListClosedChunks(bufferNodeId, exchangeId, pagingId));
    }

    private ListenableFuture<ChunkList> internalListClosedChunks(long bufferNodeId, String exchangeId, OptionalLong pagingId)
    {
        try {
            return getDataApi(bufferNodeId).listClosedChunks(exchangeId, pagingId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> markAllClosedChunksReceived(long bufferNodeId, String exchangeId)
    {
        return runWithRetry(bufferNodeId, () -> internalMarkAllClosedChunksReceived(bufferNodeId, exchangeId));
    }

    private ListenableFuture<Void> internalMarkAllClosedChunksReceived(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).markAllClosedChunksReceived(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> setChunkDeliveryMode(long bufferNodeId, String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
    {
        return runWithRetry(bufferNodeId, () -> internalSetChunkDeliveryMode(bufferNodeId, exchangeId, chunkDeliveryMode));
    }

    private ListenableFuture<Void> internalSetChunkDeliveryMode(long bufferNodeId, String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
    {
        try {
            return getDataApi(bufferNodeId).setChunkDeliveryMode(exchangeId, chunkDeliveryMode);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> registerExchange(long bufferNodeId, String exchangeId, ChunkDeliveryMode chunkDeliveryMode, Span exchangeSpan)
    {
        return runWithRetry(bufferNodeId, () -> internalRegisterExchange(bufferNodeId, exchangeId, chunkDeliveryMode, exchangeSpan));
    }

    private ListenableFuture<Void> internalRegisterExchange(long bufferNodeId, String exchangeId, ChunkDeliveryMode chunkDeliveryMode, Span exchangeSpan)
    {
        try {
            return getDataApi(bufferNodeId).registerExchange(exchangeId, chunkDeliveryMode, exchangeSpan);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<BufferNodeExchangeMetrics> pingExchange(long bufferNodeId, String exchangeId)
    {
        return runWithRetry(bufferNodeId, () -> internalPingExchange(bufferNodeId, exchangeId));
    }

    private ListenableFuture<BufferNodeExchangeMetrics> internalPingExchange(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).pingExchange(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId)
    {
        return runWithRetry(bufferNodeId, () -> internalRemoveExchange(bufferNodeId, exchangeId));
    }

    private ListenableFuture<Void> internalRemoveExchange(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).removeExchange(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<AddDataPagesResponse> addDataPages(long bufferNodeId, String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        if (useOldRateLimit) {
            return addDataPagesOldRateLimit(bufferNodeId, exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition);
        }
        return addDataPagesNewRateLimit(bufferNodeId, exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition);
    }

    @Deprecated
    private ListenableFuture<AddDataPagesResponse> addDataPagesOldRateLimit(long bufferNodeId, String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        AtomicLong triesCount = new AtomicLong();
        AtomicBoolean requestPossiblyDelivered = new AtomicBoolean(false);
        Stopwatch stopwatch = Stopwatch.createStarted();
        Stopwatch successRequestStopwatch = Stopwatch.createStarted();
        AtomicLong totalRequestDelay = new AtomicLong();
        Callable<ListenableFuture<Void>> call = () -> {
            boolean retry = triesCount.getAndIncrement() > 0;
            long requestDelayInMillis = rateMonitor.registerExecutionSchedule(bufferNodeId);
            totalRequestDelay.addAndGet(requestDelayInMillis);

            ListenableFuture<Optional<RateLimitInfo>> requestFuture;
            if (requestDelayInMillis == 0) {
                requestFuture = internalAddDataPages(bufferNodeId, exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition);
            }
            else {
                requestFuture = SettableFuture.create();
                ListenableScheduledFuture<Boolean> ignored = listeningScheduledExecutor.schedule(
                        () -> ((SettableFuture<Optional<RateLimitInfo>>) requestFuture).setFuture(internalAddDataPages(bufferNodeId, exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition)),
                        requestDelayInMillis,
                        MILLISECONDS);
            }

            SettableFuture<Void> resultFuture = SettableFuture.create();
            Futures.addCallback(requestFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(Optional<RateLimitInfo> rateLimitInfo)
                {
                    rateMonitor.updateRateLimitInfo(bufferNodeId, rateLimitInfo);
                    resultFuture.set(null);
                }

                @Override
                public void onFailure(Throwable failure)
                {
                    successRequestStopwatch.reset().start();
                    if ((failure instanceof DataApiException dataApiException)) {
                        rateMonitor.updateRateLimitInfo(bufferNodeId, dataApiException.getRateLimitInfo());
                        if (retry && requestPossiblyDelivered.get() && (dataApiException.getErrorCode() == ErrorCode.DRAINING || dataApiException.getErrorCode() == ErrorCode.DRAINED)) {
                            // If we are retrying we need to ensure that we do not propagate DRAINING error to user. We do not know if previous request
                            // was recorded by server or not. If we handle DRAINING, and send data to another buffer service node we may end up with
                            // duplicated data.
                            resultFuture.setException(new DataApiException(ErrorCode.DRAINING_ON_RETRY, "Received %s error code on retry".formatted(dataApiException.getErrorCode()), failure));
                            return;
                        }
                    }

                    requestPossiblyDelivered.compareAndSet(false, requestMayHaveAlreadyBeenDelivered(failure));
                    resultFuture.setException(failure);
                }
            }, directExecutor());

            return resultFuture;
        };
        return Futures.transform(
                runWithRetry(bufferNodeId, this::getAddDataPagesRetryExecutor, call),
                _ -> new AddDataPagesResponse(triesCount.intValue() - 1, stopwatch.elapsed(MILLISECONDS), successRequestStopwatch.elapsed(MILLISECONDS), totalRequestDelay.get()),
                directExecutor());
    }

    private ListenableFuture<AddDataPagesResponse> addDataPagesNewRateLimit(long bufferNodeId, String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        SettableFuture<AddDataPagesResponse> resultFuture = SettableFuture.create();
        PendingAddDataPagesRequest request = new PendingAddDataPagesRequest(bufferNodeId, exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition, resultFuture);

        enqueueAddDataPagesRequest(bufferNodeId, request)
                .ifPresent(state -> scheduleProcessAddDataPages(bufferNodeId, state));
        return resultFuture;
    }

    private Optional<AddDataPagesProcessingState> enqueueAddDataPagesRequest(long bufferNodeId, PendingAddDataPagesRequest request)
    {
        if (request.resultFuture.isDone()) {
            return Optional.empty();
        }
        AtomicBoolean rejected = new AtomicBoolean();
        AddDataPagesProcessingState state = addDataPagesProcessingStates.compute(bufferNodeId, (ignored, existing) -> {
            if (existing != null && existing.draining.get()) {
                rejected.set(true);
                return existing;
            }
            AddDataPagesProcessingState processingState = (existing != null) ? existing : new AddDataPagesProcessingState();
            request.enqueueTimeMillis.set(stopwatch.elapsed(MILLISECONDS));
            processingState.activeRequests.add(request);
            return processingState;
        });
        if (rejected.get()) {
            completeWithDrainedException(request);
            return Optional.empty();
        }
        return Optional.of(state);
    }

    private void tryProcessRequests(long bufferNodeId, AddDataPagesProcessingState state)
    {
        while (state.requestsProcessingRequested.get()) {
            if (!state.requestsBeingProcessed.compareAndSet(false, true)) {
                return;
            }
            state.requestsProcessingRequested.set(false);
            try {
                if (state.draining.get()) {
                    doDrainAddDataPages(state);
                }
                else {
                    doHandlePendingAddDataPages(bufferNodeId, state);
                }
            }
            finally {
                state.requestsBeingProcessed.set(false);
            }
        }
    }

    private void doHandlePendingAddDataPages(long bufferNodeId, AddDataPagesProcessingState state)
    {
        PendingAddDataPagesRequest pendingRequest;
        while ((pendingRequest = state.activeRequests.poll()) != null) {
            // drop completed requests
            if (pendingRequest.resultFuture.isDone()) {
                continue;
            }

            // bail out if too many requests
            if (state.inFlightRequests.get() >= maxConcurrentAddDataPagesPerNode) {
                state.activeRequests.addFirst(pendingRequest);
                return;
            }

            long nowMillis = stopwatch.elapsed(MILLISECONDS);
            long currentRequestIntervalMillis = rateMonitor.getCurrentIntervalMillis(bufferNodeId);
            long timeToWait = currentRequestIntervalMillis - (nowMillis - state.lastRequestStartedMillis.get());
            if (timeToWait > 0) {
                // put the request back at the head
                state.activeRequests.addFirst(pendingRequest);
                executor.schedule(() -> {
                    if (!state.requestsProcessingRequested.compareAndSet(false, true)) {
                        return;
                    }
                    tryProcessRequests(bufferNodeId, state);
                }, timeToWait, MILLISECONDS);
                return;
            }

            initiateAddDataPagesRequest(pendingRequest, state);
        }
    }

    private void doDrainAddDataPages(AddDataPagesProcessingState state)
    {
        state.backoffRequests.keySet().forEach(DataApiFacade::completeWithDrainedException);
        state.activeRequests.forEach(DataApiFacade::completeWithDrainedException);
        state.backoffRequests.clear();
        state.activeRequests.clear();
    }

    private void initiateAddDataPagesRequest(PendingAddDataPagesRequest pendingRequest, AddDataPagesProcessingState state)
    {
        state.inFlightRequests.incrementAndGet();
        state.lastRequestStartedMillis.set(stopwatch.elapsed(MILLISECONDS));

        Stopwatch currentRequestStopwatch = Stopwatch.createStarted();
        boolean retry = pendingRequest.tryCount.get() > 0;
        long now = stopwatch.elapsed(MILLISECONDS);
        if (!retry) {
            pendingRequest.firstRequestTimeMillis.set(now);
        }
        pendingRequest.cumulativeDelayMillis.addAndGet(now - pendingRequest.enqueueTimeMillis.get());

        ListenableFuture<Optional<RateLimitInfo>> requestFuture;
        requestFuture = internalAddDataPages(
                pendingRequest.bufferNodeId,
                pendingRequest.exchangeId,
                pendingRequest.taskId,
                pendingRequest.attemptId,
                pendingRequest.dataPagesId,
                pendingRequest.dataPagesByPartition);

        Futures.addCallback(requestFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(Optional<RateLimitInfo> rateLimitInfo)
            {
                state.inFlightRequests.decrementAndGet();
                addDataPagesStatsUpdater.onSuccess(currentRequestStopwatch.elapsed().toNanos());
                pendingRequest.resultFuture.set(new AddDataPagesResponse(
                        pendingRequest.tryCount.get(),
                        stopwatch.elapsed(MILLISECONDS) - pendingRequest.firstRequestTimeMillis.get(),
                        currentRequestStopwatch.elapsed(MILLISECONDS),
                        pendingRequest.cumulativeDelayMillis.get()));

                rateMonitor.updateRateLimitInfo(pendingRequest.bufferNodeId, rateLimitInfo);
                scheduleProcessAddDataPages(pendingRequest.bufferNodeId, state);
            }

            @Override
            public void onFailure(Throwable failure)
            {
                long requestTimeNanos = currentRequestStopwatch.elapsed().toNanos();
                state.inFlightRequests.decrementAndGet();
                if (pendingRequest.tryCount.get() + 1 > addDataPagesRetryExecutorConfig.maxRetries()) {
                    addDataPagesStatsUpdater.onFailure(failure, requestTimeNanos);
                    pendingRequest.resultFuture.setException(failure);
                    scheduleProcessAddDataPages(pendingRequest.bufferNodeId, state);
                    return;
                }

                if ((failure instanceof DataApiException dataApiException)) {
                    rateMonitor.updateRateLimitInfo(pendingRequest.bufferNodeId, dataApiException.getRateLimitInfo());
                    if (retry && pendingRequest.requestPossiblyDelivered.get() && (dataApiException.getErrorCode() == ErrorCode.DRAINING || dataApiException.getErrorCode() == ErrorCode.DRAINED)) {
                        // If we are retrying we need to ensure that we do not propagate DRAINING error to user. We do not know if previous request
                        // was recorded by server or not. If we handle DRAINING, and send data to another buffer service node we may end up with
                        // duplicated data.
                        addDataPagesStatsUpdater.onFailure(failure, requestTimeNanos);
                        pendingRequest.resultFuture.setException(new DataApiException(ErrorCode.DRAINING_ON_RETRY, "Received %s error code on retry".formatted(dataApiException.getErrorCode()), failure));
                        scheduleProcessAddDataPages(pendingRequest.bufferNodeId, state);
                        return;
                    }
                }
                addDataPagesStatsUpdater.onRetry(failure, requestTimeNanos);
                pendingRequest.requestPossiblyDelivered.compareAndSet(false, requestMayHaveAlreadyBeenDelivered(failure));
                int retryNumber = pendingRequest.tryCount.incrementAndGet();

                long backoffMillis = computeBackoffMillis(retryNumber);
                // requeue after backoff; route through the compute-based primitive so the re-enqueue is
                // serialized with cleanUp on the same bucket. If cleanUp completed the request with a
                // DRAINED error, enqueueAddDataPagesRequest returns Optional.empty() and nothing is resurrected.
                ScheduledFuture<?> backoffFuture = executor.schedule(() -> {
                    enqueueAddDataPagesRequest(pendingRequest.bufferNodeId, pendingRequest)
                            .ifPresent(current -> scheduleProcessAddDataPages(pendingRequest.bufferNodeId, current));
                    state.backoffRequests.remove(pendingRequest);
                }, backoffMillis, MILLISECONDS);
                state.backoffRequests.put(pendingRequest, backoffFuture);
                if (backoffFuture.isDone()) {
                    state.backoffRequests.remove(pendingRequest);
                }
                scheduleProcessAddDataPages(pendingRequest.bufferNodeId, state);
            }
        }, directExecutor());
    }

    private void finalizeResultFuture(PendingAddDataPagesRequest request, Consumer<SettableFuture<AddDataPagesResponse>> resultFutureConsumer)
    {
        resultFutureConsumer.accept(request.resultFuture);
    }

    private long computeBackoffMillis(int retryNumber)
    {
        long initialMillis = addDataPagesRetryExecutorConfig.backoffInitial().toMillis();
        long maxMillis = addDataPagesRetryExecutorConfig.backoffMax().toMillis();
        double factor = addDataPagesRetryExecutorConfig.backoffFactor();
        double jitter = addDataPagesRetryExecutorConfig.backoffJitter();

        long backoff = (long) (initialMillis * Math.pow(factor, retryNumber - 1));
        backoff = Math.min(backoff, maxMillis);
        if (jitter > 0) {
            backoff = (long) (backoff * (1.0 + ThreadLocalRandom.current().nextDouble(-jitter, jitter)));
            backoff = Math.max(backoff, 0);
        }
        return backoff;
    }

    private Future<?> scheduleProcessAddDataPages(long bufferNodeId, AddDataPagesProcessingState state)
    {
        if (!state.requestsProcessingRequested.compareAndSet(false, true)) {
            return immediateVoidFuture();
        }
        return executor.submit(() -> tryProcessRequests(bufferNodeId, state));
    }

    /**
     * Determine if given exception rules out that request sent to data server could have been consumed by it, even though client observed an error.
     */
    private boolean requestMayHaveAlreadyBeenDelivered(Throwable failure)
    {
        if (!(failure instanceof DataApiException dataApiException)) {
            return true;
        }

        return switch (dataApiException.getErrorCode()) {
            case DRAINING, USER_ERROR, BUFFER_NODE_NOT_FOUND, EXCHANGE_NOT_FOUND, CHUNK_NOT_FOUND, OVERLOADED, EXCHANGE_FINISHED, EXCHANGE_CORRUPTED, DRAINING_ON_RETRY, DRAINED -> false;
            case INTERNAL_ERROR -> true;
        };
    }

    public record AddDataPagesResponse(int retryCount, long processingTimeMillis, long successRequestTimeMillis, long rateLimitDelay) {}

    private ListenableFuture<Optional<RateLimitInfo>> internalAddDataPages(long bufferNodeId, String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        try {
            // short-circuiting DRAINING error code too. No point in sending request then.
            return getDataApi(bufferNodeId, true)
                    .addDataPages(exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> finishExchange(long bufferNodeId, String exchangeId)
    {
        return runWithRetry(bufferNodeId, () -> internalFinishExchange(bufferNodeId, exchangeId));
    }

    private ListenableFuture<Void> internalFinishExchange(long bufferNodeId, String exchangeId)
    {
        try {
            return getDataApi(bufferNodeId).finishExchange(exchangeId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<ChunkDataResponse> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId, long chunkBufferNodeId)
    {
        AtomicLong retryCount = new AtomicLong();
        Stopwatch stopwatch = Stopwatch.createStarted();
        Stopwatch successRequestStopwatch = Stopwatch.createStarted();
        CompletableFuture<DataApi.ChunkDataResponse> finalFuture = ((Function<Long, FailsafeExecutor<Object>>) this::getDefaultRetryExecutor).apply(bufferNodeId)
                .getAsyncExecution(execution -> {
                    ListenableFuture<DataApi.ChunkDataResponse> future = ((Callable<ListenableFuture<DataApi.ChunkDataResponse>>) () -> internalGetChunkData(bufferNodeId, exchangeId, partitionId, chunkId, chunkBufferNodeId)).call();
                    Futures.addCallback(future, new FutureCallback<>()
                    {
                        @Override
                        public void onSuccess(DataApi.ChunkDataResponse result)
                        {
                            execution.recordResult(result);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            execution.recordException(t);
                            retryCount.incrementAndGet();
                            successRequestStopwatch.reset().start();
                        }
                    }, directExecutor());
                });

        return Futures.transform(
                MoreFutures.toListenableFuture(finalFuture),
                dataApiResult -> new ChunkDataResponse(dataApiResult.pages(), dataApiResult.readFromSpoolingStorage(), retryCount.intValue(), stopwatch.elapsed(MILLISECONDS), successRequestStopwatch.elapsed(MILLISECONDS)),
                directExecutor());
    }

    public record ChunkDataResponse(List<DataPage> pages, boolean readFromSpoolingStorage, int retryCount, long processingTimeMillis, long successRequestTimeMillis)
    {
        public ChunkDataResponse
        {
            requireNonNull(pages, "pages is null");
            pages = ImmutableList.copyOf(pages);
        }
    }

    private ListenableFuture<DataApi.ChunkDataResponse> internalGetChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId, long chunkBufferNodeId)
    {
        try {
            return getDataApi(bufferNodeId).getChunkData(chunkBufferNodeId, exchangeId, partitionId, chunkId);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    private <T> ListenableFuture<T> runWithRetry(long bufferNodeId, Callable<ListenableFuture<T>> routine)
    {
        return runWithRetry(bufferNodeId, this::getDefaultRetryExecutor, routine);
    }

    private <T> ListenableFuture<T> runWithRetry(long bufferNodeId, Function<Long, FailsafeExecutor<Object>> retryExecutorProvider, Callable<ListenableFuture<T>> routine)
    {
        CompletableFuture<T> finalFuture = retryExecutorProvider.apply(bufferNodeId)
                .getAsyncExecution(execution -> {
                    ListenableFuture<T> future = routine.call();
                    Futures.addCallback(future, new FutureCallback<>()
                    {
                        @Override
                        public void onSuccess(T result)
                        {
                            execution.recordResult(result);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            execution.recordException(t);
                        }
                    }, directExecutor());
                });

        return MoreFutures.toListenableFuture(finalFuture);
    }

    private FailsafeExecutor<Object> getDefaultRetryExecutor(long bufferNodeId)
    {
        return defaultRetryExecutors.computeIfAbsent(bufferNodeId, ignored ->
                Failsafe.with(createDefaultRetryPolicy(), createDefaultCircuitBreakerPolicy(bufferNodeId))
                        .with(executor));
    }

    private FailsafeExecutor<Object> getAddDataPagesRetryExecutor(long bufferNodeId)
    {
        return addDataPagesRetryExecutors.computeIfAbsent(bufferNodeId, ignored ->
                Failsafe.with(createAddDataPagesRetryPolicy(), createAddDataPagesCircuitBreakerPolicy(bufferNodeId))
                        .with(executor));
    }

    private RetryPolicy<Object> createDefaultRetryPolicy()
    {
        return createRetryPolicy(defaultRetryExecutorConfig, Optional.empty());
    }

    private CircuitBreaker<Object> createDefaultCircuitBreakerPolicy(long bufferNodeId)
    {
        return createCircuitBreakerPolicy(bufferNodeId, defaultRetryExecutorConfig);
    }

    private RetryPolicy<Object> createAddDataPagesRetryPolicy()
    {
        return createRetryPolicy(addDataPagesRetryExecutorConfig, Optional.of(new AddDataPagesStatsUpdater(stats.getAddDataPagesOperationStats())));
    }

    private CircuitBreaker<Object> createAddDataPagesCircuitBreakerPolicy(long bufferNodeId)
    {
        return createCircuitBreakerPolicy(bufferNodeId, addDataPagesRetryExecutorConfig);
    }

    private static RetryPolicy<Object> createRetryPolicy(RetryExecutorConfig config, Optional<OperationLifecycleListener> lifecycleListener)
    {
        return RetryPolicy.builder()
                .withBackoff(
                        java.time.Duration.ofMillis(config.backoffInitial().toMillis()),
                        java.time.Duration.ofMillis(config.backoffMax().toMillis()),
                        config.backoffFactor())
                .withMaxRetries(config.maxRetries())
                .withJitter(config.backoffJitter())
                .onFailedAttempt(event -> {
                    rateLimitingLogger.warn(event.getLastException(), "failed DataApi request attempt (%s, %s, +%s)".formatted(event.getAttemptCount(), succinctDuration(event.getElapsedAttemptTime().toMillis(), MILLISECONDS), succinctDuration(event.getElapsedTime().toMillis(), MILLISECONDS)));
                    lifecycleListener.ifPresent(listener -> listener.onRetry(event.getLastException(), event.getElapsedAttemptTime().toNanos()));
                })
                .onFailure(event -> {
                    lifecycleListener.ifPresent(listener -> listener.onFailure(event.getException(), event.getElapsedAttemptTime().toNanos()));
                })
                .onSuccess(event -> {
                    lifecycleListener.ifPresent(listener -> listener.onSuccess(event.getElapsedAttemptTime().toNanos()));
                })
                .handleIf(throwable -> {
                    if (!(throwable instanceof DataApiException dataApiException)) {
                        return true;
                    }
                    return switch (dataApiException.getErrorCode()) {
                        case INTERNAL_ERROR, BUFFER_NODE_NOT_FOUND, OVERLOADED -> true;
                        default -> false;
                    };
                })
                .build();
    }

    private static CircuitBreaker<Object> createCircuitBreakerPolicy(long bufferNodeId, RetryExecutorConfig config)
    {
        return CircuitBreaker.builder()
                .withFailureThreshold(config.circuitBreakerFailureThreshold())
                .withSuccessThreshold(config.circuitBreakerSuccessThreshold())
                .withDelay(java.time.Duration.ofMillis(config.circuitBreakerDelay().toMillis()))
                .onOpen(event -> log.warn("switching circuit breaker for %s %s -> OPEN", bufferNodeId, event.getPreviousState()))
                .onClose(event -> log.info("switching circuit breaker for %s %s -> CLOSE", bufferNodeId, event.getPreviousState()))
                .onHalfOpen(event -> log.info("switching circuit breaker for %s %s -> HALF_OPEN", bufferNodeId, event.getPreviousState()))
                .handleIf(throwable -> {
                    if (!(throwable instanceof DataApiException dataApiException)) {
                        return true;
                    }
                    return switch (dataApiException.getErrorCode()) {
                        case INTERNAL_ERROR, OVERLOADED -> true;
                        default -> false;
                    };
                })
                .build();
    }

    private DataApi getDataApi(long bufferNodeId)
    {
        return getDataApi(bufferNodeId, false);
    }

    private DataApi getDataApi(long bufferNodeId, boolean shortCircuitDraining)
    {
        BufferNodeDiscoveryManager.BufferNodesState bufferNodes = discoveryManager.getBufferNodes();
        BufferNodeInfo bufferNodeInfo = bufferNodes.getAllBufferNodes().get(bufferNodeId);
        if (bufferNodeInfo != null && bufferNodeInfo.state() == BufferNodeState.DRAINED) {
            // Node already DRAINED according to DiscoveryService. Short-circuiting error code.
            throw new DataApiException(ErrorCode.DRAINED, format("Node %s (%s) already DRAINED", bufferNodeId, bufferNodeInfo.uri()));
        }
        if (shortCircuitDraining && bufferNodeInfo != null && bufferNodeInfo.state() == BufferNodeState.DRAINING) {
            // Node already started DRAINING according to DiscoveryService. Short-circuiting error code.
            throw new DataApiException(ErrorCode.DRAINING, format("Node %s (%s) is DRAINING", bufferNodeId, bufferNodeInfo.uri()));
        }
        return dataApiClients.computeIfAbsent(bufferNodeId, this::createDataApi);
    }

    private DataApi createDataApi(long bufferNodeId)
    {
        BufferNodeInfo bufferNodeInfo = discoveryManager.getBufferNodes().getAllBufferNodes().get(bufferNodeId);
        if (bufferNodeInfo == null) {
            // we may be not up-to-date so force refresh
            discoveryManager.forceRefresh();
            // todo: for created clients periodically check if node is still around
            throw new DataApiException(ErrorCode.BUFFER_NODE_NOT_FOUND, "Buffer node " + bufferNodeId + " not found");
        }
        return createDataApi(bufferNodeInfo);
    }

    private DataApi createDataApi(BufferNodeInfo bufferNodeInfo)
    {
        return apiFactory.createDataApi(bufferNodeInfo);
    }

    private interface OperationLifecycleListener
    {
        void onRetry(Throwable retryReason, long requestTimeNanos);

        void onFailure(Throwable failureReason, long requestTimeNanos);

        void onSuccess(long requestTimeNanos);
    }

    private static class AddDataPagesStatsUpdater
            implements OperationLifecycleListener
    {
        private final DataApiFacadeStats.AddDataPagesOperationStats stats;

        public AddDataPagesStatsUpdater(DataApiFacadeStats.AddDataPagesOperationStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public void onRetry(Throwable retryReason, long requestTimeNanos)
        {
            updateRequestErrorStats(retryReason, requestTimeNanos);
            stats.getRequestRetryCount().update(1);
        }

        @Override
        public void onFailure(Throwable failureReason, long requestTimeNanos)
        {
            updateRequestErrorStats(failureReason, requestTimeNanos);
            stats.getFailedOperationCount().update(1);
        }

        private void updateRequestErrorStats(Throwable retryReason, long requestTimeNanos)
        {
            stats.getAnyRequestErrorCount().update(1);
            if (retryReason instanceof DataApiException dataApiException && dataApiException.getErrorCode() == ErrorCode.OVERLOADED) {
                stats.getOverloadedRequestErrorCount().update(1);
            }
            else if (retryReason instanceof CircuitBreakerOpenException) {
                stats.getCircuitBreakerOpenRequestErrorCount().update(1);
            }

            if (!(retryReason instanceof CircuitBreakerOpenException)) {
                // skip time statistics if did not make a request
                stats.getFailedRequestTime().addNanos(requestTimeNanos);
            }
        }

        @Override
        public void onSuccess(long requestTimeNanos)
        {
            stats.getSuccessOperationCount().update(1);
            stats.getSuccessfulRequestTime().addNanos(requestTimeNanos);
        }
    }

    private static final class PendingAddDataPagesRequest
    {
        public final long bufferNodeId;
        public final String exchangeId;
        public final int taskId;
        public final int attemptId;
        public final long dataPagesId;
        public final ListMultimap<Integer, Slice> dataPagesByPartition;
        public final AtomicLong enqueueTimeMillis;
        public final AtomicLong firstRequestTimeMillis;
        public final AtomicInteger tryCount;
        public final AtomicLong cumulativeDelayMillis;
        public final AtomicBoolean requestPossiblyDelivered;
        public final SettableFuture<AddDataPagesResponse> resultFuture;

        public PendingAddDataPagesRequest(long bufferNodeId, String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition, SettableFuture<AddDataPagesResponse> resultFuture)
        {
            this.bufferNodeId = bufferNodeId;
            this.exchangeId = exchangeId;
            this.taskId = taskId;
            this.attemptId = attemptId;
            this.dataPagesId = dataPagesId;
            this.dataPagesByPartition = dataPagesByPartition;
            this.enqueueTimeMillis = new AtomicLong();
            this.firstRequestTimeMillis = new AtomicLong();
            this.tryCount = new AtomicInteger();
            this.cumulativeDelayMillis = new AtomicLong();
            this.requestPossiblyDelivered = new AtomicBoolean();
            this.resultFuture = resultFuture;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bufferNodeId", bufferNodeId)
                    .add("exchangeId", exchangeId)
                    .add("taskId", taskId)
                    .add("attemptId", attemptId)
                    .add("dataPagesId", dataPagesId)
                    .add("dataPagesByPartition", dataPagesByPartition)
                    .add("enqueueTimeMillis", enqueueTimeMillis)
                    .add("firstRequestTimeMillis", firstRequestTimeMillis)
                    .add("tryCount", tryCount)
                    .add("cumulativeDelayMillis", cumulativeDelayMillis)
                    .add("requestPossiblyDelivered", requestPossiblyDelivered)
                    .add("resultFuture", resultFuture)
                    .toString();
        }
    }

    private static final class AddDataPagesProcessingState
    {
        public final ConcurrentLinkedDeque<PendingAddDataPagesRequest> activeRequests = new ConcurrentLinkedDeque<>();
        public final Map<PendingAddDataPagesRequest, Future<?>> backoffRequests = new ConcurrentHashMap<>();
        public final AtomicInteger inFlightRequests = new AtomicInteger();
        public final AtomicBoolean requestsProcessingRequested = new AtomicBoolean();
        public final AtomicBoolean requestsBeingProcessed = new AtomicBoolean();
        public final AtomicLong lastRequestStartedMillis = new AtomicLong();
        public final AtomicBoolean draining = new AtomicBoolean();

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("activeRequests", activeRequests)
                    .add("backoffRequests", backoffRequests)
                    .add("inFlightRequests", inFlightRequests)
                    .add("requestsProcessingRequested", requestsProcessingRequested)
                    .add("requestsBeingProcessed", requestsBeingProcessed)
                    .add("lastRequestStartedMillis", lastRequestStartedMillis)
                    .add("draining", draining)
                    .toString();
        }
    }
}
