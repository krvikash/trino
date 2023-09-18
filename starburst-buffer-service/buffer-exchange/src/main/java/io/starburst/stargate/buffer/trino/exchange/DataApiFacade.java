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
import com.google.common.base.Ticker;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.RateLimitInfo;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DataApiFacade
{
    private static final Logger log = Logger.get(DataApiFacade.class);
    private static final boolean ENABLE_LOG_RATE_LIMITING = true;
    private static final RateLimitingLogger rateLimitingLogger = new RateLimitingLogger(log, ENABLE_LOG_RATE_LIMITING);

    private static final Duration CLEANUP_DELAY = succinctDuration(5, TimeUnit.MINUTES);

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

    private final DataApiFacadeStats.AddDataPagesOperationStats addDataPagesStats = new DataApiFacadeStats.AddDataPagesOperationStats();

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
                executor);
    }

    @VisibleForTesting
    DataApiFacade(
            BufferNodeDiscoveryManager discoveryManager,
            ApiFactory apiFactory,
            DataApiFacadeStats stats,
            RetryExecutorConfig defaultRetryExecutorConfig,
            RetryExecutorConfig addDataPagesRetryExecutorConfig,
            ScheduledExecutorService executor)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.defaultRetryExecutorConfig = requireNonNull(defaultRetryExecutorConfig, "defaultRetryExecutorConfig is null");
        this.addDataPagesRetryExecutorConfig = requireNonNull(addDataPagesRetryExecutorConfig, "addDataPagesRetryExecutorConfig is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.listeningScheduledExecutor = listeningDecorator(executor);
        this.rateMonitor = new RateMonitor(Ticker.systemTicker());
    }

    @PostConstruct
    private void init()
    {
        ScheduledFuture<?> future = this.executor.scheduleWithFixedDelay(() -> {
            try {
                cleanUp();
            }
            catch (Exception e) {
                // catch all so we are not unscheduled
                log.error(e, "Unexpected error caught in cleanUp");
            }
        }, CLEANUP_DELAY.toMillis(), CLEANUP_DELAY.toMillis(), TimeUnit.MILLISECONDS);
        destroyCloser.register(() -> future.cancel(true));
    }

    @PreDestroy
    private void destroy()
    {
        try {
            destroyCloser.close();
        }
        catch (IOException e) {
            log.error(e, "Unexpected error in destroy");
        }
    }

    private void cleanUp()
    {
        BufferNodeDiscoveryManager.BufferNodesState bufferNodes = discoveryManager.getBufferNodes();

        Predicate<Long> isBufferNodeStale = bufferNodeId -> {
            BufferNodeInfo bufferNodeInfo = bufferNodes.getAllBufferNodes().get(bufferNodeId);
            return bufferNodeInfo == null || bufferNodeInfo.state() == BufferNodeState.DRAINED;
        };

        Set<Long> staleDataApiBufferNodeIds = dataApiClients.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.info("cleaning up stale dataApi clients for buffer nodes %s", staleDataApiBufferNodeIds);
        staleDataApiBufferNodeIds.forEach(dataApiClients::remove);

        Set<Long> staleDefaultRetryExecutors = defaultRetryExecutors.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.info("cleaning up stale retry executors for buffer nodes %s", staleDefaultRetryExecutors);
        staleDefaultRetryExecutors.forEach(defaultRetryExecutors::remove);

        Set<Long> staleAddDataPagesRetryExecutors = addDataPagesRetryExecutors.keySet().stream()
                .filter(isBufferNodeStale)
                .collect(toImmutableSet());
        log.info("cleaning up stale add data pages retry executors for buffer nodes %s", staleAddDataPagesRetryExecutors);
        staleAddDataPagesRetryExecutors.forEach(addDataPagesRetryExecutors::remove);

        rateMonitor.cleanUp(isBufferNodeStale);
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

    public ListenableFuture<Void> registerExchange(long bufferNodeId, String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
    {
        return runWithRetry(bufferNodeId, () -> internalRegisterExchange(bufferNodeId, exchangeId, chunkDeliveryMode));
    }

    private ListenableFuture<Void> internalRegisterExchange(long bufferNodeId, String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
    {
        try {
            return getDataApi(bufferNodeId).registerExchange(exchangeId, chunkDeliveryMode);
        }
        catch (Throwable e) {
            // wrap exception in the future
            return Futures.immediateFailedFuture(e);
        }
    }

    public ListenableFuture<Void> pingExchange(long bufferNodeId, String exchangeId)
    {
        return runWithRetry(bufferNodeId, () -> internalPingExchange(bufferNodeId, exchangeId));
    }

    private ListenableFuture<Void> internalPingExchange(long bufferNodeId, String exchangeId)
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

    public ListenableFuture<Void> addDataPages(long bufferNodeId, String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        AtomicBoolean retryFlag = new AtomicBoolean();
        Callable<ListenableFuture<Void>> call = () -> {
            boolean retry = retryFlag.getAndSet(true);
            long requestDelayInMillis = rateMonitor.registerExecutionSchedule(bufferNodeId);

            ListenableFuture<Optional<RateLimitInfo>> requestFuture;
            if (requestDelayInMillis == 0) {
                requestFuture = internalAddDataPages(bufferNodeId, exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition);
            }
            else {
                requestFuture = SettableFuture.create();
                listeningScheduledExecutor.schedule(
                        () -> ((SettableFuture<Optional<RateLimitInfo>>) requestFuture).setFuture(internalAddDataPages(bufferNodeId, exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition)),
                        requestDelayInMillis,
                        TimeUnit.MILLISECONDS);
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
                    if ((failure instanceof DataApiException dataApiException)) {
                        rateMonitor.updateRateLimitInfo(bufferNodeId, dataApiException.getRateLimitInfo());
                        if (retry && (dataApiException.getErrorCode() == ErrorCode.DRAINING || dataApiException.getErrorCode() == ErrorCode.DRAINED)) {
                            // If we are retrying we need to ensure that we do not propagate DRAINING error to user. We do not know if previous request
                            // was recorded by server or not. If we handle DRAINING, and send data to another buffer service node we may end up with
                            // duplicated data.
                            resultFuture.setException(new DataApiException(ErrorCode.DRAINING_ON_RETRY, "Received %s error code on retry".formatted(dataApiException.getErrorCode()), failure));
                            return;
                        }
                    }
                    resultFuture.setException(failure);
                }
            }, directExecutor());

            return resultFuture;
        };
        return runWithRetry(bufferNodeId, this::getAddDataPagesRetryExecutor, call);
    }

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

    public ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId, long chunkBufferNodeId)
    {
        return runWithRetry(bufferNodeId, () -> internalGetChunkData(bufferNodeId, exchangeId, partitionId, chunkId, chunkBufferNodeId));
    }

    private ListenableFuture<List<DataPage>> internalGetChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId, long chunkBufferNodeId)
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
                    rateLimitingLogger.warn(event.getLastException(), "failed DataApi request attempt (%s, +%s)".formatted(event.getAttemptCount(), succinctDuration(event.getElapsedTime().toMillis(), TimeUnit.MILLISECONDS)));
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
            throw new DataApiException(ErrorCode.DRAINED, "Node already DRAINED");
        }
        if (shortCircuitDraining && bufferNodeInfo != null && bufferNodeInfo.state() == BufferNodeState.DRAINING) {
            // Node already started DRAINING according to DiscoveryService. Short-circuiting error code.
            throw new DataApiException(ErrorCode.DRAINING, "Node is DRAINING");
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
}
