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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.client.BufferNodeExchangeMetrics;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.trino.spi.TrinoException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.Futures.addCallback;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.starburst.stargate.buffer.trino.exchange.BufferServiceExchangeErrorCode.COMMUNICATION_FAILURE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class ChunkHandlesPoller
{
    private static final Logger log = Logger.get(ChunkHandlesPoller.class);
    private static final long PING_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private final ScheduledExecutorService executorService;
    private final String externalExchangeId;
    private final DataApiFacade dataApi;
    private final long dataNodeId;
    private final ChunksCallback callback;
    private final SettableFuture<Void> registerFuture = SettableFuture.create();
    private volatile OptionalLong pagingId = OptionalLong.empty();
    private volatile boolean closed;
    private volatile boolean pinging;
    private volatile ChunkDeliveryMode chunkDeliveryMode;
    private final Span exchangeSpan;

    private AtomicInteger requestCounter = new AtomicInteger();
    private AtomicReference<PollOrPingRequestContext> lastPollOrPingRequest = new AtomicReference<>();

    public ChunkHandlesPoller(
            ScheduledExecutorService executorService,
            String externalExchangeId,
            DataApiFacade dataApi,
            long bufferNodeId,
            ChunkDeliveryMode chunkDeliveryMode,
            Span exchangeSpan,
            ChunksCallback callback)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.externalExchangeId = requireNonNull(externalExchangeId, "externalExchangeId is null");
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.dataNodeId = bufferNodeId;
        this.chunkDeliveryMode = requireNonNull(chunkDeliveryMode, "chunkDeliveryMode is null");
        this.exchangeSpan = requireNonNull(exchangeSpan, "exchangeSpan is null");
        this.callback = requireNonNull(callback, "callback is null");
    }

    public void start()
    {
        addCallback(dataApi.registerExchange(dataNodeId, externalExchangeId, chunkDeliveryMode, exchangeSpan), new FutureCallback<>()
        {
            @Override
            public void onSuccess(Void result)
            {
                try {
                    registerFuture.set(null);
                    startPolling();
                }
                catch (Throwable t) {
                    onFailure(t);
                }
            }

            @Override
            public void onFailure(Throwable failure)
            {
                TrinoException trinoException = new TrinoException(
                        COMMUNICATION_FAILURE,
                        "Error registering exchange %s in data node %s".formatted(externalExchangeId, dataNodeId),
                        failure);
                registerFuture.setException(trinoException);
                callback.onFailure(trinoException);
            }
        }, executorService);
    }

    public void stop()
    {
        closed = true;
    }

    private void startPolling()
    {
        executorService.execute(this::doPollOrPing);
    }

    private class PollOrPingRequestContext
    {
        private final int requestCounter;
        private final Instant requestStart = Instant.now();
        private Duration requestDuration;
        private boolean finished;

        public PollOrPingRequestContext(int requestCounter)
        {
            this.requestCounter = requestCounter;
        }

        public void finish()
        {
            if (finished) {
                log.warn("poolOrPing request already finished %s", this);
                return;
            }
            finished = true;

            requestDuration = Duration.between(requestStart, Instant.now());
            ChunkHandlesPoller.this.lastPollOrPingRequest.set(this);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("requestCounter", requestCounter)
                    .add("requestStart", requestStart)
                    .add("requestDuration", requestDuration)
                    .toString();
        }
    }

    private PollOrPingRequestContext startPollOrPingRequest()
    {
        return new PollOrPingRequestContext(requestCounter.getAndIncrement());
    }

    private void doPollOrPing()
    {
        try {
            if (closed) {
                return;
            }

            PollOrPingRequestContext currentRequestContext = startPollOrPingRequest();

            if (pinging) {
                ListenableFuture<BufferNodeExchangeMetrics> pingFuture = dataApi.pingExchange(dataNodeId, externalExchangeId);
                addCallback(pingFuture, new FutureCallback<>()
                {
                    @Override
                    public void onSuccess(BufferNodeExchangeMetrics metrics)
                    {
                        try {
                            currentRequestContext.finish();
                            if (closed) {
                                return;
                            }
                            callback.onMetricsDiscovered(metrics);
                            executorService.schedule(ChunkHandlesPoller.this::doPollOrPing, PING_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
                        }
                        catch (Throwable t) {
                            // fallback to onFailure
                            onFailure(t);
                        }
                    }

                    @Override
                    public void onFailure(Throwable failure)
                    {
                        if (failure instanceof DataApiException dataApiException) {
                            ErrorCode errorCode = dataApiException.getErrorCode();
                            if (errorCode == ErrorCode.EXCHANGE_NOT_FOUND || errorCode == ErrorCode.DRAINING || errorCode == ErrorCode.DRAINED) {
                                // ignore
                                return;
                            }
                        }
                        TrinoException trinoException = new TrinoException(
                                COMMUNICATION_FAILURE,
                                "Error pinging exchange %s in data node %s".formatted(externalExchangeId, dataNodeId),
                                failure);
                        trinoException.addSuppressed(new RuntimeException("poller state: " + ChunkHandlesPoller.this));
                        callback.onFailure(trinoException);
                    }
                }, executorService);
                return;
            }

            ListenableFuture<ChunkList> pollFuture = dataApi.listClosedChunks(dataNodeId, externalExchangeId, pagingId);
            addCallback(pollFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(ChunkList result)
                {
                    try {
                        currentRequestContext.finish();
                        if (closed) {
                            return;
                        }

                        boolean noMoreChunks = result.nextPagingId().isEmpty();
                        if (!result.chunks().isEmpty() || noMoreChunks) {
                            callback.onChunksDiscovered(result.chunks(), noMoreChunks);
                        }
                        pagingId = result.nextPagingId();

                        if (noMoreChunks) {
                            markAllClosedChunksReceived();
                            pinging = true;
                        }

                        // Repeat request
                        doPollOrPing();
                    }
                    catch (Throwable t) {
                        // fallback to onFailure
                        onFailure(t);
                    }
                }

                @Override
                public void onFailure(Throwable failure)
                {
                    TrinoException trinoException = new TrinoException(
                            COMMUNICATION_FAILURE,
                            "Error listing closed chunks exchange %s in data node %s".formatted(externalExchangeId, dataNodeId),
                            failure);
                    trinoException.addSuppressed(new RuntimeException("poller state: " + ChunkHandlesPoller.this));
                    callback.onFailure(trinoException);
                }
            }, executorService);
        }
        catch (Throwable failure) {
            callback.onFailure(failure);
        }
    }

    public void markExchangeFinished()
    {
        addSuccessCallback(
                // wait until exchange is registered before sending finish
                registerFuture,
                () -> {
                    ListenableFuture<Void> finishFuture = dataApi.finishExchange(dataNodeId, externalExchangeId);
                    addExceptionCallback(finishFuture, failure -> {
                        if (failure instanceof DataApiException dataApiException) {
                            if (dataApiException.getErrorCode() == ErrorCode.DRAINED) {
                                // ignore - node gone during query runtime
                                return;
                            }
                        }
                        TrinoException trinoException = new TrinoException(
                                COMMUNICATION_FAILURE,
                                "Error marking exchange finished %s in data node %s".formatted(externalExchangeId, dataNodeId),
                                failure);
                        callback.onFailure(trinoException);
                    });
                },
                executorService);
    }

    private void markAllClosedChunksReceived()
    {
        addSuccessCallback(
                // wait until exchange is registered before sending acknowledgement on all closed chunks
                registerFuture,
                () -> {
                    ListenableFuture<Void> markAllClosedChunksReceivedFuture = dataApi.markAllClosedChunksReceived(dataNodeId, externalExchangeId);
                    addExceptionCallback(markAllClosedChunksReceivedFuture,
                            failure -> {
                                if (failure instanceof DataApiException dataApiException && dataApiException.getErrorCode() == ErrorCode.DRAINED) {
                                    // ignore - node gone in the meantime
                                    return;
                                }
                                log.warn("Failed to mark all closed chunks received for externalExchangeId %s dataNodeId %d", externalExchangeId, dataNodeId);
                            });
                },
                executorService);
    }

    public void setChunkDeliveryMode(ChunkDeliveryMode chunkDeliveryMode)
    {
        this.chunkDeliveryMode = chunkDeliveryMode;
        addSuccessCallback(
                // wait until exchange is registered before sending acknowledgement on all closed chunks
                registerFuture,
                () -> {
                    ListenableFuture<Void> future = dataApi.setChunkDeliveryMode(dataNodeId, externalExchangeId, chunkDeliveryMode);
                    addExceptionCallback(future,
                            failure -> {
                                if (failure instanceof DataApiException dataApiException && dataApiException.getErrorCode() == ErrorCode.DRAINED) {
                                    // ignore - node gone in the meantime
                                    return;
                                }
                                log.warn("Failed to set chunk delivery mode for externalExchangeId %s dataNodeId %d", externalExchangeId, dataNodeId);
                            });
                },
                executorService);
    }

    public interface ChunksCallback
    {
        void onChunksDiscovered(List<ChunkHandle> chunks, boolean noMoreChunks);

        void onMetricsDiscovered(BufferNodeExchangeMetrics metrics);

        void onFailure(Throwable failure);
    }

    @Override
    // for debugging
    public String toString()
    {
        return toStringHelper(this)
                .add("externalExchangeId", externalExchangeId)
                .add("dataNodeId", dataNodeId)
                .add("pagingId", pagingId)
                .add("closed", closed)
                .add("pinging", pinging)
                .add("chunkDeliveryMode", chunkDeliveryMode)
                .add("requestCounter", requestCounter)
                .add("lastPollOrPingRequest", lastPollOrPingRequest.get())
                .toString();
    }
}
