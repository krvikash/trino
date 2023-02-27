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
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.addCallback;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
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

    public ChunkHandlesPoller(
            ScheduledExecutorService executorService,
            String externalExchangeId,
            DataApiFacade dataApi,
            long bufferNodeId,
            ChunksCallback callback)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.externalExchangeId = requireNonNull(externalExchangeId, "externalExchangeId is null");
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.dataNodeId = bufferNodeId;
        this.callback = requireNonNull(callback, "callback is null");
    }

    public void start()
    {
        addCallback(dataApi.registerExchange(dataNodeId, externalExchangeId), new FutureCallback<>()
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
                registerFuture.setException(failure);
                callback.onFailure(failure);
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

    private void doPollOrPing()
    {
        try {
            if (closed) {
                return;
            }

            if (pinging) {
                ListenableFuture<Void> pingFuture = dataApi.pingExchange(dataNodeId, externalExchangeId);
                addCallback(pingFuture, new FutureCallback<>()
                {
                    @Override
                    public void onSuccess(Void result)
                    {
                        try {
                            if (closed) {
                                return;
                            }

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
                        callback.onFailure(failure);
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
                    callback.onFailure(failure);
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
                        callback.onFailure(failure);
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

    public interface ChunksCallback
    {
        void onChunksDiscovered(List<ChunkHandle> chunks, boolean noMoreChunks);

        void onFailure(Throwable failure);
    }
}
