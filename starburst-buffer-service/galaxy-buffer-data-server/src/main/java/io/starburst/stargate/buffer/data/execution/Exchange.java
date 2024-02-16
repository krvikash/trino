/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_CORRUPTED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_FINISHED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class Exchange
{
    private static final Logger log = Logger.get(Exchange.class);

    private final long bufferNodeId;
    private final String exchangeId;
    private final ExchangeStateMachine exchangeStateMachine;
    private final AtomicReference<Optional<Span>> bufferServerExchangeSpan = new AtomicReference<>(Optional.empty());
    private final MemoryAllocator memoryAllocator;
    private final SpoolingStorage spoolingStorage;
    private final SpooledChunksByExchange spooledChunksByExchange;
    private final int chunkTargetSizeInBytes;
    private final int chunkMaxSizeInBytes;
    private final int chunkSliceSizeInBytes;
    private final int chunkListTargetSize;
    private final int chunkListMaxSize;
    private final Duration chunkListPollTimeout;
    private final boolean calculateDataPagesChecksum;
    private final ChunkIdGenerator chunkIdGenerator;
    private final ExecutorService executor;

    // partitionId -> partition
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService longPollTimeoutExecutor;
    private final Tracer tracer;
    // FAILURE_MESSAGE is also specified in trino-main.TrinoAttirbutes, but we want to avoid this dependency
    private static final AttributeKey<String> FAILURE_MESSAGE = stringKey("failure");

    // temporary queue for newly closed chunks to allow de-synchronization of code which closes chunks
    // and code which handles polling for newly closed chunks to return those to the user
    private final Deque<ChunkHandle> recentlyClosedChunks = new ConcurrentLinkedDeque<>();
    // we track the number of recently closed chunks and decrement it only after those are moved to pendingChunkList.
    // this is needed, so we are sure we return all the closed chunks to the user in nextChunkList() before signaling
    // that there will no more.
    private final AtomicInteger recentlyClosedChunksCount = new AtomicInteger();
    @GuardedBy("this")
    private OptionalLong nextPagingId = OptionalLong.of(0);
    @GuardedBy("this")
    private ChunkList lastChunkList;
    @GuardedBy("this")
    private SettableFuture<ChunkList> pendingChunkListFuture;
    @GuardedBy("this")
    private List<ChunkHandle> pendingChunkList;
    @GuardedBy("this")
    private long lastPagingId = -1;
    @GuardedBy("this")
    private ListenableFuture<Void> finishFuture;

    private volatile ChunkDeliveryMode chunkDeliveryMode;
    private volatile long lastUpdateTime;
    private volatile boolean allClosedChunksReceived;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean spooled;

    public Exchange(
            long bufferNodeId,
            String exchangeId,
            ExchangeState initialState,
            MemoryAllocator memoryAllocator,
            SpoolingStorage spoolingStorage,
            SpooledChunksByExchange spooledChunksByExchange,
            int chunkTargetSizeInBytes,
            int chunkMaxSizeInBytes,
            int chunkSliceSizeInBytes,
            boolean calculateDataPagesChecksum,
            int chunkListTargetSize,
            int chunkListMaxSize,
            Duration chunkListPollTimeout,
            ChunkIdGenerator chunkIdGenerator,
            ChunkDeliveryMode chunkDeliveryMode,
            ExecutorService executor,
            ScheduledExecutorService longPollTimeoutExecutor,
            Tracer tracer,
            long currentTime)
    {
        this.bufferNodeId = bufferNodeId;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingStorage = requireNonNull(spoolingStorage, "spoolingStorage is null");
        this.spooledChunksByExchange = requireNonNull(spooledChunksByExchange, "spooledChunksByExchange is null");
        checkArgument(chunkTargetSizeInBytes <= chunkMaxSizeInBytes, "chunkTargetSizeInBytes %s larger than chunkMaxSizeInBytes %s", chunkTargetSizeInBytes, chunkMaxSizeInBytes);
        this.chunkTargetSizeInBytes = chunkTargetSizeInBytes;
        this.chunkMaxSizeInBytes = chunkMaxSizeInBytes;
        this.chunkSliceSizeInBytes = chunkSliceSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        checkArgument(chunkListTargetSize >= 0, "chunkListTargetSize is less than 0");
        this.chunkListTargetSize = chunkListTargetSize;
        checkArgument(chunkListMaxSize >= chunkListTargetSize, "chunkListMaxSize is less than chunkListTargetSize");
        this.chunkListMaxSize = chunkListMaxSize;
        this.chunkListPollTimeout = chunkListPollTimeout;
        this.chunkIdGenerator = requireNonNull(chunkIdGenerator, "chunkIdGenerator is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.longPollTimeoutExecutor = requireNonNull(longPollTimeoutExecutor, "longPollTimeoutExecutor is null");
        this.pendingChunkList = new ArrayList<>(chunkListTargetSize);
        this.chunkDeliveryMode = requireNonNull(chunkDeliveryMode, "chunkDeliveryMode is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.lastUpdateTime = currentTime;

        // The directExecutor can be used because StateChangeListeners will only perform complex tasks.
        // As they may generate span events, there will be the potential for lock contention but it should be minimal.
        this.exchangeStateMachine = new ExchangeStateMachine(exchangeId, initialState, directExecutor());
    }

    public AddDataPagesResult addDataPages(int partitionId, int taskId, int attemptId, long dataPagesId, List<Slice> pages)
    {
        throwIfFailed();

        Partition partition;
        synchronized (this) {
            if (finishFuture != null) {
                throw new DataServerException(EXCHANGE_FINISHED, "exchange %s already finished".formatted(exchangeId));
            }
            partition = partitions.computeIfAbsent(partitionId, ignored -> new Partition(
                    bufferNodeId,
                    exchangeId,
                    partitionId,
                    memoryAllocator,
                    spoolingStorage,
                    spooledChunksByExchange,
                    chunkTargetSizeInBytes,
                    chunkMaxSizeInBytes,
                    chunkSliceSizeInBytes,
                    calculateDataPagesChecksum,
                    chunkIdGenerator,
                    chunkDeliveryMode,
                    executor,
                    closedChunkConsumer()));
        }

        exchangeStateMachine.sourceStreaming();

        AddDataPagesResult addDataPagesResult = partition.addDataPages(taskId, attemptId, dataPagesId, pages);
        addExceptionCallback(addDataPagesResult.addDataPagesFuture(), throwable -> {
            if (failure.compareAndSet(null, throwable)) {
                exchangeStateMachine.transitionToFailed();
                this.releaseChunks();
                bufferServerExchangeSpan.get().ifPresent(span -> span.addEvent("buffer_failure", Attributes.of(FAILURE_MESSAGE, throwable.getMessage())));
            }
        }, executor);
        return addDataPagesResult;
    }

    public ChunkDataResult getChunkData(long bufferNodeId, int partitionId, long chunkId, boolean chunkSpoolMergeEnabled)
    {
        throwIfFailed();

        exchangeStateMachine.sinkStreaming();
        Partition partition = partitions.get(partitionId);
        if (partition == null) {
            throw new DataServerException(CHUNK_NOT_FOUND, "partition %d not found for exchange %s".formatted(partitionId, exchangeId));
        }
        return partition.getChunkData(bufferNodeId, chunkId, chunkSpoolMergeEnabled);
    }

    private Consumer<ChunkHandle> closedChunkConsumer()
    {
        return handle -> {
            requireNonNull(handle, "ChunkHandle passed to consumer is null");
            // record closed chunk without synchronized section; routine returned by closedChunkConsumer()
            // can be called in arbitrary synchronized section and taking monitor here may lead to a deadlocks
            recentlyClosedChunksCount.incrementAndGet();
            recentlyClosedChunks.add(handle);
            // schedule a task to handle newly recorded closed chunks asynchronousle
            executor.submit(() -> {
                try {
                    handleRecentlyClosedChunks();
                }
                catch (Exception e) {
                    log.error(e, "Unexpected error in handleRecentlyClosedChunks");
                }
            });
        };
    }

    private void handleRecentlyClosedChunks()
    {
        if (recentlyClosedChunks.isEmpty()) {
            return;
        }
        SettableFuture<ChunkList> future;
        ChunkList chunkList = null;
        synchronized (this) {
            while (true) {
                // move all recently closed chunks to pendingChunkList
                ChunkHandle handle = recentlyClosedChunks.poll();
                if (handle == null) {
                    break;
                }
                pendingChunkList.add(handle);
                recentlyClosedChunksCount.decrementAndGet();
            }

            future = pendingChunkListFuture;
            if (pendingChunkListFuture != null && pendingChunkList.size() >= chunkListTargetSize) {
                chunkList = nextChunkList(nextPagingId.orElseThrow());
                pendingChunkListFuture = null;
            }
        }
        // Avoid a deadlock from triggered callbacks by doing this outside `synchronized`
        if (chunkList != null) {
            future.set(chunkList);
        }
    }

    public synchronized ListenableFuture<ChunkList> listClosedChunks(OptionalLong pagingIdOptional)
    {
        throwIfFailed();

        long pagingId = pagingIdOptional.orElse(0);
        Optional<ListenableFuture<ChunkList>> cachedChunkList = getCachedChunkList(pagingId);
        if (cachedChunkList.isPresent()) {
            return cachedChunkList.get();
        }

        if (pendingChunkList.size() >= chunkListTargetSize) {
            return immediateFuture(nextChunkList(pagingId));
        }

        pendingChunkListFuture = SettableFuture.create();
        SettableFuture<ChunkList> originalFuture = pendingChunkListFuture;

        return FluentFuture.from(pendingChunkListFuture)
                .withTimeout(chunkListPollTimeout.toMillis(), MILLISECONDS, longPollTimeoutExecutor)
                .catchingAsync(
                        TimeoutException.class,
                        timeoutException -> {
                            synchronized (this) {
                                // Without this, if we timed out while another thread has the lock in closedChunkConsumer,
                                // we'd wait here, that other thread would try to set the underlying pendingChunkListFuture,
                                // and then we would resolve this FluentFuture with an *additional* new chunk list,
                                // losing a ChunkList.
                                if (pendingChunkListFuture == originalFuture) {
                                    pendingChunkListFuture = null;
                                    return immediateFuture(nextChunkList(pagingId));
                                }
                                else {
                                    return listClosedChunks(OptionalLong.of(pagingId));
                                }
                            }
                        },
                        longPollTimeoutExecutor);
    }

    @GuardedBy("this")
    private ChunkList nextChunkList(long pagingId)
    {
        checkArgument(nextPagingId.isEmpty() || nextPagingId.getAsLong() == pagingId, "Expected pagingId %s but got %s", nextPagingId, pagingId);
        int chunkListSize = Math.min(chunkListMaxSize, pendingChunkList.size());
        List<ChunkHandle> chunks = ImmutableList.copyOf(pendingChunkList.subList(0, chunkListSize));
        pendingChunkList = pendingChunkList.subList(chunkListSize, pendingChunkList.size());

        if (finishFuture != null && finishFuture.isDone() && pendingChunkList.size() == 0 && recentlyClosedChunksCount.get() == 0) {
            nextPagingId = OptionalLong.empty();
        }
        else {
            nextPagingId = OptionalLong.of(pagingId + 1);
        }
        ChunkList chunkList = new ChunkList(chunks, nextPagingId);

        // cache the chunk list
        lastPagingId = pagingId;
        lastChunkList = chunkList;

        return chunkList;
    }

    public void markAllClosedChunksReceived()
    {
        allClosedChunksReceived = true;
    }

    public boolean isAllClosedChunksReceived()
    {
        return allClosedChunksReceived;
    }

    public void setChunkDeliveryMode(ChunkDeliveryMode chunkDeliveryMode)
    {
        synchronized (this) {
            this.chunkDeliveryMode = requireNonNull(chunkDeliveryMode, "chunkDeliveryMode is null");
            partitions.values().forEach(partition -> partition.setChunkDeliveryMode(chunkDeliveryMode));
        }
    }

    public void eagerDeliveryModeCloseChunksIfNeeded()
    {
        if (chunkDeliveryMode != ChunkDeliveryMode.EAGER) {
            return;
        }
        partitions.values().forEach(Partition::eagerDeliveryModeCloseChunkIfNeeded);
    }

    public synchronized ListenableFuture<Void> finish()
    {
        throwIfFailed();

        if (finishFuture == null) {
            finishFuture = asVoid(allAsList(partitions.values().stream().map(Partition::finish).collect(toImmutableList())));
            Futures.addCallback(finishFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(Void result)
                {
                    exchangeStateMachine.transitionToSourceFinished();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    exchangeStateMachine.transitionToFailed();
                    releaseChunks();
                }
            }, directExecutor());
        }
        return finishFuture;
    }

    public synchronized boolean wasFinishTriggered()
    {
        return finishFuture != null;
    }

    @VisibleForTesting
    public synchronized boolean isFinished()
    {
        return finishFuture != null && finishFuture.isDone();
    }

    public int getOpenChunksCount()
    {
        return (int) partitions.values().stream().filter(Partition::hasOpenChunk).count();
    }

    public int getClosedChunksCount()
    {
        return partitions.values().stream().mapToInt(Partition::getClosedChunksCount).sum();
    }

    public ListenableFuture<Void> releaseChunks()
    {
        partitions.values().forEach(Partition::releaseChunks);
        partitions.clear();

        if (spooled) {
            ListenableFuture<Void> removeFuture = spoolingStorage.removeExchange(bufferNodeId, exchangeId);
            addExceptionCallback(removeFuture, throwable -> {
                log.warn(throwable, "error while removing stored files for exchange %s", exchangeId);
            });
            removeFuture.addListener(this::removeExchange, directExecutor());
            return removeFuture;
        }

        // did not spool
        removeExchange();
        return immediateFuture(null);
    }

    public long getLastUpdateTime()
    {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime)
    {
        this.lastUpdateTime = lastUpdateTime;
    }

    public Collection<Partition> getPartitionsSortedBySizeDesc()
    {
        Map<Integer, Integer> partitionSizes = partitions.entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> entry.getValue().getClosedChunksCount()));
        ToIntFunction<Partition> partitionSizeFunction = partition -> partitionSizes.getOrDefault(partition.getPartitionId(), 0);

        return partitions.values().stream()
                .sorted(Comparator.comparingInt(partitionSizeFunction).reversed())
                .collect(toImmutableList());
    }

    public String getExchangeId()
    {
        return exchangeId;
    }

    @GuardedBy("this")
    private Optional<ListenableFuture<ChunkList>> getCachedChunkList(long pagingId)
    {
        // is this a repeated request for the last chunk list
        if (pagingId == lastPagingId) {
            if (lastChunkList == null) {
                return Optional.of(immediateFailedFuture(new DataServerException(USER_ERROR,
                        "Provided pagingId %d, which is equal to lastPagingId, but lastChunkList is null".formatted(pagingId))));
            }
            return Optional.of(immediateFuture(lastChunkList));
        }

        // if this is a chunk list before the lastChunkList, the data is gone
        if (pagingId < lastPagingId) {
            return Optional.of(immediateFailedFuture(new DataApiException(USER_ERROR,
                    "Provided pagingId %d but lastPagingId is %d".formatted(pagingId, lastPagingId))));
        }

        // if this is a request for a chunk list after the end of the stream, return not found
        if (nextPagingId.isEmpty()) {
            return Optional.of(immediateFailedFuture(new DataApiException(USER_ERROR,
                    "Unexpected request pagingId %d after exchange %s finished and all chunk handles got acknowledged".formatted(pagingId, exchangeId))));
        }

        // if this is not a request for the next chunk list, return not found
        if (pagingId != nextPagingId.getAsLong()) {
            // unknown pagingId
            return Optional.of(immediateFailedFuture(new DataServerException(USER_ERROR,
                    "pagingId %d does not equal nextPagingId %d".formatted(pagingId, nextPagingId.getAsLong()))));
        }

        // This is either the first request for the next chunk list, or a repeated request during an ongoing long-poll
        return Optional.ofNullable(pendingChunkListFuture);
    }

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throw new DataServerException(EXCHANGE_CORRUPTED, "exchange %s is in inconsistent state".formatted(exchangeId), throwable);
        }
    }

    void startExchangeSpan(Optional<Span> parentSpan, String exchangeId)
    {
        parentSpan.ifPresentOrElse(
                span -> {
                    synchronized (this) {
                        if (bufferServerExchangeSpan.get().isEmpty()) {
                            Span exchangeSpan = tracer.spanBuilder("server-exchange")
                                    .setAttribute("BufferServerNode", bufferNodeId)
                                    .setAttribute("ExchangeId", exchangeId)
                                    .setAttribute("exchange_state", exchangeStateMachine.getState().toString())
                                    .setParent(Context.current().with(span))
                                    .startSpan();
                            bufferServerExchangeSpan.set(Optional.of(exchangeSpan));
                            // This will generate span events for all state changes except a terminal state
                            // There is an immediate callback with the current event state
                            exchangeStateMachine.attachSpan(exchangeSpan);
                        }
                    }
                },
                () -> bufferServerExchangeSpan.compareAndSet(Optional.empty(), Optional.of(Span.getInvalid())));
    }

    private void removeExchange()
    {
        if (exchangeStateMachine.transitionToRemoved()) {
            bufferServerExchangeSpan.getAndSet(Optional.of(Span.getInvalid())).ifPresent(Span::end);
        }
    }

    public void markSpooled()
    {
        spooled = true;
    }
}
