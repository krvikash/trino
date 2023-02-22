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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.sortedCopyOf;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.starburst.stargate.buffer.trino.exchange.ExternalExchangeIds.externalExchangeId;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class BufferExchange
        implements Exchange
{
    private static final Logger log = Logger.get(BufferExchange.class);

    private final ExchangeId exchangeId;
    private final String externalExchangeId;
    private final int outputPartitionCount;
    private final boolean preserveOrderWithinPartition;
    private final DataApiFacade dataApi;
    private final ScheduledExecutorService executorService;
    private final int sourceHandleTargetChunksCount;
    private final DataSize sourceHandleTargetDataSize;
    private final PartitionNodeMapper partitionNodeMapper;

    private volatile boolean noMoreSinks;
    @GuardedBy("this")
    private boolean allRequiredSinksFinished;

    private final AtomicBoolean closed = new AtomicBoolean();

    @GuardedBy("this")
    private final Map<Integer, Deque<ChunkHandle>> discoveredChunkHandles = new HashMap<>();

    @GuardedBy("this")
    private final Set<BufferExchangeSourceHandleSource> sourceHandleSources = new HashSet<>();
    @GuardedBy("this")
    private final List<ExchangeSourceHandle> readySourceHandles = new ArrayList<>();
    @GuardedBy("this")
    boolean allSourceHandlesCreated;
    @GuardedBy("this")
    private Throwable failure;

    @GuardedBy("this")
    private final Map<Long, ChunkHandlesPoller> chunkPolledBufferNodes = new HashMap<>();
    @GuardedBy("this")
    private final Set<Long> chunkPollingCompletedBufferNodes = new HashSet<>();

    public BufferExchange(
            QueryId queryId,
            ExchangeId exchangeId,
            int outputPartitionCount,
            boolean preserveOrderWithinPartition,
            DataApiFacade dataApi,
            PartitionNodeMapperFactory partitionNodeMapperFactory,
            ScheduledExecutorService executorService,
            int sourceHandleTargetChunksCount,
            DataSize sourceHandleTargetDataSize)
    {
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.externalExchangeId = externalExchangeId(queryId, exchangeId);
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.sourceHandleTargetChunksCount = sourceHandleTargetChunksCount;
        this.sourceHandleTargetDataSize = requireNonNull(sourceHandleTargetDataSize, "sourceHandleTargetDataSize is null");
        requireNonNull(partitionNodeMapperFactory, "partitionNodeMapperFactory is null");
        this.partitionNodeMapper = partitionNodeMapperFactory.getPartitionNodeMapper(exchangeId, outputPartitionCount);
    }

    @Override
    public ExchangeId getId()
    {
        return exchangeId;
    }

    @Override
    public synchronized ExchangeSinkHandle addSink(int taskPartitionId)
    {
        throwIfFailed();
        checkState(!closed.get(), "already closed");
        checkState(!noMoreSinks, "no more sinks can be added");
        return new BufferExchangeSinkHandle(
                externalExchangeId,
                taskPartitionId,
                outputPartitionCount,
                preserveOrderWithinPartition);
    }

    @Override
    public synchronized CompletableFuture<ExchangeSinkInstanceHandle> instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        throwIfFailed();
        checkState(!closed.get(), "already closed");

        BufferExchangeSinkHandle bufferExchangeSinkHandle = (BufferExchangeSinkHandle) sinkHandle;

        Map<Integer, Long> partitionToNodeMapping = partitionNodeMapper.getMapping(bufferExchangeSinkHandle.getTaskPartitionId());
        for (long nodeId : partitionToNodeMapping.values()) {
            addBufferNodeToPoll(nodeId);
        }

        return completedFuture(new BufferExchangeSinkInstanceHandle(bufferExchangeSinkHandle, taskAttemptId, partitionToNodeMapping));
    }

    @Override
    public synchronized CompletableFuture<ExchangeSinkInstanceHandle> updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        throwIfFailed();
        checkState(!closed.get(), "already closed");
        BufferExchangeSinkHandle bufferExchangeSinkHandle = (BufferExchangeSinkHandle) sinkHandle;
        partitionNodeMapper.refreshMapping();
        Map<Integer, Long> newMapping = partitionNodeMapper.getMapping(bufferExchangeSinkHandle.getTaskPartitionId());
        for (Long nodeId : newMapping.values()) {
            addBufferNodeToPoll(nodeId);
        }
        return completedFuture(new BufferExchangeSinkInstanceHandle(
                bufferExchangeSinkHandle,
                taskAttemptId,
                newMapping));
    }

    @Override
    public synchronized void noMoreSinks()
    {
        noMoreSinks = true;
    }

    @Override
    public void sinkFinished(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        checkState(!closed.get(), "already closed");
    }

    @Override
    public synchronized void allRequiredSinksFinished()
    {
        throwIfFailed();
        verify(noMoreSinks, "noMoreSinks should be called already");
        verify(!allRequiredSinksFinished, "allRequiredSinksFinished called already");
        allRequiredSinksFinished = true;
        // notify all data nodes that exchange is done.
        for (ChunkHandlesPoller poller : chunkPolledBufferNodes.values()) {
            poller.markExchangeFinished();
        }
        outputReadySourceHandles();
    }

    @GuardedBy("this")
    private void outputReadySourceHandles()
    {
        List<ExchangeSourceHandle> newSourceHandles = buildNewReadySourceHandles();
        readySourceHandles.addAll(newSourceHandles);
        for (BufferExchangeSourceHandleSource sourceHandleSource : sourceHandleSources) {
            sourceHandleSource.addSourceHandles(newSourceHandles, allSourceHandlesCreated);
        }
    }

    @GuardedBy("this")
    private List<ExchangeSourceHandle> buildNewReadySourceHandles()
    {
        boolean allChunksDiscovered = allRequiredSinksFinished
                && chunkPolledBufferNodes.size() == chunkPollingCompletedBufferNodes.size();

        if (!allChunksDiscovered && preserveOrderWithinPartition) {
            // if we preserve order we need to return single source handle per partition
            return ImmutableList.of();
        }

        List<ExchangeSourceHandle> newReadySourceHandles = new ArrayList<>();
        for (Map.Entry<Integer, Deque<ChunkHandle>> entry : discoveredChunkHandles.entrySet()) {
            int partitionId = entry.getKey();
            Deque<ChunkHandle> chunkHandles = entry.getValue();

            if (chunkHandles.isEmpty()) {
                continue;
            }

            if (preserveOrderWithinPartition) {
                long bufferNodeId = chunkHandles.peekFirst().bufferNodeId();
                chunkHandles.forEach(handle -> checkState(
                        handle.bufferNodeId() == bufferNodeId,
                        "Expected all chunk handles for given partition to have matching buffer node id when preserving order; mismatch %s vs %s",
                        handle,
                        bufferNodeId));

                newReadySourceHandles.add(BufferExchangeSourceHandle.fromChunkHandles(
                        externalExchangeId,
                        partitionId,
                        sortedCopyOf(Comparator.comparingLong(ChunkHandle::chunkId), chunkHandles),
                        true));
                chunkHandles.clear();
                continue;
            }

            List<ChunkHandle> currentSourceHandleChunks = new ArrayList<>();
            long currentSourceHandleDataSize = 0;
            int usedChunkHandlesCount = 0;
            for (ChunkHandle chunkHandle : chunkHandles) {
                currentSourceHandleChunks.add(chunkHandle);
                currentSourceHandleDataSize += chunkHandle.dataSizeInBytes();
                if (currentSourceHandleChunks.size() >= sourceHandleTargetChunksCount || currentSourceHandleDataSize >= sourceHandleTargetDataSize.toBytes()) {
                    newReadySourceHandles.add(BufferExchangeSourceHandle.fromChunkHandles(
                            externalExchangeId,
                            partitionId,
                            currentSourceHandleChunks,
                            false));
                    usedChunkHandlesCount += currentSourceHandleChunks.size();
                    currentSourceHandleDataSize = 0;
                    currentSourceHandleChunks = new ArrayList<>();
                }
            }

            // if we know no more chunks will be added create source handle from remaining chunks
            if (allChunksDiscovered && !currentSourceHandleChunks.isEmpty()) {
                newReadySourceHandles.add(BufferExchangeSourceHandle.fromChunkHandles(
                        externalExchangeId,
                        partitionId,
                        currentSourceHandleChunks,
                        false));
                usedChunkHandlesCount += currentSourceHandleChunks.size();
            }

            // remove consumed chunk handles
            for (int i = 0; i < usedChunkHandlesCount; ++i) {
                chunkHandles.removeFirst();
            }
        }

        this.allSourceHandlesCreated = allChunksDiscovered;

        return newReadySourceHandles;
    }

    @Override
    public synchronized ExchangeSourceHandleSource getSourceHandles()
    {
        BufferExchangeSourceHandleSource sourceHandleSource = new BufferExchangeSourceHandleSource();
        if (failure != null) {
            sourceHandleSource.markFailed(failure);
        }
        else {
            sourceHandleSource.addSourceHandles(readySourceHandles, allSourceHandlesCreated);
        }
        sourceHandleSources.add(sourceHandleSource);
        return sourceHandleSource;
    }

    @Override
    public synchronized void close()
    {
        if (closed.compareAndSet(false, true)) {
            stopPolling();
            triggerExchangeRemove();
        }
    }

    private synchronized void markFailed(Throwable failure)
    {
        if (this.failure != null) {
            return;
        }
        this.failure = requireNonNull(failure, "failure is null");
        // explicitly logging failure here for debugging; if this failure results in exchange being non-operational (should not happen if everything works as expected)
        // the query may fail with exception from workers, failing to access exchange which would mask the root cause of the failure
        log.warn(failure, "Marking exchange " + externalExchangeId + " as failed");
        for (BufferExchangeSourceHandleSource sourceHandleSource : sourceHandleSources) {
            sourceHandleSource.markFailed(failure);
        }
        close();
    }

    @GuardedBy("this")
    private void throwIfFailed()
    {
        if (failure != null) {
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
    }

    @GuardedBy("this")
    private void registerNewChunkHandles(List<ChunkHandle> newChunkHandles)
    {
        for (ChunkHandle chunkHandle : newChunkHandles) {
            Deque<ChunkHandle> queue = discoveredChunkHandles.computeIfAbsent(chunkHandle.partitionId(), ArrayDeque::new);
            queue.add(chunkHandle);
        }
    }

    @GuardedBy("this")
    private void addBufferNodeToPoll(long bufferNodeId)
    {
        verify(!allRequiredSinksFinished, "cannot add node %d to poll after all sinks finished", bufferNodeId);
        if (chunkPolledBufferNodes.containsKey(bufferNodeId)) {
            // already polling
            return;
        }

        ChunkHandlesPoller poller = new ChunkHandlesPoller(executorService, externalExchangeId, dataApi, bufferNodeId, new ChunkHandlesPoller.ChunksCallback()
        {
            @Override
            public void onChunksDiscovered(List<ChunkHandle> chunks, boolean noMoreChunks)
            {
                synchronized (BufferExchange.this) {
                    registerNewChunkHandles(chunks);
                    if (noMoreChunks) {
                        chunkPollingCompletedBufferNodes.add(bufferNodeId);
                    }
                    outputReadySourceHandles();
                }
            }

            @Override
            public void onFailure(Throwable failure)
            {
                markFailed(failure);
            }
        });

        chunkPolledBufferNodes.put(bufferNodeId, poller);
        poller.start();
    }

    public void triggerExchangeRemove()
    {
        executorService.submit(() -> {
            Set<Long> bufferNodeIds;
            synchronized (this) {
                bufferNodeIds = new HashSet<>(chunkPolledBufferNodes.keySet());
            }

            for (long nodeId : bufferNodeIds) {
                ListenableFuture<Void> future = Futures.catching(
                        dataApi.removeExchange(nodeId, externalExchangeId),
                        DataApiException.class,
                        dataApiException -> {
                            if (dataApiException.getErrorCode() == ErrorCode.EXCHANGE_NOT_FOUND || dataApiException.getErrorCode() == ErrorCode.BUFFER_NODE_NOT_FOUND) {
                                // ignore
                                return null;
                            }
                            throw dataApiException;
                        },
                        directExecutor());
                addExceptionCallback(future, (t) -> log.warn("Could not remove exchange %s on node %d".formatted(externalExchangeId, nodeId), t));
            }
        });
    }

    public synchronized void stopPolling()
    {
        for (ChunkHandlesPoller poller : chunkPolledBufferNodes.values()) {
            poller.stop();
        }
    }
}
