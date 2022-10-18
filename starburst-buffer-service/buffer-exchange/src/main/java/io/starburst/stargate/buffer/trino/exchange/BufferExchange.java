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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.BufferNodeStats;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.SecretKey;

import java.security.Key;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.sortedCopyOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.starburst.stargate.buffer.trino.exchange.ExternalExchangeIds.externalExchangeId;
import static java.util.Objects.requireNonNull;

public class BufferExchange
        implements Exchange
{
    private static final Logger log = Logger.get(BufferExchange.class);

    private final ExchangeId exchangeId;
    private final String externalExchangeId;
    private final int outputPartitionCount;
    private final boolean preserveOrderWithinPartition;
    private final DataApiFacade dataApi;
    private final BufferNodeDiscoveryManager discoveryManager;
    private final ScheduledExecutorService executorService;
    private final int sourceHandleTargetChunksCount;
    private final DataSize sourceHandleTargetDataSize;
    private final Optional<SecretKey> encryptionKey;

    private volatile boolean noMoreSinks;
    @GuardedBy("this")
    private boolean allRequiredSinksFinished;

    @GuardedBy("this")
    private Map<Integer, Long> partitionToNodeMapping;
    private final AtomicBoolean closed = new AtomicBoolean();

    @GuardedBy("this")
    private final Map<Integer, Deque<ChunkHandle>> discoveredChunkHandles = new HashMap<>();

    @GuardedBy("this")
    private final Set<BufferExchangeSourceHandleSource> sourceHandleSources = ConcurrentHashMap.newKeySet();
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
            BufferNodeDiscoveryManager discoveryManager,
            ScheduledExecutorService executorService,
            int sourceHandleTargetChunksCount,
            DataSize sourceHandleTargetDataSize,
            Optional<SecretKey> encryptionKey)
    {
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.externalExchangeId = externalExchangeId(queryId, exchangeId);
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.sourceHandleTargetChunksCount = sourceHandleTargetChunksCount;
        this.sourceHandleTargetDataSize = requireNonNull(sourceHandleTargetDataSize, "sourceHandleTargetDataSize is null");
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
    }

    @Override
    public ExchangeId getId()
    {
        return exchangeId;
    }

    @Override
    public synchronized ExchangeSinkHandle addSink(int taskPartitionId)
    {
        checkState(!closed.get(), "already closed");
        checkState(!noMoreSinks, "no more sinks can be added");
        return new BufferExchangeSinkHandle(
                externalExchangeId,
                taskPartitionId,
                outputPartitionCount,
                preserveOrderWithinPartition,
                encryptionKey.map(SecretKey::getEncoded));
    }

    @Override
    public synchronized ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        checkState(!closed.get(), "already closed");

        BufferExchangeSinkHandle bufferExchangeSinkHandle = (BufferExchangeSinkHandle) sinkHandle;

        if (partitionToNodeMapping == null) {
            partitionToNodeMapping = newPartitionToNodeMapping();
            for (long nodeId : partitionToNodeMapping.values()) {
                addBufferNodeToPoll(nodeId);
            }
        }

        return new BufferExchangeSinkInstanceHandle(bufferExchangeSinkHandle, taskAttemptId, partitionToNodeMapping);
    }

    @Override
    public synchronized ExchangeSinkInstanceHandle updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        checkState(!closed.get(), "already closed");
        updatePartitionToNodeMapping();
        return new BufferExchangeSinkInstanceHandle(
                (BufferExchangeSinkHandle) sinkHandle,
                taskAttemptId,
                partitionToNodeMapping);
    }

    @GuardedBy("this")
    private void updatePartitionToNodeMapping()
    {
        checkState(partitionToNodeMapping != null, "partitionToNodeMapping should be already set");
        Map<Long, BufferNodeInfo> bufferNodes = discoveryManager.getBufferNodes();
        Map<Integer, Long> newMapping = newPartitionToNodeMapping();
        ImmutableMap.Builder<Integer, Long> finalMapping = ImmutableMap.builder();

        for (Map.Entry<Integer, Long> entry : partitionToNodeMapping.entrySet()) {
            Integer partition = entry.getKey();
            Long oldBufferNodeId = entry.getValue();
            BufferNodeInfo oldBufferNodeInfo = bufferNodes.get(oldBufferNodeId);
            if (oldBufferNodeInfo != null && oldBufferNodeInfo.getState() == BufferNodeState.RUNNING) {
                // keep old mapping entry
                finalMapping.put(partition, oldBufferNodeId);
            }
            else {
                // use new mapping
                finalMapping.put(partition, newMapping.get(partition));
            }
        }
        partitionToNodeMapping = finalMapping.buildOrThrow();
        for (Long nodeId : partitionToNodeMapping.values()) {
            addBufferNodeToPoll(nodeId);
        }
    }

    private Map<Integer, Long> newPartitionToNodeMapping()
    {
        List<BufferNodeInfo> bufferNodes = discoveryManager.getBufferNodes().values().stream()
                .filter(node -> node.getState() == BufferNodeState.RUNNING)
                .filter(node -> node.getStats().isPresent())
                .collect(toImmutableList());

        if (bufferNodes.size() == 0) {
            // todo keep trying to get mapping for some time. To be figured out how to do that not blocking call to instantiateSink or refreshSinkInstanceHandle
            throw new RuntimeException("no RUNNING buffer nodes available");
        }

        long maxChunksCount = bufferNodes.stream().mapToLong(node -> node.getStats().orElseThrow().getOpenChunks() + node.getStats().orElseThrow().getClosedChunks()).max().orElseThrow();
        RandomSelector<BufferNodeInfo> selector = RandomSelector.weighted(
                bufferNodes,
                node -> {
                    BufferNodeStats stats = node.getStats().orElseThrow();
                    double memoryWeight = (double) stats.getFreeMemory() / stats.getTotalMemory();
                    int chunksCount = stats.getOpenChunks() + stats.getClosedChunks();
                    double chunksWeight;
                    if (maxChunksCount == 0) {
                        chunksWeight = 0.0;
                    }
                    else {
                        chunksWeight = 1.0 - (double) chunksCount / maxChunksCount;
                    }

                    if (memoryWeight < chunksWeight) {
                        // if we are constrained more with memory let's just use that
                        return memoryWeight;
                    }
                    // if we have plenty of memory lets take chunks count into account
                    return (memoryWeight + chunksWeight) / 2;
                });

        ImmutableMap.Builder<Integer, Long> mapping = ImmutableMap.builder();
        IntStream.range(0, outputPartitionCount).forEach(partition -> mapping.put(partition, selector.next().getNodeId()));
        return mapping.buildOrThrow();
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
                newReadySourceHandles.add(BufferExchangeSourceHandle.fromChunkHandles(
                        externalExchangeId,
                        partitionId,
                        sortedCopyOf(Comparator.comparingLong(ChunkHandle::chunkId), chunkHandles),
                        true,
                        encryptionKey.map(Key::getEncoded)));
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
                            false,
                            encryptionKey.map(Key::getEncoded)));
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
                        false,
                        encryptionKey.map(Key::getEncoded)));
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
        // explicitly logging failure here for debugging; if this failure results in exchange being non-operational (should not happen if everythin works as expected)
        // the query may fail with exception from workers, failing to access exchange which would mask the root cause of the failure
        log.warn(failure, "Marking exchange " + externalExchangeId + " as failed");
        for (BufferExchangeSourceHandleSource sourceHandleSource : sourceHandleSources) {
            sourceHandleSource.markFailed(failure);
        }
        close();
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
