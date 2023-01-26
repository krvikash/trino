/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.testing;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.BufferNodeStats;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.testing.MockBufferNodeState.DRAINED;
import static io.starburst.stargate.buffer.testing.MockBufferNodeState.GONE;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.FAILED_GET_CHUNK_DATA_CHUNK_DRAINED_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.FAILED_GET_CHUNK_DATA_NOT_FOUND_IN_DRAINED_STORAGE_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.REJECTED_DRAINING_ADD_DATA_PAGES_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.REJECTED_EXCHANGE_FINISHED_ADD_DATA_PAGES_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.SUCCESSFUL_GET_CHUNK_DATA_FROM_DRAINED_STORAGE_REQUEST_COUNT;
import static java.util.Objects.requireNonNull;

class MockDataNode
        implements DataApi
{
    private final long nodeId;
    private final MockDrainedStorage drainedStorage;
    @GuardedBy("this")
    private final Map<String, ExchangeData> exchangesData = new HashMap<>();
    @GuardedBy("this")
    private MockBufferNodeState nodeState = MockBufferNodeState.RUNNING;
    @GuardedBy("this")
    private final MockDataNodeStats.Builder stats = MockDataNodeStats.builder();

    public MockDataNode(long nodeId, MockDrainedStorage drainedStorage)
    {
        this.nodeId = nodeId;
        this.drainedStorage = requireNonNull(drainedStorage, "drainedStorage is null");
    }

    public synchronized Optional<BufferNodeInfo> getMockInfo()
    {
        if (nodeState == GONE) {
            // drained node is effectively gone, there is no externally visible BufferNodeInfo for it
            return Optional.empty();
        }
        return Optional.of(getInfo());
    }

    @Override
    public BufferNodeInfo getInfo()
    {
        BufferNodeStats stats = new BufferNodeStats(
                64_000_000_000L,
                64_000_000_000L,
                exchangesData.size(),
                exchangesData.values().stream().mapToInt(ExchangeData::getOpenChunksCount).sum(),
                exchangesData.values().stream().mapToInt(ExchangeData::getClosedChunksCount).sum(),
                0);
        BufferNodeState bufferNodeState = switch (this.nodeState) {
            case RUNNING -> BufferNodeState.ACTIVE;
            case DRAINING, DRAINED -> BufferNodeState.DRAINING;
            case GONE -> throw new IllegalArgumentException("not supported");
        };
        return new BufferNodeInfo(nodeId, URI.create("http://mock." + nodeId), Optional.of(stats), bufferNodeState);
    }

    @Override
    public synchronized ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        throwIfNodeGone();
        return getOrCreateExchangeData(exchangeId).listClosedChunks(pagingId);
    }

    @Override
    public ListenableFuture<Void> markAllClosedChunksReceived(String exchangeId)
    {
        throwIfNodeGone();
        // do nothing
        return immediateVoidFuture();
    }

    @Override
    public synchronized ListenableFuture<Void> registerExchange(String exchangeId)
    {
        throwIfNodeGone();
        getOrCreateExchangeData(exchangeId);
        return immediateVoidFuture();
    }

    @Override
    public synchronized ListenableFuture<Void> pingExchange(String exchangeId)
    {
        throwIfNodeGone();
        // no-op
        return immediateVoidFuture();
    }

    @Override
    public synchronized ListenableFuture<Void> removeExchange(String exchangeId)
    {
        throwIfNodeGone();
        checkArgument(exchangesData.remove(exchangeId) != null, "Exchange %s not found", exchangeId);
        drainedStorage.flushExchange(exchangeId); // will be called by each node but it is not a problem
        return immediateVoidFuture();
    }

    @Override
    public synchronized ListenableFuture<Void> addDataPages(String exchangeId, int taskId, int attemptId, long dataPageId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        throwIfNodeGone();
        return getOrCreateExchangeData(exchangeId).addDataPages(taskId, attemptId, dataPageId, dataPagesByPartition);
    }

    @Override
    public synchronized ListenableFuture<Void> finishExchange(String exchangeId)
    {
        throwIfNodeGone();
        getOrCreateExchangeData(exchangeId).setFinished();
        return immediateVoidFuture();
    }

    @Override
    public synchronized ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        throwIfNodeGone();
        return getExchangeData(exchangeId).getChunkData(bufferNodeId, exchangeId, partitionId, chunkId);
    }

    public synchronized MockDataNodeStats getStats()
    {
        return stats.build();
    }

    public synchronized void markNodeDraining()
    {
        transitionToState(MockBufferNodeState.DRAINING);
        closeAllChunks();
    }

    public synchronized void markNodeDrained()
    {
        transitionToState(DRAINED);
        closeAllChunks();
        drainChunks();
    }

    public synchronized void markNodeGone()
    {
        transitionToState(GONE);
    }

    @GuardedBy("this")
    private void transitionToState(MockBufferNodeState newState)
    {
        checkArgument(this.nodeState.ordinal() < newState.ordinal(), "cannot transition from %s to %s", this.nodeState, newState);
        this.nodeState = newState;
    }

    @GuardedBy("this")
    private void closeAllChunks()
    {
        for (ExchangeData exchange : exchangesData.values()) {
            exchange.closeAllChunks();
        }
    }

    @GuardedBy("this")
    private void drainChunks()
    {
        for (ExchangeData exchange : exchangesData.values()) {
            exchange.drainChunks();
        }
    }

    @GuardedBy("this")
    private ExchangeData getOrCreateExchangeData(String exchangeId)
    {
        return exchangesData.computeIfAbsent(exchangeId, (key) -> new ExchangeData(exchangeId));
    }

    @GuardedBy("this")
    private ExchangeData getExchangeData(String exchangeId)
    {
        ExchangeData exchangeData = exchangesData.get(exchangeId);
        checkArgument(exchangeData != null, "Exchange %s not found", exchangeId);
        return exchangeData;
    }

    @GuardedBy("this")
    private void throwIfNodeGone()
    {
        if (nodeState == GONE) {
            throw new DataApiException(ErrorCode.INTERNAL_ERROR, "cannot connect to GONE node %d".formatted(nodeId));
        }
    }

    private class ExchangeData
    {
        private final String exchangeId;
        private final Map<ChunkKey, ClosedChunkData> closedChunks = new HashMap<>();
        private final ListMultimap<Integer, DataPage> pendingDataPages = ArrayListMultimap.create();
        private boolean finished;
        private long nextChunkId;

        public ExchangeData(String exchangeId)
        {
            this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        }

        @GuardedBy("MockDataNode.this")
        public ListenableFuture<ChunkList> listClosedChunks(OptionalLong optionalPagingId)
        {
            long pagingId = optionalPagingId.orElse(0);
            List<ChunkHandle> chunkHandles = closedChunks.entrySet().stream()
                    .filter(entry -> entry.getKey().chunkId() >= pagingId)
                    .map(entry -> new ChunkHandle(
                            nodeId,
                            entry.getKey().partitionId(),
                            entry.getKey().chunkId(),
                            entry.getValue().getDataSize()))
                    .collect(toImmutableList());

            OptionalLong nextPagingId;

            if ((nodeState == MockBufferNodeState.DRAINING || nodeState == DRAINED || finished) && chunkHandles.isEmpty()) {
                nextPagingId = OptionalLong.empty();
            }
            else {
                nextPagingId = OptionalLong.of(nextChunkId);
            }

            return immediateFuture(new ChunkList(
                    chunkHandles,
                    nextPagingId));
        }

        @GuardedBy("MockDataNode.this")
        public int getOpenChunksCount()
        {
            return pendingDataPages.keySet().size();
        }

        @GuardedBy("MockDataNode.this")
        public int getClosedChunksCount()
        {
            return closedChunks.size();
        }

        @GuardedBy("MockDataNode.this")
        public ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
        {
            ChunkKey key = new ChunkKey(bufferNodeId, partitionId, chunkId);
            ClosedChunkData chunkData = closedChunks.get(key);

            if (bufferNodeId != nodeId) {
                Optional<List<DataPage>> drainedChunkData = drainedStorage.getChunkData(bufferNodeId, exchangeId, partitionId, chunkId);
                if (drainedChunkData.isEmpty()) {
                    stats.increment(FAILED_GET_CHUNK_DATA_NOT_FOUND_IN_DRAINED_STORAGE_REQUEST_COUNT);
                    return immediateFailedFuture(new DataApiException(CHUNK_NOT_FOUND, "not present in drained storage"));
                }
                stats.increment(SUCCESSFUL_GET_CHUNK_DATA_FROM_DRAINED_STORAGE_REQUEST_COUNT);
                return immediateFuture(drainedChunkData.get());
            }

            if (nodeState == MockBufferNodeState.DRAINED) {
                stats.increment(FAILED_GET_CHUNK_DATA_CHUNK_DRAINED_REQUEST_COUNT);
                return immediateFailedFuture(new DataApiException(ErrorCode.DRAINED, "Node %d is already drained".formatted(nodeId)));
            }

            if (chunkData == null) {
                return immediateFailedFuture(new DataApiException(CHUNK_NOT_FOUND, "chunk [%s/%s/%s/%s] not found".formatted(exchangeId, partitionId, chunkId, bufferNodeId)));
            }

            return immediateFuture(chunkData.dataPages());
        }

        @GuardedBy("MockDataNode.this")
        public ListenableFuture<Void> addDataPages(int taskId, int attemptId, long dataPageId, ListMultimap<Integer, Slice> dataPagesByPartition)
        {
            if (finished) {
                stats.increment(REJECTED_EXCHANGE_FINISHED_ADD_DATA_PAGES_REQUEST_COUNT);
                return immediateFailedFuture(new DataApiException(ErrorCode.EXCHANGE_FINISHED, "exchange finished"));
            }
            if (nodeState.isDraining()) {
                stats.increment(REJECTED_DRAINING_ADD_DATA_PAGES_REQUEST_COUNT);
                return immediateFailedFuture(new DataApiException(ErrorCode.DRAINING, "Node %d is draining and not accepting any more data".formatted(nodeId)));
            }

            // todo check for duplicates using dataPageId
            for (Map.Entry<Integer, Collection<Slice>> entry : dataPagesByPartition.asMap().entrySet()) {
                int partitionId = entry.getKey();
                Collection<Slice> dataPages = entry.getValue();
                for (Slice dataPage : dataPages) {
                    pendingDataPages.put(partitionId, new DataPage(taskId, attemptId, dataPage));
                    if (pendingDataPages.get(partitionId).size() >= MockBufferService.PAGES_PER_CHUNK) {
                        ChunkKey chunkKey = new ChunkKey(nodeId, partitionId, nextChunkId);
                        ClosedChunkData chunkData = new ClosedChunkData(ImmutableList.copyOf(pendingDataPages.get(partitionId)));
                        nextChunkId++;
                        pendingDataPages.removeAll(partitionId);
                        closedChunks.put(chunkKey, chunkData);
                    }
                }
            }
            stats.increment(SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT);
            return immediateVoidFuture();
        }

        @GuardedBy("MockDataNode.this")
        public void setFinished()
        {
            checkState(!finished, "already finished");
            closeAllChunks();
            finished = true;
        }

        @GuardedBy("MockDataNode.this")
        private void closeAllChunks()
        {
            for (Map.Entry<Integer, Collection<DataPage>> entry : pendingDataPages.asMap().entrySet()) {
                int partitionId = entry.getKey();
                ChunkKey chunkKey = new ChunkKey(nodeId, partitionId, nextChunkId);
                ClosedChunkData chunkData = new ClosedChunkData(ImmutableList.copyOf(entry.getValue()));
                nextChunkId++;
                closedChunks.put(chunkKey, chunkData);
            }
            pendingDataPages.clear();
        }

        public void drainChunks()
        {
            checkState(pendingDataPages.isEmpty(), "pending data pages not empty");
            for (Map.Entry<ChunkKey, ClosedChunkData> entry : closedChunks.entrySet()) {
                ChunkKey chunkKey = entry.getKey();
                ClosedChunkData chunkData = entry.getValue();
                drainedStorage.addChunk(exchangeId, chunkKey.partitionId(), chunkKey.chunkId(), chunkKey.bufferNodeId(), chunkData.dataPages());
            }
        }
    }
}
