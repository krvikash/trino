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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.BufferNodeState;
import io.starburst.stargate.buffer.discovery.client.BufferNodeStats;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class MockBufferService
        implements DiscoveryApi
{
    private static final long PAGES_PER_CHUNK = 2;

    private final Map<Long, MockDataNode> dataNodes;

    public MockBufferService(int dataNodesCount)
    {
        ImmutableMap.Builder<Long, MockDataNode> dataNodes = ImmutableMap.<Long, MockDataNode>builder();
        for (long nodeId = 0; nodeId < dataNodesCount; ++nodeId) {
            dataNodes.put(nodeId, new MockDataNode(nodeId));
        }
        this.dataNodes = dataNodes.buildOrThrow();
    }

    public DiscoveryApi getDiscoveryApi()
    {
        return this;
    }

    public DataApi getDataApi(long nodeId)
    {
        MockDataNode mockDataNode = dataNodes.get(nodeId);
        checkArgument(mockDataNode != null, "Node with id %d not found", nodeId);
        return mockDataNode;
    }

    @Override
    public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public BufferNodeInfoResponse getBufferNodes()
    {
        return new BufferNodeInfoResponse(
                true,
                dataNodes.values().stream().map(MockDataNode::getNodeInfo).collect(toImmutableSet()));
    }

    private class MockDataNode
            implements DataApi
    {
        @GuardedBy("this")
        private final Map<String, ExchangeData> exchangesData = new HashMap<>();
        private final long nodeId;

        public MockDataNode(long nodeId)
        {
            this.nodeId = nodeId;
        }

        public synchronized BufferNodeInfo getNodeInfo()
        {
            Optional<BufferNodeStats> stats = Optional.of(new BufferNodeStats(
                            64_000_000_000L,
                            64_000_000_000L,
                            exchangesData.size(),
                            exchangesData.values().stream().mapToInt(ExchangeData::getOpenChunksCount).sum(),
                            exchangesData.values().stream().mapToInt(ExchangeData::getClosedChunksCount).sum()));
            return new BufferNodeInfo(nodeId, URI.create("http://mock." + nodeId), stats, BufferNodeState.RUNNING);
        }

        @Override
        public synchronized ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
        {
            return getOrCreateExchangeData(exchangeId).listClosedChunks(pagingId);
        }

        @Override
        public synchronized ListenableFuture<Void> registerExchange(String exchangeId)
        {
            getOrCreateExchangeData(exchangeId);
            return immediateVoidFuture();
        }

        @Override
        public synchronized ListenableFuture<Void> pingExchange(String exchangeId)
        {
            // no-op
            return immediateVoidFuture();
        }

        @Override
        public synchronized ListenableFuture<Void> removeExchange(String exchangeId)
        {
            checkArgument(exchangesData.remove(exchangeId) != null, "Exchange %s not found", exchangeId);
            return immediateVoidFuture();
        }

        @Override
        public synchronized ListenableFuture<Void> addDataPages(String exchangeId, int partitionId, int taskId, int attemptId, long dataPageId, List<Slice> dataPages)
        {
            return getOrCreateExchangeData(exchangeId).addDataPages(partitionId, taskId, attemptId, dataPageId, dataPages);
        }

        @Override
        public synchronized ListenableFuture<Void> finishExchange(String exchangeId)
        {
            getOrCreateExchangeData(exchangeId).setFinished();
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<List<DataPage>> getChunkData(String exchangeId, int partitionId, long chunkId, long bufferNodeId)
        {
            return getExchangeData(exchangeId).getChunkData(exchangeId, partitionId, chunkId, bufferNodeId);
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
                        .filter(entry -> entry.getKey().chunkId >= pagingId)
                        .map(entry -> new ChunkHandle(
                                nodeId,
                                entry.getKey().partitionId(),
                                entry.getKey().chunkId(),
                                entry.getValue().getDataSize()))
                        .collect(toImmutableList());

                OptionalLong nextPagingId;
                if (finished && chunkHandles.isEmpty()) {
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
            public ListenableFuture<List<DataPage>> getChunkData(String exchangeId, int partitionId, long chunkId, long bufferNodeId)
            {
                ChunkKey key = new ChunkKey(partitionId, chunkId, bufferNodeId);
                ClosedChunkData chunkData = closedChunks.get(key);
                if (chunkData == null) {
                    return immediateFailedFuture(new DataApiException(CHUNK_NOT_FOUND, "chunk [%s/%s/%s/%s] not found".formatted(exchangeId, partitionId, chunkId, bufferNodeId)));
                }

                return immediateFuture(chunkData.dataPages());
            }

            @GuardedBy("MockDataNode.this")
            public ListenableFuture<Void> addDataPages(int partitionId, int taskId, int attemptId, long dataPageId, List<Slice> dataPages)
            {
                if (finished) {
                    return immediateFailedFuture(new DataApiException(ErrorCode.EXCHANGE_FINISHED, "exchange finished"));
                }
                // todo check for duplicates using dataPageId
                for (Slice dataPage : dataPages) {
                    pendingDataPages.put(partitionId, new DataPage(taskId, attemptId, dataPage));
                    if (pendingDataPages.get(partitionId).size() >= PAGES_PER_CHUNK) {
                        ChunkKey chunkKey = new ChunkKey(partitionId, nextChunkId, nodeId);
                        ClosedChunkData chunkData = new ClosedChunkData(ImmutableList.copyOf(pendingDataPages.get(partitionId)));
                        nextChunkId++;
                        pendingDataPages.removeAll(partitionId);
                        closedChunks.put(chunkKey, chunkData);
                    }
                }
                return immediateVoidFuture();
            }

            @GuardedBy("MockDataNode.this")
            public void setFinished()
            {
                checkState(!finished, "already finished");
                for (Map.Entry<Integer, Collection<DataPage>> entry : pendingDataPages.asMap().entrySet()) {
                    int partitionId = entry.getKey();
                    ChunkKey chunkKey = new ChunkKey(partitionId, nextChunkId, nodeId);
                    ClosedChunkData chunkData = new ClosedChunkData(ImmutableList.copyOf(entry.getValue()));
                    nextChunkId++;
                    closedChunks.put(chunkKey, chunkData);
                }
                pendingDataPages.clear();
                finished = true;
            }
        }

        private record ChunkKey(int partitionId, long chunkId, long bufferNodeId) {}

        private record ClosedChunkData(List<DataPage> dataPages) {
            public int getDataSize()
            {
                return dataPages.stream().map(DataPage::data).mapToInt(Slice::length).sum();
            }
        }
    }
}
