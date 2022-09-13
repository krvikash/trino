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

import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.server.BufferNodeId;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ChunkManager
{
    private final long bufferNodeId;
    private final int chunkSizeInBytes;
    private final MemoryAllocator memoryAllocator;

    // exchangeId -> exchange
    private final Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    private final AtomicLong nextChunkIdGenerator = new AtomicLong();

    @Inject
    public ChunkManager(
            @BufferNodeId long bufferNodeId,
            ChunkManagerConfig config,
            MemoryAllocator memoryAllocator)
    {
        this.bufferNodeId = bufferNodeId;
        this.chunkSizeInBytes = toIntExact(config.getChunkSize().toBytes());
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
    }

    public void addDataPage(String exchangeId, int partitionId, int taskId, int attemptId, long dataPageId, Slice data)
    {
        checkArgument(taskId <= Short.MAX_VALUE, "taskId %s larger than %s", taskId, Short.MAX_VALUE);
        checkArgument(attemptId <= Byte.MAX_VALUE, "attemptId %s larger than %s", attemptId, Byte.MAX_VALUE);

        Exchange exchange = exchanges.computeIfAbsent(exchangeId, ignored -> new Exchange(bufferNodeId, exchangeId, memoryAllocator, chunkSizeInBytes, nextChunkIdGenerator));
        exchange.addDataPage(partitionId, taskId, attemptId, dataPageId, data);
    }

    public List<DataPage> getChunkData(String exchangeId, int partitionId, long chunkId)
    {
        Exchange exchange = getExchangeOrThrow(exchangeId);
        return exchange.getChunkData(partitionId, chunkId);
    }

    public ChunkList listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        Exchange exchange = getExchangeOrThrow(exchangeId);
        return exchange.listClosedChunks(pagingId);
    }

    // TODO: implement pingExchange
    public void pingExchange(String exchangeId)
    {
        throw new UnsupportedOperationException();
    }

    public void finishExchange(String exchangeId)
    {
        Exchange exchange = getExchangeOrThrow(exchangeId);
        exchange.finish();
    }

    public void removeExchange(String exchangeId)
    {
        Exchange exchange = exchanges.remove(exchangeId);
        if (exchange != null) {
            exchange.releaseMemory();
        }
    }

    public int getTrackedExchanges()
    {
        return exchanges.size();
    }

    public int getOpenChunks()
    {
        return exchanges.values().stream().mapToInt(Exchange::getOpenChunksCount).sum();
    }

    public int getClosedChunks()
    {
        return exchanges.values().stream().mapToInt(Exchange::getClosedChunksCount).sum();
    }

    private Exchange getExchangeOrThrow(String exchangeId)
    {
        Exchange exchange = exchanges.get(exchangeId);
        if (exchange == null) {
            throw new DataServerException(ErrorCode.EXCHANGE_NOT_FOUND, "exchange %s not found".formatted(exchangeId));
        }
        return exchange;
    }
}
