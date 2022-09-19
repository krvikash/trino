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
import com.google.common.base.Ticker;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.server.BufferNodeId;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.succinctDuration;
import static java.lang.Math.toIntExact;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class ChunkManager
{
    private static final Logger LOG = Logger.get(ChunkManager.class);

    private final long bufferNodeId;
    private final int chunkSizeInBytes;
    private final Duration exchangeStalenessThreshold;
    private final MemoryAllocator memoryAllocator;
    private final Ticker ticker;

    // exchangeId -> exchange
    private final Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    private final AtomicLong nextChunkIdGenerator = new AtomicLong();
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

    @Inject
    public ChunkManager(
            @BufferNodeId long bufferNodeId,
            ChunkManagerConfig config,
            MemoryAllocator memoryAllocator,
            @ForChunkManager Ticker ticker)
    {
        this.bufferNodeId = bufferNodeId;
        this.chunkSizeInBytes = toIntExact(config.getChunkSize().toBytes());
        this.exchangeStalenessThreshold = config.getExchangeStalenessThreshold();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    @PostConstruct
    public void start()
    {
        long interval = exchangeStalenessThreshold.toMillis();
        executor.scheduleWithFixedDelay(this::cleanupStaleExchanges, interval, interval, MILLISECONDS);
    }

    public void addDataPage(String exchangeId, int partitionId, int taskId, int attemptId, long dataPageId, Slice data)
    {
        checkArgument(taskId <= Short.MAX_VALUE, "taskId %s larger than %s", taskId, Short.MAX_VALUE);
        checkArgument(attemptId <= Byte.MAX_VALUE, "attemptId %s larger than %s", attemptId, Byte.MAX_VALUE);

        Exchange exchange = exchanges.computeIfAbsent(exchangeId, ignored -> new Exchange(bufferNodeId, exchangeId, memoryAllocator, chunkSizeInBytes, nextChunkIdGenerator, tickerReadMillis()));
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

    public void pingExchange(String exchangeId)
    {
        getExchangeOrThrow(exchangeId).setLastUpdateTime(tickerReadMillis());
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

    @VisibleForTesting
    void cleanupStaleExchanges()
    {
        Iterator<Map.Entry<String, Exchange>> iterator = exchanges.entrySet().iterator();
        long now = tickerReadMillis();
        long cleanupThreshold = now - exchangeStalenessThreshold.toMillis();
        while (iterator.hasNext()) {
            Map.Entry<String, Exchange> entry = iterator.next();
            long lastUpdateTime = entry.getValue().getLastUpdateTime();
            if (lastUpdateTime < cleanupThreshold) {
                LOG.info("forgetting exchange %s; no update for %s", entry.getKey(), succinctDuration(now - lastUpdateTime, MILLISECONDS));
                iterator.remove();
            }
        }
    }

    private long tickerReadMillis()
    {
        return ticker.read() / 1_000_000;
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForChunkManager {}
}
