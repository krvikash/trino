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
import io.starburst.stargate.buffer.data.server.DataServerStats;

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

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static java.lang.Math.toIntExact;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class ChunkManager
{
    private static final Logger LOG = Logger.get(ChunkManager.class);

    private final long bufferNodeId;
    private final int chunkSizeInBytes;
    private final Duration exchangeStalenessThreshold;
    private final MemoryAllocator memoryAllocator;
    private final Ticker ticker;
    private final DataServerStats dataServerStats;

    // exchangeId -> exchange
    private final Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    private final ChunkIdGenerator chunkIdGenerator = new ChunkIdGenerator();
    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService statsReportingExecutor = newSingleThreadScheduledExecutor();

    @Inject
    public ChunkManager(
            @BufferNodeId long bufferNodeId,
            ChunkManagerConfig config,
            MemoryAllocator memoryAllocator,
            @ForChunkManager Ticker ticker,
            DataServerStats dataServerStats)
    {
        this.bufferNodeId = bufferNodeId;
        this.chunkSizeInBytes = toIntExact(config.getChunkSize().toBytes());
        this.exchangeStalenessThreshold = config.getExchangeStalenessThreshold();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
    }

    @PostConstruct
    public void start()
    {
        long interval = exchangeStalenessThreshold.toMillis();
        cleanupExecutor.scheduleWithFixedDelay(this::cleanupStaleExchanges, interval, interval, MILLISECONDS);
        statsReportingExecutor.scheduleWithFixedDelay(this::reportStats, 0, 1, SECONDS);
    }

    public void addDataPage(String exchangeId, int partitionId, int taskId, int attemptId, long dataPageId, Slice data)
    {
        int requiredStorageSize = DATA_PAGE_HEADER_SIZE + data.length();
        checkArgument(requiredStorageSize <= chunkSizeInBytes,
                "requiredStorageSize %d exceeded chunkSizeInBytes %d", requiredStorageSize, chunkSizeInBytes);

        registerExchange(exchangeId);
        getExchangeAndHeartbeat(exchangeId).addDataPage(partitionId, taskId, attemptId, dataPageId, data);
    }

    public void finishTask(String exchangeId, int partitionId, int taskId, int attemptId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        exchange.finishTask(partitionId, taskId, attemptId);
    }

    public List<DataPage> getChunkData(String exchangeId, int partitionId, long chunkId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.getChunkData(partitionId, chunkId);
    }

    public ChunkList listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.listClosedChunks(pagingId);
    }

    public void registerExchange(String exchangeId)
    {
        exchanges.computeIfAbsent(exchangeId, ignored -> new Exchange(bufferNodeId, exchangeId, memoryAllocator, chunkSizeInBytes, chunkIdGenerator, tickerReadMillis()));
    }

    public void pingExchange(String exchangeId)
    {
        getExchangeAndHeartbeat(exchangeId);
    }

    public void finishExchange(String exchangeId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        exchange.finish();
    }

    public void removeExchange(String exchangeId)
    {
        Exchange exchange = exchanges.remove(exchangeId);
        if (exchange != null) {
            exchange.releaseMemory();
        }
        else {
            throw new DataServerException(ErrorCode.EXCHANGE_NOT_FOUND, "exchange %s not found".formatted(exchangeId));
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

    private Exchange getExchangeAndHeartbeat(String exchangeId)
    {
        Exchange exchange = exchanges.get(exchangeId);
        if (exchange == null) {
            throw new DataServerException(ErrorCode.EXCHANGE_NOT_FOUND, "exchange %s not found".formatted(exchangeId));
        }
        exchange.setLastUpdateTime(tickerReadMillis());
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

    private void reportStats()
    {
        dataServerStats.getTrackedExchanges().add(getTrackedExchanges());
        dataServerStats.getOpenChunks().add(getOpenChunks());
        dataServerStats.getClosedChunks().add(getClosedChunks());
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
