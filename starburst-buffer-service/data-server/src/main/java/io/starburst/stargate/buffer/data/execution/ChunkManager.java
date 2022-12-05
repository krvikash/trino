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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.ChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Qualifier;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_NOT_FOUND;
import static java.lang.Math.toIntExact;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class ChunkManager
{
    private static final Logger LOG = Logger.get(ChunkManager.class);

    private final long bufferNodeId;
    private final int chunkMaxSizeInBytes;
    private final int chunkSliceSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final Duration exchangeStalenessThreshold;
    private final Duration chunkSpoolInterval;
    private final int chunkSpoolConcurrency;
    private final MemoryAllocator memoryAllocator;
    private final SpoolingStorage spoolingStorage;
    private final Ticker ticker;
    private final DataServerStats dataServerStats;
    private final ExecutorService executor;

    // exchangeId -> exchange
    private final Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    private final ChunkIdGenerator chunkIdGenerator = new ChunkIdGenerator();
    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService statsReportingExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService chunkSpoolExecutor = newSingleThreadScheduledExecutor();
    private final Cache<String, Object> recentlyRemovedExchanges = CacheBuilder.newBuilder().expireAfterWrite(5, MINUTES).build();

    @Inject
    public ChunkManager(
            BufferNodeId bufferNodeId,
            ChunkManagerConfig chunkManagerConfig,
            DataServerConfig dataServerConfig,
            MemoryAllocator memoryAllocator,
            SpoolingStorage spoolingStorage,
            @ForChunkManager Ticker ticker,
            DataServerStats dataServerStats,
            ExecutorService executor)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.chunkMaxSizeInBytes = toIntExact(chunkManagerConfig.getChunkMaxSize().toBytes());
        this.chunkSliceSizeInBytes = toIntExact(chunkManagerConfig.getChunkSliceSize().toBytes());
        this.calculateDataPagesChecksum = dataServerConfig.getIncludeChecksumInDataResponse();
        this.exchangeStalenessThreshold = chunkManagerConfig.getExchangeStalenessThreshold();
        this.chunkSpoolInterval = chunkManagerConfig.getChunkSpoolInterval();
        this.chunkSpoolConcurrency = chunkManagerConfig.getChunkSpoolConcurrency();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingStorage = requireNonNull(spoolingStorage, "spoolingStorage is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @PostConstruct
    public void start()
    {
        long exchangeCleanupInterval = exchangeStalenessThreshold.toMillis();
        cleanupExecutor.scheduleWithFixedDelay(() -> {
            try {
                cleanupStaleExchanges();
            }
            catch (Throwable e) {
                LOG.error(e, "Error cleaning up stale exchanges");
            }
        }, exchangeCleanupInterval, exchangeCleanupInterval, MILLISECONDS);
        statsReportingExecutor.scheduleWithFixedDelay(this::reportStats, 0, 1, SECONDS);
        long spoolInterval = chunkSpoolInterval.toMillis();
        chunkSpoolExecutor.scheduleWithFixedDelay(() -> {
            try {
                spoolIfNecessary();
            }
            catch (Throwable e) {
                LOG.error(e, "Error spooling chunks");
            }
        }, spoolInterval, spoolInterval, MILLISECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        Closer closer = Closer.create();
        closer.register(cleanupExecutor::shutdownNow);
        closer.register(statsReportingExecutor::shutdownNow);
        closer.register(chunkSpoolExecutor::shutdownNow);
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ListenableFuture<Void> addDataPages(
            String exchangeId,
            int partitionId,
            int taskId,
            int attemptId,
            long dataPagesId,
            List<Slice> pages)
    {
        registerExchange(exchangeId);
        return getExchangeAndHeartbeat(exchangeId).addDataPages(partitionId, taskId, attemptId, dataPagesId, pages);
    }

    public ChunkDataLease getChunkData(String exchangeId, int partitionId, long chunkId, long bufferNodeId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.getChunkData(partitionId, chunkId, bufferNodeId);
    }

    public ChunkList listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.listClosedChunks(pagingId);
    }

    public void registerExchange(String exchangeId)
    {
        exchanges.computeIfAbsent(exchangeId, ignored -> {
            if (recentlyRemovedExchanges.getIfPresent(exchangeId) != null) {
                throw new DataServerException(EXCHANGE_NOT_FOUND, "exchange %s already removed".formatted(exchangeId));
            }

            return new Exchange(
                    bufferNodeId,
                    exchangeId,
                    memoryAllocator,
                    spoolingStorage,
                    chunkMaxSizeInBytes,
                    chunkSliceSizeInBytes,
                    calculateDataPagesChecksum,
                    chunkSpoolConcurrency,
                    chunkIdGenerator,
                    executor,
                    tickerReadMillis());
        });
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
        recentlyRemovedExchanges.put(exchangeId, "marker");
        Exchange exchange = exchanges.remove(exchangeId);
        if (exchange != null) {
            exchange.releaseChunks();
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

    public int getSpooledChunks()
    {
        return spoolingStorage.getSpooledChunks();
    }

    private Exchange getExchangeAndHeartbeat(String exchangeId)
    {
        Exchange exchange = exchanges.get(exchangeId);
        if (exchange == null) {
            throw new DataServerException(EXCHANGE_NOT_FOUND, "exchange %s not found".formatted(exchangeId));
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
            Exchange exchange = entry.getValue();
            long lastUpdateTime = exchange.getLastUpdateTime();
            if (lastUpdateTime < cleanupThreshold) {
                LOG.info("forgetting exchange %s; no update for %s", entry.getKey(), succinctDuration(now - lastUpdateTime, MILLISECONDS));
                iterator.remove();
                exchange.releaseChunks();
            }
        }
    }

    @VisibleForTesting
    void spoolIfNecessary()
    {
        if (memoryAllocator.aboveHighWatermark()) {
            LOG.info("Memory allocation ratio %.2f%%, starting to spool closed chunks",
                    memoryAllocator.getAllocationPercentage());
            Predicate<MemoryAllocator> stopCriteria = MemoryAllocator::belowLowWatermark;
            List<Exchange> exchangesSortedBySizeDesc = exchanges.values().stream()
                    .sorted(Comparator.comparingInt(Exchange::getClosedChunksCount).reversed())
                    .collect(toImmutableList());
            for (Exchange exchange : exchangesSortedBySizeDesc) {
                exchange.spool(stopCriteria);
                if (stopCriteria.test(memoryAllocator)) {
                    return;
                }
            }

            LOG.info("Having spooled all closed chunks, memory allocation ratio %.2f%%, starting to spool open chunks",
                    memoryAllocator.getAllocationPercentage());
            // To avoid deadlock, now we start to close and spool open chunks that are not pending
            for (Exchange exchange : exchangesSortedBySizeDesc) {
                exchange.closeOpenChunksAndSpool(stopCriteria);
                if (stopCriteria.test(memoryAllocator)) {
                    return;
                }
            }

            // It's still possible for us to reach here, as we fulfill memory requests as soon as we release new
            // memory. It's totally fine as long as we don't deadlock.
        }
    }

    private void reportStats()
    {
        dataServerStats.updateTrackedExchanges(getTrackedExchanges());
        dataServerStats.updateOpenChunks(getOpenChunks());
        dataServerStats.updateClosedChunks(getClosedChunks());
        dataServerStats.updateSpooledChunks(getSpooledChunks());
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
