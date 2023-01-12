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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.AsyncSemaphore.processAll;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
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
    private final int drainingMaxAttempts;
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

    private volatile boolean startedDraining;

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
        this.calculateDataPagesChecksum = dataServerConfig.isDataIntegrityVerificationEnabled();
        this.drainingMaxAttempts = dataServerConfig.getDrainingMaxAttempts();
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
        if (!startedDraining) {
            closer.register(chunkSpoolExecutor::shutdownNow);
        }
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
        checkState(!startedDraining, "addDataPages called in ChunkManager after we started draining");

        registerExchange(exchangeId);
        return getExchangeAndHeartbeat(exchangeId).addDataPages(partitionId, taskId, attemptId, dataPagesId, pages);
    }

    public ChunkDataResult getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.getChunkData(bufferNodeId, partitionId, chunkId, startedDraining);
    }

    public ChunkList listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.listClosedChunks(pagingId);
    }

    public void markAllClosedChunksReceived(String exchangeId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        exchange.markAllClosedChunksReceived();
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

    public synchronized void drainAllChunks()
    {
        checkState(!startedDraining, "already started draining");
        startedDraining = true;

        LOG.info("Start draining all chunks");
        chunkSpoolExecutor.shutdownNow(); // deschedule background spooling

        // finish all exchanges
        exchanges.values().forEach(Exchange::finish);

        // spool all chunks
        long backoff = 1_000;
        long maxBackOff = 60_000;
        int delayScaleFactor = 2;
        for (int i = 0; i < drainingMaxAttempts; ++i) {
            try {
                ImmutableList.Builder<Chunk> chunks = ImmutableList.builder();
                for (Exchange exchange : exchanges.values()) {
                    for (Partition partition : exchange.getPartitionsSortedBySizeDesc()) {
                        for (Chunk chunk : partition.getClosedChunks()) {
                            chunks.add(chunk);
                        }
                    }
                }
                spoolChunksSync(chunks.build());
                break;
            }
            catch (Throwable e) {
                LOG.warn(e, "spooling all chunks failed, retrying in %d milliseconds", backoff);
                try {
                    Thread.sleep(backoff);
                }
                catch (InterruptedException ignored) {
                    // ignore
                }
                backoff = Math.min(backoff * delayScaleFactor, maxBackOff);
            }
        }

        verify(getOpenChunks() == 0, "open chunks exist after spooling all chunks");
        verify(getClosedChunks() == 0, "closed chunks exist after spooling all chunks");

        LOG.info("Finished draining all chunks");

        LOG.info("Waiting for Trino to acknowledge all closed chunks of all exchanges");
        for (int i = 0; i < 1000; ++i) {
            if (exchanges.values().stream().allMatch(Exchange::isAllClosedChunksReceived)) {
                LOG.info("All closed chunks of all exchanges have been consumed by Trino, transition node state to DRAINED");
                return;
            }
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for all closed chunks to be acknowledged", e);
            }
        }

        for (Map.Entry<String, Exchange> entry : exchanges.entrySet()) {
            String exchangeId = entry.getKey();
            Exchange exchange = entry.getValue();
            if (!exchange.isAllClosedChunksReceived()) {
                LOG.warn("Failed to receive acknowledgement of receiving all closed chunks from exchange " + exchangeId);
            }
        }
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
    synchronized void spoolIfNecessary()
    {
        if (startedDraining || memoryAllocator.belowHighWatermark()) {
            return;
        }

        do {
            Map<String, Integer> exchangeSizes = exchanges.entrySet().stream().collect(toImmutableMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().getClosedChunksCount()));
            ToIntFunction<Exchange> exchangeSizeFunction = exchange -> exchangeSizes.getOrDefault(exchange.getExchangeId(), 0);

            List<Exchange> exchangesSortedBySizeDesc = exchanges.values().stream()
                    .sorted(Comparator.comparingInt(exchangeSizeFunction).reversed())
                    .collect(toImmutableList());
            long requiredMemory = memoryAllocator.getRequiredMemoryToRelease();
            long nominatedMemory = 0L;
            ImmutableList.Builder<Chunk> chunks = ImmutableList.builder();
            for (Exchange exchange : exchangesSortedBySizeDesc) {
                for (Partition partition : exchange.getPartitionsSortedBySizeDesc()) {
                    for (Chunk chunk : partition.getClosedChunks()) {
                        int allocatedMemory = chunk.getAllocatedMemory();
                        if (allocatedMemory > 0) {
                            chunks.add(chunk);
                        }
                        nominatedMemory += allocatedMemory;
                        if (nominatedMemory >= requiredMemory) {
                            break;
                        }
                    }
                    if (nominatedMemory >= requiredMemory) {
                        break;
                    }
                }
                if (nominatedMemory >= requiredMemory) {
                    break;
                }
            }

            List<Chunk> spoolCandidates = chunks.build();
            if (spoolCandidates.isEmpty()) {
                LOG.info("Memory allocation ratio %.2f%%, starting to close open chunks",
                        memoryAllocator.getAllocationPercentage());

                requiredMemory = memoryAllocator.getRequiredMemoryToRelease();
                nominatedMemory = 0L;
                for (Exchange exchange : exchangesSortedBySizeDesc) {
                    for (Partition partition : exchange.getPartitionsSortedBySizeDesc()) {
                        Optional<Chunk> candidate = partition.closeOpenChunkAndGet();
                        if (candidate.isPresent()) {
                            nominatedMemory += candidate.get().getAllocatedMemory();
                        }
                        if (nominatedMemory >= requiredMemory) {
                            break;
                        }
                    }
                    if (nominatedMemory >= requiredMemory) {
                        break;
                    }
                }

                if (nominatedMemory == 0) {
                    // No more chunks can be spooled at this point.
                    // It's possible for us to reach here, since we fulfill memory requests as soon as we release new
                    // memory. It's totally fine as long as we don't deadlock.
                    return;
                }
            }
            else {
                LOG.info("Memory allocation ratio %.2f%%, starting to spool closed chunks",
                        memoryAllocator.getAllocationPercentage());

                // blocking call here to make sure:
                // 1. No duplicate spooling
                // 2. Wait for pending writes to make progress as we release memory as a result of chunk spooling
                spoolChunksSync(spoolCandidates);
            }
        } while (memoryAllocator.aboveLowWatermark());
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

    private void spoolChunksSync(List<Chunk> chunks)
    {
        getFutureValue(processAll(
                chunks,
                chunk -> {
                    ChunkDataHolder chunkDataHolder = chunk.getChunkData();
                    if (chunkDataHolder == null) {
                        // already released
                        return immediateVoidFuture();
                    }
                    return Futures.transform(
                            spoolingStorage.writeChunk(bufferNodeId, chunk.getExchangeId(), chunk.getChunkId(), chunkDataHolder),
                            ignored -> {
                                chunk.release();
                                return null;
                            },
                            executor);
                },
                chunkSpoolConcurrency,
                executor));
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
