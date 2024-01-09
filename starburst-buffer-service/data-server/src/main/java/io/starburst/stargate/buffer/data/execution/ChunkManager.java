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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.BufferNodeStateManager;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.AsyncSemaphore.processAll;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.CREATED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SOURCE_STREAMING;
import static io.starburst.stargate.buffer.data.execution.SpooledChunksByExchange.decodeMetadataSlice;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getMetadataFileName;
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
    private final BufferNodeStateManager bufferNodeStateManager;
    private final int chunkTargetSizeInBytes;
    private final int chunkMaxSizeInBytes;
    private final int chunkSliceSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final int drainingMaxAttempts;
    private final Duration minDrainingDuration;
    private final int chunkListTargetSize;
    private final int chunkListMaxSize;
    private final Duration chunkListPollTimeout;
    private final Duration exchangeStalenessThreshold;
    private final Duration chunkSpoolInterval;
    private final int chunkSpoolConcurrency;
    private final boolean chunkSpoolMergeEnabled;
    private final int chunkSpoolMergeThreshold;
    private final MemoryAllocator memoryAllocator;
    private final SpoolingStorage spoolingStorage;
    private final Ticker ticker;
    private final SpooledChunksByExchange spooledChunksByExchange;
    private final DataServerStats dataServerStats;
    private final ExecutorService executor;

    // exchangeId -> exchange
    private final Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    private final ChunkIdGenerator chunkIdGenerator = new ChunkIdGenerator();
    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService statsReportingExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService chunkSpoolExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService exchangeTimeoutExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService eagerDeliveryModeExecutor = newSingleThreadScheduledExecutor();
    private final Cache<String, Object> recentlyRemovedExchanges = CacheBuilder.newBuilder().expireAfterWrite(5, MINUTES).build();
    private final LoadingCache<Long, Map<Long, SpooledChunk>> drainedSpooledChunkMap;
    private final Set<String> exchangesBeingReleased = ConcurrentHashMap.newKeySet();

    private volatile boolean startedDraining;

    @Inject
    public ChunkManager(
            BufferNodeId bufferNodeId,
            BufferNodeStateManager bufferNodeStateManager,
            ChunkManagerConfig chunkManagerConfig,
            DataServerConfig dataServerConfig,
            MemoryAllocator memoryAllocator,
            SpoolingStorage spoolingStorage,
            @ForChunkManager Ticker ticker,
            SpooledChunksByExchange spooledChunksByExchange,
            DataServerStats dataServerStats,
            ExecutorService executor)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.bufferNodeStateManager = requireNonNull(bufferNodeStateManager, "bufferNodeStateManager is null");
        this.chunkTargetSizeInBytes = toIntExact(chunkManagerConfig.getChunkTargetSize().toBytes());
        this.chunkMaxSizeInBytes = toIntExact(chunkManagerConfig.getChunkMaxSize().toBytes());
        this.chunkSliceSizeInBytes = toIntExact(chunkManagerConfig.getChunkSliceSize().toBytes());
        this.calculateDataPagesChecksum = dataServerConfig.isDataIntegrityVerificationEnabled();
        this.drainingMaxAttempts = dataServerConfig.getDrainingMaxAttempts();
        this.minDrainingDuration = dataServerConfig.getMinDrainingDuration();
        this.chunkListTargetSize = dataServerConfig.getChunkListTargetSize();
        this.chunkListMaxSize = dataServerConfig.getChunkListMaxSize();
        this.chunkListPollTimeout = dataServerConfig.getChunkListPollTimeout();
        this.exchangeStalenessThreshold = chunkManagerConfig.getExchangeStalenessThreshold();
        this.chunkSpoolInterval = chunkManagerConfig.getChunkSpoolInterval();
        this.chunkSpoolConcurrency = chunkManagerConfig.getChunkSpoolConcurrency();
        this.chunkSpoolMergeEnabled = chunkManagerConfig.isChunkSpoolMergeEnabled();
        this.chunkSpoolMergeThreshold = chunkManagerConfig.getChunkSpoolMergeThreshold();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingStorage = requireNonNull(spoolingStorage, "spoolingStorage is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.spooledChunksByExchange = requireNonNull(spooledChunksByExchange, "spooledChunkMapByExchange is null");
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.drainedSpooledChunkMap = CacheBuilder.newBuilder()
                .softValues()
                .build(new CacheLoader<>()
                {
                    @Override
                    public Map<Long, SpooledChunk> load(Long key)
                    {
                        try {
                            Slice slice = getFutureValue(spoolingStorage.readMetadataFile(key));
                            LOG.info("reading spooled chunk map for buffer node " + key + "; serialized size=" + slice.length());
                            return decodeMetadataSlice(slice);
                        }
                        catch (Throwable t) {
                            throw new DataServerException(INTERNAL_ERROR, "Error decoding metadata from file " + getMetadataFileName(key), t);
                        }
                    }
                });
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

        long eagerDeliveryModeCloseChunksInterval = Partition.MIN_EAGER_CLOSE_CHUNK_INTERVAL.toMillis() / 2;
        eagerDeliveryModeExecutor.scheduleWithFixedDelay(() -> {
            try {
                eagerDeliveryModeCloseChunksIfNeeded();
            }
            catch (Throwable e) {
                LOG.error(e, "Error calling eagerDeliveryModeCloseChunksIfNeeded");
            }
        }, eagerDeliveryModeCloseChunksInterval, eagerDeliveryModeCloseChunksInterval, MILLISECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        Closer closer = Closer.create();
        closer.register(cleanupExecutor::shutdownNow);
        closer.register(statsReportingExecutor::shutdownNow);
        closer.register(exchangeTimeoutExecutor::shutdownNow);
        closer.register(eagerDeliveryModeExecutor::shutdownNow);
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

    public AddDataPagesResult addDataPages(
            String exchangeId,
            int partitionId,
            int taskId,
            int attemptId,
            long dataPagesId,
            List<Slice> pages)
    {
        checkState(!startedDraining, "addDataPages called in ChunkManager after we started draining");

        internalRegisterExchange(exchangeId, SOURCE_STREAMING, STANDARD); // addDataPages may be sent before exchange is explicitly registered from coordinator
        return getExchangeAndHeartbeat(exchangeId).addDataPages(partitionId, taskId, attemptId, dataPagesId, pages);
    }

    public ChunkDataResult getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        if (chunkSpoolMergeEnabled) {
            return getChunkDataMerge(bufferNodeId, exchangeId, partitionId, chunkId);
        }
        return getChunkDataNoMerge(bufferNodeId, exchangeId, partitionId, chunkId);
    }

    public ChunkDataResult getChunkDataMerge(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        if (bufferNodeId != this.bufferNodeId) {
            // this is a request to get drained chunk data on a different node, return spooling file info directly
            Map<Long, SpooledChunk> spooledChunkMap = drainedSpooledChunkMap.getUnchecked(bufferNodeId);
            SpooledChunk spooledChunk = spooledChunkMap.get(chunkId);
            if (spooledChunk != null) {
                return ChunkDataResult.of(spooledChunk);
            }
            throw new DataServerException(CHUNK_NOT_FOUND, "No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(bufferNodeId, exchangeId, chunkId));
        }

        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.getChunkData(bufferNodeId, partitionId, chunkId, chunkSpoolMergeEnabled);
    }

    public ChunkDataResult getChunkDataNoMerge(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        if (bufferNodeId != this.bufferNodeId) {
            // this is a request to get drained chunk data on a different node, return spooling file info directly
            return ChunkDataResult.of(spoolingStorage.getSpooledChunk(bufferNodeId, exchangeId, chunkId));
        }

        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.getChunkData(bufferNodeId, partitionId, chunkId, chunkSpoolMergeEnabled);
    }

    public ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.listClosedChunks(pagingId);
    }

    public void markAllClosedChunksReceived(String exchangeId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        exchange.markAllClosedChunksReceived();
    }

    public void setChunkDeliveryMode(String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        exchange.setChunkDeliveryMode(chunkDeliveryMode);
    }

    public void registerExchange(String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
    {
        Exchange exchange = internalRegisterExchange(exchangeId, CREATED, chunkDeliveryMode);
        // Exchange could have been already registered explicitly with STANDARD chunkDeliveryMode
        // ensure proper value
        exchange.setChunkDeliveryMode(chunkDeliveryMode);
    }

    private Exchange internalRegisterExchange(String exchangeId, ExchangeState initialState, ChunkDeliveryMode chunkDeliveryMode)
    {
        return exchanges.computeIfAbsent(exchangeId, ignored -> {
            if (recentlyRemovedExchanges.getIfPresent(exchangeId) != null) {
                throw new DataServerException(EXCHANGE_NOT_FOUND, "exchange %s already removed".formatted(exchangeId));
            }

            return new Exchange(
                    bufferNodeId,
                    exchangeId,
                    initialState,
                    memoryAllocator,
                    spoolingStorage,
                    spooledChunksByExchange,
                    chunkTargetSizeInBytes,
                    chunkMaxSizeInBytes,
                    chunkSliceSizeInBytes,
                    calculateDataPagesChecksum,
                    chunkListTargetSize,
                    chunkListMaxSize,
                    chunkListPollTimeout,
                    chunkIdGenerator,
                    chunkDeliveryMode,
                    executor,
                    exchangeTimeoutExecutor,
                    tickerReadMillis());
        });
    }

    public void pingExchange(String exchangeId)
    {
        getExchangeAndHeartbeat(exchangeId);
    }

    public ListenableFuture<Void> finishExchange(String exchangeId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.finish();
    }

    public void removeExchange(String exchangeId)
    {
        spooledChunksByExchange.removeExchange(exchangeId);
        recentlyRemovedExchanges.put(exchangeId, "marker");
        Exchange exchange = exchanges.remove(exchangeId);
        if (exchange != null) {
            releaseChunks(exchange);
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

    public int getSpooledChunksCount()
    {
        if (chunkSpoolMergeEnabled) {
            return spooledChunksByExchange.getSpooledChunksCount();
        }
        return spoolingStorage.getSpooledChunksCount();
    }

    public synchronized void drainAllChunks()
    {
        checkState(!startedDraining, "already started draining");
        startedDraining = true;

        LOG.info("Start draining all chunks");
        long drainingStart = System.currentTimeMillis();
        chunkSpoolExecutor.shutdownNow(); // deschedule background spooling

        // finish all exchanges
        exchanges.values().forEach(exchange -> getFutureValue(exchange.finish()));

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

        // persist spooledChunkMapByExchange to S3
        if (chunkSpoolMergeEnabled && spooledChunksByExchange.size() > 0) {
            getFutureValue(spoolingStorage.writeMetadataFile(bufferNodeId, spooledChunksByExchange.encodeMetadataSlice()));
            LOG.info("Finished writing metadata of spooled chunks");
        }

        long remainingDrainingWaitMillis = minDrainingDuration.toMillis() - (System.currentTimeMillis() - drainingStart);

        if (remainingDrainingWaitMillis > 0) {
            // Ensure enough time passed before we enter phase when we wait for all exchanges has be acknowledged by owning Trino coordinators.
            // We want to be sure DRAINING state of buffer node is propagated to all Trino clusters and no new exchanges will be registered after
            // we are past that phase. If some exchanges are returned after that it does not impose correctness errors but queries which would use those
            // will fail eventually, when data node is shut down.

            LOG.info("Sleeping for %s so buffer node is kept in DRAINING state for at least %s", succinctDuration(remainingDrainingWaitMillis, MILLISECONDS), minDrainingDuration);
            sleepUninterruptibly(remainingDrainingWaitMillis, MILLISECONDS);
        }

        LOG.info("Waiting for Trino to acknowledge all closed chunks of all exchanges");
        for (int i = 0; i < 1000; ++i) {
            if (exchanges.values().stream().allMatch(Exchange::isAllClosedChunksReceived)) {
                LOG.info("All closed chunks of all exchanges have been consumed by Trino");
                return;
            }
            List<Exchange> pendingExchanges = exchanges.values().stream().filter(exchage -> !exchage.isAllClosedChunksReceived()).collect(Collectors.toList());
            for (Exchange pendingExchange : pendingExchanges) {
                // some exchanges could be added after we already started draining.
                // we wait for all addDataPages requests to complete before we enter drainAllChunks method
                // and not allow any new ones; so these new exchange are guaranteed to not contain any chunks.
                // We still need to finish those so information that they will not produce any chunk is propagated to
                // relevant Trino clusters.
                if (!pendingExchange.wasFinishTriggered()) {
                    verify(pendingExchange.getOpenChunksCount() == 0, "open chunk present in exchange %s", pendingExchange.getExchangeId());
                    verify(pendingExchange.getClosedChunksCount() == 0, "closed chunk present in exchange %s", pendingExchange.getExchangeId());
                    getFutureValue(pendingExchange.finish());
                }
            }
            sleepUninterruptibly(100, MILLISECONDS);
        }

        for (Map.Entry<String, Exchange> entry : exchanges.entrySet()) {
            String exchangeId = entry.getKey();
            Exchange exchange = entry.getValue();
            if (!exchange.isAllClosedChunksReceived()) {
                LOG.warn("Failed to receive acknowledgement of receiving all closed chunks from exchange " + exchangeId);
            }
        }

        int remainingExchangesBeingReleased = exchangesBeingReleased.size();
        if (remainingExchangesBeingReleased > 0) {
            LOG.warn("%s exchanges did not finish releasing spooled chunks", remainingExchangesBeingReleased);
        }
    }

    @VisibleForTesting
    void cleanupStaleExchanges()
    {
        if (bufferNodeStateManager.isDrainingStarted()) {
            // Do not clean up if node is already shutting down.
            // At this stage Trino clusters may no longer ping valid exchanges and
            // if draining process takes exceeds cleanup thresholds we may drop spooled
            // data needed by running queries.
            return;
        }
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
                spooledChunksByExchange.removeExchange(exchange.getExchangeId());
                releaseChunks(exchange);
            }
        }
    }

    private void releaseChunks(Exchange exchange)
    {
        String exchangeId = exchange.getExchangeId();
        exchangesBeingReleased.add(exchangeId);
        ListenableFuture<Void> future = exchange.releaseChunks();
        future.addListener(() -> exchangesBeingReleased.remove(exchangeId), directExecutor());
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

    @VisibleForTesting
    Exchange getExchangeAndHeartbeat(String exchangeId)
    {
        Exchange exchange = exchanges.get(exchangeId);
        if (exchange == null) {
            throw new DataServerException(EXCHANGE_NOT_FOUND, "exchange %s not found".formatted(exchangeId));
        }
        exchange.setLastUpdateTime(tickerReadMillis());
        return exchange;
    }

    // only used for simulating the case where we drain chunks and start a new data server
    @VisibleForTesting
    void clearSpooledChunkByExchange()
    {
        spooledChunksByExchange.clear();
    }

    private void spoolChunksSync(List<Chunk> chunks)
    {
        if (chunkSpoolMergeEnabled) {
            spoolChunksMerge(chunks);
        }
        else {
            // TODO: drop this code path after spooling merged chunks is stable (https://github.com/starburstdata/trino-buffer-service/issues/414)
            spoolChunksNoMerge(chunks);
        }
    }

    private void spoolChunksMerge(List<Chunk> chunks)
    {
        ImmutableList.Builder<ChunksWithExchangeId> chunksWithExchangeIdBuilder = ImmutableList.builder();
        for (Map.Entry<String, Collection<Chunk>> entry : Multimaps.index(chunks, Chunk::getExchangeId).asMap().entrySet()) {
            String exchangeId = entry.getKey();
            // order by chunk id to reduce disk seeks on cloud object storage when reading
            List<Chunk> chunkList = entry.getValue().stream().sorted(Comparator.comparingLong(Chunk::getChunkId)).collect(toImmutableList());
            for (List<Chunk> partitionedChunkList : Lists.partition(chunkList, chunkSpoolMergeThreshold)) {
                chunksWithExchangeIdBuilder.add(new ChunksWithExchangeId(exchangeId, partitionedChunkList));
            }
        }
        getFutureValue(processAll(
                chunksWithExchangeIdBuilder.build(),
                chunksWithExchangeId -> {
                    String exchangeId = chunksWithExchangeId.exchangeId();
                    List<Chunk> chunkList = chunksWithExchangeId.chunks();
                    ImmutableMap.Builder<Chunk, ChunkDataLease> chunkDataLeasesMapBuilder = ImmutableMap.builder();
                    long contentLength = 0;
                    for (Chunk chunk : chunkList) {
                        ChunkDataLease chunkDataLease = chunk.getChunkDataLease();
                        if (chunkDataLease == null) {
                            continue;
                        }
                        contentLength += chunkDataLease.serializedSizeInBytes();
                        chunkDataLeasesMapBuilder.put(chunk, chunkDataLease);
                    }
                    Map<Chunk, ChunkDataLease> chunkDataLeaseMap = chunkDataLeasesMapBuilder.build();

                    if (chunkDataLeaseMap.isEmpty()) {
                        // all chunks released in the meantime
                        return immediateVoidFuture();
                    }

                    ListenableFuture<Map<Long, SpooledChunk>> spoolingFuture = spoolingStorage.writeMergedChunks(
                            bufferNodeId,
                            exchangeId,
                            chunkDataLeaseMap,
                            contentLength);
                    // in case of failure we still need to decrease reference count to avoid memory leak
                    addExceptionCallback(spoolingFuture, failure -> chunkDataLeaseMap.values().forEach(ChunkDataLease::release));
                    return Futures.transform(
                            spoolingFuture,
                            spooledChunkMap -> {
                                spooledChunksByExchange.update(exchangeId, spooledChunkMap);
                                chunkDataLeaseMap.forEach((chunk, chunkDataLease) -> {
                                    chunkDataLease.release();
                                    chunk.release();
                                });
                                return null;
                            },
                            executor);
                },
                chunkSpoolConcurrency,
                executor));
    }

    private void spoolChunksNoMerge(List<Chunk> chunks)
    {
        getFutureValue(processAll(
                chunks,
                chunk -> {
                    ChunkDataLease chunkDataLease = chunk.getChunkDataLease();
                    if (chunkDataLease == null) {
                        // already released
                        return immediateVoidFuture();
                    }

                    ListenableFuture<Void> spoolingFuture = spoolingStorage.writeChunk(bufferNodeId, chunk.getExchangeId(), chunk.getChunkId(), chunkDataLease);
                    // in case of failure we still need to decrease reference count to avoid memory leak
                    addExceptionCallback(spoolingFuture, failure -> chunkDataLease.release());

                    return Futures.transform(
                            spoolingFuture,
                            ignored -> {
                                chunkDataLease.release();
                                chunk.release();
                                return null;
                            },
                            executor);
                },
                chunkSpoolConcurrency,
                executor));
    }

    private void eagerDeliveryModeCloseChunksIfNeeded()
    {
        exchanges.values().forEach(Exchange::eagerDeliveryModeCloseChunksIfNeeded);
    }

    private void reportStats()
    {
        dataServerStats.updateTrackedExchanges(getTrackedExchanges());
        dataServerStats.updateOpenChunks(getOpenChunks());
        dataServerStats.updateClosedChunks(getClosedChunks());
        dataServerStats.updateSpooledChunks(getSpooledChunksCount());
        dataServerStats.updateSpooledChunksByExchangeSize(spooledChunksByExchange.size());
    }

    private long tickerReadMillis()
    {
        return ticker.read() / 1_000_000;
    }

    record ChunksWithExchangeId(
            String exchangeId,
            List<Chunk> chunks)
    {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForChunkManager {}
}
