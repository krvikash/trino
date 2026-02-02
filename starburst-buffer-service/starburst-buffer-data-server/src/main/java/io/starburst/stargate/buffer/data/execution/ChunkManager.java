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
import com.google.common.hash.HashFunction;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.concurrent.AsyncSemaphore;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.buffer.data.client.BufferNodeExchangeMetrics;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateCancelledFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_CORRUPTED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.EXCHANGE_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.execution.ChunkManager.ExchangeRemovalReason.ABANDONED;
import static io.starburst.stargate.buffer.data.execution.ChunkManager.ExchangeRemovalReason.EXPLICIT;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.CREATED;
import static io.starburst.stargate.buffer.data.execution.ExchangeState.SOURCE_STREAMING;
import static io.starburst.stargate.buffer.data.execution.SpooledChunksByExchange.decodeMetadataSlice;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getMetadataFileName;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
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
    private static final Logger log = Logger.get(ChunkManager.class);
    private static final HashFunction HASH_FUNC = murmur3_32_fixed();

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
    private final int chunkSpoolMergeThreshold;
    private final MemoryAllocator memoryAllocator;
    private final SpoolingStorage spoolingStorage;
    private final Ticker ticker;
    private final SpooledChunksByExchange spooledChunksByExchange;
    private final DataServerStats dataServerStats;
    private final Tracer tracer;
    private final ExecutorService executor;

    enum ExchangeRemovalReason {
        EXPLICIT,
        ABANDONED
    }

    // exchangeId -> exchange
    private final Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    private final ChunkIdGenerator chunkIdGenerator = new ChunkIdGenerator();
    private final ScheduledExecutorService cleanupExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("chunk-manager-cleanup-%s"));
    private final ScheduledExecutorService statsReportingExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("chunk-manager-stats-reporting-%s"));
    private final ScheduledExecutorService chunkSpoolExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("chunk-manager-spool-%s"));
    private final ScheduledExecutorService exchangeTimeoutExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("chunk-manager-exchange-timeout-%s"));
    private final ScheduledExecutorService eagerDeliveryModeExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("chunk-manager-eager-delivery-%s"));
    private final ScheduledExecutorService traceResourceReportExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("chunk-manager-trace-resource-report-%s"));
    private final Cache<String, ExchangeRemovalReason> recentlyRemovedExchanges = buildNonEvictableCache(CacheBuilder.newBuilder().expireAfterWrite(5, MINUTES));
    private final LoadingCache<Long, Map<Long, SpooledChunk>> drainedSpooledChunkMap;
    private final Set<String> exchangesBeingReleased = ConcurrentHashMap.newKeySet();
    private final boolean traceResourceReportingEnabled;
    private final Duration traceResourceReportInterval;
    final int traceResourceMaximumReportsPerExchange;
    private final String trinoPlaneId;
    private final AtomicBoolean resourceReportInProgress = new AtomicBoolean();

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
            Tracer tracer,
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
        this.traceResourceReportingEnabled = dataServerConfig.isTraceResourceReportingEnabled();
        this.traceResourceReportInterval = requireNonNull(dataServerConfig.getTraceResourceReportInterval(), "traceResourceReportInterval is null");
        this.traceResourceMaximumReportsPerExchange = dataServerConfig.getTraceResourceMaximumReportsPerExchange();
        this.trinoPlaneId = dataServerConfig.getTrinoPlaneId();
        if (traceResourceReportingEnabled) {
            requireNonNull(trinoPlaneId, "trinoPlaneId must be set if trace resource reporting is enabled");
        }
        this.exchangeStalenessThreshold = chunkManagerConfig.getExchangeStalenessThreshold();
        this.chunkSpoolInterval = chunkManagerConfig.getChunkSpoolInterval();
        this.chunkSpoolConcurrency = chunkManagerConfig.getChunkSpoolConcurrency();
        this.chunkSpoolMergeThreshold = chunkManagerConfig.getChunkSpoolMergeThreshold();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.spoolingStorage = requireNonNull(spoolingStorage, "spoolingStorage is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.spooledChunksByExchange = requireNonNull(spooledChunksByExchange, "spooledChunkMapByExchange is null");
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.drainedSpooledChunkMap = buildNonEvictableCache(
                CacheBuilder.newBuilder().softValues(),
                new CacheLoader<>()
                {
                    private final Set<Long> decodingFailures = ConcurrentHashMap.newKeySet();

                    @Override
                    public Map<Long, SpooledChunk> load(Long key)
                    {
                        Slice slice = null;
                        try {
                            slice = getFutureValue(spoolingStorage.readMetadataFile(key));
                            log.info("reading spooled chunk map for buffer node " + key + "; serialized size=" + slice.length());
                            return decodeMetadataSlice(slice);
                        }
                        catch (Throwable t) {
                            if (slice != null && !decodingFailures.contains(key)) {
                                dumpMetadataFile(key, slice);
                                // ensure we log only once to not overwhelm logging system
                                decodingFailures.add(key);
                            }
                            throw new DataServerException(EXCHANGE_CORRUPTED, "Error decoding metadata from file " + getMetadataFileName(key), t);
                        }
                    }

                    private void dumpMetadataFile(Long key, Slice slice)
                    {
                        String sliceBase64 = Base64.getEncoder().encodeToString(slice.getBytes());
                        int parts = (sliceBase64.length() - 1) / 1000 + 1;
                        log.warn("Error decoding metadata from file " + getMetadataFileName(key));
                        for (int i = 0; i < parts; ++i) {
                            log.warn("metadata contents (%s/%s): %s", i + 1, parts, sliceBase64.substring(i * 1000, Math.min(sliceBase64.length(), (i + 1) * 1000)));
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
                log.error(e, "Error cleaning up stale exchanges");
            }
        }, exchangeCleanupInterval, exchangeCleanupInterval, MILLISECONDS);
        statsReportingExecutor.scheduleWithFixedDelay(this::reportStats, 0, 1, SECONDS);
        long spoolInterval = chunkSpoolInterval.toMillis();
        chunkSpoolExecutor.scheduleWithFixedDelay(() -> {
            try {
                spoolIfNecessary();
            }
            catch (Throwable e) {
                log.error(e, "Error spooling chunks");
            }
        }, spoolInterval, spoolInterval, MILLISECONDS);

        long eagerDeliveryModeCloseChunksInterval = Partition.MIN_EAGER_CLOSE_CHUNK_INTERVAL.toMillis() / 2;
        eagerDeliveryModeExecutor.scheduleWithFixedDelay(() -> {
            try {
                eagerDeliveryModeCloseChunksIfNeeded();
            }
            catch (Throwable e) {
                log.error(e, "Error calling eagerDeliveryModeCloseChunksIfNeeded");
            }
        }, eagerDeliveryModeCloseChunksInterval, eagerDeliveryModeCloseChunksInterval, MILLISECONDS);

        if (traceResourceReportingEnabled) {
            // Add a cluster unique adjustment to the start value to minimize herd reporting spans
            long adjustment = HASH_FUNC.hashBytes(trinoPlaneId.getBytes(StandardCharsets.UTF_8)).asInt() % traceResourceReportInterval.toMillis();
            // When reporting intervals are large (minutes), it is ideal if all servers in the cluster reported events at roughly the same time.
            long nextCheckpointTime = traceResourceReportInterval.toMillis() + adjustment - (Instant.now().toEpochMilli() % traceResourceReportInterval.toMillis());
            traceResourceReportExecutor.scheduleAtFixedRate(() -> {
                try {
                    // Avoid having multiple reports running concurrently.
                    if (resourceReportInProgress.compareAndSet(false, true)) {
                        writeResourceReportToExchangeSpans();
                    }
                    else {
                        log.warn("New resource report started before the prior report completed");
                    }
                }
                catch (Throwable e) {
                    log.error(e, "Error calling writeResourceReportToExchangeSpans");
                }
                finally {
                    resourceReportInProgress.set(false);
                }
            }, nextCheckpointTime, traceResourceReportInterval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        Closer closer = Closer.create();
        closer.register(cleanupExecutor::shutdownNow);
        closer.register(statsReportingExecutor::shutdownNow);
        closer.register(exchangeTimeoutExecutor::shutdownNow);
        closer.register(eagerDeliveryModeExecutor::shutdownNow);
        closer.register(traceResourceReportExecutor::shutdownNow);
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

    public void logAddDataPagesInProgressDebugInfo()
    {
        log.info("In progress addDataPages debug info:");
        exchanges.values().forEach(Exchange::logAddDataPagesInProgressDebugInfo);
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
        return exchange.getChunkData(bufferNodeId, partitionId, chunkId);
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

    public void registerExchange(String exchangeId, ChunkDeliveryMode chunkDeliveryMode, Optional<Span> parentSpan)
    {
        Exchange exchange = internalRegisterExchange(exchangeId, CREATED, chunkDeliveryMode);
        // Exchange could have been already registered explicitly with STANDARD chunkDeliveryMode
        // ensure proper value
        exchange.setChunkDeliveryMode(chunkDeliveryMode);
        exchange.startExchangeSpan(parentSpan, exchangeId);
    }

    private Exchange internalRegisterExchange(String exchangeId, ExchangeState initialState, ChunkDeliveryMode chunkDeliveryMode)
    {
        return exchanges.computeIfAbsent(exchangeId, ignored -> {
            ExchangeRemovalReason removalReason = recentlyRemovedExchanges.getIfPresent(exchangeId);
            if (removalReason != null) {
                throw new DataServerException(EXCHANGE_NOT_FOUND, "exchange %s already removed (%s)".formatted(exchangeId, removalReason));
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
                    tracer,
                    traceResourceMaximumReportsPerExchange,
                    tickerReadMillis());
        });
    }

    public BufferNodeExchangeMetrics pingExchange(String exchangeId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.collectBufferNodeMetrics();
    }

    public ListenableFuture<Void> finishExchange(String exchangeId)
    {
        Exchange exchange = getExchangeAndHeartbeat(exchangeId);
        return exchange.finish();
    }

    public void removeExchange(String exchangeId)
    {
        try {
            spooledChunksByExchange.removeExchange(exchangeId);
        }
        catch (SpooledChunksByExchange.AlreadyFrozenException e) {
            // this is valid to get exchange remove request while node is draining
            log.debug("Exchange %s removed after spooled chunks map is already frozen", exchangeId);
        }
        recentlyRemovedExchanges.put(exchangeId, EXPLICIT);
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
        return spooledChunksByExchange.getSpooledChunksCount();
    }

    public synchronized void drainAllChunks()
    {
        checkState(!startedDraining, "already started draining");
        startedDraining = true;

        log.info("Start draining all chunks");
        long drainingStart = System.currentTimeMillis();
        chunkSpoolExecutor.shutdownNow(); // deschedule background spooling

        List<Exchange> validExchanges = new ArrayList<>();
        Set<Exchange> failedExchanges = new HashSet<>();

        // finish all exchanges
        exchanges.values().forEach(exchange -> {
            try {
                getFutureValue(exchange.finish());
                validExchanges.add(exchange);
            }
            catch (Throwable e) {
                // skip failed exchanges; there should be none, but we want to be conservative and reduce blast radius in case we have some.
                log.error(e, "Exception while draining exchange %s", exchange.getExchangeId());
                failedExchanges.add(exchange);
            }
        });

        // spool all chunks
        long backoff = 1_000;
        long maxBackOff = 60_000;
        int delayScaleFactor = 2;
        for (int i = 0; i < drainingMaxAttempts; ++i) {
            try {
                ImmutableList.Builder<Chunk> chunks = ImmutableList.builder();
                for (Exchange exchange : validExchanges) {
                    for (Partition partition : exchange.getPartitionsSortedBySizeDesc()) {
                        for (Chunk chunk : partition.getClosedChunks()) {
                            chunks.add(chunk);
                        }
                    }
                }
                if (spoolChunksSync(chunks.build())) {
                    break;
                }
                log.warn("spooling all chunks failed, retrying in %d milliseconds", backoff);
            }
            catch (RuntimeException e) {
                log.warn(e, "spooling all chunks failed, retrying in %d milliseconds", backoff);
            }
            try {
                Thread.sleep(backoff);
            }
            catch (InterruptedException ignored) {
                // ignore
            }
            backoff = Math.min(backoff * delayScaleFactor, maxBackOff);
        }

        verify(getOpenChunks() == 0, "open chunks exist after spooling all chunks");
        verify(getClosedChunks() == 0, "closed chunks exist after spooling all chunks");

        log.info("Finished draining all chunks");

        // persist spooledChunkMapByExchange to S3
        if (spooledChunksByExchange.size() > 0) {
            Map<String, Map<Long, SpooledChunk>> spooledChunksMap = spooledChunksByExchange.freeze();
            getFutureValue(spoolingStorage.writeMetadataFile(bufferNodeId, SpooledChunksByExchange.encodeMetadataSlice(spooledChunksMap)));
            log.info("Finished writing metadata of spooled chunks");
        }

        long remainingDrainingWaitMillis = minDrainingDuration.toMillis() - (System.currentTimeMillis() - drainingStart);

        if (remainingDrainingWaitMillis > 0) {
            // Ensure enough time passed before we enter phase when we wait for all exchanges has be acknowledged by owning Trino coordinators.
            // We want to be sure DRAINING state of buffer node is propagated to all Trino clusters and no new exchanges will be registered after
            // we are past that phase. If some exchanges are returned after that it does not impose correctness errors but queries which would use those
            // will fail eventually, when data node is shut down.

            log.info("Sleeping for %s so buffer node is kept in DRAINING state for at least %s", succinctDuration(remainingDrainingWaitMillis, MILLISECONDS), minDrainingDuration);
            sleepUninterruptibly(remainingDrainingWaitMillis, MILLISECONDS);
        }

        log.info("Waiting for Trino to acknowledge all closed chunks of all exchanges");
        for (int i = 0; i < 1000; ++i) {
            if (validExchanges.stream().allMatch(Exchange::isAllClosedChunksReceived)) {
                log.info("All closed chunks of all exchanges have been consumed by Trino");
                return;
            }
            List<Exchange> pendingExchanges = exchanges.values().stream().filter(exchange -> !failedExchanges.contains(exchange) && !exchange.isAllClosedChunksReceived()).toList();
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

        for (Exchange exchange : validExchanges) {
            if (!exchange.isAllClosedChunksReceived()) {
                log.warn("Failed to receive acknowledgement of receiving all closed chunks from exchange " + exchange.getExchangeId());
            }
        }

        int remainingExchangesBeingReleased = exchangesBeingReleased.size();
        if (remainingExchangesBeingReleased > 0) {
            log.warn("%s exchanges did not finish releasing spooled chunks", remainingExchangesBeingReleased);
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
                log.info("forgetting exchange %s; no update for %s", entry.getKey(), succinctDuration(now - lastUpdateTime, MILLISECONDS));
                recentlyRemovedExchanges.put(exchange.getExchangeId(), ABANDONED);
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
            Map<String, Integer> exchangeSizes = exchanges.entrySet().stream()
                    .collect(toImmutableMap(
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
                log.info("Memory allocation ratio %.2f%%, starting to close open chunks",
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
                log.debug("Memory allocation ratio %.2f%%, starting to spool closed chunks",
                        memoryAllocator.getAllocationPercentage());

                // blocking call here to make sure:
                // 1. No duplicate spooling
                // 2. Wait for pending writes to make progress as we release memory as a result of chunk spooling
                if (!spoolChunksSync(spoolCandidates)) {
                    // If any spool tasks fail break the loop and try again in the next time interval.
                    return;
                }
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

    // TODO: Remove when method is available in airlift
    /**
     * Process a list of tasks as a single unit
     * (similar to {@link com.google.common.util.concurrent.Futures#successfulAsList(ListenableFuture[])})
     * with limiting the number of tasks running in parallel.
     * <p>
     * This method may be useful for limiting the number of concurrent requests sent to a remote server when
     * trying to run multiple related entities concurrently:
     * <p>
     * For example:
     * <pre>{@code
     * List<Integer> userIds = Lists.of(1, 2, 3);
     * ListenableFuture<List<UserInfo>> future = processAllToCompletion(ids, client::getUserInfoById, 2, executor);
     * List<UserInfo> userInfos = future.get(...);
     * }</pre>
     *
     * @param tasks tasks to process
     * @param submitter task submitter
     * @param maxConcurrency maximum number of tasks allowed to run in parallel
     * @param submitExecutor task submission executor
     * @return {@link ListenableFuture} containing a list of values returned by the {@code tasks}.
     * The order of elements in the list matches the order of {@code tasks}.
     * If the result future is cancelled all the remaining tasks are cancelled (submitted tasks will be cancelled, pending tasks will not be submitted).
     * If any of the submitted tasks fails or are cancelled, the remaining tasks will continue to execute.
     * If any of the submitted tasks fails or are cancelled, the remaining pending tasks are cancelled.
     */
    private static <T, R> ListenableFuture<List<R>> processAllToCompletion(List<T> tasks, Function<T, ListenableFuture<R>> submitter, int maxConcurrency, Executor submitExecutor)
    {
        SettableFuture<List<R>> resultFuture = SettableFuture.create();
        AsyncSemaphore<T, R> semaphore = new AsyncSemaphore<>(maxConcurrency, submitExecutor, task -> {
            if (resultFuture.isCancelled()) {
                // Task cancellation tends to happen in task submission order, which can race with subsequent task submissions after previous cancellations.
                // This eager check prevents this race from occurring, and can reduce the number of unnecessary submissions.
                return immediateCancelledFuture();
            }
            return submitter.apply(task);
        });
        resultFuture.setFuture(successfulAsList(tasks.stream()
                .map(semaphore::submit)
                .collect(toImmutableList())));
        return resultFuture;
    }

    private boolean spoolChunksSync(List<Chunk> chunks)
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
        List<ChunksWithExchangeId> chunksWithExchangeIds = chunksWithExchangeIdBuilder.build();
        CountDownLatch countCompletions = new CountDownLatch(chunksWithExchangeIds.size());
        ArrayList<Throwable> failures = new ArrayList<>();

        getFutureValue(processAllToCompletion(
                chunksWithExchangeIds,
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
                    Map<Chunk, ChunkDataLease> chunkDataLeaseMap = chunkDataLeasesMapBuilder.buildOrThrow();

                    if (chunkDataLeaseMap.isEmpty()) {
                        // all chunks released in the meantime
                        countCompletions.countDown();
                        return immediateFuture(true);
                    }

                    Exchange exchange = exchanges.get(exchangeId);
                    if (exchange == null) {
                        // exchange gone; not spooling
                        countCompletions.countDown();
                        return immediateFuture(true);
                    }
                    ListenableFuture<Map<Long, SpooledChunk>> spoolingFuture = spoolingStorage.writeMergedChunks(
                            bufferNodeId,
                            exchangeId,
                            chunkDataLeaseMap,
                            contentLength);
                    addCallback(spoolingFuture, new FutureCallback<>()
                            {
                                @Override
                                public void onSuccess(Map<Long, SpooledChunk> spooledChunkMap)
                                {
                                    try {
                                        exchange.markSpooled();
                                        spooledChunksByExchange.update(exchangeId, spooledChunkMap);
                                        chunkDataLeaseMap.forEach((chunk, chunkDataLease) -> {
                                            exchange.chunkSpooled(chunk.getHandle());
                                            chunkDataLease.release();
                                            chunk.release();
                                        });
                                    }
                                    finally {
                                        countCompletions.countDown();
                                    }
                                }

                                @Override
                                public void onFailure(Throwable t)
                                {
                                    try {
                                        // in case of failure we still need to decrease reference count to avoid memory leak
                                        chunkDataLeaseMap.values().forEach(ChunkDataLease::release);
                                        failures.add(t);
                                    }
                                    finally {
                                        countCompletions.countDown();
                                    }
                                }
                            },
                            executor);
                    return null;
                },
                chunkSpoolConcurrency,
                executor));
        try {
            countCompletions.await();
            if (!failures.isEmpty()) {
                printSpoolingExceptions(failures);
                return false;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("spoolChunksSync was interrupted. Some leases may not have been released.", e);
        }
        return true;
    }

    private void printSpoolingExceptions(List<Throwable> failures)
    {
        if (failures.size() > 1) {
            StringBuilder sb = new StringBuilder("Spooled chunks failed with multiple exceptions.\nSubsequent exception(s):\n");
            for (int offset = 1; offset < failures.size(); offset++) {
                Throwable failure = failures.get(offset);
                sb.append(failure.getClass().getName()).append(": ").append(failure.getMessage()).append("\n");
                Throwable cause = failure.getCause();
                while (cause != null) {
                    sb.append("  Caused by ").append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append("\n");
                    cause = cause.getCause();
                }
            }
            sb.append("\nFirst exception:");
            log.error(failures.get(0), sb.toString());
        }
        else {
            log.error(failures.get(0), "Spooling chunks failed");
        }
    }

    private void eagerDeliveryModeCloseChunksIfNeeded()
    {
        exchanges.values().forEach(Exchange::eagerDeliveryModeCloseChunksIfNeeded);
    }

    private void writeResourceReportToExchangeSpans()
    {
        exchanges.values().forEach(Exchange::writeResourceReportToSpan);
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

    public int getMaxPageLength()
    {
        return chunkMaxSizeInBytes - DATA_PAGE_HEADER_SIZE;
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
