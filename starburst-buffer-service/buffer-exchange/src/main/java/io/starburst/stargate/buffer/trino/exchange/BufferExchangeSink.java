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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class BufferExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(BufferExchangeSink.class);
    private static final Duration MAPPING_SCALER_INTERVAL = Duration.succinctDuration(3, TimeUnit.SECONDS);

    private final DataApiFacade dataApi;
    private final String externalExchangeId;
    private final int taskPartitionId;
    private final int taskAttemptId;
    private final boolean preserveOrderWithinPartition;

    @GuardedBy("this")
    // temporary holder for new mapping set during mapping update
    // TODO should be possible to get rid of it we change the update flow in such way that we
    // do not replace writers
    Optional<PartitionNodeMapping> mappingForUpdate = Optional.empty();

    @GuardedBy("this")
    private final Map<Long, SinkWriter> writers = new HashMap<>(); // buffer node -> writer

    // active mapping; can be extended at runtime if we see some buffer nodes are over or under utilized
    // due to access structure access this field cannot be synchronized
    private final AtomicReference<ActiveMapping> activeMapping = new AtomicReference<>();

    private final SinkMappingScaler mappingScaler;
    private final ScheduledFuture<?> callMappingScalerFuture;

    @GuardedBy("this")
    private boolean handleUpdateInProgress;
    @GuardedBy("this")
    private volatile boolean handleUpdateRequired;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final ExecutorService executor;
    private final AtomicReference<CompletableFuture<Void>> blockedFutureReference = new AtomicReference<>(CompletableFuture.completedFuture(null));
    @GuardedBy("this")
    private volatile CompletableFuture<Void> finishFuture; // volatile for perfoming best-effort sanity checks

    private final SinkDataPool dataPool;
    private final DataPagesIdGenerator dataPagesIdGenerator = new DataPagesIdGenerator();

    public BufferExchangeSink(
            DataApiFacade dataApi,
            BufferExchangeSinkInstanceHandle sinkInstanceHandle,
            DataSize memoryLowWaterMark,
            DataSize memoryHighWaterMark,
            Duration maxWait,
            int minWrittenPagesCount,
            DataSize minWrittenPagesSize,
            int targetWrittenPagesCount,
            DataSize targetWrittenPagesSize,
            int targetWrittenPartitionsCount,
            Duration minTimeBetweenWritersScaleUps,
            double maxWritersScaleUpGrowthFactor,
            ExecutorService executor,
            ScheduledExecutorService scheduledExecutor)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        requireNonNull(sinkInstanceHandle, "sinkInstanceHandle is null");
        this.externalExchangeId = sinkInstanceHandle.getExternalExchangeId();
        this.taskPartitionId = sinkInstanceHandle.getTaskPartitionId();
        this.taskAttemptId = sinkInstanceHandle.getTaskAttemptId();
        this.preserveOrderWithinPartition = sinkInstanceHandle.isPreserveOrderWithinPartition();
        this.executor = requireNonNull(executor, "executor is null");
        this.dataPool = new SinkDataPool(
                memoryLowWaterMark,
                memoryHighWaterMark,
                maxWait,
                minWrittenPagesCount,
                minWrittenPagesSize,
                targetWrittenPagesCount,
                targetWrittenPagesSize,
                targetWrittenPartitionsCount,
                Ticker.systemTicker());

        PartitionNodeMapping partitionNodeMapping = sinkInstanceHandle.getPartitionNodeMapping();
        this.mappingScaler = new SinkMappingScaler(
                partitionNodeMapping,
                minTimeBetweenWritersScaleUps,
                maxWritersScaleUpGrowthFactor,
                externalExchangeId,
                taskPartitionId);

        activeMapping.set(new ActiveMapping(ImmutableListMultimap.of()));
        callMappingScaler();

        callMappingScalerFuture = scheduledExecutor.scheduleWithFixedDelay(this::callMappingScalerSafe, MAPPING_SCALER_INTERVAL.toMillis(), MAPPING_SCALER_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void callMappingScalerSafe()
    {
        try {
            callMappingScaler();
        }
        catch (Exception e) {
            log.error(e, "Unexpected exception from MappingScaler");
            markFailed(e);
        }
    }

    private void callMappingScaler()
    {
        ActiveMapping mapping;
        synchronized (this) {
            if (handleUpdateInProgress) {
                return;
            }
            mapping = activeMapping.get();
        }
        SinkMappingScaler.Result result = mappingScaler.process(dataPool, mapping);

        Set<SinkWriter> newOrUpdatedWriters = new HashSet<>();

        boolean finishing;
        synchronized (this) {
            if (handleUpdateInProgress) {
                return;
            }

            if (dataPool.whenFinished().isDone()) {
                return;
            }

            if (result.writersToAdd().isEmpty()) {
                return;
            }

            // ensure writers are created for all bufferNodeIds
            Set<Long> bufferNodeIds = result.writersToAdd().entries().stream().map(Map.Entry::getValue).collect(toImmutableSet());
            for (Long bufferNodeId : bufferNodeIds) {
                if (!writers.containsKey(bufferNodeId)) {
                    SinkWriter writer = createWriter(bufferNodeId);
                    writers.put(bufferNodeId, writer);
                }
                newOrUpdatedWriters.add(writers.get(bufferNodeId));
            }

            // update active mappings
            for (Map.Entry<Integer, Long> entry : result.writersToAdd().entries()) {
                Integer partition = entry.getKey();
                Long bufferNodeId = entry.getValue();
                activeMapping.get().add(partition, bufferNodeId);
            }
            finishing = finishFuture != null;
        }
        // trigger work for new writers
        newOrUpdatedWriters.forEach(writer -> writer.scheduleWriting(Optional.empty(), finishing));
    }

    private SinkWriter createWriter(long bufferNodeId)
    {
        return new SinkWriter(
                dataApi,
                dataPool,
                executor,
                externalExchangeId,
                taskPartitionId,
                taskAttemptId,
                preserveOrderWithinPartition,
                bufferNodeId,
                () -> activeMapping.get().getPartitionsForBufferNode(bufferNodeId),
                dataPagesIdGenerator,
                new SinkWriter.FinishCallback()
                {
                    private final AtomicReference<String> called = new AtomicReference<>();

                    @Override
                    public void done()
                    {
                        try {
                            verifyNotCalled("done");
                            writerDone(bufferNodeId);
                        }
                        catch (Throwable t) {
                            log.error(t, "unexpected exception thrown from writerDone for exchange %s, bufferNode %s", externalExchangeId, bufferNodeId);
                            markFailed(t);
                            throw t;
                        }
                    }

                    @Override
                    public void targetDraining()
                    {
                        try {
                            verifyNotCalled("targetDraining");
                            writerTargetDraining(bufferNodeId);
                        }
                        catch (Throwable t) {
                            log.error(t, "unexpected exception thrown from writerTargetDraining for exchange %s, bufferNode %s", externalExchangeId, bufferNodeId);
                            markFailed(t);
                            throw t;
                        }
                    }

                    @Override
                    public void failed(Throwable failure)
                    {
                        try {
                            verifyNotCalled("failed");
                            writerFailed(bufferNodeId, failure);
                        }
                        catch (Throwable t) {
                            log.error(t, "unexpected exception thrown from writerFailed for exchange %s, bufferNode %s", externalExchangeId, bufferNodeId);
                            markFailed(t);
                            throw t;
                        }
                    }

                    private void verifyNotCalled(String operation)
                    {
                        verify(called.compareAndSet(null, operation), "Finish callback already called(%s) for %s", called.get(), bufferNodeId);
                    }
                });
    }

    private synchronized void writerDone(long bufferNodeId)
    {
        removeWriter(bufferNodeId);
        progressOnHandleUpdate();
    }

    @GuardedBy("this")
    private void removeWriter(long bufferNodeId)
    {
        SinkWriter removedWriter = writers.remove(bufferNodeId);
        verify(removedWriter != null, "no writer found for buffer node %d", bufferNodeId);
    }

    private synchronized void writerTargetDraining(long bufferNodeId)
    {
        removeWriter(bufferNodeId);
        initializeHandleUpdateIfNeeded();
        progressOnHandleUpdate();
    }

    @GuardedBy("this")
    private void initializeHandleUpdateIfNeeded()
    {
        if (handleUpdateInProgress) {
            // already in progress
            return;
        }

        // mark that we need new mapping and that update is in progress
        handleUpdateRequired = true;
        handleUpdateInProgress = true;

        // stop all writers
        for (SinkWriter writer : writers.values()) {
            writer.stop(); // result will be delivered via FinishCallback
        }
    }

    private synchronized void writerFailed(long bufferNodeId, Throwable failure)
    {
        removeWriter(bufferNodeId);
        markFailed(failure);
        // abort all writers
        for (SinkWriter writer : writers.values()) {
            writer.abort();
        }
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        while (true) {
            if (failure.get() != null) {
                return NOT_BLOCKED;
            }

            CompletableFuture<Void> oldBlockedFuture = blockedFutureReference.get();
            if (!oldBlockedFuture.isDone()) {
                return oldBlockedFuture;
            }

            ListenableFuture<Void> dataPoolBlocked = dataPool.isBlocked();
            if (dataPoolBlocked.isDone()) {
                return NOT_BLOCKED;
            }

            blockedFutureReference.compareAndSet(oldBlockedFuture, MoreFutures.toCompletableFuture(dataPoolBlocked));
            // run one more loop as unblocking in the meantime could happen on old future
        }
    }

    private void markFailed(Throwable failure)
    {
        this.failure.compareAndSet(null, failure);
        blockedFutureReference.get().complete(null);
        if (finishFuture != null) {
            finishFuture.completeExceptionally(failure);
        }
    }

    @Override
    public boolean isHandleUpdateRequired()
    {
        return handleUpdateRequired;
    }

    @Override
    public synchronized void updateHandle(ExchangeSinkInstanceHandle newSinkInstanceHandle)
    {
        BufferExchangeSinkInstanceHandle newBufferExchangeSinkInstanceHandle = (BufferExchangeSinkInstanceHandle) newSinkInstanceHandle;
        checkArgument(mappingForUpdate.isEmpty(), "mappingForUpdate already set to %s", mappingForUpdate);
        this.mappingForUpdate = Optional.of(newBufferExchangeSinkInstanceHandle.getPartitionNodeMapping());
        handleUpdateRequired = false;
        progressOnHandleUpdate();
    }

    @GuardedBy("this")
    private void progressOnHandleUpdate()
    {
        if (!handleUpdateInProgress) {
            // we are not in the middle of handle update procedure
            // todo - should I verify here?
            return;
        }
        if (handleUpdateRequired) {
            // we still did not get new mapping
            return;
        }

        if (failure.get() != null) {
            // sink is failed already anyway
            return;
        }

        if (!writers.isEmpty()) {
            // not all writers finished yet
            return;
        }

        handleUpdateInProgress = false;

        // update base mapping in scaler
        mappingScaler.receivedUpdatedPartitionNodeMapping(mappingForUpdate.orElseThrow());
        // reset active mapping
        activeMapping.set(new ActiveMapping(ImmutableListMultimap.of()));
        // update active mapping
        callMappingScaler();
        mappingForUpdate = Optional.empty();
    }

    @Override
    public void add(int partitionId, Slice data)
    {
        throwIfFailed();
        checkState(finishFuture == null, "data cannot be added to finished sink");

        if (data.length() == 0) {
            // ignore empty pages
            return;
        }

        dataPool.add(partitionId, data);
        Long bufferNodeId = activeMapping.get().getRandomBufferNodeForPartition(partitionId);

        if (bufferNodeId == null) {
            // can happen if we are in process of refreshing mapping
            return;
        }

        SinkWriter sinkWriter;
        synchronized (this) {
            sinkWriter = writers.get(bufferNodeId);
        }
        if (sinkWriter == null) {
            // can happen if we are in process of refreshing mapping
            return;
        }
        sinkWriter.scheduleWriting(Optional.of(partitionId), false);
    }

    @Override
    public long getMemoryUsage()
    {
        return dataPool.getMemoryUsage();
    }

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public CompletableFuture<Void> finish()
    {
        callMappingScalerFuture.cancel(true);
        Set<SinkWriter> activeWriters;
        synchronized (this) {
            checkState(finishFuture == null, "finish called more than once");
            finishFuture = new CompletableFuture<>();
            dataPool.noMoreData();
            if (failure.get() != null) {
                finishFuture.completeExceptionally(failure.get());
            }
            else {
                MoreFutures.addSuccessCallback(dataPool.whenFinished(), () -> {
                    finishFuture.complete(null);
                }, executor);
            }
            activeWriters = ImmutableSet.copyOf(writers.values());
        }

        for (SinkWriter writer : activeWriters) {
            // schedule writing for all writers to flush data remaining in dataPool
            writer.scheduleWriting(Optional.empty(), true);
        }

        return finishFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> abort()
    {
        callMappingScalerFuture.cancel(true);
        for (SinkWriter writer : writers.values()) {
            writer.abort();
        }
        // todo - do I need to wait here
        return CompletableFuture.completedFuture(null);
    }

    @ThreadSafe
    static class ActiveMapping
    {
        private final ConcurrentMap<Integer, Set<Long>> partitionToBufferNodes;
        private final ConcurrentMap<Long, Set<Integer>> bufferNodeToPartitions;

        public ActiveMapping(ListMultimap<Integer, Long> initialPartitionToNodeMapping)
        {
            bufferNodeToPartitions = new ConcurrentHashMap<>();
            partitionToBufferNodes = new ConcurrentHashMap<>();
            for (Map.Entry<Integer, Long> entry : initialPartitionToNodeMapping.entries()) {
                Integer partition = entry.getKey();
                Long bufferNodeId = entry.getValue();
                add(partition, bufferNodeId);
            }
        }

        @Nullable
        public Long getRandomBufferNodeForPartition(Integer partition)
        {
            // TODO make more optimal - will require changing data structure
            Set<Long> bufferNodes = partitionToBufferNodes.get(partition);
            if (bufferNodes == null) {
                return null;
            }
            int selector = ThreadLocalRandom.current().nextInt(bufferNodes.size());

            Iterator<Long> iterator = bufferNodes.iterator();
            while (true) {
                Long bufferNodeId = iterator.next(); // assume at least one element
                if (selector == 0 || !iterator.hasNext()) {
                    return bufferNodeId;
                }
                selector--;
            }
        }

        public Set<Integer> getPartitionsForBufferNode(long bufferNodeId)
        {
            return bufferNodeToPartitions.getOrDefault(bufferNodeId, ImmutableSet.of());
        }

        public Set<Long> getBufferNodesForPartition(Integer partition)
        {
            return partitionToBufferNodes.getOrDefault(partition, ImmutableSet.of());
        }

        public void add(Integer partition, Long bufferNodeId)
        {
            bufferNodeToPartitions.computeIfAbsent(bufferNodeId, ignored -> Sets.newConcurrentHashSet()).add(partition);
            partitionToBufferNodes.computeIfAbsent(partition, ignored -> Sets.newConcurrentHashSet()).add(bufferNodeId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("partitionToBufferNodes", partitionToBufferNodes)
                    .add("bufferNodeToPartitions", bufferNodeToPartitions)
                    .toString();
        }
    }
}
