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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class BufferExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(BufferExchangeSink.class);

    private final DataApiFacade dataApi;
    private final String externalExchangeId;
    private final int taskPartitionId;
    private final int taskAttemptId;
    private final boolean preserveOrderWithinPartition;

    @GuardedBy("this")
    private Map<Integer, Long> partitionToBufferNode; // partition -> buffer node (may not match what writers are currently created)

    @GuardedBy("this")
    private Map<Long, SinkWriter> writers; // buffer node -> writer
    @GuardedBy("this")
    private Map<Integer, SinkWriter> writersByPartition; // managed partition -> writer

    @GuardedBy("this")
    private boolean handleUpdateInProgress;
    @GuardedBy("this")
    private volatile boolean handleUpdateRequired;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean finishing = new AtomicBoolean();
    private final ExecutorService executor;
    private final AtomicReference<CompletableFuture<Void>> blockedFutureReference = new AtomicReference<>(CompletableFuture.completedFuture(null));

    private final SinkDataPool dataPool;
    private final DataPagesIdGenerator dataPagesIdGenerator = new DataPagesIdGenerator();

    public BufferExchangeSink(
            DataApiFacade dataApi,
            BufferExchangeSinkInstanceHandle sinkInstanceHandle,
            DataSize memoryLowWaterMark,
            DataSize memoryHighWaterMark,
            ExecutorService executor)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        requireNonNull(sinkInstanceHandle, "sinkInstanceHandle is null");
        this.externalExchangeId = sinkInstanceHandle.getExternalExchangeId();
        this.taskPartitionId = sinkInstanceHandle.getTaskPartitionId();
        this.taskAttemptId = sinkInstanceHandle.getTaskAttemptId();
        this.partitionToBufferNode = ImmutableMap.copyOf(sinkInstanceHandle.getPartitionToBufferNode());
        this.preserveOrderWithinPartition = sinkInstanceHandle.isPreserveOrderWithinPartition();
        this.executor = requireNonNull(executor, "executor is null");
        this.dataPool = new SinkDataPool(memoryLowWaterMark, memoryHighWaterMark);

        createNewWriters();
    }

    @GuardedBy("this")
    private void createNewWriters()
    {
        verify(writers == null || writers.isEmpty(), "writers already set");
        verify(writersByPartition == null || writersByPartition.isEmpty(), "writers by partition already set");

        Multimap<Long, Integer> bufferNodeToPartition = ImmutableMultimap.copyOf(Multimaps.forMap(partitionToBufferNode)).inverse();
        Map<Long, SinkWriter> newWriters = new HashMap<>();
        Map<Integer, SinkWriter> newWritersByPartition = new HashMap<>();
        for (Map.Entry<Long, Collection<Integer>> entry : bufferNodeToPartition.asMap().entrySet()) {
            long bufferNodeId = entry.getKey();
            Set<Integer> managedPartitions = ImmutableSet.copyOf(entry.getValue());
            SinkWriter writer = createWriter(bufferNodeId, managedPartitions);
            newWriters.put(bufferNodeId, writer);
            for (int partition : managedPartitions) {
                newWritersByPartition.put(partition, writer);
            }
        }
        this.writers = newWriters;
        this.writersByPartition = newWritersByPartition;
    }

    private SinkWriter createWriter(long bufferNodeId, Set<Integer> managedPartitions)
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
                managedPartitions,
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
                        verify(called.compareAndSet(null, operation), "Finish callback already called(%s) for %s/%s", called.get(), bufferNodeId, managedPartitions);
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
        SinkWriter removed = writers.remove(bufferNodeId);
        verify(removed != null, "no writer found for buffer node %d", bufferNodeId);
        for (Integer partition : removed.getManagedPartitions()) {
            SinkWriter removedByPartition = writersByPartition.remove(partition);
            verify(removedByPartition != null, "no writer found for partition %d", partition);
            verify(removedByPartition == removed, "expected writer for buffer node %d be responsible for partition %d", bufferNodeId, partition);
        }
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
        partitionToBufferNode = ImmutableMap.copyOf(newBufferExchangeSinkInstanceHandle.getPartitionToBufferNode());
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

        createNewWriters();
        handleUpdateInProgress = false;

        for (SinkWriter writer : writers.values()) {
            // todo: nice to have to move out of synchronized section
            writer.scheduleWriting();
        }
    }

    @Override
    public void add(int partitionId, Slice data)
    {
        throwIfFailed();
        checkState(!finishing.get(), "data cannot be added to finished sink");

        dataPool.add(partitionId, data);

        SinkWriter sinkWriter;
        synchronized (this) {
            sinkWriter = writersByPartition.get(partitionId);
        }
        if (sinkWriter != null) {
            // can be null if we are in progress of writers update
            sinkWriter.scheduleWriting();
        }
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
    public synchronized CompletableFuture<Void> finish()
    {
        Set<SinkWriter> activeWriters;
        CompletableFuture<Void> finishFuture;
        synchronized (this) {
            finishFuture = new CompletableFuture<>();
            dataPool.noMoreData();
            MoreFutures.addSuccessCallback(dataPool.whenFinished(), () -> {
                finishFuture.complete(null);
            }, executor);
            activeWriters = ImmutableSet.copyOf(writers.values());
        }

        for (SinkWriter writer : activeWriters) {
            // schedule writing for all writers to flush data remaining in dataPool
            writer.scheduleWriting();
        }

        return finishFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> abort()
    {
        for (SinkWriter writer : writers.values()) {
            writer.abort();
        }
        // todo - do I need to wait here
        return CompletableFuture.completedFuture(null);
    }
}
