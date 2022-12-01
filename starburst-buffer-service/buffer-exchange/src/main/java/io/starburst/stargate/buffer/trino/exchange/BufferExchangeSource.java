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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeState;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.openjdk.jol.info.ClassLayout;
import sun.misc.Unsafe;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.starburst.stargate.buffer.trino.exchange.MoreSizeOf.OBJECT_HEADER_SIZE;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class BufferExchangeSource
        implements ExchangeSource
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(BufferExchangeSource.class).instanceSize());
    private static final Logger log = Logger.get(BufferExchangeSource.class);

    private final DataApiFacade dataApi;
    private final BufferNodeDiscoveryManager discoveryManager;
    private final DataSize memoryLowWaterMark;
    private final DataSize memoryHighWaterMark;
    private final int parallelism;

    @GuardedBy("this")
    private ExchangeSourceOutputSelector latestSourceOutputSelector;
    @GuardedBy("this")
    private final SpeculativeSourceOutputChoices speculativeSourceOutputChoices = new SpeculativeSourceOutputChoices();
    @GuardedBy("this")
    private final Queue<SourceChunk> sourceChunks = new ArrayDeque<>();
    @GuardedBy("this")
    private final AtomicLong sourceChunksEstimatedSize;
    private volatile boolean noMoreChunks;
    @GuardedBy("this")
    private final Queue<Slice> readSlices = new ArrayDeque<>();
    private volatile boolean readSlicesHasElements; // extra duplicated state allows smaller synchronized section in isBlocked
    private final AtomicReference<CompletableFuture<Void>> isBlockedReference = new AtomicReference<>(completedFuture(null));
    private volatile boolean closed;
    private final AtomicLong readSlicesMemoryUsage = new AtomicLong(0);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    @GuardedBy("this")
    private SettableFuture<Void> memoryUsageExceeded;
    @GuardedBy("this")
    private PreserveOrderingMode preserveOrderingMode = PreserveOrderingMode.UNKNOWN;

    @GuardedBy("this")
    private final Set<ChunkReader> currentReaders = ConcurrentHashMap.newKeySet();

    public BufferExchangeSource(
            DataApiFacade dataApi,
            BufferNodeDiscoveryManager discoveryManager,
            DataSize memoryLowWaterMark,
            DataSize memoryHighWaterMark,
            int parallelism)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");

        this.memoryUsageExceeded = SettableFuture.create();
        this.memoryUsageExceeded.set(null); // not exceeded initially
        this.memoryLowWaterMark = requireNonNull(memoryLowWaterMark, "memoryLowWaterMark is null");
        this.memoryHighWaterMark = requireNonNull(memoryHighWaterMark, "memoryHighWaterMark is null");
        this.parallelism = parallelism;
        this.sourceChunksEstimatedSize = new AtomicLong(MoreSizeOf.estimatedSizeOf(sourceChunks, SourceChunk::getRetainedSize));
    }

    private void scheduleReadChunks()
    {
        List<ChunkReader> newChunkReaders;
        synchronized (this) {
            if (latestSourceOutputSelector == null) {
                // wait for initial output selector before scheduling reading
                return;
            }
            if (!memoryUsageExceeded.isDone()) {
                // no need to wrap `this::doScheduleReadChunks` in `try...catch` as we are using directExecutor; exception will be propagated outside
                memoryUsageExceeded.addListener(this::scheduleReadChunks, directExecutor());
                return;
            }
            newChunkReaders = doScheduleReadChunks();
        }
        for (ChunkReader chunkReader : newChunkReaders) {
            chunkReader.start();
        }
    }

    @GuardedBy("this")
    private List<ChunkReader> doScheduleReadChunks()
    {
        int selectedParallelism = switch (preserveOrderingMode) {
            case ALLOW_REORDERING -> parallelism;
            case PRESERVE_ORDERING -> 1;
            case UNKNOWN -> throw new IllegalStateException("preserverOrderingMode should be set by now");
        };

        int missing = selectedParallelism - currentReaders.size();
        ImmutableList.Builder<ChunkReader> newReaders = ImmutableList.builder();
        for (int i = 0; i < missing; ++i) {
            SourceChunk chunk = pollSourceChunk();
            if (chunk == null) {
                return newReaders.build();
            }
            ChunkReader reader = new ChunkReader(chunk);
            newReaders.add(reader);
            currentReaders.add(reader);
        }
        return newReaders.build();
    }

    private void finishChunkReader(ChunkReader chunkReader)
    {
        CompletableFuture<Void> futureToUnblock = null;
        synchronized (this) {
            checkState(currentReaders.remove(chunkReader), "ChunkReader %s not found in currentReaders set", chunkReader);
            if (allDataReturned()) {
                futureToUnblock = isBlockedReference.get();
            }
        }
        if (futureToUnblock != null) {
            futureToUnblock.complete(null);
        }
    }

    private long selectRandomRunningBufferNode()
    {
        return selectRandomRunningBufferNodeExcluding(emptySet());
    }

    private long selectRandomRunningBufferNodeExcluding(Set<Long> excludedNodes)
    {
        List<BufferNodeInfo> runningNodes = discoveryManager.getBufferNodes().bufferNodeInfos().values().stream()
                .filter(node -> node.state() == BufferNodeState.ACTIVE)
                .filter(node -> !excludedNodes.contains(node.nodeId()))
                .collect(toImmutableList());

        if (runningNodes.isEmpty()) {
            throw new RuntimeException("no RUNNING nodes available");
        }

        return runningNodes.get(ThreadLocalRandom.current().nextInt(runningNodes.size())).nodeId();
    }

    private void setFailed(Throwable throwable)
    {
        CompletableFuture<Void> futureToUnblock;
        synchronized (this) {
            futureToUnblock = isBlockedReference.get();
            this.failure.compareAndSet(null, throwable);
        }
        if (futureToUnblock != null) {
            futureToUnblock.complete(null);
        }
    }

    private void receivedNewDataPages(String externalExchangeId, List<DataPage> result)
    {
        AtomicReference<CompletableFuture<Void>> futureToUnblock = new AtomicReference<>();
        synchronized (this) {
            ExchangeId exchangeId = ExternalExchangeIds.internalExchangeId(externalExchangeId);
            verify(latestSourceOutputSelector != null, "latestSourceOutputSelector should have been set already");
            result.stream()
                    .filter(page -> switch (latestSourceOutputSelector.getSelection(exchangeId, page.taskId(), page.attemptId())) {
                        case INCLUDED -> true;
                        case EXCLUDED -> false;
                        case UNKNOWN -> {
                            // assume first observed attempt for given partition is the one; will be revalidated later on next call to setOutputSelector
                            int selectedAttemptId = speculativeSourceOutputChoices.getOrStoreSelectedAttemptId(externalExchangeId, page.taskId(), page.attemptId());
                            yield selectedAttemptId == page.attemptId();
                        }
                    }).forEach(page -> {
                        readSlices.offer(page.data());
                        readSlicesHasElements = true;
                        futureToUnblock.set(isBlockedReference.get());
                        updateReadSlicesMemoryUsage(page.data().getRetainedSize());
                    });
        }
        if (futureToUnblock.get() != null) {
            futureToUnblock.get().complete(null);
        }
    }

    @Override
    public void addSourceHandles(List<ExchangeSourceHandle> sourceHandles)
    {
        synchronized (this) {
            requireNonNull(sourceHandles, "sourceHandles is null");
            if (sourceHandles.isEmpty()) {
                return;
            }

            for (ExchangeSourceHandle sourceHandle : sourceHandles) {
                BufferExchangeSourceHandle bufferSourceHandle = (BufferExchangeSourceHandle) sourceHandle;

                handlePreserveOrderingFlagFromSourceHandle(bufferSourceHandle);

                int chunksCount = bufferSourceHandle.getChunksCount();

                for (int chunkNum = 0; chunkNum < chunksCount; chunkNum++) {
                    long bufferNodeId = bufferSourceHandle.getBufferNodeId(chunkNum);
                    long chunkId = bufferSourceHandle.getChunkId(chunkNum);
                    offerSourceChunk(sourceHandle, bufferSourceHandle, bufferNodeId, chunkId);
                }
            }

            scheduleReadChunks();
        }
    }

    @GuardedBy("this")
    private void offerSourceChunk(ExchangeSourceHandle sourceHandle, BufferExchangeSourceHandle bufferSourceHandle, long bufferNodeId, long chunkId)
    {
        SourceChunk sourceChunk = new SourceChunk(bufferSourceHandle.getExternalExchangeId(), bufferNodeId, sourceHandle.getPartitionId(), chunkId);
        sourceChunks.offer(sourceChunk);
        sourceChunksEstimatedSize.updateAndGet(oldValue -> oldValue + Unsafe.ARRAY_OBJECT_INDEX_SCALE + sourceChunk.getRetainedSize());
    }

    @GuardedBy("this")
    private SourceChunk pollSourceChunk()
    {
        SourceChunk sourceChunk = sourceChunks.poll();
        if (sourceChunk != null) {
            sourceChunksEstimatedSize.updateAndGet(oldValue -> oldValue - Unsafe.ARRAY_OBJECT_INDEX_SCALE - sourceChunk.getRetainedSize());
        }
        return sourceChunk;
    }

    @GuardedBy("this")
    private void handlePreserveOrderingFlagFromSourceHandle(BufferExchangeSourceHandle bufferSourceHandle)
    {
        boolean preserveOrderingFlag = bufferSourceHandle.isPreserveOrderWithinPartition();

        if ((preserveOrderingMode == PreserveOrderingMode.ALLOW_REORDERING && preserveOrderingFlag)
                || (preserveOrderingMode == PreserveOrderingMode.PRESERVE_ORDERING && !preserveOrderingFlag)) {
            throw new IllegalArgumentException("Cannot mix splits with preserveOrdering flag set to true and false");
        }

        if (preserveOrderingMode == PreserveOrderingMode.UNKNOWN) {
            preserveOrderingMode = preserveOrderingFlag ? PreserveOrderingMode.PRESERVE_ORDERING : PreserveOrderingMode.ALLOW_REORDERING;
        }
    }

    @Override
    public void noMoreSourceHandles()
    {
        CompletableFuture<Void> futureToUnblock = null;
        synchronized (this) {
            this.noMoreChunks = true;
            if (allDataReturned()) {
                futureToUnblock = isBlockedReference.get();
            }
        }
        if (futureToUnblock != null) {
            futureToUnblock.complete(null);
        }
    }

    @Override
    public synchronized void setOutputSelector(ExchangeSourceOutputSelector selector)
    {
        if (latestSourceOutputSelector != null && selector.getVersion() <= latestSourceOutputSelector.getVersion()) {
            return;
        }
        speculativeSourceOutputChoices.synchronizeWithNewOutputSelector(selector);
        this.latestSourceOutputSelector = selector;
        if (!sourceChunks.isEmpty()) {
            scheduleReadChunks();
        }
    }

    @GuardedBy("this")
    private boolean allDataReturned()
    {
        return currentReaders.isEmpty() && sourceChunks.isEmpty() && noMoreChunks;
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        while (true) {
            if (readSlicesHasElements) {
                return NOT_BLOCKED;
            }

            if (failure.get() != null) {
                return NOT_BLOCKED;
            }

            CompletableFuture<Void> currentIsBlocked = isBlockedReference.get();
            if (!currentIsBlocked.isDone()) {
                return currentIsBlocked;
            }

            synchronized (this) {
                if (allDataReturned()) {
                    return NOT_BLOCKED;
                }
                isBlockedReference.compareAndSet(currentIsBlocked, new CompletableFuture<>());
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        if (closed) {
            return true;
        }

        // quick checks without synchronization
        if (!noMoreChunks) {
            return false;
        }
        //noinspection FieldAccessNotGuarded
        if (!currentReaders.isEmpty()) {
            return false;
        }

        synchronized (this) {
            // if failure is set we cannot return that source is finished. Otherwise, we may continue with query execution based on fractional data.
            return !readSlicesHasElements && sourceChunks.isEmpty() && currentReaders.isEmpty() && noMoreChunks && failure.get() == null && latestSourceOutputSelector != null && latestSourceOutputSelector.isFinal();
        }
    }

    @Nullable
    @Override
    public synchronized Slice read()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }

        if (!readSlicesHasElements) {
            return null;
        }

        Slice slice = readSlices.poll();
        verify(slice != null, "expected non empty readSlices");
        updateReadSlicesMemoryUsage(-slice.getRetainedSize());
        if (readSlices.isEmpty()) {
            readSlicesHasElements = false;
        }
        return slice;
    }

    private synchronized void updateReadSlicesMemoryUsage(long delta)
    {
        long currentMemoryUsage = readSlicesMemoryUsage.addAndGet(delta);
        if (currentMemoryUsage < memoryLowWaterMark.toBytes() && !memoryUsageExceeded.isDone()) {
            // TODO refactor to not set in synchronized section if possible
            memoryUsageExceeded.set(null);
        }

        if (currentMemoryUsage > memoryHighWaterMark.toBytes() && memoryUsageExceeded.isDone()) {
            memoryUsageExceeded = SettableFuture.create();
        }

        verify(currentMemoryUsage >= 0, "negative memory usage");
    }

    @SuppressWarnings("FieldAccessNotGuarded") // accesses are safe crash-wise; we do not need to 100% accurate in this method
    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE
                + speculativeSourceOutputChoices.getRetainedSize()
                + sourceChunksEstimatedSize.get()
                + readSlicesMemoryUsage.get()
                + estimatedSizeOf(currentReaders, ChunkReader::getRetainedSize);
    }

    @Override
    public synchronized void close()
    {
        currentReaders.forEach(ChunkReader::close);

        readSlices.clear();
        readSlicesHasElements = false;
        updateReadSlicesMemoryUsage(-readSlicesMemoryUsage.get());
        closed = true;
    }

    @NotThreadSafe
    private class ChunkReader
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(ChunkReader.class).instanceSize());

        private final SourceChunk sourceChunk;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicReference<ListenableFuture<List<DataPage>>> getChunkDataFutureReference = new AtomicReference<>();
        private final Set<Long> excludedNodes = new HashSet<>();
        private boolean finished;

        public ChunkReader(SourceChunk sourceChunk)
        {
            this.sourceChunk = requireNonNull(sourceChunk, "sourceChunk is null");
        }

        public void start()
        {
            long sourceBufferNodeId = sourceChunk.bufferNodeId();
            Map<Long, BufferNodeInfo> bufferNodes = discoveryManager.getBufferNodes().bufferNodeInfos();
            BufferNodeInfo sourceBufferNodeInfo = bufferNodes.get(sourceBufferNodeId);
            if (sourceBufferNodeInfo == null || !(sourceBufferNodeInfo.state() == BufferNodeState.ACTIVE || sourceBufferNodeInfo.state() == BufferNodeState.DRAINING)) {
                sourceBufferNodeId = selectRandomRunningBufferNode();
            }
            scheduleReadUsingNode(sourceBufferNodeId);
        }

        private void scheduleReadUsingNode(long sourceBufferNodeId)
        {
            if (closed.get()) {
                // do not schedule a new request if we are closed
                finish();
                return;
            }

            ListenableFuture<List<DataPage>> future = dataApi.getChunkData(
                    sourceBufferNodeId,
                    sourceChunk.externalExchangeId(),
                    sourceChunk.partitionId(),
                    sourceChunk.chunkId(),
                    sourceChunk.bufferNodeId());
            verify(getChunkDataFutureReference.compareAndSet(null, future), "getChunkDataFuture already set");

            if (closed.get()) {
                // if we got closed in the meantime immediately cancel request
                future.cancel(true);
            }
            Futures.addCallback(future, new FutureCallback<>() {
                @Override
                public void onSuccess(List<DataPage> dataPages)
                {
                    try {
                        getChunkDataFutureReference.set(null);

                        receivedNewDataPages(sourceChunk.externalExchangeId(), dataPages);
                        finish();
                        scheduleReadChunks();
                    }
                    catch (Throwable t) {
                        onFailure(t);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    try {
                        getChunkDataFutureReference.set(null);
                        if (!(t instanceof DataApiException dataApiException)) {
                            finish();
                            setFailed(t);
                            return;
                        }

                        switch (dataApiException.getErrorCode()) {
                            case CHUNK_DRAINED -> {
                                // we need to reach out to different buffer node
                                excludedNodes.add(sourceBufferNodeId);
                                long newBufferNodeId = selectRandomRunningBufferNodeExcluding(excludedNodes);
                                scheduleReadUsingNode(newBufferNodeId);
                            }
                            case CHUNK_NOT_FOUND -> {
                                setFailed(new RuntimeException("chunk " + sourceChunk + " not found; reading from " + sourceBufferNodeId));
                                finish();
                            }
                            default -> setFailed(t);
                        }
                    }
                    catch (Throwable otherFailure) {
                        if (otherFailure != t) {
                            t.addSuppressed(otherFailure);
                            setFailed(t);
                            finish();
                        }
                    }
                }
            }, directExecutor());
        }

        private void finish()
        {
            if (finished) {
                // ignore another call; finish() maybe be called more than once in case we catch exception and final fallback is called from `catch` clause
                return;
            }
            finished = true;
            finishChunkReader(this);
        }

        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            ListenableFuture<List<DataPage>> future = getChunkDataFutureReference.get();
            if (future != null) {
                future.cancel(true);
            }
        }

        public long getRetainedSize()
        {
            return INSTANCE_SIZE + sourceChunk.getRetainedSize();
        }
    }

    private record SourceChunk(
            String externalExchangeId,
            long bufferNodeId,
            int partitionId,
            long chunkId) {
        public long getRetainedSize()
        {
            return OBJECT_HEADER_SIZE + Long.BYTES + Integer.BYTES + Long.BYTES;
        }
    }

    private enum PreserveOrderingMode {
        UNKNOWN,
        ALLOW_REORDERING,
        PRESERVE_ORDERING
    }

    private static class SpeculativeSourceOutputChoices
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SpeculativeSourceOutputChoices.class).instanceSize());
        private final Map<String, Map<Integer, Integer>> speculativeSourceOutputChoices = new HashMap<>();
        private final AtomicLong estimatedSize;

        public SpeculativeSourceOutputChoices()
        {
            estimatedSize = new AtomicLong(estimateSize());
        }

        public int getOrStoreSelectedAttemptId(String externalExchangeId, Integer taskId, int observedUnknownAttemptId)
        {
            boolean sizeChanged = false;
            Map<Integer, Integer> exchangeOutputChoices = speculativeSourceOutputChoices.get(externalExchangeId);
            if (exchangeOutputChoices == null) {
                exchangeOutputChoices = new HashMap<>();
                speculativeSourceOutputChoices.put(externalExchangeId, exchangeOutputChoices);
                sizeChanged = true;
            }
            Integer selectedAttemptId = exchangeOutputChoices.get(taskId);
            if (selectedAttemptId == null) {
                selectedAttemptId = observedUnknownAttemptId;
                exchangeOutputChoices.put(taskId, observedUnknownAttemptId);
                sizeChanged = true;
            }
            if (sizeChanged) {
                estimatedSize.set(estimateSize());
            }
            return selectedAttemptId;
        }

        public void synchronizeWithNewOutputSelector(ExchangeSourceOutputSelector selector)
        {
            for (Map.Entry<String, Map<Integer, Integer>> exchangesAttemptsEntry : speculativeSourceOutputChoices.entrySet()) {
                ExchangeId exchangeId = ExternalExchangeIds.internalExchangeId(exchangesAttemptsEntry.getKey());
                Map<Integer, Integer> partitionToAttempt = exchangesAttemptsEntry.getValue();

                Iterator<Map.Entry<Integer, Integer>> entryIterator = partitionToAttempt.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    Map.Entry<Integer, Integer> entry = entryIterator.next();
                    int taskPartitionId = entry.getKey();
                    int attemptId = entry.getValue();

                    ExchangeSourceOutputSelector.Selection selection = selector.getSelection(exchangeId, taskPartitionId, attemptId);
                    switch (selection) {
                        case INCLUDED -> {
                            // entry in speculativeSourceOutputChoices no longer needed
                            entryIterator.remove();
                        }
                        case EXCLUDED -> {
                            // we made wrong decision in the past
                            throw new RuntimeException("speculative tasks selection mismatch; picked %s.%s.%s which turned out to be excluded".formatted(exchangeId, taskPartitionId, partitionToAttempt));
                        }
                        case UNKNOWN -> {
                            // keep the speculative choice entry
                        }
                    }
                }
            }
            estimatedSize.set(estimateSize());
        }

        public long getRetainedSize()
        {
            return INSTANCE_SIZE + estimatedSize.get();
        }

        private long estimateSize()
        {
            return estimatedSizeOf(speculativeSourceOutputChoices, SizeOf::estimatedSizeOf, value -> estimatedSizeOf(value, SizeOf::sizeOf, SizeOf::sizeOf));
        }
    }
}
