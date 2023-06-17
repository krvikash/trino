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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class SinkWriter
{
    private static final Logger log = Logger.get(SinkWriter.class);

    private final DataApiFacade dataApi;
    private final SinkDataPool dataPool;
    private final ExecutorService executor;
    private final String externalExchangeId;
    private final int taskPartitionId;
    private final int taskAttemptId;
    private final boolean preserveOrderWithinPartition;
    private final long bufferNodeId;
    private final Supplier<Set<Integer>> managedPartitionsSupplier;
    private final DataPagesIdGenerator dataPagesIdGenerator;
    private final FinishCallback finishCallback;
    @GuardedBy("this")
    private boolean someDataSent;
    @GuardedBy("this")
    boolean stopping;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean aborted;
    @GuardedBy("this")
    private ListenableFuture<Void> currentRequestFuture;
    @GuardedBy("this")
    private boolean sinkFinishing;
    @GuardedBy("this")
    private final Set<Integer> nextScheduleRequiredPartitions = new HashSet<>();

    public SinkWriter(
            DataApiFacade dataApi,
            SinkDataPool dataPool,
            ExecutorService executor,
            String externalExchangeId,
            int taskPartitionId,
            int taskAttemptId,
            boolean preserveOrderWithinPartition,
            long bufferNodeId,
            Supplier<Set<Integer>> managedPartitionsSupplier,
            DataPagesIdGenerator dataPagesIdGenerator,
            FinishCallback finishCallback)
    {
        this.dataApi = requireNonNull(dataApi, "dataApi is null");
        this.dataPool = requireNonNull(dataPool, "dataPool is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.externalExchangeId = requireNonNull(externalExchangeId, "externalExchangeId is null");
        this.taskPartitionId = taskPartitionId;
        this.taskAttemptId = taskAttemptId;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
        this.bufferNodeId = bufferNodeId;
        this.managedPartitionsSupplier = requireNonNull(managedPartitionsSupplier, "managedPartitionsSupplier is null");
        this.dataPagesIdGenerator = requireNonNull(dataPagesIdGenerator, "dataPagesIdGenerator is null");
        this.finishCallback = requireNonNull(finishCallback, "callback is null");
    }

    public synchronized void scheduleWriting(Optional<Integer> requiredPartition, boolean sinkFinishing)
    {
        // ensure we are polling for requiredPartition on next poll;
        // the writer was responsible for that partition when scheduleWriting is called, but
        // it may no longer be true when managedPartitionsSupplier is called below.
        requiredPartition.ifPresent(nextScheduleRequiredPartitions::add);

        // store finishing flag in case we return quickly and final poll should be done
        // after current request completes
        this.sinkFinishing |= sinkFinishing;

        if (currentRequestFuture != null) {
            // request already in progress
            return;
        }

        if (closed) {
            // closed already
            nextScheduleRequiredPartitions.clear();
            return;
        }

        Set<Integer> partitionSetFromSupplier = managedPartitionsSupplier.get();
        Set<Integer> partitionSet;
        if (nextScheduleRequiredPartitions.isEmpty() || partitionSetFromSupplier.containsAll(nextScheduleRequiredPartitions)) {
            partitionSet = partitionSetFromSupplier;
        }
        else {
            partitionSet = new HashSet<>(partitionSetFromSupplier);
            partitionSet.addAll(nextScheduleRequiredPartitions);
        }
        nextScheduleRequiredPartitions.clear();

        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(partitionSet, sinkFinishing);
        if (pollResult.isEmpty()) {
            return;
        }

        scheduleWriting(pollResult.get());
    }

    @GuardedBy("this")
    private void scheduleWriting(SinkDataPool.PollResult pollResult)
    {
        currentRequestFuture = dataApi.addDataPages(
                bufferNodeId,
                externalExchangeId,
                taskPartitionId,
                taskAttemptId,
                dataPagesIdGenerator.nextId(),
                pollResult.getDataByPartition());
        Futures.addCallback(currentRequestFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(Void result)
            {
                try {
                    boolean callFinishCallback = false;
                    synchronized (SinkWriter.this) {
                        currentRequestFuture = null;
                        pollResult.commit();
                        someDataSent = true;
                        if (stopping) {
                            // if there is pending request to stop writer
                            stopping = false;
                            if (!closed) {
                                callFinishCallback = true;
                                closed = true;
                            }
                            else {
                                // writer was closed in the meantime due to abort
                                verify(aborted, "writer marked as closed asynchronously but aborted flag not set");
                            }
                        }
                        else if (!closed) {
                            scheduleWriting(Optional.empty(), sinkFinishing);
                        }
                    }
                    if (callFinishCallback) {
                        callDoneFinishCallback();
                    }
                }
                catch (Throwable t) {
                    onFailure(t);
                }
            }

            @Override
            public void onFailure(Throwable failure)
            {
                boolean callDrainingCallback = false;
                synchronized (SinkWriter.this) {
                    try {
                        currentRequestFuture = null;
                        if (closed && aborted) {
                            // aborted - ignore failure
                            return;
                        }
                        verify(!closed, "unexpected failure after writer was already closed; %s", failure.getMessage());

                        if (failure instanceof DataApiException dataApiException) {
                            ErrorCode errorCode = dataApiException.getErrorCode();
                            if (errorCode == ErrorCode.DRAINING || errorCode == ErrorCode.DRAINED) {
                                if (preserveOrderWithinPartition && someDataSent) {
                                    // todo[https://github.com/starburstdata/trino-buffer-service/issues/34] we cannot switch buffer node now
                                    failure = new RuntimeException("target node draining when preserving order", failure);
                                }
                                else {
                                    pollResult.rollback();
                                    callDrainingCallback = true;
                                }
                            }
                        }
                        closed = true;
                    }
                    catch (Throwable otherFailure) {
                        failure.addSuppressed(new RuntimeException("unexpected error in onFailureHandling", otherFailure));
                    }
                }
                if (callDrainingCallback) {
                    callTargetDrainingFinishCallback();
                }
                else {
                    callFailedFinishCallback(failure);
                }
            }
        }, executor);
    }

    public void abort()
    {
        synchronized (this) {
            if (closed) {
                // already closed
                return;
            }
            if (currentRequestFuture != null) {
                currentRequestFuture.cancel(true);
            }
            closed = true;
            aborted = true;
        }
        callFailedFinishCallback(new RuntimeException("aborted"));
    }

    private void callTargetDrainingFinishCallback()
    {
        executor.submit(() -> {
            try {
                finishCallback.targetDraining();
            }
            catch (Throwable t) {
                log.error(t, "unexpected exception thrown from finishCallback.targetDraining() for exchange %s", externalExchangeId);
            }
        });
    }

    private void callDoneFinishCallback()
    {
        executor.submit(() -> {
            try {
                finishCallback.done();
            }
            catch (Throwable t) {
                log.error(t, "unexpected exception thrown from finishCallback.done() for exchange %s", externalExchangeId);
            }
        });
    }

    private void callFailedFinishCallback(Throwable failure)
    {
        executor.submit(() -> {
            try {
                finishCallback.failed(failure);
            }
            catch (Throwable t) {
                log.error(t, "unexpected exception thrown from finishCallback.failed() for exchange %s", externalExchangeId);
            }
        });
    }

    public void stop()
    {
        boolean callFinishCallback = false;
        synchronized (this) {
            if (closed) {
                // already closed
                return;
            }
            if (currentRequestFuture != null) {
                stopping = true;
                // finalizing stop will be handled by callback on currentRequestFuture
            }
            else {
                closed = true;
                callFinishCallback = true;
            }
        }
        if (callFinishCallback) {
            callDoneFinishCallback();
        }
    }

    public interface FinishCallback
    {
        void done();

        void targetDraining();

        void failed(Throwable failure);
    }
}
