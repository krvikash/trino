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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.trino.spi.TrinoException;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
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
    private final Optional<SecretKey> encryptionKey;
    private final long bufferNodeId;
    private final Set<Integer> managedPartitions;
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

    public SinkWriter(
            DataApiFacade dataApi,
            SinkDataPool dataPool,
            ExecutorService executor,
            String externalExchangeId,
            int taskPartitionId,
            int taskAttemptId,
            boolean preserveOrderWithinPartition,
            Optional<SecretKey> encryptionKey,
            long bufferNodeId,
            Set<Integer> managedPartitions,
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
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
        this.bufferNodeId = bufferNodeId;
        requireNonNull(managedPartitions, "managedPartitions is null");
        this.managedPartitions = ImmutableSet.copyOf(managedPartitions);
        this.dataPagesIdGenerator = requireNonNull(dataPagesIdGenerator, "dataPagesIdGenerator is null");
        this.finishCallback = requireNonNull(finishCallback, "callback is null");
    }

    public Set<Integer> getManagedPartitions()
    {
        return managedPartitions;
    }

    public synchronized void scheduleWriting()
    {
        if (currentRequestFuture != null) {
            // request already in progress
            return;
        }

        if (closed) {
            // closed already
            return;
        }

        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(managedPartitions);
        if (pollResult.isEmpty()) {
            return;
        }

        scheduleWriting(pollResult.get());
    }

    @GuardedBy("this")
    private void scheduleWriting(SinkDataPool.PollResult pollResult)
    {
        List<Slice> dataPages = pollResult.getData();
        int partition = pollResult.getPartition();

        if (encryptionKey.isPresent()) {
            dataPages = encryptDataPages(dataPages, encryptionKey.get());
        }

        currentRequestFuture = dataApi.addDataPages(
                bufferNodeId,
                externalExchangeId,
                partition,
                taskPartitionId,
                taskAttemptId,
                dataPagesIdGenerator.nextId(),
                dataPages);
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
                            // else writer was closed in the meantime (probably due to abort)
                        }
                        else if (!closed) {
                            scheduleWriting();
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
                            if (dataApiException.getErrorCode() == ErrorCode.DRAINING) {
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

    private List<Slice> encryptDataPages(List<Slice> dataPages, SecretKey secretKey)
    {
        return dataPages.stream()
                .map(page -> encryptDataPage(page, secretKey))
                .collect(toImmutableList());
    }

    private Slice encryptDataPage(Slice page, SecretKey secretKey)
    {
        try {
            Cipher cipher = EncryptionKeys.getCipher();
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            Slice encryptedPage = Slices.allocate(cipher.getOutputSize(page.length()));
            CipherOutputStream encryptedOutputStream = new CipherOutputStream(encryptedPage.getOutput(), cipher);
            if (page.hasByteArray()) {
                encryptedOutputStream.write(page.byteArray(), page.byteArrayOffset(), page.length());
            }
            else {
                encryptedOutputStream.write(page.getBytes());
            }
            encryptedOutputStream.close();
            return encryptedPage;
        }
        catch (InvalidKeyException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherOutputStream: " + e.getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to encrypt page: " + e.getMessage(), e);
        }
    }

    public void abort()
    {
        synchronized (this) {
            if (closed) {
                // already closed
                return;
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
