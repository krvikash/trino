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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.starburst.stargate.buffer.data.disk.DiskChunkSlot;
import io.starburst.stargate.buffer.data.disk.DiskPreAllocator;
import io.starburst.stargate.buffer.data.disk.DiskSpaceLease;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.CountedReference.Ref;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.finalizeChecksum;
import static io.starburst.stargate.buffer.data.execution.ChunkData.ChunkPlacement.LOCAL_DISK;
import static java.util.Objects.requireNonNull;

public final class DiskChunkData
        implements ChunkData
{
    private static final Logger log = Logger.get(DiskChunkData.class);

    private final Executor executor;
    private final Path file;
    private final long chunkId;
    private final int chunkSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final Runnable materializeDirectory;
    private final Ref<DiskSpaceLease> diskLease;

    private volatile FileChannel writeChannel;
    private volatile FileChannel readChannel;
    @GuardedBy("this")
    private boolean materialized;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private final XxHash64 hash = new XxHash64();
    private volatile Throwable poisonCause;

    @GuardedBy("this")
    private int writtenBytes;
    @GuardedBy("this")
    private int dataSizeInBytes;
    @GuardedBy("this")
    private int numDataPages;

    // Starts at 1. write() increments before submitting to executor, decrements
    // in the finally block. close() decrements the open token
    private final AtomicInteger pendingOperations = new AtomicInteger(1);
    private final SettableFuture<Void> closeFuture = SettableFuture.create();

    public DiskChunkData(Executor executor, long chunkId, int chunkSizeInBytes, boolean calculateDataPagesChecksum, DiskChunkSlot chunkSlot)
    {
        this(executor,
                requireNonNull(chunkSlot, "chunkSlot is null").file(),
                chunkId,
                chunkSizeInBytes,
                calculateDataPagesChecksum,
                chunkSlot.lease(),
                chunkSlot.diskRelease(),
                chunkSlot.materializeDirectory());
    }

    DiskChunkData(
            Executor executor,
            Path file,
            long chunkId,
            int chunkSizeInBytes,
            boolean calculateDataPagesChecksum,
            DiskSpaceLease spaceLease,
            Runnable directoryRelease,
            Runnable materializeDirectory)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.file = requireNonNull(file, "file is null");
        this.chunkId = chunkId;
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        this.materializeDirectory = requireNonNull(materializeDirectory, "materializeDirectory is null");
        requireNonNull(spaceLease, "spaceLease is null");
        requireNonNull(directoryRelease, "directoryRelease is null");
        this.diskLease = CountedReference.create(
                () -> spaceLease,
                lease -> {
                    closeReadChannel();
                    try {
                        Files.deleteIfExists(file);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("failed to delete " + file, e);
                    }
                    finally {
                        lease.release();
                        directoryRelease.run();
                    }
                });
        log.debug("Chunk opened: chunkId=%s file=%s sizeBytes=%d", chunkId, file, chunkSizeInBytes);
    }

    @Override
    public ListenableFuture<Void> write(int taskId, int attemptId, Slice data)
    {
        Throwable poison = poisonCause;
        if (poison != null) {
            return immediateFailedFuture(poison);
        }
        int dataSize = data.length();
        int requiredStorageSize = DATA_PAGE_HEADER_SIZE + dataSize;
        Slice headerSlice = Slices.allocate(DATA_PAGE_HEADER_SIZE);
        SliceOutput headerOutput = headerSlice.getOutput();
        headerOutput.writeShort(taskId);
        headerOutput.writeByte(attemptId);
        headerOutput.writeInt(dataSize);
        ByteBuffer header = headerSlice.toByteBuffer();
        ByteBuffer payload = data.toByteBuffer();

        int writeOffset;
        synchronized (this) {
            if (closed) {
                return immediateFailedFuture(new IllegalStateException("write() called on closed DiskChunkData for chunk " + chunkId));
            }
            int writableBytes = chunkSizeInBytes - writtenBytes;
            checkArgument(requiredStorageSize <= writableBytes,
                    "requiredStorageSize %s larger than writableBytes %s",
                    requiredStorageSize,
                    writableBytes);
            writeOffset = writtenBytes;
            writtenBytes += requiredStorageSize;
            dataSizeInBytes += dataSize;
            numDataPages++;

            if (calculateDataPagesChecksum) {
                hash.update(data);
            }

            // Increment under the lock so close() cannot observe count=1 and close the channel
            // between lock release here and the executor picking up the task.
            pendingOperations.incrementAndGet();
        }

        SettableFuture<Void> result = SettableFuture.create();
        executor.execute(() -> {
            try {
                doWrite(writeOffset, header, payload, result);
            }
            finally {
                // Reaches 0 only when close() was called AND all writes finished; see close().
                if (pendingOperations.decrementAndGet() == 0) {
                    closeChannel();
                }
            }
        });
        return result;
    }

    private void doWrite(int offset, ByteBuffer header, ByteBuffer payload, SettableFuture<Void> result)
    {
        try {
            // Fast path: writeChannel already materialized
            FileChannel localChannel = writeChannel;
            if (localChannel == null) {
                synchronized (this) {
                    if (poisonCause != null) {
                        result.setException(poisonCause);
                        return;
                    }
                    ensureMaterialized();
                    localChannel = writeChannel;
                }
            }
            else if (poisonCause != null) {
                result.setException(poisonCause);
                return;
            }

            writePositionalLoop(localChannel, offset, header);
            writePositionalLoop(localChannel, offset + DATA_PAGE_HEADER_SIZE, payload);
            result.set(null);
        }
        catch (Throwable t) {
            synchronized (this) {
                if (poisonCause == null) {
                    poisonCause = t;
                    log.warn(t, "Write failed, poisoning chunk: chunkId=%s file=%s", chunkId, file);
                }
            }
            result.setException(t);
        }
    }

    private static void writePositionalLoop(FileChannel channel, long position, ByteBuffer buffer)
            throws IOException
    {
        long pos = position;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, pos);
            if (written <= 0) {
                throw new IOException("FileChannel.write returned " + written);
            }
            pos += written;
        }
    }

    @GuardedBy("this")
    private void ensureMaterialized()
            throws IOException
    {
        if (materialized) {
            return;
        }
        materializeDirectory.run();
        if (DiskPreAllocator.tryCreateAndPreAllocate(file, chunkSizeInBytes)) {
            writeChannel = FileChannel.open(file, StandardOpenOption.WRITE);
        }
        else {
            writeChannel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        materialized = true;
        log.debug("Chunk file materialized: chunkId=%s file=%s", chunkId, file);
    }

    @Override
    public synchronized boolean hasEnoughSpace(int requiredStorageSize)
    {
        return requiredStorageSize <= chunkSizeInBytes - writtenBytes;
    }

    @Override
    public synchronized int dataSizeInBytes()
    {
        return dataSizeInBytes;
    }

    @Override
    public synchronized boolean isEmpty()
    {
        return writtenBytes == 0;
    }

    @Override
    public synchronized ChunkDataLease get()
    {
        checkState(closed, "get() called before DiskChunkData was closed for chunk %s", chunkId);
        checkState(closeFuture.isDone(), "get() called before closeFuture completed for chunk %s", chunkId);
        Throwable poison = poisonCause;
        if (poison != null) {
            throw new DataServerException(INTERNAL_ERROR, "chunk %s has write failure".formatted(chunkId), poison);
        }
        // Empty / never-materialized chunks have no backing file. Callers (Partition) route empty
        // chunks to release() and never call get(); fail loudly rather than open a missing file.
        if (!materialized || writtenBytes == 0) {
            throw new DataServerException(INTERNAL_ERROR, "get() called on empty or never-materialized disk chunk %s".formatted(chunkId));
        }
        if (readChannel == null) {
            try {
                readChannel = FileChannel.open(file, StandardOpenOption.READ);
            }
            catch (IOException e) {
                throw new UncheckedIOException("failed to open disk chunk for reading " + file, e);
            }
        }
        // The ref count prevents both file deletion and partition directory cleanup
        // until the last reader releases its lease.
        Ref<DiskSpaceLease> readerRef = diskLease.addReference();
        try {
            long checksum = calculateDataPagesChecksum ? finalizeChecksum(hash) : NO_CHECKSUM;
            return new DiskChunkDataLease(file, readChannel, writtenBytes, checksum, numDataPages, readerRef::release);
        }
        catch (Throwable t) {
            try {
                readerRef.release();
            }
            catch (Throwable ignored) {
                // already handling an exception; suppress to preserve the original
            }
            throw t;
        }
    }

    @Override
    public ChunkPlacement chunkPlacement()
    {
        return LOCAL_DISK;
    }

    @Override
    public int getReclaimableBytes()
    {
        return chunkSizeInBytes;
    }

    @Override
    public ListenableFuture<Void> close()
    {
        synchronized (this) {
            if (closed) {
                return closeFuture;
            }
            closed = true;
            log.debug(
                    "Chunk closed: chunkId=%s file=%s writtenBytes=%d",
                    chunkId,
                    file,
                    writtenBytes);
        }
        // If no writes are in flight this reaches 0 and closes the channel.
        if (pendingOperations.decrementAndGet() == 0) {
            closeChannel();
        }
        return closeFuture;
    }

    private void closeChannel()
    {
        synchronized (this) {
            if (writeChannel != null) {
                try {
                    writeChannel.close();
                }
                catch (IOException e) {
                    log.warn(e, "Failed to close chunk file: chunkId=%s file=%s", chunkId, file);
                }
                writeChannel = null;
            }
            closeFuture.set(null);
        }
    }

    private void closeReadChannel()
    {
        FileChannel localReadChannel = readChannel;
        if (localReadChannel != null) {
            readChannel = null;
            try {
                localReadChannel.close();
            }
            catch (IOException e) {
                log.warn(e, "Failed to close read channel for chunk: chunkId=%s file=%s", chunkId, file);
            }
        }
    }

    @Override
    public void release()
    {
        try {
            close();
        }
        finally {
            diskLease.release();
        }
    }
}
