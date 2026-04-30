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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.starburst.stargate.buffer.data.disk.DiskChunkSlot;
import io.starburst.stargate.buffer.data.disk.DiskSpaceLease;
import io.starburst.stargate.buffer.data.execution.CountedReference.Ref;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.finalizeChecksum;
import static java.util.Objects.requireNonNull;

public final class DiskChunkData
        implements ChunkData
{
    private final Path file;
    private final long chunkId;
    private final int chunkSizeInBytes;
    private final boolean calculateDataPagesChecksum;
    private final Ref<DiskSpaceLease> diskLease;
    private final Slice headerSlice = Slices.allocate(DATA_PAGE_HEADER_SIZE);

    @GuardedBy("this")
    private FileChannel channel;
    @GuardedBy("this")
    private final XxHash64 hash = new XxHash64();
    @GuardedBy("this")
    private Throwable poisonCause;

    @GuardedBy("this")
    private int writtenBytes;
    @GuardedBy("this")
    private int dataSizeInBytes;
    @GuardedBy("this")
    private int numDataPages;

    public DiskChunkData(long chunkId, int chunkSizeInBytes, boolean calculateDataPagesChecksum, DiskChunkSlot chunkSlot)
    {
        this(requireNonNull(chunkSlot, "chunkSlot is null").file(), chunkId, chunkSizeInBytes, calculateDataPagesChecksum, chunkSlot.lease(), chunkSlot.diskRelease());
    }

    DiskChunkData(Path file, long chunkId, int chunkSizeInBytes, boolean calculateDataPagesChecksum, DiskSpaceLease spaceLease, Runnable directoryRelease)
    {
        this.file = requireNonNull(file, "file is null");
        this.chunkId = chunkId;
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        requireNonNull(spaceLease, "spaceLease is null");
        try {
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        catch (IOException e) {
            spaceLease.release();
            throw new UncheckedIOException("failed to open disk chunk file " + file, e);
        }
        this.diskLease = CountedReference.create(
                () -> spaceLease,
                lease -> {
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
    }

    @Override
    public synchronized ListenableFuture<Void> write(int taskId, int attemptId, Slice data)
    {
        // Intentionally synchronous: capacity is reserved upfront, the FileChannel is open,
        // and the bytes are committed in a single FileChannel.write(ByteBuffer[]) syscall.
        // MemoryChunkData is async because it has to await new SliceLease allocations
        // mid-chunk; we have no such await points. Wrapping in an executor hop would only
        // add latency. Revisit only if profiling shows write-side syscall overhead dominating
        // (e.g., very small pages where batching N pages into one writev would help, or if
        // we ever switch to a writer that needs to wait for a disk-IO executor for fairness
        // with reads).
        if (poisonCause != null) {
            return immediateFailedFuture(poisonCause);
        }
        if (channel == null) {
            return immediateFailedFuture(new IllegalStateException("write() called on closed DiskChunkData for chunk " + chunkId));
        }
        int dataSize = data.length();
        int requiredStorageSize = DATA_PAGE_HEADER_SIZE + dataSize;
        int writableBytes = chunkSizeInBytes - writtenBytes;
        checkArgument(requiredStorageSize <= writableBytes,
                "requiredStorageSize %s larger than writableBytes %s", requiredStorageSize, writableBytes);

        SliceOutput headerOutput = headerSlice.getOutput();
        headerOutput.writeShort(taskId);
        headerOutput.writeByte(attemptId);
        headerOutput.writeInt(dataSize);

        ByteBuffer[] buffers = {
                headerSlice.toByteBuffer(),
                data.toByteBuffer(),
        };

        try {
            while (buffers[0].hasRemaining() || buffers[1].hasRemaining()) {
                long written = channel.write(buffers);
                if (written <= 0) {
                    throw new IOException("FileChannel.write returned " + written);
                }
            }
        }
        catch (IOException e) {
            poisonCause = new UncheckedIOException("write failed for chunk file " + chunkId, e);
            return immediateFailedFuture(poisonCause);
        }

        if (calculateDataPagesChecksum) {
            hash.update(data);
        }
        writtenBytes += requiredStorageSize;
        dataSizeInBytes += dataSize;
        numDataPages++;
        return immediateVoidFuture();
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
        checkState(channel == null, "get() called before DiskChunkData was closed for chunk %s", chunkId);
        // The ref count prevents both file deletion and partition directory cleanup
        // until the last reader releases its lease.
        Ref<DiskSpaceLease> readerRef = diskLease.addReference();
        try {
            long checksum = calculateDataPagesChecksum ? finalizeChecksum(hash) : NO_CHECKSUM;
            return new DiskChunkDataLease(file, writtenBytes, checksum, numDataPages, readerRef::release);
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
    public int getReclaimableHeapBytes()
    {
        // Disk-backed chunks hold no heap memory to reclaim
        return 0;
    }

    @Override
    public synchronized void close()
    {
        if (channel != null) {
            try {
                channel.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException("failed to close chunk file " + chunkId, e);
            }
            channel = null;
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
