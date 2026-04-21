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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static java.util.Objects.requireNonNull;

public final class DiskChunkData
        implements ChunkData
{
    private final Path partitionDirectory;
    private final long chunkId;
    private final int chunkSizeInBytes;
    @SuppressWarnings("UnusedVariable") // to be read by future write()/get() implementations
    private final boolean calculateDataPagesChecksum;

    @GuardedBy("this")
    private FileChannel channel;

    private int writtenBytes;
    private int dataSizeInBytes;
    private int numDataPages;

    public DiskChunkData(Path partitionDirectory, long chunkId, int chunkSizeInBytes, boolean calculateDataPagesChecksum)
    {
        this.partitionDirectory = requireNonNull(partitionDirectory, "partitionDirectory is null");
        this.chunkId = chunkId;
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.calculateDataPagesChecksum = calculateDataPagesChecksum;
        Path file = file();
        try {
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to open disk chunk file " + file, e);
        }
    }

    @Override
    public ListenableFuture<Void> write(int taskId, int attemptId, Slice data)
    {
        // TODO: gathering write e.g via ByteBuffer[], atomic writtenBytes,
        // finalize checksum post-append (keeps hash state consistent with file state on I/O
        // failure), poison-on-failure so subsequent writes short-circuit.
        throw new UnsupportedOperationException("DiskChunkData.write not yet implemented");
    }

    @Override
    public boolean hasEnoughSpace(int requiredStorageSize)
    {
        return requiredStorageSize <= chunkSizeInBytes - writtenBytes;
    }

    @Override
    public int dataSizeInBytes()
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
        // TODO: snapshot writtenBytes at call time, finalize the running XxHash64 when
        // calculateDataPagesChecksum is true, and wire the release callback through a
        // CountedReference destroy action so the file is only deleted once the last reader
        // lease releases. Consider holding a back-reference to this instead of duplicating
        // state in the lease.
        return new DiskChunkDataLease(writtenBytes, NO_CHECKSUM, numDataPages, () -> {});
    }

    @Override
    public int getReclaimableHeapBytes()
    {
        // Disk-backed chunks hold no heap memory to reclaim. Returning ZERO
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
                throw new UncheckedIOException("failed to close " + file(), e);
            }
            channel = null;
        }
    }

    @Override
    public synchronized void release()
    {
        // TODO: this must become the destroy action of a CountedReference that tracks
        // outstanding reader leases from get() - currently it deletes the file unconditionally,
        // which would race with in-flight reads once writes actually happen.
        close();
        Path file = file();
        try {
            Files.deleteIfExists(file);
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to delete " + file, e);
        }
    }

    private Path file()
    {
        return partitionDirectory.resolve("chunk-" + chunkId + ".data");
    }
}
