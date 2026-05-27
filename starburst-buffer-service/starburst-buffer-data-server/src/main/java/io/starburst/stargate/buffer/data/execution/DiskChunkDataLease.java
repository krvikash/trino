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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class DiskChunkDataLease
        implements ChunkDataLease
{
    private final Path file;
    private final FileChannel channel;
    private final int length;
    private final long checksum;
    private final int numDataPages;
    private final Runnable releaseCallback;
    private final AtomicBoolean released = new AtomicBoolean();

    public DiskChunkDataLease(
            Path file,
            FileChannel channel,
            int length,
            long checksum,
            int numDataPages,
            Runnable releaseCallback)
    {
        this.file = requireNonNull(file, "file is null");
        this.channel = requireNonNull(channel, "channel is null");
        this.length = length;
        this.checksum = checksum;
        this.numDataPages = numDataPages;
        this.releaseCallback = requireNonNull(releaseCallback, "releaseCallback is null");
    }

    public Path file()
    {
        return file;
    }

    public int read(ByteBuffer dst, long position)
            throws IOException
    {
        return channel.read(dst, position);
    }

    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException
    {
        return channel.transferTo(position, count, target);
    }

    public int length()
    {
        return length;
    }

    @Override
    public long getChecksum()
    {
        return checksum;
    }

    @Override
    public int getNumDataPages()
    {
        return numDataPages;
    }

    @Override
    public int serializedSizeInBytes()
    {
        return length + CHUNK_SLICES_METADATA_SIZE;
    }

    @Override
    public void release()
    {
        checkState(released.compareAndSet(false, true), "already released");
        releaseCallback.run();
    }
}
