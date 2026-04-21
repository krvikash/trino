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

import com.google.common.base.Throwables;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class DiskChunkDataLease
        implements ChunkDataLease
{
    private final int length;
    private final long checksum;
    private final int numDataPages;
    private final Runnable releaseCallback;
    private boolean released;
    private String releaseStackTrace;

    public DiskChunkDataLease(
            int length,
            long checksum,
            int numDataPages,
            Runnable releaseCallback)
    {
        this.length = length;
        this.checksum = checksum;
        this.numDataPages = numDataPages;
        this.releaseCallback = requireNonNull(releaseCallback, "releaseCallback is null");
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
        checkState(!released, "already released; previous release: %s", releaseStackTrace);
        released = true;
        releaseStackTrace = Throwables.getStackTraceAsString(new RuntimeException());
        releaseCallback.run();
    }
}
