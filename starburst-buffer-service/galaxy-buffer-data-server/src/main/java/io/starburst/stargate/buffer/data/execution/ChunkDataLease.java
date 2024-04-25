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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ChunkDataLease
{
    private ImmutableList<Slice> chunkSlices;
    private final long checksum;
    private final int numDataPages;
    private final Runnable releaseCallback;

    public static final int CHUNK_SLICES_METADATA_SIZE = Long.BYTES + Integer.BYTES;

    public ChunkDataLease(List<Slice> chunkSlices, long checksum, int numDataPages, Runnable releaseCallback)
    {
        this.chunkSlices = ImmutableList.copyOf(requireNonNull(chunkSlices, "chunkSlices is null"));
        this.checksum = checksum;
        this.numDataPages = numDataPages;
        this.releaseCallback = requireNonNull(releaseCallback, "releaseCallback is null");
    }

    public ImmutableList<Slice> getChunkSlices()
    {
        checkState(chunkSlices != null, "already released");
        return chunkSlices;
    }

    public long getChecksum()
    {
        return checksum;
    }

    public int getNumDataPages()
    {
        return numDataPages;
    }

    public int serializedSizeInBytes()
    {
        return getChunkSlices().stream().mapToInt(Slice::length).sum() + CHUNK_SLICES_METADATA_SIZE;
    }

    public void release()
    {
        checkState(chunkSlices != null, "already released");
        releaseCallback.run();
        chunkSlices = null; // ensure no dangling reference
    }
}
