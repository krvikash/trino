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

import static java.util.Objects.requireNonNull;

public record ChunkDataHolder(
        List<Slice> chunkSlices,
        long checksum,
        int numDataPages,
        Runnable releaseCallback)
{
    public static final int CHUNK_SLICES_METADATA_SIZE = Long.BYTES + Integer.BYTES;

    public ChunkDataHolder {
        chunkSlices = ImmutableList.copyOf(requireNonNull(chunkSlices, "chunkSlices is null"));
        requireNonNull(releaseCallback, "releaseCallback is null");
    }

    public int serializedSizeInBytes()
    {
        return chunkSlices.stream().mapToInt(Slice::length).sum() + CHUNK_SLICES_METADATA_SIZE;
    }

    public void release()
    {
        releaseCallback.run();
    }
}
