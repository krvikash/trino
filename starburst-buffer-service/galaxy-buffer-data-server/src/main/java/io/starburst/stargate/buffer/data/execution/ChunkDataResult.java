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

import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record ChunkDataResult(
        Optional<ChunkDataLease> chunkDataLease,
        Optional<SpooledChunk> spooledChunk)
{
    public ChunkDataResult {
        requireNonNull(chunkDataLease, "chunkDataLease is null");
        requireNonNull(spooledChunk, "spooledChunk is null");
        checkArgument(chunkDataLease.isPresent() ^ spooledChunk.isPresent(), "Either chunkDataLease or spooledChunk should be present");
    }

    public static ChunkDataResult of(ChunkDataLease chunkDataLease)
    {
        return new ChunkDataResult(Optional.of(chunkDataLease), Optional.empty());
    }

    public static ChunkDataResult of(SpooledChunk spooledChunk)
    {
        return new ChunkDataResult(Optional.empty(), Optional.of(spooledChunk));
    }
}
