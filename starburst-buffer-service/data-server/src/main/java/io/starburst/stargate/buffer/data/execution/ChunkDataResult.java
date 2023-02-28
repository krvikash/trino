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

import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record ChunkDataResult(
        Optional<ChunkDataLease> chunkDataLease,
        Optional<SpoolingFile> spoolingFile)
{
    public ChunkDataResult {
        requireNonNull(chunkDataLease, "chunkDataLease is null");
        requireNonNull(spoolingFile, "spoolingFile is null");
        checkArgument(chunkDataLease.isPresent() ^ spoolingFile.isPresent(), "Either chunkDataLease or spoolingFile should be present");
    }

    public static ChunkDataResult of(ChunkDataLease chunkDataLease)
    {
        return new ChunkDataResult(Optional.of(chunkDataLease), Optional.empty());
    }

    public static ChunkDataResult of(SpoolingFile spoolingFile)
    {
        return new ChunkDataResult(Optional.empty(), Optional.of(spoolingFile));
    }
}
