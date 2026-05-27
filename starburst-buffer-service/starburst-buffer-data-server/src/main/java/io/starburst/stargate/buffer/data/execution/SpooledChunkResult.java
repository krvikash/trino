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

import static java.util.Objects.requireNonNull;

public record SpooledChunkResult(SpooledChunk spooledChunk)
        implements ChunkDataResult
{
    public SpooledChunkResult
    {
        requireNonNull(spooledChunk, "spooledChunk is null");
    }

    @Override
    public int localSizeInBytes()
    {
        return 0;
    }
}
