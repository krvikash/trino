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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ChunkManagerConfig
{
    private DataSize chunkSize = DataSize.of(16, MEGABYTE);

    @NotNull
    @MinDataSize("16MB")
    // TODO: Chunk size should be 3 bytes larger than exchange.max-page-storage-size (to store taskId and attemptId).
    // TODO: This may not be an issue any more when we support dynamic chunk sizing.
    public DataSize getChunkSize()
    {
        return chunkSize;
    }

    @Config("chunk.size")
    public ChunkManagerConfig setChunkSize(DataSize chunkSize)
    {
        this.chunkSize = chunkSize;
        return this;
    }
}
