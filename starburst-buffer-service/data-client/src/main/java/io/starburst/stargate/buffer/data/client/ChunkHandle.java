/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A handle for a chunk.
 *
 * Contains fields which uniquely represents a chunk for a given exchange. As same handle may represent different chunks for different exchanges
 * the chunkHandle->chunk resolution should always happen in an exchange context.
 *
 * Additionally, object stores basic chunk statistics.
 */
public record ChunkHandle(
        long bufferNodeId,
        int partitionId,
        long chunkId,
        int dataSizeInBytes) {
    public ChunkHandle {
        checkArgument(dataSizeInBytes >= 0, "dataSizeInBytes must be greater or equal to 0");
    }
}
