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
    // todo: parseClass does not work on records
    // java.lang.UnsupportedOperationException: can't get field offset on a record class: private final long io.starburst.stargate.buffer.data.client.ChunkHandle.bufferNodeId
    //private static final int INSTANCE_SIZE = ClassLayout.parseClass(ChunkHandle.class).instanceSize();
    private static final int OBJECT_HEADER_SIZE = 16; /* object header with possible padding */
    public static final int INSTANCE_SIZE = OBJECT_HEADER_SIZE + Long.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES;

    public ChunkHandle {
        checkArgument(dataSizeInBytes >= 0, "dataSizeInBytes must be greater or equal to 0");
    }
}
