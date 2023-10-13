/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;

import java.util.Map;

public interface SpoolingStorage
        extends AutoCloseable
{
    SpooledChunk getSpooledChunk(long chunkBufferNodeId, String exchangeId, long chunkId);

    // TODO: drop this code path after spooling merged chunks is stable (https://github.com/starburstdata/trino-buffer-service/issues/414)
    ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataLease chunkDataLease);

    // Have another method as:
    // 1. Keep old code untouched in case we need to fall back
    // 2. writeMergedChunks needs a different signature to update metadata
    ListenableFuture<Map<Long, SpooledChunk>> writeMergedChunks(long bufferNodeId, String exchangeId, Map<Chunk, ChunkDataLease> chunkDataLeaseMap, long contentLength);

    ListenableFuture<Void> writeMetadataFile(long bufferNodeId, Slice metadataSlice);

    ListenableFuture<Slice> readMetadataFile(long bufferNodeId);

    ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId);

    int getSpooledChunks();
}
