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
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;

public interface SpoolingStorage
        extends AutoCloseable
{
    ChunkDataLease readChunk(String exchangeId, long chunkId, long bufferNodeId);

    ListenableFuture<Void> writeChunk(String exchangeId, long chunkId, long bufferNodeId, ChunkDataHolder chunkDataHolder);

    ListenableFuture<Void> removeExchange(String exchangeId);
}
