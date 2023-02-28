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
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;

public interface SpoolingStorage
        extends AutoCloseable
{
    SpoolingFile getSpoolingFile(long bufferNodeId, String exchangeId, long chunkId);

    ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataLease chunkDataLease);

    ListenableFuture<Void> removeExchange(String exchangeId);

    int getSpooledChunks();
}
