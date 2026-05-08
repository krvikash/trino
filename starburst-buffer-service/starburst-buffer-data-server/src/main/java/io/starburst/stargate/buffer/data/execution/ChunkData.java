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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

public sealed interface ChunkData
        permits MemoryChunkData, DiskChunkData
{
    enum ChunkPlacement
    {
        MEMORY,
        LOCAL_DISK
    }

    ChunkPlacement chunkPlacement();

    int getReclaimableBytes();

    ListenableFuture<Void> write(int taskId, int attemptId, Slice data);

    boolean hasEnoughSpace(int requiredStorageSize);

    int dataSizeInBytes();

    boolean isEmpty();

    ChunkDataLease get();

    void close();

    void release();
}
