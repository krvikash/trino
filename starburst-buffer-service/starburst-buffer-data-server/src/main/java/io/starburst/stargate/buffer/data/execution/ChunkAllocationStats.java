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

import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class ChunkAllocationStats
{
    private final AtomicLong diskChunks = new AtomicLong();
    private final AtomicLong memoryChunks = new AtomicLong();

    public void recordDiskChunk()
    {
        diskChunks.incrementAndGet();
    }

    public void recordMemoryChunk()
    {
        memoryChunks.incrementAndGet();
    }

    public long diskChunks()
    {
        return diskChunks.get();
    }
}
