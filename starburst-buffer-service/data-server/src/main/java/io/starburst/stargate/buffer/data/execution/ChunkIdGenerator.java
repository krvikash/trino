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

import java.util.concurrent.atomic.AtomicLong;

public class ChunkIdGenerator
{
    private final AtomicLong atomicLong = new AtomicLong();

    public long getNextChunkId()
    {
        return atomicLong.getAndIncrement();
    }
}
