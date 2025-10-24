/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.memory;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.NotNull;

public class FullHeapMemoryConfig
        implements MemoryConfig
{
    private static final DataSize HEAP_SIZE = HeapSizeParser.DEFAULT.parse("100%");

    private DataSize heapHeadroom = HeapSizeParser.DEFAULT.parse("16%");

    @NotNull
    public DataSize getHeapHeadroom()
    {
        return heapHeadroom;
    }

    @Config("memory.heap-headroom")
    @ConfigDescription("The amount of heap memory to set aside as headroom/buffer (e.g., for untracked allocations)")
    public FullHeapMemoryConfig setHeapHeadroom(String heapHeadroom)
    {
        this.heapHeadroom = HeapSizeParser.DEFAULT.parse(heapHeadroom);
        return this;
    }

    @Override
    public DataSize getBaseMemory()
    {
        return HEAP_SIZE;
    }

    @Override
    public DataSize getChunksMemory()
    {
        return DataSize.ofBytes(HEAP_SIZE.toBytes() - heapHeadroom.toBytes());
    }
}
