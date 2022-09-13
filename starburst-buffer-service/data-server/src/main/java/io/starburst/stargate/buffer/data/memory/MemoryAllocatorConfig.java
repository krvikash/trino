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

import javax.validation.constraints.NotNull;

public class MemoryAllocatorConfig
{
    private DataSize heapHeadroom = DataSize.ofBytes(Math.round(Runtime.getRuntime().maxMemory() * 0.1));

    @NotNull
    public DataSize getHeapHeadroom()
    {
        return heapHeadroom;
    }

    @Config("memory.heap-headroom")
    @ConfigDescription("The amount of heap memory to set aside as headroom/buffer (e.g., for untracked allocations)")
    public MemoryAllocatorConfig setHeapHeadroom(DataSize heapHeadroom)
    {
        this.heapHeadroom = heapHeadroom;
        return this;
    }
}
