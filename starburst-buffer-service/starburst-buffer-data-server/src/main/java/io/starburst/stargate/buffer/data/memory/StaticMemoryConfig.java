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

import java.util.Optional;

public class StaticMemoryConfig
        implements MemoryConfig
{
    private DataSize baseMemory = HeapSizeParser.DEFAULT.parse("10%");
    private Optional<DataSize> chunksMemory = Optional.empty();

    @Config("memory.base")
    @ConfigDescription("Total amount of memory assigned to buffer service")
    public StaticMemoryConfig setBaseMemory(String heapHeadroom)
    {
        this.baseMemory = HeapSizeParser.DEFAULT.parse(heapHeadroom);
        return this;
    }

    @Override
    @NotNull
    public DataSize getBaseMemory()
    {
        return baseMemory;
    }

    @Config("memory.chunks")
    @ConfigDescription("Total amount of memory assigned to buffer service")
    public StaticMemoryConfig setChunksMemory(String chunksMemory)
    {
        this.chunksMemory = Optional.of(HeapSizeParser.DEFAULT.parse(chunksMemory));
        return this;
    }

    @Override
    @NotNull
    public DataSize getChunksMemory()
    {
        return chunksMemory.orElse(DataSize.ofBytes((long) (baseMemory.toBytes() * 0.8)));
    }
}
