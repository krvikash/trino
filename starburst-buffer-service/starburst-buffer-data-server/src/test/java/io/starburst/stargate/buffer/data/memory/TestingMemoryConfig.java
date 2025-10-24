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

import io.airlift.units.DataSize;

public class TestingMemoryConfig
        implements MemoryConfig
{
    private final DataSize memorySize;

    public TestingMemoryConfig(DataSize memorySize)
    {
        this.memorySize = memorySize;
    }

    @Override
    public DataSize getBaseMemory()
    {
        return memorySize;
    }

    @Override
    public DataSize getChunksMemory()
    {
        return memorySize;
    }
}
