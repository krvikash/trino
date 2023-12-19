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
    private DataSize heapHeadroom = DataSize.ofBytes(Math.round(Runtime.getRuntime().maxMemory() * 0.16));
    private double allocationRatioLowWatermark = 0.75;
    private double allocationRatioHighWatermark = 0.9;
    private double chunkSlicePoolingFraction = 0.8;

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

    public double getAllocationRatioLowWatermark()
    {
        return allocationRatioLowWatermark;
    }

    @Config("memory.allocation-low-watermark")
    @ConfigDescription("Memory blocked low allocation watermark")
    public MemoryAllocatorConfig setAllocationRatioLowWatermark(double allocationRatioLowWatermark)
    {
        this.allocationRatioLowWatermark = allocationRatioLowWatermark;
        return this;
    }

    public double getAllocationRatioHighWatermark()
    {
        return allocationRatioHighWatermark;
    }

    @Config("memory.allocation-high-watermark")
    @ConfigDescription("Memory blocked high allocation watermark")
    public MemoryAllocatorConfig setAllocationRatioHighWatermark(double allocationRatioHighWatermark)
    {
        this.allocationRatioHighWatermark = allocationRatioHighWatermark;
        return this;
    }

    public double getChunkSlicePoolingFraction()
    {
        return chunkSlicePoolingFraction;
    }

    @Config("memory.chunk-slice-pool-fraction")
    @ConfigDescription("Max fraction of available bytes for chunk slices pooling")
    public MemoryAllocatorConfig setChunkSlicePoolingFraction(double chunkSlicePoolingFraction)
    {
        this.chunkSlicePoolingFraction = chunkSlicePoolingFraction;
        return this;
    }
}
