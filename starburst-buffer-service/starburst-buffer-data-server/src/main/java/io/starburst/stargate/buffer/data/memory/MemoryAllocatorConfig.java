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
import io.airlift.configuration.LegacyConfig;

public class MemoryAllocatorConfig
{
    private double spoolingRatioLowWatermark = 0.75;
    private double spoolingRatioHighWatermark = 0.9;
    private double chunkSlicePoolingFraction = 0.8;

    public double getSpoolingRatioLowWatermark()
    {
        return spoolingRatioLowWatermark;
    }

    @Config("memory.spooling-low-watermark")
    @LegacyConfig("memory.allocation-low-watermark")
    @ConfigDescription("Memory utilization ratio at which spooling stops")
    public MemoryAllocatorConfig setSpoolingRatioLowWatermark(double spoolingRatioLowWatermark)
    {
        this.spoolingRatioLowWatermark = spoolingRatioLowWatermark;
        return this;
    }

    public double getSpoolingRatioHighWatermark()
    {
        return spoolingRatioHighWatermark;
    }

    @Config("memory.spooling-high-watermark")
    @LegacyConfig("memory.allocation-high-watermark")
    @ConfigDescription("Memory utilization ratio above which spooling is triggered")
    public MemoryAllocatorConfig setSpoolingRatioHighWatermark(double spoolingRatioHighWatermark)
    {
        this.spoolingRatioHighWatermark = spoolingRatioHighWatermark;
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
