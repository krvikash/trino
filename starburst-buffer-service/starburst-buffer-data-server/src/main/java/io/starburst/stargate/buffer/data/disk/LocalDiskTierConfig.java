/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;

public class LocalDiskTierConfig
{
    private Path directory;
    private DataSize capacity;
    private double memoryHighWatermark = 0.8;
    private double memoryLowWatermark = 0.5;
    private double spoolingHighWatermark = 0.8;
    private double spoolingLowWatermark = 0.6;
    private boolean allowDirectoryCreation;
    private int ioThreads = 128;
    private int maxOpenDiskChunks = 4096;
    private double exchangeMemoryFraction = 0.3;

    @NotNull
    public Path getDirectory()
    {
        return directory;
    }

    @Config("local-disk.directory")
    @ConfigDescription("Local filesystem path for disk tier data")
    public LocalDiskTierConfig setDirectory(Path directory)
    {
        this.directory = directory;
        return this;
    }

    @NotNull
    public DataSize getCapacity()
    {
        return capacity;
    }

    @Config("local-disk.capacity")
    @ConfigDescription("Maximum disk space to use for the disk tier")
    public LocalDiskTierConfig setCapacity(DataSize capacity)
    {
        checkArgument(capacity == null || capacity.toBytes() > 0, "capacity must be positive");
        this.capacity = capacity;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryHighWatermark()
    {
        return memoryHighWatermark;
    }

    @Config("local-disk.routing.memory-high-watermark")
    @ConfigDescription("Fraction of memory capacity at which disk routing activates (0.0–1.0)")
    public LocalDiskTierConfig setMemoryHighWatermark(double memoryHighWatermark)
    {
        this.memoryHighWatermark = memoryHighWatermark;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryLowWatermark()
    {
        return memoryLowWatermark;
    }

    @Config("local-disk.routing.memory-low-watermark")
    @ConfigDescription("Fraction of memory capacity at which disk routing deactivates after being triggered (0.0–1.0); must be less than memory-high-watermark")
    public LocalDiskTierConfig setMemoryLowWatermark(double memoryLowWatermark)
    {
        this.memoryLowWatermark = memoryLowWatermark;
        return this;
    }

    public double getSpoolingHighWatermark()
    {
        return spoolingHighWatermark;
    }

    @Config("local-disk.spooling-high-watermark")
    @ConfigDescription("Disk utilization ratio above which spooling to remote storage is triggered")
    public LocalDiskTierConfig setSpoolingHighWatermark(double spoolingHighWatermark)
    {
        checkArgument(spoolingHighWatermark > 0 && spoolingHighWatermark <= 1, "spoolingHighWatermark must be in (0, 1]");
        this.spoolingHighWatermark = spoolingHighWatermark;
        return this;
    }

    public double getSpoolingLowWatermark()
    {
        return spoolingLowWatermark;
    }

    @Config("local-disk.spooling-low-watermark")
    @ConfigDescription("Disk utilization ratio at which spooling to remote storage stops")
    public LocalDiskTierConfig setSpoolingLowWatermark(double spoolingLowWatermark)
    {
        checkArgument(spoolingLowWatermark > 0 && spoolingLowWatermark < 1, "spoolingLowWatermark must be in (0, 1)");
        this.spoolingLowWatermark = spoolingLowWatermark;
        return this;
    }

    public boolean isAllowDirectoryCreation()
    {
        return allowDirectoryCreation;
    }

    @Config("local-disk.testing.allow-directory-creation")
    @ConfigDescription("Create the disk tier root directory on startup if it does not exist. Intended for testing only.")
    public LocalDiskTierConfig setAllowDirectoryCreation(boolean allowDirectoryCreation)
    {
        this.allowDirectoryCreation = allowDirectoryCreation;
        return this;
    }

    @Min(1)
    public int getIoThreads()
    {
        return ioThreads;
    }

    @Config("local-disk.io-threads")
    @ConfigDescription("Number of threads used to dispatch disk-chunk writes; caps in-flight I/O so kernel writeback bursts do not blow up tail latency")
    public LocalDiskTierConfig setIoThreads(int ioThreads)
    {
        this.ioThreads = ioThreads;
        return this;
    }

    @Min(1)
    public int getMaxOpenDiskChunks()
    {
        return maxOpenDiskChunks;
    }

    @Config("local-disk.routing.max-open-disk-chunks")
    @ConfigDescription("Maximum number of concurrently open disk chunks; routing pauses when this limit is reached")
    public LocalDiskTierConfig setMaxOpenDiskChunks(int maxOpenDiskChunks)
    {
        this.maxOpenDiskChunks = maxOpenDiskChunks;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getExchangeMemoryFraction()
    {
        return exchangeMemoryFraction;
    }

    @Config("local-disk.routing.exchange-memory-fraction")
    @ConfigDescription("Route exchange to disk when its cumulative allocated bytes exceed this fraction of total memory capacity; 0.0 disables the check")
    public LocalDiskTierConfig setExchangeMemoryFraction(double exchangeMemoryFraction)
    {
        this.exchangeMemoryFraction = exchangeMemoryFraction;
        return this;
    }
}
