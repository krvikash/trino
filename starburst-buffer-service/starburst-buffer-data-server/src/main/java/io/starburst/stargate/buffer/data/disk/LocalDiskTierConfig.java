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
import io.airlift.configuration.ConfigHidden;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.NotNull;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class LocalDiskTierConfig
{
    private Path directory;
    private DataSize capacity;
    private DataSize memorySkipThreshold;
    private double spoolingHighWatermark = 0.8;
    private double spoolingLowWatermark = 0.5;

    @NotNull
    @FileExists
    public Path getDirectory()
    {
        return directory;
    }

    @Config("local-disk.directory")
    @ConfigHidden
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
    @ConfigHidden
    @ConfigDescription("Maximum disk space to use for the disk tier")
    public LocalDiskTierConfig setCapacity(DataSize capacity)
    {
        checkArgument(capacity == null || capacity.toBytes() > 0, "capacity must be positive");
        this.capacity = capacity;
        return this;
    }

    public Optional<DataSize> getMemorySkipThreshold()
    {
        return Optional.ofNullable(memorySkipThreshold);
    }

    @Config("local-disk.memory-skip-threshold")
    @ConfigHidden
    @ConfigDescription("Per-exchange cumulative closed-chunk bytes after which subsequent open chunks are written directly to disk. Default empty disables the policy.")
    public LocalDiskTierConfig setMemorySkipThreshold(DataSize memorySkipThreshold)
    {
        checkArgument(memorySkipThreshold == null || memorySkipThreshold.toBytes() > 0, "memorySkipThreshold must be positive");
        this.memorySkipThreshold = memorySkipThreshold;
        return this;
    }

    public double getSpoolingHighWatermark()
    {
        return spoolingHighWatermark;
    }

    @Config("local-disk.spooling-high-watermark")
    @ConfigHidden
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
    @ConfigHidden
    @ConfigDescription("Disk utilization ratio at which spooling to remote storage stops")
    public LocalDiskTierConfig setSpoolingLowWatermark(double spoolingLowWatermark)
    {
        checkArgument(spoolingLowWatermark > 0 && spoolingLowWatermark < 1, "spoolingLowWatermark must be in (0, 1)");
        this.spoolingLowWatermark = spoolingLowWatermark;
        return this;
    }
}
