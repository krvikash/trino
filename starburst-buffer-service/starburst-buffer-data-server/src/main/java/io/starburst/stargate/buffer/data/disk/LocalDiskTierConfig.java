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

import static com.google.common.base.Preconditions.checkArgument;

public class LocalDiskTierConfig
{
    private Path directory;
    private DataSize capacity;

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
}
