/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jfr;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import java.nio.file.Path;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class FlightRecorderConfig
{
    private DataSize maxRecordingSize = DataSize.of(128, MEGABYTE);
    private Optional<Path> temporaryDirectory = Optional.empty();

    @Config("troubleshooting.jfr.temporary-directory")
    @ConfigDescription("Path that will be used for saving Java Flight Recorder files")
    public FlightRecorderConfig setTemporaryDirectory(Path temporaryDirectory)
    {
        this.temporaryDirectory = Optional.ofNullable(temporaryDirectory);
        return this;
    }

    public Optional<@FileExists Path> getTemporaryDirectory()
    {
        return temporaryDirectory;
    }

    @Config("troubleshooting.jfr.max-recording-size")
    @ConfigDescription("Maximum size of a single Java Flight Recorder file that will be captured")
    public FlightRecorderConfig setMaxRecordingSize(DataSize maxRecordingSize)
    {
        this.maxRecordingSize = maxRecordingSize;
        return this;
    }

    @MinDataSize("8MB")
    @MaxDataSize("512MB")
    public DataSize getMaxRecordingSize()
    {
        return maxRecordingSize;
    }
}
