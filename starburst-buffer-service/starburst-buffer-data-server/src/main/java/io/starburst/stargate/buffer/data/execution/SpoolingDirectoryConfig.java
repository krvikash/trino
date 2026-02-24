/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;

public class SpoolingDirectoryConfig
{
    private URI spoolingDirectory;

    @NotNull
    public URI getSpoolingDirectory()
    {
        return spoolingDirectory;
    }

    @Config("spooling.directory")
    public SpoolingDirectoryConfig setSpoolingDirectory(String spoolingDirectory)
    {
        if (spoolingDirectory != null) {
            if (!spoolingDirectory.endsWith(PATH_SEPARATOR)) {
                spoolingDirectory += PATH_SEPARATOR;
            }
            this.spoolingDirectory = URI.create(spoolingDirectory);
        }
        return this;
    }
}
