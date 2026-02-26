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
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;

public class SpoolingDirectoryConfig
{
    private URI spoolingDirectory;
    private boolean allowLocalSpooling;

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

    public boolean isAllowLocalSpooling()
    {
        return allowLocalSpooling;
    }

    @ConfigHidden
    @Config("testing.allow-local-spooling")
    @ConfigDescription("Allow to use local filesystem for spooling. This is intended for testing purposes only and should not be used in production.")
    public SpoolingDirectoryConfig setAllowLocalSpooling(boolean allowLocalSpooling)
    {
        this.allowLocalSpooling = allowLocalSpooling;
        return this;
    }
}
