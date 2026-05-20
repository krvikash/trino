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

public class LocalDiskTierFeatureConfig
{
    private boolean enabled;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("local-disk.enabled")
    @ConfigHidden
    @ConfigDescription("Enable local disk as intermediate tier between memory and remote storage. Requires spooling.storage-driver=TRINO_FS.")
    public LocalDiskTierFeatureConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }
}
