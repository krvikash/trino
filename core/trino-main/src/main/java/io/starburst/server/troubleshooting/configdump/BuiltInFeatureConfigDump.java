/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.configdump;

import static java.util.Objects.requireNonNull;

public record BuiltInFeatureConfigDump(String fileName, byte[] serializedConfig)
{
    public BuiltInFeatureConfigDump
    {
        requireNonNull(fileName, "fileName is null");
        requireNonNull(serializedConfig, "serializedConfig is null");
    }
}
