/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.providers;

import com.google.common.collect.ImmutableMap;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.trino.client.NodeVersion;

import javax.inject.Inject;

import java.io.InputStream;
import java.util.Map;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;
import static java.util.Objects.requireNonNull;

public class SoftwareVersionProvider
        implements TroubleshootingProvider
{
    private final NodeVersion nodeVersion;

    @Inject
    public SoftwareVersionProvider(NodeVersion nodeVersion)
    {
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        return ImmutableMap.of("version.txt", toInputStream(nodeVersion.getVersion()));
    }
}
