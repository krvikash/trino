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

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;

@ResourceSecurity(INTERNAL_ONLY)
@Path("/api/v1/troubleshooting/config")
public class ConfigDumpResource
{
    private final ConfigDumper configDumper;

    @Inject
    public ConfigDumpResource(ConfigDumper configDumper)
    {
        this.configDumper = requireNonNull(configDumper, "configDumper is null");
    }

    @GET
    public Response download()
            throws IOException
    {
        InputStream localConfigInputStream = configDumper.dumpLocalConfig();
        return Response.ok(localConfigInputStream)
                .header("Content-Type", "application/zip")
                .header("Content-disposition", "attachment; filename=\"config-worker.zip\"")
                .build();
    }
}
