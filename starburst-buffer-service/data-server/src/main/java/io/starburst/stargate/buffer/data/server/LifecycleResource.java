/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.inject.Inject;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static java.util.Objects.requireNonNull;

@Path("/api/v1/buffer/data")
public class LifecycleResource
{
    private final DrainService drainService;
    private final BufferNodeStateManager bufferNodeStateManager;

    @Inject
    public LifecycleResource(
            DrainService drainService,
            BufferNodeStateManager bufferNodeStateManager)
    {
        this.drainService = requireNonNull(drainService, "drainService is null");
        this.bufferNodeStateManager = requireNonNull(bufferNodeStateManager, "bufferNodeStateManager is null");
    }

    @GET
    @Path("drain")
    public Response drain()
    {
        try {
            drainService.drain();
        }
        catch (Exception e) {
            return errorResponse(e);
        }
        return Response.ok().build();
    }

    @GET
    @Path("state")
    @Produces("text/plain")
    public Response state()
    {
        try {
            return Response.ok(
                            "%s%n".formatted(
                                    bufferNodeStateManager.getState()
                                            .toString()))
                    .build();
        }
        catch (Exception e) {
            return errorResponse(e);
        }
    }

    @GET
    @Path("preShutdown")
    public Response preShutdown()
    {
        try {
            bufferNodeStateManager.preShutdownCleanup();
        }
        catch (Exception e) {
            return errorResponse(e);
        }
        return Response.ok().build();
    }

    private static Response errorResponse(Exception e)
    {
        return Response.status(Status.INTERNAL_SERVER_ERROR)
                .header(ERROR_CODE_HEADER, ErrorCode.INTERNAL_ERROR)
                .entity(e.getMessage())
                .build();
    }
}
