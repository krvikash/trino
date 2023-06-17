/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server.failures;

import com.google.inject.Inject;
import io.starburst.stargate.buffer.discovery.client.failures.FailureInfo;
import io.starburst.stargate.buffer.discovery.client.failures.FailuresStatusInfo;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Produces(APPLICATION_JSON)
@Path("/api/v1/buffer/failures")
public class FailureTrackingResource
{
    private final FailuresTrackingManager manager;

    @Inject
    public FailureTrackingResource(FailuresTrackingManager failuresTrackingManager)
    {
        this.manager = requireNonNull(failuresTrackingManager, "failuresTrackingManager is null");
    }

    @POST
    @Path("register")
    @Consumes(APPLICATION_JSON)
    public Response registerFailure(FailureInfo failureInfo)
    {
        manager.registerFailure(failureInfo.exchangeId(), failureInfo.observingClient(), failureInfo.failureDetails());
        return Response.ok().build();
    }

    @GET
    @Produces(APPLICATION_JSON)
    public FailuresStatusInfo getFailuresStatusInfo()
    {
        return new FailuresStatusInfo(manager.getRecentFailuresCount());
    }
}
