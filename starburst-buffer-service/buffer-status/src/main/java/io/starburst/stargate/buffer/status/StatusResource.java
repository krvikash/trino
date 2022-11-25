/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.status;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

@Path("/status")
public class StatusResource
{
    private final StatusManager statusManager;

    @Inject
    public StatusResource(StatusManager statusManager)
    {
        this.statusManager = requireNonNull(statusManager, "statusManager is null");
    }

    @GET
    @Path("started")
    public Response checkStarted()
    {
        return checkStatus(statusManager::checkStarted);
    }

    @GET
    @Path("ready")
    public Response checkReady()
    {
        return checkStatus(statusManager::checkReady);
    }

    @GET
    @Path("alive")
    public Response checkAlive()
    {
        return checkStatus(statusManager::checkAlive);
    }

    @GET
    @Path("service")
    @Produces(MediaType.APPLICATION_JSON)
    public ServicesStatus getServicesStatus()
    {
        return statusManager.getServicesStatus();
    }

    private Response checkStatus(Supplier<Boolean> statusExtractor)
    {
        if (statusExtractor.get()) {
            return Response.ok().build();
        }
        return Response.serverError().build();
    }
}
