/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import io.starburst.stargate.buffer.discovery.client.BufferNodeInfo;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.InvalidBufferNodeUpdateException;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.util.Set;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Produces(APPLICATION_JSON)
@Path("/api/v1/buffer/discovery")
public class DiscoveryResource
{
    private final DiscoveryManager discoveryManager;

    @Inject
    public DiscoveryResource(DiscoveryManager discoveryManager)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
    }

    @POST
    @Path("nodes/update")
    @Consumes(APPLICATION_JSON)
    public Response updateBufferNode(BufferNodeInfo nodeInfo)
    {
        try {
            discoveryManager.updateNodeInfos(nodeInfo);
        }
        catch (InvalidBufferNodeUpdateException e) {
            return Response.status(BAD_REQUEST)
                    .type(TEXT_PLAIN)
                    .entity(e.getMessage())
                    .build();
        }
        return Response.ok().build();
    }

    @GET
    @Path("nodes")
    @Produces(APPLICATION_JSON)
    public BufferNodeInfoResponse listBufferNodes()
    {
        boolean inGracePeriod = discoveryManager.isInGracePeriod();
        Set<BufferNodeInfo> nodeInfos = discoveryManager.getNodeInfos();
        return new BufferNodeInfoResponse(!inGracePeriod, nodeInfos);
    }
}
