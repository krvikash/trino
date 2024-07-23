/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.tracing;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_DISPOSITION;
import static java.util.Objects.requireNonNull;

@ResourceSecurity(INTERNAL_ONLY)
@Path("/api/v1/troubleshooting/trace/{queryId}")
public class TroubleshootingTraceResource
{
    private static final Logger log = Logger.get(TroubleshootingTraceResource.class);
    private final SpanInterceptor spanInterceptor;

    @Inject
    public TroubleshootingTraceResource(SpanInterceptor spanInterceptor)
    {
        this.spanInterceptor = requireNonNull(spanInterceptor, "spanInterceptor is null");
    }

    @POST
    @Path("/start")
    public void start(@PathParam("queryId") QueryId queryId)
    {
        log.info("start queryId: %s", queryId);
        spanInterceptor.requestSpanCollect(queryId);
    }

    @DELETE
    public void remove(@PathParam("queryId") QueryId queryId)
    {
        log.info("remove queryId: %s", queryId);
        spanInterceptor.remove(queryId);
    }

    @GET
    @Path("/download")
    public Response download(@PathParam("queryId") QueryId queryId)
            throws IOException
    {
        log.info("download queryId: %s", queryId);
        InputStream is = spanInterceptor.getSpans(queryId);
        return Response.ok(is)
                .header(CONTENT_DISPOSITION, "attachment; filename=\"opentelemetry-worker.grpc.gz\"")
                .build();
    }
}
