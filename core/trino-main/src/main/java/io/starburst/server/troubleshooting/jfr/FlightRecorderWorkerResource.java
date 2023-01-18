/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jfr;

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.server.troubleshooting.jfr.FlightRecorderWorkerResource.BASE_PATH_API_V1;
import static io.starburst.server.troubleshooting.jfr.LocalRecordingFactory.RECORDING_FILENAME;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.accepted;
import static javax.ws.rs.core.Response.serverError;

@ResourceSecurity(PUBLIC)
@Path(BASE_PATH_API_V1)
public class FlightRecorderWorkerResource
{
    public static final String BASE_PATH_API_V1 = "/api/v1/jfr/{queryId}/";
    private final LocalRecordingFactory recordingFactory;

    @Inject
    public FlightRecorderWorkerResource(LocalRecordingFactory recordingFactory)
    {
        this.recordingFactory = requireNonNull(recordingFactory, "recordingFactory is null");
    }

    @GET
    @Path("/start")
    public Response startRecording(@PathParam("queryId") QueryId queryId)
    {
        recordingFactory.createStarted(queryId);
        return accepted().build();
    }

    @GET
    @Path("/finish")
    public Response finishRecording(@PathParam("queryId") QueryId queryId)
    {
        Optional<FlightRecording> recording = recordingFactory.findByQueryId(queryId);
        if (recording.isPresent()) {
            recording.get().finish();
            return accepted().build();
        }
        return serverError().build();
    }

    @DELETE
    public Response removeRecording(@PathParam("queryId") QueryId queryId)
    {
        Optional<FlightRecording> recording = recordingFactory.findByQueryId(queryId);
        if (recording.isPresent()) {
            recording.get().remove();
            return accepted().build();
        }
        return serverError().build();
    }

    @GET
    @Path("/download")
    public Response getRecording(@PathParam("queryId") QueryId queryId)
    {
        return recordingFactory.findByQueryId(queryId)
                .map(FlightRecording::getInputStreams)
                .map(FlightRecorderWorkerResource::responseFromInputStreams)
                .orElse(serverError())
                .build();
    }

    private static Response.ResponseBuilder responseFromInputStreams(Map<String, InputStream> streams)
    {
        return Response.ok(getOnlyElement(streams.values())).header("Content-disposition", "attachment; filename=\"%s\"".formatted(RECORDING_FILENAME));
    }
}
