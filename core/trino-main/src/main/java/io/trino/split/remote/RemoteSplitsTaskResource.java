/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.split.remote;

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/splits/task")
@ResourceSecurity(INTERNAL_ONLY)
public class RemoteSplitsTaskResource
{
    private final RemoteSplitsTaskManager remoteSplitsTaskManager;

    @Inject
    public RemoteSplitsTaskResource(RemoteSplitsTaskManager remoteSplitsTaskManager)
    {
        this.remoteSplitsTaskManager = requireNonNull(remoteSplitsTaskManager, "remoteSplitsTaskManager is null");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void runSplitsTask(
            CreateRemoteSplitsTaskRequest request,
            @Suspended AsyncResponse asyncResponse)
    {
        remoteSplitsTaskManager.runSplitsTaskAsync(request)
                .whenComplete((requestedDynamicFilterWaitTimeoutMillis, error) -> {
                    if (error != null) {
                        asyncResponse.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(CreateRemoteSplitsTaskResponse.fail(error))
                                .build());
                    }
                    else {
                        asyncResponse.resume(Response.ok()
                                .entity(CreateRemoteSplitsTaskResponse.forCreatedTask(requestedDynamicFilterWaitTimeoutMillis))
                                .build());
                    }
                });
    }

    @POST
    @Path("/{taskId}/result")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void fetchResult(
            @PathParam("taskId") String taskId,
            GetRemoteSplitsTaskRequest request,
            @Suspended AsyncResponse asyncResponse)
    {
        remoteSplitsTaskManager.fetchNextBatch(taskId, request)
                .whenComplete((response, error) -> {
                    if (error != null) {
                        asyncResponse.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(RemoteSplitsTaskResponse.fail(request.token(), error))
                                .build());
                    }
                    else {
                        asyncResponse.resume(Response.ok().entity(response).build());
                    }
                });
    }

    @POST
    @Path("/{taskId}/heartbeat")
    @Produces(MediaType.APPLICATION_JSON)
    public Response heartbeat(@PathParam("taskId") String taskId)
    {
        if (remoteSplitsTaskManager.touch(taskId)) {
            return Response.ok().build();
        }
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @DELETE
    @Path("/{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response close(@PathParam("taskId") String taskId)
    {
        remoteSplitsTaskManager.close(taskId);
        return Response.ok().build();
    }
}
