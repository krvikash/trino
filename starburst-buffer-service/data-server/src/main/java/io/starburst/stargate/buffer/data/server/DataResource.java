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

import com.google.common.reflect.TypeToken;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static java.util.Objects.requireNonNull;

@Path("/api/v1/buffer/data")
public class DataResource
{
    private final ChunkManager chunkManager;
    private final MemoryAllocator memoryAllocator;

    @Inject
    public DataResource(
            ChunkManager chunkManager,
            MemoryAllocator memoryAllocator)
    {
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
    }

    @GET
    @Path("{exchangeId}/closedChunks")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listClosedChunks(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("pagingId") Long pagingId)
    {
        try {
            ChunkList chunkList = chunkManager.listClosedChunks(exchangeId, pagingId == null ? OptionalLong.empty() : OptionalLong.of(pagingId));
            return Response.ok().entity(chunkList).build();
        }
        catch (Exception e) {
            return errorResponse(e);
        }
    }

    @POST
    @Path("{exchangeId}/addDataPages/{partitionId}/{taskId}/{attemptId}/{dataPagesId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response addDataPage(
            @PathParam("exchangeId") String exchangeId,
            @PathParam("partitionId") int partitionId,
            @PathParam("taskId") int taskId,
            @PathParam("attemptId") int attemptId,
            @PathParam("dataPagesId") long dataPagesId,
            InputStream inputStream)
    {
        requireNonNull(inputStream, "inputStream is null");

        List<Slice> pages = new ArrayList<>();
        try {
            SliceInput sliceInput = new InputStreamSliceInput(inputStream);
            while (sliceInput.isReadable()) {
                int length = sliceInput.readInt();
                Slice slice = memoryAllocator.allocate(length)
                        .orElseThrow(() -> new IllegalStateException("Failed to create a new open chunk due to memory allocation failure"));
                sliceInput.readBytes(slice);
                pages.add(slice);
            }
            chunkManager.addDataPages(exchangeId, partitionId, taskId, attemptId, dataPagesId, pages);
        }
        catch (Exception e) {
            return errorResponse(e);
        }
        finally {
            pages.forEach(memoryAllocator::release);
        }
        return Response.ok().build();
    }

    @GET
    @Path("{exchangeId}/pages/{partitionId}/{chunkId}/{bufferNodeId}")
    @Produces(TRINO_CHUNK_DATA)
    public Response getChunkData(
            @PathParam("exchangeId") String exchangeId,
            @PathParam("partitionId") int partitionId,
            @PathParam("chunkId") long chunkId,
            @PathParam("bufferNodeId") long bufferNodeId)
    {
        try {
            Chunk.ChunkDataRepresentation chunkDataRepresentation = chunkManager.getChunkData(exchangeId, partitionId, chunkId, bufferNodeId);
            if (chunkDataRepresentation.chunkSlices().isEmpty()) {
                return Response.noContent().build();
            }
            return Response.ok(new GenericEntity<>(chunkDataRepresentation, new TypeToken<Chunk.ChunkDataRepresentation>() {}.getType())).build();
        }
        catch (Exception e) {
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/register")
    public Response registerExchange(@PathParam("exchangeId") String exchangeId)
    {
        chunkManager.registerExchange(exchangeId);
        return Response.ok().build();
    }

    @GET
    @Path("{exchangeId}/ping")
    public Response pingExchange(@PathParam("exchangeId") String exchangeId)
    {
        chunkManager.pingExchange(exchangeId);
        return Response.ok().build();
    }

    @GET
    @Path("{exchangeId}/finish")
    public Response finishExchange(@PathParam("exchangeId") String exchangeId)
    {
        try {
            chunkManager.finishExchange(exchangeId);
        }
        catch (Exception e) {
            return errorResponse(e);
        }
        return Response.ok().build();
    }

    @DELETE
    @Path("{exchangeId}")
    public Response removeExchange(@PathParam("exchangeId") String exchangeId)
    {
        chunkManager.removeExchange(exchangeId);
        return Response.ok().build();
    }

    private static Response errorResponse(Exception e)
    {
        if (e instanceof DataServerException dataServerException) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .header(ERROR_CODE_HEADER, dataServerException.getErrorCode())
                    .entity(e.getMessage())
                    .build();
        }
        return Response.status(Status.INTERNAL_SERVER_ERROR)
                .header(ERROR_CODE_HEADER, ErrorCode.INTERNAL_ERROR)
                .entity(e.getMessage())
                .build();
    }
}
