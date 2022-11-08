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

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.SliceLease;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.InputStream;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static java.util.Objects.requireNonNull;

@Path("/api/v1/buffer/data")
public class DataResource
{
    private final ChunkManager chunkManager;
    private final MemoryAllocator memoryAllocator;
    private final boolean dropUploadedPages;
    private final Executor responseExecutor;
    private final ExecutorService chunkWriteExecutor;

    @Inject
    public DataResource(
            ChunkManager chunkManager,
            MemoryAllocator memoryAllocator,
            DataServerConfig config,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            ExecutorService chunkWriteExecutor)
    {
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.dropUploadedPages = config.isTestingDropUploadedPages();
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.chunkWriteExecutor = requireNonNull(chunkWriteExecutor, "chunkWriteExecutor is null");
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
    @Path("{exchangeId}/addDataPages/{taskId}/{attemptId}/{dataPagesId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public void addDataPages(
            @PathParam("exchangeId") String exchangeId,
            @PathParam("taskId") int taskId,
            @PathParam("attemptId") int attemptId,
            @PathParam("dataPagesId") long dataPagesId,
            @HeaderParam(CONTENT_LENGTH) Integer contentLength,
            @Suspended AsyncResponse asyncResponse,
            InputStream inputStream)
    {
        requireNonNull(inputStream, "inputStream is null");

        if (dropUploadedPages) {
            asyncResponse.resume(Response.ok().build());
        }

        SliceLease sliceLease = new SliceLease(memoryAllocator, contentLength);
        ListenableFuture<Void> addDataPagesFuture = Futures.transformAsync(
                sliceLease.getSliceFuture(),
                slice -> {
                    SliceOutput sliceOutput = slice.getOutput();
                    sliceOutput.writeBytes(inputStream, contentLength);

                    ImmutableList.Builder<ListenableFuture<Void>> addDataPagesFutures = ImmutableList.builder();
                    SliceInput sliceInput = slice.getInput();
                    while (sliceInput.isReadable()) {
                        int partitionId = sliceInput.readInt();
                        int bytes = sliceInput.readInt();
                        ImmutableList.Builder<Slice> pages = ImmutableList.builder();
                        while (bytes > 0 && sliceInput.isReadable()) {
                            int pageLength = sliceInput.readInt();
                            bytes -= Integer.BYTES;
                            pages.add(sliceInput.readSlice(pageLength));
                            bytes -= pageLength;
                        }
                        checkState(bytes == 0, "no more data in input stream but remaining bytes counter > 0 (%d)".formatted(bytes));
                        addDataPagesFutures.add(chunkManager.addDataPages(
                                exchangeId,
                                partitionId,
                                taskId,
                                attemptId,
                                dataPagesId,
                                pages.build()));
                    }
                    return asVoid(Futures.allAsList(addDataPagesFutures.build()));
                },
                chunkWriteExecutor);
        addDataPagesFuture.addListener(sliceLease::release, chunkWriteExecutor);
        bindAsyncResponse(
                asyncResponse,
                translateExceptions(Futures.transform(
                        addDataPagesFuture,
                        ignored -> Response.ok().build(),
                        directExecutor())),
                responseExecutor);
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
            ChunkDataHolder chunkData = chunkManager.getChunkData(exchangeId, partitionId, chunkId, bufferNodeId);
            if (chunkData.chunkSlices().isEmpty()) {
                return Response.noContent().build();
            }
            return Response.ok(new GenericEntity<>(chunkData, new TypeToken<ChunkDataHolder>() {}.getType())).build();
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

    public static ListenableFuture<Response> translateExceptions(ListenableFuture<Response> listenableFuture)
    {
        return Futures.catchingAsync(listenableFuture, Exception.class, e -> immediateFuture(errorResponse(e)), directExecutor());
    }
}
