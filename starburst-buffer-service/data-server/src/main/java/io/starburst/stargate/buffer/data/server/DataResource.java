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

import com.google.common.collect.AbstractIterator;
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
import io.starburst.stargate.buffer.data.memory.SliceLease;

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
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static java.util.Objects.requireNonNull;

@Path("/api/v1/buffer/data")
public class DataResource
{
    private final ChunkManager chunkManager;
    private final MemoryAllocator memoryAllocator;
    private final boolean dropUploadedPages;

    @Inject
    public DataResource(
            ChunkManager chunkManager,
            MemoryAllocator memoryAllocator,
            DataServerConfig config)
    {
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.dropUploadedPages = config.isTestingDropUploadedPages();
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
    public Response addDataPages(
            @PathParam("exchangeId") String exchangeId,
            @PathParam("taskId") int taskId,
            @PathParam("attemptId") int attemptId,
            @PathParam("dataPagesId") long dataPagesId,
            InputStream inputStream)
    {
        requireNonNull(inputStream, "inputStream is null");

        if (dropUploadedPages) {
            return Response.ok().build();
        }

        try {
            SliceInput input = new InputStreamSliceInput(inputStream);
            while (true) {
                Optional<SliceLeasesIterator> pagesIterator = getNextPartitionPages(input);
                if (pagesIterator.isEmpty()) {
                    break;
                }
                chunkManager.addDataPages(exchangeId, pagesIterator.get().getPartitionId(), taskId, attemptId, dataPagesId, pagesIterator.get());
            }
        }
        catch (Exception e) {
            return errorResponse(e);
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

    private Optional<SliceLeasesIterator> getNextPartitionPages(SliceInput sliceInput)
    {
        if (!sliceInput.isReadable()) {
            return Optional.empty();
        }

        int partitionId = sliceInput.readInt();
        int length = sliceInput.readInt();
        checkArgument(length > 0, "length should be greater than 0; got %s", length);
        // todo check if length <= HTTP content length
        return Optional.of(new SliceLeasesIterator(partitionId, memoryAllocator, sliceInput, length));
    }

    private static class SliceLeasesIterator
            extends AbstractIterator<SliceLease>
    {
        private final int partitionId;
        private final MemoryAllocator memoryAllocator;
        private final SliceInput sliceInput;
        private int remainingBytes;

        public SliceLeasesIterator(int partitionId, MemoryAllocator memoryAllocator, SliceInput sliceInput, int length)
        {
            this.partitionId = partitionId;
            this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
            this.sliceInput = sliceInput;
            this.remainingBytes = length;
        }

        @Override
        protected SliceLease computeNext()
        {
            if (remainingBytes == 0 || !sliceInput.isReadable()) {
                checkArgument(sliceInput.isReadable() || remainingBytes == 0, "no more data in input stream but remaining bytes counter > 0 (%s)", remainingBytes);
                return endOfData();
            }

            checkState(remainingBytes >= 4, "expected at least 4 bytes remaining; got %s", remainingBytes);
            int length = sliceInput.readInt();
            remainingBytes -= 4;

            checkArgument(length >= 0, "length should be greater than 0; got %s", length);
            checkArgument(remainingBytes >= length, "page length is %s but have only %s bytes remaining", length, remainingBytes);
            Slice page = memoryAllocator.allocate(length)
                    .orElseThrow(() -> new IllegalStateException("Unable to allocate %d bytes".formatted(length)));
            sliceInput.readBytes(page);
            remainingBytes -= length;
            return new SliceLease(memoryAllocator, page);
        }

        public int getPartitionId()
        {
            return partitionId;
        }
    }
}
