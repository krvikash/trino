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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.XxHash64;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import io.starburst.stargate.buffer.data.execution.ChunkDataResult;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.SliceLease;

import javax.annotation.Nullable;
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
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.ConnectionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.InputStream;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.starburst.stargate.buffer.data.client.ErrorCode.DRAINING;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_LOCATION_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_SIZE_HEADER;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Path("/api/v1/buffer/data")
public class DataResource
{
    private static final Logger logger = Logger.get(DataResource.class);

    private final long bufferNodeId;
    private final ChunkManager chunkManager;
    private final MemoryAllocator memoryAllocator;
    private final BufferNodeStateManager bufferNodeStateManager;
    private final boolean dataIntegrityVerificationEnabled;
    private final boolean dropUploadedPages;
    private final Executor responseExecutor;
    private final ExecutorService executor;
    private final CounterStat writtenDataSize;
    private final DistributionStat writtenDataSizeDistribution;
    private final DistributionStat writtenDataSizePerPartitionDistribution;
    private final CounterStat readDataSize;
    private final DistributionStat readDataSizeDistribution;
    private final BufferNodeInfoService bufferNodeInfoService;

    private final AtomicInteger inProgressAddDataPagesRequests = new AtomicInteger();

    @Inject
    public DataResource(
            BufferNodeId bufferNodeId,
            ChunkManager chunkManager,
            MemoryAllocator memoryAllocator,
            BufferNodeStateManager bufferNodeStateManager,
            DataServerConfig config,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            DataServerStats stats,
            ExecutorService executor,
            BufferNodeInfoService bufferNodeInfoService)
    {
        this.bufferNodeId = requireNonNull(bufferNodeId, "bufferNodeId is null").getLongValue();
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.bufferNodeStateManager = requireNonNull(bufferNodeStateManager, "bufferNodeStateManager is null");
        this.dataIntegrityVerificationEnabled = config.isDataIntegrityVerificationEnabled();
        this.dropUploadedPages = config.isTestingDropUploadedPages();
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.bufferNodeInfoService = requireNonNull(bufferNodeInfoService, "bufferNodeInfoService is null");

        writtenDataSize = stats.getWrittenDataSize();
        writtenDataSizeDistribution = stats.getWrittenDataSizeDistribution();
        writtenDataSizePerPartitionDistribution = stats.getWrittenDataSizePerPartitionDistribution();
        readDataSize = stats.getReadDataSize();
        readDataSizeDistribution = stats.getReadDataSizeDistribution();
    }

    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getInfo(@QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            return Response.ok().entity(bufferNodeInfoService.getNodeInfo()).build();
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /info");
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/closedChunks")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listClosedChunks(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("pagingId") Long pagingId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            ChunkList chunkList = chunkManager.listClosedChunks(exchangeId, pagingId == null ? OptionalLong.empty() : OptionalLong.of(pagingId));
            return Response.ok().entity(chunkList).build();
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /%s/closedChunks?pagingId=%s", exchangeId, pagingId);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/markAllClosedChunksReceived")
    public Response markAllClosedChunksReceived(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.markAllClosedChunksReceived(exchangeId);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /%s/markAllClosedChunksReceived", exchangeId);
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
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(CONTENT_LENGTH) Integer contentLength,
            @Suspended AsyncResponse asyncResponse,
            InputStream inputStream)
    {
        requireNonNull(inputStream, "inputStream is null");
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
        }
        catch (RuntimeException e) {
            asyncResponse.resume(errorResponse(e));
        }

        if (dropUploadedPages) {
            asyncResponse.resume(Response.ok().build());
            return;
        }

        inProgressAddDataPagesRequests.incrementAndGet();
        if (bufferNodeStateManager.isDrainingStarted()) {
            inProgressAddDataPagesRequests.decrementAndGet();
            asyncResponse.resume(errorResponse(new DataServerException(DRAINING, "Node %d is draining and not accepting any more data".formatted(bufferNodeId))));
            return;
        }

        SliceLease sliceLease = new SliceLease(memoryAllocator, contentLength);
        ListenableFuture<Void> addDataPagesFuture = Futures.transformAsync(
                sliceLease.getSliceFuture(),
                slice -> {
                    SliceOutput sliceOutput = slice.getOutput();
                    sliceOutput.writeBytes(inputStream, contentLength);

                    ImmutableList.Builder<ListenableFuture<Void>> addDataPagesFutures = ImmutableList.builder();
                    SliceInput sliceInput = slice.getInput();
                    long readChecksum = sliceInput.readLong();
                    XxHash64 hash = new XxHash64();
                    while (sliceInput.isReadable()) {
                        int partitionId = sliceInput.readInt();
                        int bytes = sliceInput.readInt();
                        writtenDataSizePerPartitionDistribution.add(bytes);
                        ImmutableList.Builder<Slice> pages = ImmutableList.builder();
                        while (bytes > 0 && sliceInput.isReadable()) {
                            int pageLength = sliceInput.readInt();
                            bytes -= Integer.BYTES;
                            Slice page = sliceInput.readSlice(pageLength);
                            if (dataIntegrityVerificationEnabled) {
                                hash = hash.update(page);
                            }
                            pages.add(page);
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
                    if (dataIntegrityVerificationEnabled) {
                        long calculatedChecksum = hash.hash();
                        if (calculatedChecksum == NO_CHECKSUM) {
                            calculatedChecksum++;
                        }
                        if (readChecksum != calculatedChecksum) {
                            throw new DataServerException(USER_ERROR, format("Data corruption, read checksum: 0x%08x, calculated checksum: 0x%08x", readChecksum, calculatedChecksum));
                        }
                    }
                    else {
                        if (readChecksum != NO_CHECKSUM) {
                            throw new DataServerException(USER_ERROR, format("Expected checksum to be NO_CHECKSUM (0x%08x) but is 0x%08x", NO_CHECKSUM, readChecksum));
                        }
                    }

                    writtenDataSize.update(contentLength);
                    writtenDataSizeDistribution.add(contentLength);
                    return asVoid(Futures.allAsList(addDataPagesFutures.build()));
                },
                executor);

        // callbacks must be registered before bindAsyncResponse is called; otherwise callback may be not called
        // if request is completed quickly
        AtomicBoolean completionFlag = new AtomicBoolean(); // guard in case both callback would trigger (not sure if possible)
        asyncResponse.register((CompletionCallback) throwable -> {
            if (throwable != null) {
                logger.warn(throwable, "Unmapped throwable when processing POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            }
            if (completionFlag.getAndSet(true)) {
                return;
            }
            sliceLease.release();
            inProgressAddDataPagesRequests.decrementAndGet();
        });

        asyncResponse.register((ConnectionCallback) response -> {
            logger.warn("Client disconnected when processing POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            if (completionFlag.getAndSet(true)) {
                return;
            }
            sliceLease.release();
            inProgressAddDataPagesRequests.decrementAndGet();
        });

        bindAsyncResponse(
                asyncResponse,
                logAndTranslateExceptions(
                        Futures.transform(
                                addDataPagesFuture,
                                ignored -> Response.ok().build(),
                                directExecutor()),
                        () -> "POST /%s/addDataPages/%s/%s/%s".formatted(exchangeId, taskId, attemptId, dataPagesId)),
                responseExecutor);
    }

    @GET
    @Path("{bufferNodeId}/{exchangeId}/pages/{partitionId}/{chunkId}")
    @Produces(TRINO_CHUNK_DATA)
    public Response getChunkData(
            @PathParam("bufferNodeId") long bufferNodeId,
            @PathParam("exchangeId") String exchangeId,
            @PathParam("partitionId") int partitionId,
            @PathParam("chunkId") long chunkId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            ChunkDataResult chunkDataResult = chunkManager.getChunkData(bufferNodeId, exchangeId, partitionId, chunkId);
            if (chunkDataResult.chunkDataHolder().isPresent()) {
                ChunkDataHolder chunkDataHolder = chunkDataResult.chunkDataHolder().get();
                int serializedSizeInBytes = chunkDataHolder.serializedSizeInBytes(); // not strictly accurate but good enough for stats
                readDataSize.update(serializedSizeInBytes);
                readDataSizeDistribution.add(serializedSizeInBytes);
                return Response.ok(new GenericEntity<>(chunkDataResult.chunkDataHolder().get(), new TypeToken<ChunkDataHolder>() {}.getType())).build();
            }
            else {
                verify(chunkDataResult.spoolingFile().isPresent(), "Either chunkDataHolder or spoolingFile should be present");
                SpoolingFile spoolingFile = chunkDataResult.spoolingFile().get();
                return Response.status(Status.NOT_FOUND)
                        .header(SPOOLING_FILE_LOCATION_HEADER, spoolingFile.location())
                        .header(SPOOLING_FILE_SIZE_HEADER, String.valueOf(spoolingFile.length()))
                        .build();
            }
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /%s/pages/%s/%s/%s", exchangeId, partitionId, chunkId, bufferNodeId);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/register")
    public Response registerExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.registerExchange(exchangeId);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /%s/register", exchangeId);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/ping")
    public Response pingExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.pingExchange(exchangeId);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /%s/ping", exchangeId);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/finish")
    public Response finishExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.finishExchange(exchangeId);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /%s/finish", exchangeId);
            return errorResponse(e);
        }
    }

    @DELETE
    @Path("{exchangeId}")
    public Response removeExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.removeExchange(exchangeId);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on DELETE /%s", exchangeId);
            return errorResponse(e);
        }
    }

    public int getInProgressAddDataPagesRequests()
    {
        return inProgressAddDataPagesRequests.get();
    }

    private void checkTargetBufferNodeId(@Nullable Long targetBufferNodeId)
    {
        if (targetBufferNodeId == null) {
            return;
        }
        if (bufferNodeId != targetBufferNodeId) {
            throw new DataServerException(USER_ERROR, "target buffer node mismatch (%s vs %s)".formatted(targetBufferNodeId, bufferNodeId));
        }
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

    private static ListenableFuture<Response> logAndTranslateExceptions(ListenableFuture<Response> listenableFuture, Supplier<String> loggingContext)
    {
        return Futures.catchingAsync(listenableFuture, Exception.class, e -> {
            logger.warn(e, "error on %s", loggingContext.get());
            return immediateFuture(errorResponse(e));
        }, directExecutor());
    }
}
