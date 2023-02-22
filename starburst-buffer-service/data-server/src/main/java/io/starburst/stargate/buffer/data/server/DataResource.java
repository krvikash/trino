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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.Duration;
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
import javax.servlet.AsyncContext;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.DataClientHeaders.MAX_WAIT;
import static io.starburst.stargate.buffer.data.client.ErrorCode.DRAINING;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SERIALIZED_CHUNK_DATA_MAGIC;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_LOCATION_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_SIZE_HEADER;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static io.starburst.stargate.buffer.data.execution.ChunkDataHolder.CHUNK_SLICES_METADATA_SIZE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Path("/api/v1/buffer/data")
public class DataResource
{
    private static final Logger logger = Logger.get(DataResource.class);

    private static final Duration CLIENT_MAX_WAIT_LIMIT = succinctDuration(60, TimeUnit.SECONDS);

    private final long bufferNodeId;
    private final ChunkManager chunkManager;
    private final MemoryAllocator memoryAllocator;
    private final BufferNodeStateManager bufferNodeStateManager;
    private final boolean dataIntegrityVerificationEnabled;
    private final boolean dropUploadedPages;
    private final Executor responseExecutor;
    private final ExecutorService executor;
    private final DataServerStats stats;
    private final CounterStat writtenDataSize;
    private final DistributionStat writtenDataSizeDistribution;
    private final DistributionStat writtenDataSizePerPartitionDistribution;
    private final CounterStat readDataSize;
    private final DistributionStat readDataSizeDistribution;
    private final BufferNodeInfoService bufferNodeInfoService;
    private final int maxInProgressAddDataPagesRequests;

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
        this.maxInProgressAddDataPagesRequests = config.getMaxInProgressAddDataPagesRequests();

        this.stats = requireNonNull(stats, "stats is null");
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
    public void listClosedChunks(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("pagingId") Long pagingId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait,
            @Suspended AsyncResponse asyncResponse)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
        }
        catch (RuntimeException e) {
            asyncResponse.resume(errorResponse(e));
            return;
        }

        ListenableFuture<ChunkList> chunkListFuture = chunkManager.listClosedChunks(
                exchangeId,
                pagingId == null ? OptionalLong.empty() : OptionalLong.of(pagingId));
        bindAsyncResponse(
                asyncResponse,
                logAndTranslateExceptions(
                        Futures.transform(chunkListFuture, chunkList -> Response.ok().entity(chunkList).build(), directExecutor()),
                        () -> "GET /%s/closedChunks?pagingId=%s".formatted(exchangeId, pagingId)),
                responseExecutor)
                .withTimeout(getAsyncTimeout(clientMaxWait));
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
            @Context HttpServletRequest request,
            @PathParam("exchangeId") String exchangeId,
            @PathParam("taskId") int taskId,
            @PathParam("attemptId") int attemptId,
            @PathParam("dataPagesId") long dataPagesId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(CONTENT_LENGTH) Integer contentLength,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait,
            @Suspended AsyncResponse asyncResponse)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
        }
        catch (RuntimeException e) {
            asyncResponse.resume(errorResponse(e));
            return;
        }

        if (dropUploadedPages) {
            asyncResponse.resume(Response.ok().build());
            return;
        }

        int currentInProgressAddDataPagesRequests = incrementAddDataPagesRequests();
        if (bufferNodeStateManager.isDrainingStarted()) {
            decrementAddDataPagesRequests();
            asyncResponse.resume(errorResponse(new DataServerException(DRAINING, "Node %d is draining and not accepting any more data".formatted(bufferNodeId))));
            return;
        }

        if (currentInProgressAddDataPagesRequests > maxInProgressAddDataPagesRequests) {
            decrementAddDataPagesRequests();
            asyncResponse.resume(errorResponse(
                    new DataServerException(INTERNAL_ERROR, "Exceeded maximum in progress addDataPages requests (%s)".formatted(maxInProgressAddDataPagesRequests))));
            return;
        }

        asyncResponse.setTimeout(getAsyncTimeout(clientMaxWait).toMillis(), TimeUnit.MILLISECONDS);

        AsyncContext asyncContext = request.getAsyncContext();
        ServletInputStream inputStream;
        try {
            inputStream = asyncContext.getRequest().getInputStream();
        }
        catch (IOException e) {
            try {
                logger.warn(e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                asyncResponse.resume(errorResponse(e));
                return;
            }
            finally {
                decrementAddDataPagesRequests();
            }
        }

        SliceLease sliceLease = new SliceLease(memoryAllocator, contentLength);

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
            try {
                sliceLease.release(false);
            }
            finally {
                decrementAddDataPagesRequests();
            }
        });

        asyncResponse.register((ConnectionCallback) response -> {
            logger.warn("Client disconnected when processing POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            if (completionFlag.getAndSet(true)) {
                return;
            }
            try {
                sliceLease.release(false);
            }
            finally {
                decrementAddDataPagesRequests();
            }
        });

        Futures.addCallback(
                sliceLease.getSliceFuture(),
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(Slice slice)
                    {
                        inputStream.setReadListener(new ReadListener() {
                            private int bytesRead;

                            @Override
                            public void onDataAvailable()
                                    throws IOException
                            {
                                while (inputStream.isReady()) {
                                    int n = inputStream.read(slice.byteArray(), slice.byteArrayOffset() + bytesRead, contentLength - bytesRead);
                                    if (n == -1) {
                                        break;
                                    }
                                    bytesRead += n;
                                }
                            }

                            @Override
                            public void onAllDataRead()
                            {
                                verify(bytesRead == contentLength,
                                        "Actual number of bytes read %s not equal to contentLength %s", bytesRead, contentLength);

                                executor.submit(() -> {
                                    try {
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

                                        ListenableFuture<Void> addDataPagesFuture = asVoid(Futures.allAsList(addDataPagesFutures.build()));
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
                                    catch (Throwable throwable) {
                                        this.onError(throwable);
                                    }
                                });
                            }

                            @Override
                            public void onError(Throwable throwable)
                            {
                                logger.warn(throwable, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                                asyncResponse.resume(errorResponse(throwable));
                            }
                        });
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        logger.warn(t, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                        asyncResponse.resume(errorResponse(t));
                    }
                },
                executor);
    }

    private int incrementAddDataPagesRequests()
    {
        int currentRequestsCount = inProgressAddDataPagesRequests.incrementAndGet();
        stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
        return currentRequestsCount;
    }

    private void decrementAddDataPagesRequests()
    {
        int currentRequestsCount = inProgressAddDataPagesRequests.decrementAndGet();
        stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
    }

    Duration getAsyncTimeout(@Nullable Duration clientMaxWait)
    {
        if (clientMaxWait == null || clientMaxWait.toMillis() == 0 || clientMaxWait.compareTo(CLIENT_MAX_WAIT_LIMIT) > 0) {
            return CLIENT_MAX_WAIT_LIMIT;
        }
        return succinctDuration(clientMaxWait.toMillis() * 0.95, TimeUnit.MILLISECONDS);
    }

    @GET
    @Path("{bufferNodeId}/{exchangeId}/pages/{partitionId}/{chunkId}")
    public void getChunkData(
            @Context HttpServletRequest request,
            @PathParam("bufferNodeId") long bufferNodeId,
            @PathParam("exchangeId") String exchangeId,
            @PathParam("partitionId") int partitionId,
            @PathParam("chunkId") long chunkId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait,
            @Suspended AsyncResponse asyncResponse)
    {
        ChunkDataResult chunkDataResult = null;
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkDataResult = chunkManager.getChunkData(bufferNodeId, exchangeId, partitionId, chunkId);
            if (chunkDataResult.chunkDataHolder().isPresent()) {
                ChunkDataHolder chunkDataHolder = chunkDataResult.chunkDataHolder().get();
                int dataSize = chunkDataHolder.serializedSizeInBytes() - CHUNK_SLICES_METADATA_SIZE;
                readDataSize.update(dataSize);
                readDataSizeDistribution.add(dataSize);

                AsyncContext asyncContext = request.getAsyncContext();
                asyncContext.setTimeout(getAsyncTimeout(clientMaxWait).toMillis());
                ServletResponse response = asyncContext.getResponse();
                ServletOutputStream outputStream = response.getOutputStream();
                response.setContentType(TRINO_CHUNK_DATA);
                response.setContentLength(Integer.BYTES + chunkDataHolder.serializedSizeInBytes());

                Slice metaDataSlice = Slices.allocate(Integer.BYTES + CHUNK_SLICES_METADATA_SIZE);
                SliceOutput sliceOutput = metaDataSlice.getOutput();
                sliceOutput.writeInt(SERIALIZED_CHUNK_DATA_MAGIC);
                sliceOutput.writeLong(chunkDataHolder.checksum());
                sliceOutput.writeInt(chunkDataHolder.numDataPages());

                ArrayDeque<Slice> sliceQueue = new ArrayDeque<>(chunkDataHolder.chunkSlices().size() + 1);
                sliceQueue.add(metaDataSlice);
                sliceQueue.addAll(chunkDataHolder.chunkSlices());

                outputStream.setWriteListener(new WriteListener() {
                    @Override
                    public void onWritePossible()
                            throws IOException
                    {
                        while (outputStream.isReady()) {
                            if (sliceQueue.isEmpty()) {
                                chunkDataHolder.release();
                                asyncContext.complete();
                                return;
                            }

                            Slice slice = sliceQueue.poll();
                            outputStream.write(slice.byteArray(), slice.byteArrayOffset(), slice.length());
                        }
                    }

                    @Override
                    public void onError(Throwable throwable)
                    {
                        logger.warn(throwable, "error on GET /%s/pages/%s/%s/%s", exchangeId, partitionId, chunkId, bufferNodeId);
                        chunkDataHolder.release();
                        asyncContext.complete();
                    }
                });
            }
            else {
                verify(chunkDataResult.spoolingFile().isPresent(), "Either chunkDataHolder or spoolingFile should be present");
                SpoolingFile spoolingFile = chunkDataResult.spoolingFile().get();
                asyncResponse.resume(Response.status(Status.NOT_FOUND)
                        .header(SPOOLING_FILE_LOCATION_HEADER, spoolingFile.location())
                        .header(SPOOLING_FILE_SIZE_HEADER, String.valueOf(spoolingFile.length()))
                        .build());
            }
        }
        catch (RuntimeException | IOException e) {
            logger.warn(e, "error on GET /%s/pages/%s/%s/%s", exchangeId, partitionId, chunkId, bufferNodeId);
            if (chunkDataResult != null && chunkDataResult.chunkDataHolder().isPresent()) {
                chunkDataResult.chunkDataHolder().get().release();
            }
            asyncResponse.resume(errorResponse(e));
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

    private static Response errorResponse(Throwable throwable)
    {
        if (throwable instanceof DataServerException dataServerException) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .header(ERROR_CODE_HEADER, dataServerException.getErrorCode())
                    .entity(throwable.getMessage())
                    .build();
        }
        return Response.status(Status.INTERNAL_SERVER_ERROR)
                .header(ERROR_CODE_HEADER, ErrorCode.INTERNAL_ERROR)
                .entity(throwable.getMessage())
                .build();
    }

    private static ListenableFuture<Response> logAndTranslateExceptions(ListenableFuture<Response> listenableFuture, Supplier<String> loggingContext)
    {
        return Futures.catching(listenableFuture, Exception.class, e -> {
            logger.warn(e, "error on %s", loggingContext.get());
            return errorResponse(e);
        }, directExecutor());
    }
}
