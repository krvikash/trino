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
import com.google.common.collect.ImmutableMap;
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
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.AddDataPagesResult;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
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
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.DataClientHeaders.MAX_WAIT;
import static io.starburst.stargate.buffer.data.client.ErrorCode.DRAINING;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.ErrorCode.OVERLOADED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.RATE_LIMIT_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_LOCATION_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_SIZE_HEADER;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static io.starburst.stargate.buffer.data.execution.ChunkDataLease.CHUNK_SLICES_METADATA_SIZE;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
    private final AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator;
    private final int maxInProgressAddDataPagesRequests;

    // tracks addDataPages requests for which HTTP response may have already been returned (e.g. due to timeout) but we still need to finish processing incoming data
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
            BufferNodeInfoService bufferNodeInfoService,
            AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator)
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
        this.addDataPagesThrottlingCalculator = requireNonNull(addDataPagesThrottlingCalculator, "addDataPagesThrottlingCalculator is null");
        this.maxInProgressAddDataPagesRequests = config.getMaxInProgressAddDataPagesRequests();

        this.stats = requireNonNull(stats, "stats is null");
        this.writtenDataSize = stats.getWrittenDataSize();
        this.writtenDataSizeDistribution = stats.getWrittenDataSizeDistribution();
        this.writtenDataSizePerPartitionDistribution = stats.getWrittenDataSizePerPartitionDistribution();
        this.readDataSize = stats.getReadDataSize();
        this.readDataSizeDistribution = stats.getReadDataSizeDistribution();
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
            throws IOException
    {
        long start = System.currentTimeMillis();
        AsyncContext asyncContext = request.getAsyncContext();
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            completeServletResponse(asyncContext, Optional.of(e));
            return;
        }

        if (dropUploadedPages) {
            completeServletResponse(asyncContext, Optional.empty());
            return;
        }

        int currentInProgressAddDataPagesRequests = incrementInProgressAddDataPagesRequests();
        if (bufferNodeStateManager.isDrainingStarted()) {
            decrementInProgressAddDataPagesRequests();
            logger.info("rejecting POST /%s/addDataPages/%s/%s/%s; node already DRAINING", exchangeId, taskId, attemptId, dataPagesId);
            completeServletResponse(asyncContext, Optional.of(new DataServerException(DRAINING, "Node %d is draining and not accepting any more data".formatted(bufferNodeId))));
            return;
        }

        if (currentInProgressAddDataPagesRequests > maxInProgressAddDataPagesRequests) {
            decrementInProgressAddDataPagesRequests();
            addDataPagesThrottlingCalculator.recordThrottlingEvent();
            logger.warn("rejecting POST /%s/addDataPages/%s/%s/%s; exceeded maximum in progress addDataPages requests (%s > %s)",
                    exchangeId, taskId, attemptId, dataPagesId, currentInProgressAddDataPagesRequests, maxInProgressAddDataPagesRequests);
            completeServletResponse(
                    asyncContext,
                    Optional.of(new DataServerException(OVERLOADED, "Exceeded maximum in progress addDataPages requests (%s)".formatted(maxInProgressAddDataPagesRequests))));
            return;
        }
        asyncResponse.setTimeout(getAsyncTimeout(clientMaxWait).toMillis(), MILLISECONDS);

        ServletInputStream inputStream;
        try {
            inputStream = asyncContext.getRequest().getInputStream();
        }
        catch (IOException e) {
            try {
                logger.warn(e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                completeServletResponse(asyncContext, Optional.of(e));
                return;
            }
            finally {
                decrementInProgressAddDataPagesRequests();
            }
        }

        SliceLease sliceLease = new SliceLease(memoryAllocator, contentLength);
        try {
            // callbacks must be registered before bindAsyncResponse is called; otherwise callback may be not called
            // if request is completed quickly
            AtomicBoolean servingCompletionFlag = new AtomicBoolean(); // guard in case both callback would trigger (not sure if possible)
            asyncResponse.register((CompletionCallback) throwable -> {
                if (throwable != null) {
                    logger.warn(throwable, "Unmapped throwable when processing POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                }
                if (servingCompletionFlag.getAndSet(true)) {
                    return;
                }
                // try to cancel opportunistically to prevent from `Futures.addCallback` running if possible
                sliceLease.cancel();
            });

            asyncResponse.register((ConnectionCallback) response -> {
                logger.warn("Client disconnected when processing POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                if (servingCompletionFlag.getAndSet(true)) {
                    return;
                }
                // try to cancel opportunistically to prevent from `Futures.addCallback` running if possible
                sliceLease.cancel();
            });
        }
        catch (Exception e) {
            // Unexpected exception; catch just to handle decrementing of inProgress response counter
            // We also immediately release sliceLease. This is ok as we know underlying slice is not yet used by any background processes.
            try {
                sliceLease.release();
            }
            finally {
                decrementInProgressAddDataPagesRequests();
            }
            throw e;
        }

        AtomicReference<ReleasableReadListener> releasableReadListenerWrapper = new AtomicReference<>();
        AtomicBoolean inProgressCompletionFlag = new AtomicBoolean();
        int x = 0;
        Futures.addCallback(
                sliceLease.getSliceFuture(),
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(Slice slice)
                    {
                        ReadListener readListener = new ReadListener()
                        {
                            private final List<ListenableFuture<Void>> addDataPagesFutures = new ArrayList<>();

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

                                SliceInput sliceInput = slice.getInput();
                                long readChecksum = sliceInput.readLong();
                                XxHash64 hash = new XxHash64();
                                boolean shouldRetainMemory = false;
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
                                    AddDataPagesResult addDataPagesResult = chunkManager.addDataPages(
                                            exchangeId,
                                            partitionId,
                                            taskId,
                                            attemptId,
                                            dataPagesId,
                                            pages.build());
                                    addDataPagesFutures.add(addDataPagesResult.addDataPagesFuture());
                                    shouldRetainMemory = shouldRetainMemory || addDataPagesResult.shouldRetainMemory();
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

                                if (shouldRetainMemory) {
                                    // only release memory when all addDataPagesFutures complete
                                    finalizeAddDataPagesRequest(addDataPagesFutures, sliceLease);
                                }
                                else {
                                    // addDataPagesFutures are all old futures and we can release sliceLease early
                                    finalizeAddDataPagesRequest(emptyList(), sliceLease);
                                }

                                bindAsyncResponse(
                                        asyncResponse,
                                        logAndTranslateExceptions(
                                                Futures.transform(
                                                        nonCancellationPropagating(allAsList(addDataPagesFutures)),
                                                        ignored -> {
                                                            OptionalDouble rateLimit = addDataPagesThrottlingCalculator.getRateLimit(request.getRemoteHost(), inProgressAddDataPagesRequests.get());
                                                            if (rateLimit.isPresent()) {
                                                                return Response.ok()
                                                                        .header(RATE_LIMIT_HEADER, Double.toString(rateLimit.getAsDouble()))
                                                                        .header(AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER, Long.toString(addDataPagesThrottlingCalculator.getAverageProcessTimeInMillis()))
                                                                        .build();
                                                            }
                                                            else {
                                                                return Response.ok().build();
                                                            }
                                                        },
                                                        directExecutor()),
                                                () -> "POST /%s/addDataPages/%s/%s/%s".formatted(exchangeId, taskId, attemptId, dataPagesId)),
                                        responseExecutor);
                            }

                            @Override
                            public void onError(Throwable throwable)
                            {
                                finalizeAddDataPagesRequest(addDataPagesFutures, sliceLease);
                                logger.warn(throwable, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                                asyncResponse.resume(errorResponse(throwable, getRateLimitHeaders(request)));
                            }
                        };

                        // wrap readListener in releasableReadListenerWrapper to allow breaking reference chain
                        releasableReadListenerWrapper.set(new ReleasableReadListener(readListener));
                        inputStream.setReadListener(releasableReadListenerWrapper.get());
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        finalizeAddDataPagesRequest(emptyList(), sliceLease);
                        logger.warn(t, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                        asyncResponse.resume(errorResponse(t, getRateLimitHeaders(request)));
                    }

                    private void finalizeAddDataPagesRequest(List<ListenableFuture<Void>> addDataPagesFutures, SliceLease sliceLease)
                    {
                        Futures.whenAllComplete(addDataPagesFutures).run(() -> {
                            // Only mark request no longer in-progress when all futures complete.
                            // The HTTP request may return to caller earlier if one of the futures
                            // returned by chunkManager.addDataPages() fails.
                            if (!inProgressCompletionFlag.getAndSet(true)) {
                                try {
                                    sliceLease.release();
                                }
                                finally {
                                    addDataPagesThrottlingCalculator.recordProcessTimeInMillis(System.currentTimeMillis() - start);
                                    addDataPagesThrottlingCalculator.updateCounterStat(request.getRemoteHost(), 1);
                                    decrementInProgressAddDataPagesRequests();
                                    addDataPagesFutures.clear(); // clear to dereference and release memory retained by SliceLease

                                    // break reference chain from Jetty's HttpInput (implementation of ServletInputStream) to registered ReadListener.
                                    // For some reason Jetty keeps reference to ReadListener attached to ServletInputStream even after releases is already
                                    // complete. We need to break references chain as ReadListener we use has reference to Slice used for holding request data
                                    // while at this point this memory is no longer accounted for in MemoryAllocator. This was resulting in OOMs
                                    ReleasableReadListener listener = releasableReadListenerWrapper.get();
                                    if (listener != null) {
                                        listener.releaseDelegate();
                                    }
                                }
                            }
                        }, directExecutor());
                    }
                },
                executor);
    }

    private int incrementInProgressAddDataPagesRequests()
    {
        int currentRequestsCount = inProgressAddDataPagesRequests.incrementAndGet();
        stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
        return currentRequestsCount;
    }

    private void decrementInProgressAddDataPagesRequests()
    {
        int currentRequestsCount = inProgressAddDataPagesRequests.decrementAndGet();
        stats.updateInProgressAddDataPagesRequests(currentRequestsCount);
    }

    Duration getAsyncTimeout(@Nullable Duration clientMaxWait)
    {
        if (clientMaxWait == null || clientMaxWait.toMillis() == 0 || clientMaxWait.compareTo(CLIENT_MAX_WAIT_LIMIT) > 0) {
            return CLIENT_MAX_WAIT_LIMIT;
        }
        return succinctDuration(clientMaxWait.toMillis() * 0.95, MILLISECONDS);
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
            if (chunkDataResult.chunkDataLease().isPresent()) {
                ChunkDataLease chunkDataLease = chunkDataResult.chunkDataLease().get();
                int dataSize = chunkDataLease.serializedSizeInBytes() - CHUNK_SLICES_METADATA_SIZE;
                readDataSize.update(dataSize);
                readDataSizeDistribution.add(dataSize);

                AsyncContext asyncContext = request.getAsyncContext();
                asyncContext.setTimeout(getAsyncTimeout(clientMaxWait).toMillis());
                ServletResponse response = asyncContext.getResponse();
                ServletOutputStream outputStream = response.getOutputStream();
                response.setContentType(TRINO_CHUNK_DATA);
                response.setContentLength(chunkDataLease.serializedSizeInBytes());

                Slice metaDataSlice = Slices.allocate(CHUNK_SLICES_METADATA_SIZE);
                SliceOutput sliceOutput = metaDataSlice.getOutput();
                sliceOutput.writeLong(chunkDataLease.checksum());
                sliceOutput.writeInt(chunkDataLease.numDataPages());

                ArrayDeque<Slice> sliceQueue = new ArrayDeque<>(chunkDataLease.chunkSlices().size() + 1);
                sliceQueue.add(metaDataSlice);
                sliceQueue.addAll(chunkDataLease.chunkSlices());

                outputStream.setWriteListener(new WriteListener() {
                    @Override
                    public void onWritePossible()
                            throws IOException
                    {
                        while (outputStream.isReady()) {
                            if (sliceQueue.isEmpty()) {
                                chunkDataLease.release();
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
                        chunkDataLease.release();
                        asyncContext.complete();
                    }
                });
            }
            else {
                verify(chunkDataResult.spoolingFile().isPresent(), "Either chunkDataLease or spoolingFile should be present");
                SpoolingFile spoolingFile = chunkDataResult.spoolingFile().get();
                asyncResponse.resume(Response.status(Status.NOT_FOUND)
                        .header(SPOOLING_FILE_LOCATION_HEADER, spoolingFile.location())
                        .header(SPOOLING_FILE_SIZE_HEADER, String.valueOf(spoolingFile.length()))
                        .build());
            }
        }
        catch (RuntimeException | IOException e) {
            logger.warn(e, "error on GET /%s/pages/%s/%s/%s", exchangeId, partitionId, chunkId, bufferNodeId);
            if (chunkDataResult != null && chunkDataResult.chunkDataLease().isPresent()) {
                chunkDataResult.chunkDataLease().get().release();
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
            // registerExchange call can happen after node already started draining.
            // There is a temptation to just reject request here with DRAINING error code, but it would not be correct.
            // When Trino coordinator calls registerExchange it could be that data was already written by some worker to the exchange
            // and Trino coordinator must go through registration to start polling for data chunks.
            //
            // Consider following flow of events:
            // * Trino worker call addDataPages (it implicitly register exchange in data node)
            // * Data node start draining
            // * Trino coordinator calls registerExchange
            // at this point Trino coordinator must proceed with polling for chunks which would not happen if `registerExchange` returned DRAINING error code.
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
    public void finishExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait,
            @Suspended AsyncResponse asyncResponse)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            bindAsyncResponse(
                    asyncResponse,
                    logAndTranslateExceptions(
                            Futures.transform(
                                    chunkManager.finishExchange(exchangeId),
                                    ignored -> Response.ok().build(),
                                    directExecutor()),
                            () -> "GET /%s/finish".formatted(exchangeId)),
                    responseExecutor)
                    .withTimeout(getAsyncTimeout(clientMaxWait));
        }
        catch (RuntimeException e) {
            logger.warn(e, "error on GET /%s/finish", exchangeId);
            asyncResponse.resume(errorResponse(e));
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

    private Map<String, String> getRateLimitHeaders(ServletRequest request)
    {
        OptionalDouble rateLimit = addDataPagesThrottlingCalculator.getRateLimit(request.getRemoteHost(), inProgressAddDataPagesRequests.get());
        if (rateLimit.isPresent()) {
            return ImmutableMap.of(
                    RATE_LIMIT_HEADER, Double.toString(rateLimit.getAsDouble()),
                    AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER, Long.toString(addDataPagesThrottlingCalculator.getAverageProcessTimeInMillis()));
        }
        else {
            return ImmutableMap.of();
        }
    }

    // this function is needed to immediately return http response without consuming request payload
    // for more information, see https://github.com/starburstdata/trino-buffer-service/issues/269
    private void completeServletResponse(AsyncContext asyncContext, Optional<Throwable> throwable)
            throws IOException
    {
        HttpServletResponse servletResponse = (HttpServletResponse) asyncContext.getResponse();
        servletResponse.setContentType("text/plain");
        getRateLimitHeaders(asyncContext.getRequest()).forEach(servletResponse::setHeader);

        if (throwable.isPresent()) {
            servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            servletResponse.getWriter().write(throwable.get().getMessage());
            if (throwable.get() instanceof DataServerException dataServerException) {
                servletResponse.setHeader(ERROR_CODE_HEADER, dataServerException.getErrorCode().toString());
            }
            else {
                servletResponse.setHeader(ERROR_CODE_HEADER, INTERNAL_ERROR.toString());
            }
        }
        else {
            servletResponse.setStatus(HttpServletResponse.SC_OK);
        }

        asyncContext.complete();
    }

    private static Response errorResponse(Throwable throwable)
    {
        return errorResponse(throwable, ImmutableMap.of());
    }

    private static Response errorResponse(Throwable throwable, Map<String, String> headers)
    {
        Response.ResponseBuilder responseBuilder;
        if (throwable instanceof DataServerException dataServerException) {
            responseBuilder = Response.status(Status.INTERNAL_SERVER_ERROR)
                    .header(ERROR_CODE_HEADER, dataServerException.getErrorCode())
                    .entity(throwable.getMessage());
        }
        else {
            responseBuilder = Response.status(Status.INTERNAL_SERVER_ERROR)
                    .header(ERROR_CODE_HEADER, INTERNAL_ERROR)
                    .entity(throwable.getMessage());
        }

        headers.forEach(responseBuilder::header);
        return responseBuilder.build();
    }

    private static ListenableFuture<Response> logAndTranslateExceptions(ListenableFuture<Response> listenableFuture, Supplier<String> loggingContext)
    {
        return Futures.catching(listenableFuture, Exception.class, e -> {
            logger.warn(e, "error on %s", loggingContext.get());
            return errorResponse(e);
        }, directExecutor());
    }

    private static class ReleasableReadListener
            implements ReadListener
    {
        private enum State {
            DELEGATE_SET,
            DELEGATE_RELEASED
        }

        private volatile ReadListener delegate;
        private final AtomicReference<State> state;

        public ReleasableReadListener(ReadListener delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.state = new AtomicReference<>(State.DELEGATE_SET);
        }

        private ReadListener getDelegate()
        {
            checkState(state.get() == State.DELEGATE_SET, "Delegate already released");
            ReadListener readListener = delegate;
            verify(readListener != null);
            return readListener;
        }

        public void releaseDelegate()
        {
            checkState(state.compareAndSet(State.DELEGATE_SET, State.DELEGATE_RELEASED), "Cannot set delegate; current state is %s", state.get());
            delegate = null;
        }

        @Override
        public void onDataAvailable()
                throws IOException
        {
            getDelegate().onDataAvailable();
        }

        @Override
        public void onAllDataRead()
                throws IOException
        {
            getDelegate().onAllDataRead();
        }

        @Override
        public void onError(Throwable t)
        {
            getDelegate().onError(t);
        }
    }
}
