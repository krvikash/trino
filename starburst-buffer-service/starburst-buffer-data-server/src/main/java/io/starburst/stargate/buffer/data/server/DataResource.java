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
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.AddDataPagesResult;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkDataResult;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.SliceLease;
import io.starburst.stargate.buffer.data.server.AddDataPagesInProgressTracker.InProgressLatch;
import jakarta.annotation.Nullable;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ConnectionCallback;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.Futures.withTimeout;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.starburst.stargate.buffer.BufferServiceLimits.validateAttemptId;
import static io.starburst.stargate.buffer.BufferServiceLimits.validateTaskId;
import static io.starburst.stargate.buffer.data.client.DataClientHeaders.MAX_WAIT;
import static io.starburst.stargate.buffer.data.client.ErrorCode.DRAINING;
import static io.starburst.stargate.buffer.data.client.ErrorCode.OVERLOADED;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLED_CHUNK_LENGTH_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLED_CHUNK_OFFSET_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_LOCATION_HEADER;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static io.starburst.stargate.buffer.data.execution.ChunkDataLease.CHUNK_SLICES_METADATA_SIZE;
import static io.starburst.stargate.buffer.data.server.DataRequestHelper.getAsyncTimeout;
import static io.starburst.stargate.buffer.data.server.DataRequestHelper.getClientId;
import static io.starburst.stargate.buffer.data.server.HttpResponseHelper.errorResponse;
import static io.starburst.stargate.buffer.data.server.HttpResponseHelper.okResponse;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Path("/api/v1/buffer/data")
public class DataResource
        extends BaseDataResource
{
    private static final Logger logger = Logger.get(DataResource.class);

    private static final int SKIP_BUFFER_SIZE = 8192;

    private final Executor responseExecutor;
    private final ExecutorService executor;
    private final ScheduledExecutorService timeoutExecutor;

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
            ScheduledExecutorService timeoutExecutor,
            BufferNodeInfoService bufferNodeInfoService,
            AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator,
            AddDataPagesInProgressTracker inProgressTracker,
            JsonCodec<Span> spanJsonCodec)
    {
        super(bufferNodeId,
                chunkManager,
                memoryAllocator,
                bufferNodeStateManager,
                config,
                bufferNodeInfoService,
                addDataPagesThrottlingCalculator,
                inProgressTracker,
                spanJsonCodec,
                stats);
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @GET
    @Path("{exchangeId}/closedChunks")
    @Produces(MediaType.APPLICATION_JSON)
    public void listClosedChunks(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("pagingId") Long pagingId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
        }
        catch (Throwable e) {
            if (!asyncResponse.isDone()) {
                asyncResponse.resume(errorResponse(e));
            }
            return;
        }

        ListenableFuture<ChunkList> chunkListFuture = withTimeout(chunkManager.listClosedChunks(
                exchangeId,
                pagingId == null ? OptionalLong.empty() : OptionalLong.of(pagingId)), getAsyncTimeout(clientMaxWait).toJavaTime(), timeoutExecutor);

        addCallback(chunkListFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(ChunkList result)
            {
                if (!asyncResponse.isDone()) {
                    asyncResponse.resume(Response.ok(result).build());
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                reportException(logger, throwable, "error on %s", "GET /%s/closedChunks?pagingId=%s".formatted(exchangeId, pagingId));
                if (!asyncResponse.isDone()) {
                    asyncResponse.resume(errorResponse(throwable));
                }
            }
        }, responseExecutor);
    }

    @POST
    @Path("{exchangeId}/addDataPages/{taskId}/{attemptId}/{dataPagesId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public void addDataPages(
            @Suspended AsyncResponse asyncResponse,
            @Context HttpServletRequest request,
            @PathParam("exchangeId") String exchangeId,
            @PathParam("taskId") int taskId,
            @PathParam("attemptId") int attemptId,
            @PathParam("dataPagesId") long dataPagesId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(CONTENT_LENGTH) Integer contentLength,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait)
            throws IOException
    {
        String clientId = getClientId(request);
        long asyncTimeout = getAsyncTimeout(clientMaxWait).toMillis();
        long processingStart = System.currentTimeMillis();
        long processingDeadline = processingStart + asyncTimeout;

        ServletInputStream inputStream;
        try {
            inputStream = request.getInputStream();
        }
        catch (IOException e) {
            reportException(logger, e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            completeServletResponse(clientId, asyncResponse, processingStart, Optional.of(e));
            return;
        }

        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            validateTaskId(taskId);
            validateAttemptId(attemptId);
        }
        catch (Throwable e) {
            reportException(logger, e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            consumeRequestAndCompleteAsyncResponse(clientId, asyncResponse, inputStream, processingStart, Optional.of(e));
            return;
        }

        if (dropUploadedPages) {
            consumeRequestAndCompleteAsyncResponse(clientId, asyncResponse, inputStream, processingStart, Optional.empty());
            return;
        }

        InProgressLatch inProgressLatch = inProgressTracker.incrementAndGetLatch();

        try {
            if (bufferNodeStateManager.isDrainingStarted()) {
                inProgressLatch.release();
                logger.debug("rejecting POST /%s/addDataPages/%s/%s/%s; node already DRAINING", exchangeId, taskId, attemptId, dataPagesId);
                consumeRequestAndCompleteAsyncResponse(clientId, asyncResponse, inputStream, processingStart, Optional.of(new DataServerException(DRAINING, "Node %d is draining and not accepting any more data".formatted(bufferNodeId))));
                return;
            }

            if (inProgressLatch.currentRequestsCount() > maxInProgressAddDataPagesRequests) {
                inProgressLatch.release();
                stats.getOverloadedAddDataPagesCount().update(1);
                addDataPagesThrottlingCalculator.recordThrottlingEvent();
                logger.debug("rejecting POST /%s/addDataPages/%s/%s/%s; exceeded maximum in progress addDataPages requests (%s > %s)",
                        exchangeId, taskId, attemptId, dataPagesId, inProgressLatch, maxInProgressAddDataPagesRequests);
                consumeRequestAndCompleteAsyncResponse(
                        clientId,
                        asyncResponse,
                        inputStream,
                        processingStart,
                        Optional.of(new DataServerException(OVERLOADED, "Exceeded maximum in progress addDataPages requests (%s)".formatted(maxInProgressAddDataPagesRequests))));
                return;
            }
        }
        catch (Throwable e) {
            // ensure we are not loosing counter
            inProgressLatch.release();
            throw e;
        }

        SliceLease sliceLease;
        try {
            sliceLease = new SliceLease(memoryAllocator, contentLength);
            timeoutExecutor.schedule(sliceLease::cancel, asyncTimeout, MILLISECONDS);
        }
        catch (Throwable e) {
            inProgressLatch.release();
            throw e;
        }

        try {
            // callbacks must be registered before bindAsyncResponse is called; otherwise callback may be not called
            // if request is completed quickly
            AtomicBoolean servingCompletionFlag = new AtomicBoolean(); // guard in case both callback would trigger (not sure if possible)
            asyncResponse.register((CompletionCallback) throwable -> {
                if (throwable != null) {
                    reportException(logger, throwable, "Unmapped throwable when processing POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                }
                if (servingCompletionFlag.getAndSet(true)) {
                    return;
                }
                // try to cancel opportunistically to prevent FutureCallback.onSuccess() from being executed.
                // Call to `sliceLease.release`, which is still needed to ensure proper memory accounting in MemoryAllocator will still
                // be called via finalizeAddDataPagesRequest called from FutureCallback.onFailure.
                sliceLease.cancel();
            });

            asyncResponse.register((ConnectionCallback) response -> {
                logger.warn("Client disconnected when processing POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                if (servingCompletionFlag.getAndSet(true)) {
                    return;
                }
                // try to cancel opportunistically to prevent FutureCallback.onSuccess() from being executed.
                // Call to `sliceLease.release`, which is still needed to ensure proper memory accounting in MemoryAllocator will still
                // be called via finalizeAddDataPagesRequest called from FutureCallback.onFailure.
                sliceLease.cancel();
            });
        }
        catch (Throwable e) {
            // Unexpected exception; catch just to handle decrementing of inProgress response counter
            // We also immediately release sliceLease. This is ok as we know underlying slice is not yet used by any background processes.
            try {
                sliceLease.release();
            }
            finally {
                inProgressLatch.release();
            }
            throw e;
        }

        Supplier<String> errorPrefix = () -> "error on POST /%s/addDataPages/%s/%s/%s".formatted(exchangeId, taskId, attemptId, dataPagesId);
        try {
            AtomicReference<ReleasableReadListener> releasableReadListenerWrapper = new AtomicReference<>();
            AtomicBoolean inProgressCompletionFlag = new AtomicBoolean();
            addCallback(
                    sliceLease.getSliceFuture(),
                    new FutureCallback<>()
                    {
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
                                    // && !inputStream.isFinished() seems unnecessary but it is still
                                    // added in Jetty 12 examples using ReadListener
                                    // Keeping for now to see if we still get some spurious internal
                                    // race conditions with it.
                                    while (inputStream.isReady() && !inputStream.isFinished()) {
                                        if (processingDeadline < System.currentTimeMillis()) {
                                            // we've exceeded client timeout for consuming an input stream
                                            if (!asyncResponse.isDone()) {
                                                onFailure(new TimeoutException("Exceeded deadline"));
                                            }
                                            break;
                                        }
                                        if (bytesRead < contentLength) {
                                            int readLength = inputStream.read(slice.byteArray(), slice.byteArrayOffset() + bytesRead, contentLength - bytesRead);
                                            if (readLength == -1) {
                                                break;
                                            }
                                            bytesRead += readLength;
                                        }
                                        else {
                                            // we need extra call to read after we read number of bytes denoted by contentLength,
                                            // otherwise inputStream will never signal EOF and `onAllDataRead` will not be called.
                                            int readLength = inputStream.read();
                                            checkState(readLength == -1, "expected EOF but read %s", readLength);
                                            break;
                                        }
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

                                    Map<Integer, List<Slice>> pagesMap = new HashMap<>();
                                    while (sliceInput.isReadable()) {
                                        int partitionId = sliceInput.readInt();
                                        int bytes = sliceInput.readInt();
                                        writtenDataSizePerPartitionDistribution.add(bytes);
                                        ImmutableList.Builder<Slice> pages = ImmutableList.builder();
                                        while (bytes > 0 && sliceInput.isReadable()) {
                                            int pageLength = sliceInput.readInt();

                                            if (pageLength > chunkManager.getMaxPageLength()) {
                                                resumeWithError(errorPrefix.get(), format("Data page too large (%d > %d)".formatted(pageLength, chunkManager.getMaxPageLength())), USER_ERROR);
                                                return;
                                            }

                                            bytes -= Integer.BYTES;
                                            Slice page = sliceInput.readSlice(pageLength);
                                            if (dataIntegrityVerificationEnabled) {
                                                hash = hash.update(page);
                                            }
                                            pages.add(page);
                                            bytes -= pageLength;
                                        }

                                        if (bytes != 0) {
                                            resumeWithError(errorPrefix.get(), format("Data corruption, no more data in input stream but remaining bytes counter > 0 (%d)".formatted(bytes)), USER_ERROR);
                                            return;
                                        }
                                        // do not call chunkManager.addDataPages(exchangeId, partitionId, ...)
                                        // just yet so we verify checksums for whole request first
                                        pagesMap.put(partitionId, pages.build());
                                    }

                                    writtenDataSize.update(contentLength);
                                    writtenDataSizeDistribution.add(contentLength);

                                    if (dataIntegrityVerificationEnabled) {
                                        long calculatedChecksum = hash.hash();
                                        if (calculatedChecksum == NO_CHECKSUM) {
                                            calculatedChecksum++;
                                        }
                                        if (readChecksum != calculatedChecksum) {
                                            resumeWithError(errorPrefix.get(), format("Data corruption, read checksum: 0x%08x, calculated checksum: 0x%08x", readChecksum, calculatedChecksum), USER_ERROR);
                                            return;
                                        }
                                    }
                                    else if (readChecksum != NO_CHECKSUM) {
                                        resumeWithError(errorPrefix.get(), format("Expected checksum to be NO_CHECKSUM (0x%08x) but is 0x%08x", NO_CHECKSUM, readChecksum), USER_ERROR);
                                        return;
                                    }
                                    try {
                                        for (Map.Entry<Integer, List<Slice>> entry : pagesMap.entrySet()) {
                                            Integer partitionId = entry.getKey();
                                            List<Slice> pages = entry.getValue();
                                            AddDataPagesResult addDataPagesResult = chunkManager.addDataPages(
                                                    exchangeId,
                                                    partitionId,
                                                    taskId,
                                                    attemptId,
                                                    dataPagesId,
                                                    pages);
                                            addDataPagesFutures.add(addDataPagesResult.addDataPagesFuture());
                                            shouldRetainMemory = shouldRetainMemory || addDataPagesResult.shouldRetainMemory();
                                        }
                                    }
                                    catch (DataApiException e) {
                                        resumeWithError(errorPrefix.get(), e);
                                        return;
                                    }

                                    if (shouldRetainMemory) {
                                        // only release memory when all addDataPagesFutures complete
                                        finalizeAddDataPagesRequest(addDataPagesFutures, sliceLease);
                                    }
                                    else {
                                        // addDataPagesFutures are all old futures and we can release sliceLease early
                                        finalizeAddDataPagesRequest(emptyList(), sliceLease);
                                    }

                                    // complete http response if not completed yet via timeout
                                    addCallback(nonCancellationPropagating(allAsList(addDataPagesFutures)), new FutureCallback<>()
                                    {
                                        @Override
                                        public void onSuccess(List<Void> value)
                                        {
                                            Response response = okResponse(getRateLimitHeaders(clientId));

                                            if (!asyncResponse.isDone()) {
                                                asyncResponse.resume(response);
                                            }
                                        }

                                        @Override
                                        public void onFailure(Throwable throwable)
                                        {
                                            reportException(logger, throwable, "%s", errorPrefix.get());
                                            if (!asyncResponse.isDone()) {
                                                asyncResponse.resume(errorResponse(throwable));
                                            }
                                        }
                                    }, responseExecutor);
                                }

                                private void resumeWithError(String prefix, String message, ErrorCode errorCode)
                                {
                                    try {
                                        logger.warn("%s; %s; %s", prefix, errorCode, message);
                                        asyncResponse.resume(errorResponse(errorCode, message, getRateLimitHeaders(clientId)));
                                    }
                                    finally {
                                        finalizeAddDataPagesRequest(addDataPagesFutures, sliceLease);
                                    }
                                }

                                private void resumeWithError(String prefix, Throwable exception)
                                {
                                    try {
                                        reportException(logger, exception, "%s", prefix);
                                        asyncResponse.resume(errorResponse(exception, getRateLimitHeaders(clientId)));
                                    }
                                    finally {
                                        finalizeAddDataPagesRequest(addDataPagesFutures, sliceLease);
                                    }
                                }

                                @Override
                                public void onError(Throwable throwable)
                                {
                                    resumeWithError(errorPrefix.get(), throwable);
                                }
                            };

                            // wrap readListener in releasableReadListenerWrapper to allow breaking reference chain
                            releasableReadListenerWrapper.set(new ReleasableReadListener(readListener));
                            inputStream.setReadListener(releasableReadListenerWrapper.get());
                        }

                        @Override
                        public void onFailure(Throwable throwable)
                        {
                            try {
                                reportException(logger, throwable, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                                if (!asyncResponse.isDone()) {
                                    asyncResponse.resume(errorResponse(throwable, getRateLimitHeaders(clientId)));
                                }
                            }
                            finally {
                                finalizeAddDataPagesRequest(emptyList(), sliceLease);
                            }
                        }

                        private void finalizeAddDataPagesRequest(List<ListenableFuture<Void>> addDataPagesFutures, SliceLease sliceLease)
                        {
                            ListenableFuture<?> future = Futures.whenAllComplete(addDataPagesFutures).run(() -> {
                                // Only mark request no longer in-progress when all futures complete.
                                // The HTTP request may return to caller earlier if one of the futures
                                // returned by chunkManager.addDataPages() fails.
                                if (!inProgressCompletionFlag.getAndSet(true)) {
                                    try {
                                        sliceLease.release();
                                    }
                                    finally {
                                        inProgressLatch.release();
                                        recordAddDataPagesRequest(processingStart, clientId);

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
                            addExceptionCallback(future, throwable -> logger.error(throwable, "Unexpected error during finalizeAddDataPagesRequest"), directExecutor());
                        }
                    },
                    executor);
        }
        catch (Throwable e) {
            inProgressLatch.release();
            throw e;
        }
    }

    @GET
    @Path("{bufferNodeId}/{exchangeId}/pages/{partitionId}/{chunkId}")
    public void getChunkData(
            @Suspended AsyncResponse asyncResponse,
            @Context HttpServletRequest request,
            @Context HttpServletResponse response,
            @PathParam("bufferNodeId") long bufferNodeId,
            @PathParam("exchangeId") String exchangeId,
            @PathParam("partitionId") int partitionId,
            @PathParam("chunkId") long chunkId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        ServletOutputStream outputStream;
        try {
            outputStream = response.getOutputStream();
        }
        catch (IOException e) {
            reportException(logger, e, "error on GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
            asyncResponse.resume(errorResponse(e));
            return;
        }

        checkTargetBufferNodeId(targetBufferNodeId);
        ChunkDataResult chunkDataResult;

        try {
            chunkDataResult = chunkManager.getChunkData(bufferNodeId, exchangeId, partitionId, chunkId);
        }
        catch (Throwable e) {
            reportException(logger, e, "error on GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
            asyncResponse.resume(errorResponse(e));
            return;
        }

        if (chunkDataResult.chunkDataLease().isEmpty()) {
            verify(chunkDataResult.spooledChunk().isPresent(), "Either chunkDataLease or spooledChunk should be present");
            SpooledChunk spooledChunk = chunkDataResult.spooledChunk().get();

            asyncResponse.resume(Response.status(Status.NOT_FOUND)
                    .header(SPOOLING_FILE_LOCATION_HEADER, spooledChunk.location())
                    .header(SPOOLED_CHUNK_OFFSET_HEADER, String.valueOf(spooledChunk.offset()))
                    .header(SPOOLED_CHUNK_LENGTH_HEADER, String.valueOf(spooledChunk.length()))
                    .build());
            return;
        }

        ChunkDataLease chunkDataLease = chunkDataResult.chunkDataLease().get();
        ArrayDeque<Slice> sliceQueue;
        AsyncContext context;
        try {
            int dataSize = chunkDataLease.serializedSizeInBytes() - CHUNK_SLICES_METADATA_SIZE;
            readDataSize.update(dataSize);
            readDataSizeDistribution.add(dataSize);

            // We need AsyncContext to complete an asynchronous write
            context = request.getAsyncContext();
            response.setStatus(Status.OK.getStatusCode());
            response.setContentType(TRINO_CHUNK_DATA);
            response.setContentLength(chunkDataLease.serializedSizeInBytes());

            Slice metaDataSlice = Slices.allocate(CHUNK_SLICES_METADATA_SIZE);
            SliceOutput sliceOutput = metaDataSlice.getOutput();
            sliceOutput.writeLong(chunkDataLease.getChecksum());
            sliceOutput.writeInt(chunkDataLease.getNumDataPages());

            sliceQueue = new ArrayDeque<>(chunkDataLease.getChunkSlices().size() + 1);
            sliceQueue.add(metaDataSlice);
            sliceQueue.addAll(chunkDataLease.getChunkSlices());
        }
        catch (Throwable e) {
            logger.warn(e, "error staging GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
            chunkDataLease.release();
            return;
        }

        outputStream.setWriteListener(new WriteListener() {
            private final AtomicBoolean done = new AtomicBoolean();

            @Override
            public void onWritePossible()
                    throws IOException
            {
                if (done.get()) {
                    logger.warn("onWritePossible when already done on GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
                    return;
                }
                while (outputStream.isReady()) {
                    if (sliceQueue.isEmpty()) {
                        if (done.compareAndSet(false, true)) {
                            chunkDataLease.release();
                            context.complete();
                            return;
                        }
                        else {
                            logger.warn("onWritePossible done in the meantime on GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
                            return;
                        }
                    }

                    Slice slice = sliceQueue.poll();
                    outputStream.write(slice.byteArray(), slice.byteArrayOffset(), slice.length());
                }
            }

            @Override
            public void onError(Throwable throwable)
            {
                try {
                    logger.warn(throwable, "error on GET /%s/%s/pages/%s/%s; alreadyDone=%s", bufferNodeId, exchangeId, partitionId, chunkId, done);
                    if (done.compareAndSet(false, true)) {
                        chunkDataLease.release();
                    }
                }
                catch (Throwable e) {
                    logger.error(e, "error in error handler for GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
                    throw e;
                }
                finally {
                    context.complete();
                }
            }
        });
    }

    @GET
    @Path("{exchangeId}/finish")
    @Produces(MediaType.TEXT_PLAIN)
    public void finishExchange(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);

            ListenableFuture<Void> completedFuture = withTimeout(chunkManager.finishExchange(exchangeId), getAsyncTimeout(clientMaxWait).toJavaTime(), timeoutExecutor);
            addCallback(completedFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(Void result)
                {
                    if (!asyncResponse.isDone()) {
                        asyncResponse.resume(Response.ok().build());
                    }
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    reportException(logger, throwable, "error on GET /%s/finish", exchangeId);
                    if (!asyncResponse.isDone()) {
                        asyncResponse.resume(errorResponse(throwable));
                    }
                }
            }, responseExecutor);
        }
        catch (Throwable e) {
            reportException(logger, e, "error on GET /%s/finish", exchangeId);
            if (!asyncResponse.isDone()) {
                asyncResponse.resume(errorResponse(e));
            }
        }
    }

    // Consume payload and return response; payload need to be consumed so client is able to see response.
    // For more information, see https://github.com/starburstdata/trino-buffer-service/issues/269
    private void consumeRequestAndCompleteAsyncResponse(String clientId, AsyncResponse response, ServletInputStream inputStream, long processingStart, Optional<Throwable> throwable)
    {
        byte[] skipBuffer = new byte[SKIP_BUFFER_SIZE];
        inputStream.setReadListener(new ReadListener() {
            @Override
            public void onDataAvailable()
                    throws IOException
            {
                while (inputStream.isReady()) {
                    if (inputStream.read(skipBuffer) == -1) {
                        return;
                    }
                }
            }

            @Override
            public void onAllDataRead()
            {
                completeServletResponse(clientId, response, processingStart, throwable);
            }

            @Override
            public void onError(Throwable e)
            {
                reportException(logger, e, "Got error while consuming request");
                completeServletResponse(clientId, response, processingStart, throwable);
            }
        });
    }

    private void completeServletResponse(String clientId, AsyncResponse asyncResponse, long processingStart, Optional<Throwable> throwable)
    {
        if (throwable.isPresent()) {
            if (!asyncResponse.isDone()) {
                asyncResponse.resume(errorResponse(throwable.get()));
            }
            return;
        }

        if (!asyncResponse.isDone()) {
            asyncResponse.resume(okResponse(getRateLimitHeaders(clientId)));
        }
        recordAddDataPagesRequest(processingStart, clientId);
    }

    private static class ReleasableReadListener
            implements ReadListener
    {
        private enum State {
            DELEGATE_SET,
            DELEGATE_RELEASED
        }

        private volatile Optional<ReadListener> delegate;
        private final AtomicReference<State> state;

        public ReleasableReadListener(ReadListener delegate)
        {
            this.delegate = Optional.of(requireNonNull(delegate, "delegate is null"));
            this.state = new AtomicReference<>(State.DELEGATE_SET);
        }

        private Optional<ReadListener> getDelegate()
        {
            if (state.get() == State.DELEGATE_RELEASED) {
                logger.warn("Delegate already released");
            }
            return delegate;
        }

        public void releaseDelegate()
        {
            if (state.get() == State.DELEGATE_RELEASED) {
                logger.warn("Cannot release delegate; delegate already released");
            }
            delegate = Optional.empty();
        }

        @Override
        public void onDataAvailable()
                throws IOException
        {
            try {
                Optional<ReadListener> currentDelegate = getDelegate();
                if (currentDelegate.isPresent()) {
                    currentDelegate.get().onDataAvailable();
                }
            }
            catch (Throwable callbackError) {
                reportException(logger, callbackError, "unexpected error in onDataAvailable");
                throw callbackError;
            }
        }

        @Override
        public void onAllDataRead()
                throws IOException
        {
            try {
                Optional<ReadListener> currentDelegate = getDelegate();
                if (currentDelegate.isPresent()) {
                    currentDelegate.get().onAllDataRead();
                }
            }
            catch (Throwable callbackError) {
                reportException(logger, callbackError, "unexpected error in onAllDataRead");
                throw callbackError;
            }
        }

        @Override
        public void onError(Throwable throwable)
        {
            try {
                Optional<ReadListener> currentDelegate = getDelegate();
                currentDelegate.ifPresent(readListener -> readListener.onError(throwable));
            }
            catch (Throwable callbackError) {
                if (callbackError != throwable) {
                    callbackError.addSuppressed(throwable);
                }
                reportException(logger, callbackError, "unexpected error in onError");
                throw callbackError;
            }
        }
    }
}
