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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
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
import jakarta.annotation.Nullable;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.util.concurrent.Futures.allAsList;
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
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Path("/api/v1/buffer/data")
public class BlockingDataResource
        extends BaseDataResource
{
    private static final Logger logger = Logger.get(BlockingDataResource.class);

    private static final int SKIP_BUFFER_SIZE = 8192;

    @Inject
    public BlockingDataResource(
            BufferNodeId bufferNodeId,
            ChunkManager chunkManager,
            MemoryAllocator memoryAllocator,
            BufferNodeStateManager bufferNodeStateManager,
            DataServerConfig config,
            BufferNodeInfoService bufferNodeInfoService,
            AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator,
            AddDataPagesInProgressTracker inProgressTracker,
            JsonCodec<Span> spanJsonCodec,
            DataServerStats stats)
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
    }

    @GET
    @Path("{exchangeId}/closedChunks")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listClosedChunks(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("pagingId") Long pagingId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            return Response.ok(
                    awaitFuture(
                            chunkManager.listClosedChunks(
                                    exchangeId,
                                    pagingId == null ? OptionalLong.empty() : OptionalLong.of(pagingId)),
                            getAsyncTimeout(clientMaxWait))).build();
        }
        catch (Throwable e) {
            reportException(logger, e, "error on %s", "GET /%s/closedChunks?pagingId=%s".formatted(exchangeId, pagingId));
            return errorResponse(e);
        }
    }

    @POST
    @Path("{exchangeId}/addDataPages/{taskId}/{attemptId}/{dataPagesId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response addDataPages(
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
        // Initial client info and timing
        String clientId = getClientId(request);
        Duration asyncTimeout = getAsyncTimeout(clientMaxWait);
        long processingStart = System.currentTimeMillis();
        long processingDeadline = processingStart + asyncTimeout.toMillis();

        // Obtain input stream from the request to read the request body.
        // If we can't get the stream, report error and complete response with error
        ServletInputStream inputStream;
        try {
            inputStream = request.getInputStream();
        }
        catch (IOException e) {
            reportException(logger, e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            return response(clientId, processingStart, Optional.of(e));
        }

        // Validate request parameters
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            validateTaskId(taskId);
            validateAttemptId(attemptId);
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            return consumeRequestAndBuildResponse(clientId, inputStream, processingStart, Optional.of(e));
        }

        if (dropUploadedPages) {
            return consumeRequestAndBuildResponse(clientId, inputStream, processingStart, Optional.empty());
        }

        // Concurrency control: check if we can accept more in-progress addDataPages requests
        AddDataPagesInProgressTracker.InProgressLatch inProgressLatch = inProgressTracker.incrementAndGetLatch();

        try {
            // Reject requests when node is shutting down. Still consume the request body to unblock client.
            if (bufferNodeStateManager.isDrainingStarted()) {
                inProgressLatch.release();
                logger.debug("rejecting POST /%s/addDataPages/%s/%s/%s; node already DRAINING", exchangeId, taskId, attemptId, dataPagesId);
                return consumeRequestAndBuildResponse(
                        clientId,
                        inputStream,
                        processingStart,
                        Optional.of(new DataServerException(DRAINING, "Node %d is draining and not accepting any more data".formatted(bufferNodeId))));
            }

            //Overload protection. Reject requests if we have too many in-progress addDataPages requests (backpressure)
            if (inProgressLatch.currentRequestsCount() > maxInProgressAddDataPagesRequests) {
                inProgressLatch.release();
                stats.getOverloadedAddDataPagesCount().update(1);
                addDataPagesThrottlingCalculator.recordThrottlingEvent();
                logger.debug("rejecting POST /%s/addDataPages/%s/%s/%s; exceeded maximum in progress addDataPages requests (%s > %s)",
                        exchangeId, taskId, attemptId, dataPagesId, inProgressLatch.currentRequestsCount(), maxInProgressAddDataPagesRequests);
                return consumeRequestAndBuildResponse(
                        clientId,
                        inputStream,
                        processingStart,
                        Optional.of(new DataServerException(OVERLOADED, "Exceeded maximum in progress addDataPages requests (%s)".formatted(maxInProgressAddDataPagesRequests))));
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
        }
        catch (Throwable e) {
            inProgressLatch.release();
            throw e;
        }

        List<ListenableFuture<Void>> addDataPagesFutures = new ArrayList<>();
        try {
            Supplier<String> errorPrefix = () -> "error on POST /%s/addDataPages/%s/%s/%s".formatted(exchangeId, taskId, attemptId, dataPagesId);

            // Block waiting for memory allocation
            Slice slice = awaitFuture(sliceLease.getSliceFuture(), asyncTimeout);

            // Read data synchronously from input stream with deadline checking
            int bytesRead = 0;
            while (bytesRead < contentLength) {
                if (System.currentTimeMillis() > processingDeadline) {
                    return finalizeAndReturnError(
                            addDataPagesFutures,
                            sliceLease,
                            inProgressLatch,
                            processingStart,
                            clientId,
                            asyncTimeout,
                            errorPrefix.get(),
                            new TimeoutException("Exceeded deadline while reading data"));
                }
                int readLength = inputStream.read(slice.byteArray(), slice.byteArrayOffset() + bytesRead, contentLength - bytesRead);
                if (readLength == -1) {
                    break;
                }
                bytesRead += readLength;
            }

            // Read final EOF marker
            int eofMarker = inputStream.read();
            checkState(eofMarker == -1, "expected EOF but read %s", eofMarker);

            verify(bytesRead == contentLength,
                    "Actual number of bytes read %s not equal to contentLength %s", bytesRead, contentLength);

            // Parse pages and validate checksum
            SliceInput sliceInput = slice.getInput();
            long readChecksum = sliceInput.readLong();
            XxHash64 hash = new XxHash64();

            Map<Integer, List<Slice>> pagesMap = new HashMap<>();
            while (sliceInput.isReadable()) {
                int partitionId = sliceInput.readInt();
                int bytes = sliceInput.readInt();
                writtenDataSizePerPartitionDistribution.add(bytes);
                ImmutableList.Builder<Slice> pages = ImmutableList.builder();
                while (bytes > 0 && sliceInput.isReadable()) {
                    int pageLength = sliceInput.readInt();

                    if (pageLength > chunkManager.getMaxPageLength()) {
                        return finalizeAndReturnError(
                                addDataPagesFutures,
                                sliceLease,
                                inProgressLatch,
                                processingStart,
                                clientId,
                                asyncTimeout,
                                errorPrefix.get(),
                                USER_ERROR,
                                format("Data page too large (%d > %d)", pageLength, chunkManager.getMaxPageLength()));
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
                    return finalizeAndReturnError(
                            addDataPagesFutures,
                            sliceLease,
                            inProgressLatch,
                            processingStart,
                            clientId,
                            asyncTimeout,
                            errorPrefix.get(),
                            USER_ERROR,
                            format("Data corruption, no more data in input stream but remaining bytes counter > 0 (%d)", bytes));
                }
                pagesMap.put(partitionId, pages.build());
            }

            writtenDataSize.update(contentLength);
            writtenDataSizeDistribution.add(contentLength);

            // Verify checksum
            if (dataIntegrityVerificationEnabled) {
                long calculatedChecksum = hash.hash();
                if (calculatedChecksum == NO_CHECKSUM) {
                    calculatedChecksum++;
                }
                if (readChecksum != calculatedChecksum) {
                    return finalizeAndReturnError(
                            addDataPagesFutures,
                            sliceLease,
                            inProgressLatch,
                            processingStart,
                            clientId,
                            asyncTimeout,
                            errorPrefix.get(),
                            USER_ERROR,
                            format("Data corruption, read checksum: 0x%08x, calculated checksum: 0x%08x", readChecksum, calculatedChecksum));
                }
            }
            else if (readChecksum != NO_CHECKSUM) {
                return finalizeAndReturnError(
                        addDataPagesFutures,
                        sliceLease,
                        inProgressLatch,
                        processingStart,
                        clientId,
                        asyncTimeout,
                        errorPrefix.get(),
                        USER_ERROR,
                        format("Expected checksum to be NO_CHECKSUM (0x%08x) but is 0x%08x", NO_CHECKSUM, readChecksum));
            }

            // Add pages to chunk manager
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
                }
            }
            catch (DataApiException e) {
                return finalizeAndReturnError(
                        addDataPagesFutures,
                        sliceLease,
                        inProgressLatch,
                        processingStart,
                        clientId,
                        asyncTimeout,
                        errorPrefix.get(),
                        e);
            }

            // Wait for all futures to complete before returning response.
            // Unlike the DataResource version, we don't optimize for early memory release when !shouldRetainMemory
            // because the virtual thread blocks until completion anyway, making early release negligible.
            // The async version separates resource cleanup (finalizeAddDataPagesRequest) from response
            // determination (addCallback on allAsList(futures)) — both always wait for ALL futures.
            Optional<Throwable> futureFailure = finalizeAddDataPagesRequest(
                    addDataPagesFutures, sliceLease, inProgressLatch, processingStart, clientId, asyncTimeout);

            if (futureFailure.isPresent()) {
                reportException(logger, futureFailure.get(), "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
                return errorResponse(futureFailure.get());
            }
            return okResponse(getRateLimitHeaders(clientId));
        }
        catch (Throwable e) {
            // Clean up resources and return error response.
            // In the DataResource ReadListener.onError() serves as this safety net
            // and routes through finalizeAddDataPagesRequest which awaits futures and records metrics.
            awaitFuturesQuietlyOnError(addDataPagesFutures, asyncTimeout, e);
            releaseResources(sliceLease, inProgressLatch, processingStart, clientId);
            reportException(logger, e, "error on POST /%s/addDataPages/%s/%s/%s", exchangeId, taskId, attemptId, dataPagesId);
            return errorWithRateLimit(e, clientId);
        }
    }

    @GET
    @Path("{bufferNodeId}/{exchangeId}/pages/{partitionId}/{chunkId}")
    public Response getChunkData(
            @PathParam("bufferNodeId") long bufferNodeId,
            @PathParam("exchangeId") String exchangeId,
            @PathParam("partitionId") int partitionId,
            @PathParam("chunkId") long chunkId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        ChunkDataResult chunkDataResult;

        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkDataResult = chunkManager.getChunkData(bufferNodeId, exchangeId, partitionId, chunkId);
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
            return errorResponse(e);
        }

        if (chunkDataResult.chunkDataLease().isEmpty()) {
            verify(chunkDataResult.spooledChunk().isPresent(), "Either chunkDataLease or spooledChunk should be present");
            SpooledChunk spooledChunk = chunkDataResult.spooledChunk().get();

            return Response.status(Response.Status.NOT_FOUND)
                    .header(SPOOLING_FILE_LOCATION_HEADER, spooledChunk.location())
                    .header(SPOOLED_CHUNK_OFFSET_HEADER, String.valueOf(spooledChunk.offset()))
                    .header(SPOOLED_CHUNK_LENGTH_HEADER, String.valueOf(spooledChunk.length()))
                    .build();
        }

        ChunkDataLease chunkDataLease = chunkDataResult.chunkDataLease().get();
        ArrayDeque<Slice> sliceQueue;
        try {
            int dataSize = chunkDataLease.serializedSizeInBytes() - CHUNK_SLICES_METADATA_SIZE;
            readDataSize.update(dataSize);
            readDataSizeDistribution.add(dataSize);

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
            return errorResponse(e);
        }

        return Response.ok()
                .type(TRINO_CHUNK_DATA)
                .header(HttpHeaders.CONTENT_LENGTH, chunkDataLease.serializedSizeInBytes())
                .entity(new StreamingOutput() {
                    private final AtomicBoolean done = new AtomicBoolean();
                    @Override
                    public void write(OutputStream outputStream)
                            throws IOException, WebApplicationException
                    {
                        if (done.get()) {
                            logger.warn("write was already done on GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
                            return;
                        }

                        try {
                            while (!sliceQueue.isEmpty()) {
                                Slice slice = sliceQueue.poll();
                                outputStream.write(slice.byteArray(), slice.byteArrayOffset(), slice.length());
                            }
                            outputStream.flush();
                        }
                        catch (Throwable e) {
                            reportException(logger, e, "error writing GET /%s/%s/pages/%s/%s", bufferNodeId, exchangeId, partitionId, chunkId);
                            throw new IOException("failed to write chunk data", e);
                        }
                        finally {
                            if (done.compareAndSet(false, true)) {
                                chunkDataLease.release();
                            }
                        }
                    }
                }).build();
    }

    @GET
    @Path("{exchangeId}/finish")
    @Produces(MediaType.TEXT_PLAIN)
    public Response finishExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @HeaderParam(MAX_WAIT) Duration clientMaxWait)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            awaitFuture(chunkManager.finishExchange(exchangeId), getAsyncTimeout(clientMaxWait));
            return Response.ok().build();
        }
        catch (Throwable e) {
            reportException(logger, e, "error on GET /%s/finish", exchangeId);
            return errorResponse(e);
        }
    }

    private void consumeInputStream(ServletInputStream inputStream)
            throws IOException
    {
        byte[] skipBuffer = new byte[SKIP_BUFFER_SIZE];
        while (inputStream.read(skipBuffer) != -1) {
            // Consume and discard
        }
    }

    /**
     * Finalizes the addDataPages request by waiting for futures to complete, then releasing resources.
     * This mirrors the async version's finalizeAddDataPagesRequest behavior.
     */
    private Optional<Throwable> finalizeAddDataPagesRequest(
            List<ListenableFuture<Void>> addDataPagesFutures,
            SliceLease sliceLease,
            AddDataPagesInProgressTracker.InProgressLatch inProgressLatch,
            long processingStart,
            String clientId,
            Duration asyncTimeout)
    {
        // Wait for all provided futures to complete (virtual thread will park)
        Optional<Throwable> futureFailure = Optional.empty();
        if (!addDataPagesFutures.isEmpty()) {
            try {
                awaitFuture(allAsList(addDataPagesFutures), asyncTimeout);
            }
            catch (Throwable e) {
                futureFailure = Optional.of(e);
            }
        }

        releaseResources(sliceLease, inProgressLatch, processingStart, clientId);
        return futureFailure;
    }

    private void releaseResources(
            SliceLease sliceLease,
            AddDataPagesInProgressTracker.InProgressLatch inProgressLatch,
            long processingStart,
            String clientId)
    {
        try {
            sliceLease.release();
        }
        finally {
            inProgressLatch.release();
            recordAddDataPagesRequest(processingStart, clientId);
        }
    }

    private Response finalizeAndReturnError(
            List<ListenableFuture<Void>> addDataPagesFutures,
            SliceLease sliceLease,
            AddDataPagesInProgressTracker.InProgressLatch inProgressLatch,
            long processingStart,
            String clientId,
            Duration asyncTimeout,
            String errorPrefix,
            ErrorCode errorCode,
            String message)
    {
        logger.warn("%s; %s; %s", errorPrefix, errorCode, message);
        finalizeAddDataPagesRequest(addDataPagesFutures, sliceLease, inProgressLatch, processingStart, clientId, asyncTimeout);
        return errorResponse(errorCode, message, getRateLimitHeaders(clientId));
    }

    private Response finalizeAndReturnError(
            List<ListenableFuture<Void>> addDataPagesFutures,
            SliceLease sliceLease,
            AddDataPagesInProgressTracker.InProgressLatch inProgressLatch,
            long processingStart,
            String clientId,
            Duration asyncTimeout,
            String errorPrefix,
            Throwable throwable)
    {
        reportException(logger, throwable, "%s", errorPrefix);
        finalizeAddDataPagesRequest(addDataPagesFutures, sliceLease, inProgressLatch, processingStart, clientId, asyncTimeout);
        return errorWithRateLimit(throwable, clientId);
    }

    /**
     * Consumes the request body and returns an appropriate response.
     * This method must be used when rejecting requests to ensure the client can receive the error response.
     * See https://github.com/starburstdata/trino-buffer-service/issues/269
     * <p>
     * This method always records metrics, even for rejected requests.
     */
    private Response consumeRequestAndBuildResponse(
            String clientId,
            ServletInputStream inputStream,
            long processingStart,
            Optional<Throwable> throwable)
    {
        // Consume payload so client can receive the response
        try {
            consumeInputStream(inputStream);
            return response(clientId, processingStart, throwable);
        }
        catch (Throwable e) {
            reportException(logger, e, "Got error while consuming request");
            return response(clientId, processingStart, throwable);
        }
    }

    private Response response(String clientId, long processingStart, Optional<Throwable> throwable)
    {
        if (throwable.isPresent()) {
            return errorResponse(throwable.get());
        }

        recordAddDataPagesRequest(processingStart, clientId);
        return okResponse(getRateLimitHeaders(clientId));
    }

    private Response errorWithRateLimit(Throwable throwable, String clientId)
    {
        return errorResponse(throwable, getRateLimitHeaders(clientId));
    }

    /**
     * Waits for already-created futures to complete before releasing resources.
     * Future failures are attached as suppressed exceptions to the primary error.
     * This matches the DataResource version's whenAllComplete behavior in finalizeAddDataPagesRequest
     */
    private static void awaitFuturesQuietlyOnError(List<ListenableFuture<Void>> futures, Duration timeout, Throwable primaryError)
    {
        if (!futures.isEmpty()) {
            try {
                awaitFuture(allAsList(futures), timeout);
            }
            catch (Throwable futureError) {
                primaryError.addSuppressed(futureError);
            }
        }
    }

    private static <T> T awaitFuture(ListenableFuture<T> future, Duration timeout)
    {
        try {
            return future.get(timeout.toMillis(), MILLISECONDS);
        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException(cause);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
