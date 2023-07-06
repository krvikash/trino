/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.ByteBufferBodyGenerator;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.ToStringHelper;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.starburst.stargate.buffer.data.client.DataClientHeaders.MAX_WAIT;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA_TYPE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HttpDataClient
        implements DataApi
{
    public static final String ERROR_CODE_HEADER = "X-trino-buffer-error-code";
    public static final String RATE_LIMIT_HEADER = "X-trino-rate-limit";
    public static final String AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER = "X-trino-average-process-time-in-millis";
    public static final String SPOOLING_FILE_LOCATION_HEADER = "X-trino-buffer-spooling-file-location";
    public static final String SPOOLING_FILE_SIZE_HEADER = "X-trino-buffer-spooling-file-size";
    public static final String CLIENT_ID_HEADER = "X-trino-buffer-client-id";

    private static final JsonCodec<ChunkList> CHUNK_LIST_JSON_CODEC = jsonCodec(ChunkList.class);
    private static final JsonCodec<BufferNodeInfo> BUFFER_NODE_INFO_JSON_CODEC = jsonCodec(BufferNodeInfo.class);

    private final URI baseUri;
    private final long targetBufferNodeId;
    private final HttpClient httpClient;
    private final Duration httpIdleTimeout;
    private final SpooledChunkReader spooledChunkReader;
    private final boolean dataIntegrityVerificationEnabled;
    private final Optional<String> clientId;

    public HttpDataClient(
            URI baseUri,
            long targetBufferNodeId,
            HttpClient httpClient,
            Duration httpIdleTimeout,
            SpooledChunkReader spooledChunkReader,
            boolean dataIntegrityVerificationEnabled,
            Optional<String> clientId)
    {
        this.httpIdleTimeout = httpIdleTimeout;
        requireNonNull(baseUri, "baseUri is null");
        requireNonNull(httpClient, "httpClient is null");
        checkArgument(baseUri.getPath().isBlank(), "expected base URI with no path; got " + baseUri);
        this.baseUri = uriBuilderFrom(requireNonNull(baseUri, "baseUri is null"))
                .replacePath("/api/v1/buffer/data")
                .build();
        this.targetBufferNodeId = targetBufferNodeId;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.spooledChunkReader = requireNonNull(spooledChunkReader, "spooledChunkReader is null");
        this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        this.clientId = requireNonNull(clientId, "clientId is null");
    }

    @Override
    public BufferNodeInfo getInfo()
    {
        Request request = prepareGet()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("info")
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .build();
        return httpClient.execute(request, createFullJsonResponseHandler(BUFFER_NODE_INFO_JSON_CODEC)).getValue();
    }

    @Override
    public ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        requireNonNull(exchangeId, "exchangeId is null");
        requireNonNull(pagingId, "pagingId is null");

        HttpUriBuilder uri = uriBuilderFrom(baseUri).appendPath("%s/closedChunks".formatted(exchangeId));
        pagingId.ifPresent(id -> uri.addParameter("pagingId", String.valueOf(id)));
        uri.addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId));
        Request request = prepareGet()
                .setUri(uri.build())
                .setHeader(MAX_WAIT, httpIdleTimeout.toString())
                .build();

        ListenableFuture<JsonResponse<ChunkList>> responseFuture = catchAndDecorateExceptions(request, httpClient.executeAsync(request, createFullJsonResponseHandler(CHUNK_LIST_JSON_CODEC)));

        return transformAsync(responseFuture, response -> {
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                String errorCode = response.getHeader(ERROR_CODE_HEADER);
                String errorMessage = requestErrorMessage(request, response.getResponseBody());
                return immediateFailedFuture(new DataApiException(errorCode == null ? INTERNAL_ERROR : ErrorCode.valueOf(errorCode), errorMessage));
            }
            return immediateFuture(response.getValue());
        }, directExecutor());
    }

    @Override
    public ListenableFuture<Void> markAllClosedChunksReceived(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("%s/markAllClosedChunksReceived".formatted(exchangeId))
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(request, responseFuture);
    }

    @Override
    public ListenableFuture<Void> registerExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("%s/register".formatted(exchangeId))
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(request, responseFuture);
    }

    @Override
    public ListenableFuture<Void> pingExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("%s/ping".formatted(exchangeId))
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(request, responseFuture);
    }

    @Override
    public ListenableFuture<Void> removeExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareDelete()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath(exchangeId)
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(request, responseFuture);
    }

    @Override
    public ListenableFuture<Optional<RateLimitInfo>> addDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPages)
    {
        requireNonNull(exchangeId, "exchangeId is null");
        requireNonNull(dataPages, "dataPage is null");

        int contentLength = Long.BYTES; // checksum
        int numByteBuffers = 1;
        for (Map.Entry<Integer, Collection<Slice>> entry : dataPages.asMap().entrySet()) {
            Collection<Slice> pages = entry.getValue();
            contentLength += Integer.BYTES * 2; // partitionId, totalLength
            numByteBuffers++;
            contentLength += pages.stream().mapToInt(slice -> Integer.BYTES + slice.length()).sum();
            numByteBuffers += 2 * pages.size();
        }

        ByteBuffer[] byteBuffers = new ByteBuffer[numByteBuffers];
        int index = 0;
        if (dataIntegrityVerificationEnabled) {
            XxHash64 hash = new XxHash64();
            for (Collection<Slice> pages : dataPages.asMap().values()) {
                for (Slice page : pages) {
                    hash = hash.update(page);
                }
            }
            long checksum = hash.hash();
            if (checksum == NO_CHECKSUM) {
                checksum++;
            }
            byteBuffers[index++] = toByteBuffer(checksum);
        }
        else {
            byteBuffers[index++] = toByteBuffer(NO_CHECKSUM);
        }
        for (Map.Entry<Integer, Collection<Slice>> entry : dataPages.asMap().entrySet()) {
            Integer partitionId = entry.getKey();
            Collection<Slice> pages = entry.getValue();
            int totalLength = 0;
            for (Slice page : pages) {
                totalLength += SIZE_OF_INT;
                totalLength += page.length();
            }
            byteBuffers[index++] = toByteBuffer(partitionId, totalLength);
            for (Slice page : pages) {
                byteBuffers[index++] = toByteBuffer(page.length());
                byteBuffers[index++] = page.toByteBuffer();
            }
        }

        Request.Builder requestBuilder = preparePost()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("%s/addDataPages/%d/%d/%d".formatted(exchangeId, taskId, attemptId, dataPagesId))
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .setBodyGenerator(new ByteBufferBodyGenerator(byteBuffers))
                .setHeader(CONTENT_LENGTH, String.valueOf(contentLength))
                .setHeader(MAX_WAIT, httpIdleTimeout.toString());
        clientId.ifPresent(clientId -> requestBuilder.setHeader(CLIENT_ID_HEADER, clientId));
        Request request = requestBuilder.build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return transform(
                catchAndDecorateExceptions(request, responseFuture),
                response -> {
                    String rateLimit = response.getHeader(RATE_LIMIT_HEADER);
                    String averageProcessTimeInMillis = response.getHeader(AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER);
                    Optional<RateLimitInfo> rateLimitInfo;
                    if (rateLimit != null && averageProcessTimeInMillis != null) {
                        rateLimitInfo = Optional.of(new RateLimitInfo(Double.parseDouble(rateLimit), Long.parseLong(averageProcessTimeInMillis)));
                    }
                    else {
                        rateLimitInfo = Optional.empty();
                    }

                    if (response.getStatusCode() != HttpStatus.OK.code()) {
                        String errorCode = response.getHeader(ERROR_CODE_HEADER);
                        String errorMessage = requestErrorMessage(request, response.getBody());
                        throw new DataApiException(errorCode == null ? INTERNAL_ERROR : ErrorCode.valueOf(errorCode), errorMessage, rateLimitInfo);
                    }
                    return rateLimitInfo;
                },
                directExecutor());
    }

    @Override
    public ListenableFuture<Void> finishExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("%s/finish".formatted(exchangeId))
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .setHeader(MAX_WAIT, httpIdleTimeout.toString())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(request, responseFuture);
    }

    @Override
    public ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("%d/%s/pages/%d/%d".formatted(bufferNodeId, exchangeId, partitionId, chunkId))
                        .addParameter("targetBufferNodeId", String.valueOf(targetBufferNodeId))
                        .build())
                .setHeader(MAX_WAIT, httpIdleTimeout.toString())
                .build();

        ListenableFuture<ChunkDataResponse> responseFuture = catchAndDecorateExceptions(request, httpClient.executeAsync(request, new ChunkDataResponseHandler(dataIntegrityVerificationEnabled)));

        return transformAsync(responseFuture, chunkDataResponse -> {
            if (chunkDataResponse.getPages().isPresent()) {
                return immediateFuture(chunkDataResponse.getPages().get());
            }
            verify(chunkDataResponse.getSpoolingFile().isPresent(), "Either pages or spoolingFile should be present");
            return spooledChunkReader.getDataPages(chunkDataResponse.getSpoolingFile().get());
        }, directExecutor());
    }

    public static class ChunkDataResponseHandler
            implements ResponseHandler<ChunkDataResponse, RuntimeException>
    {
        private final boolean dataIntegrityVerificationEnabled;

        private ChunkDataResponseHandler(boolean dataIntegrityVerificationEnabled)
        {
            this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        }

        @Override
        public ChunkDataResponse handleException(Request request, Exception exception)
                throws RuntimeException
        {
            throw propagate(request, exception);
        }

        @Override
        public ChunkDataResponse handle(Request request, Response response)
                throws RuntimeException
        {
            if (response.getStatusCode() == HttpStatus.NOT_FOUND.code() && response.getHeader(SPOOLING_FILE_LOCATION_HEADER) != null) {
                String location = response.getHeader(SPOOLING_FILE_LOCATION_HEADER);
                String length = response.getHeader(SPOOLING_FILE_SIZE_HEADER);

                if (length == null) {
                    throw new DataApiException(INTERNAL_ERROR, requestErrorMessage(request,
                            "Expected %s and %s to be both present in response")
                            .formatted(SPOOLING_FILE_LOCATION_HEADER, SPOOLING_FILE_SIZE_HEADER));
                }
                return ChunkDataResponse.createSpoolingFileResponse(location, Integer.parseInt(length));
            }
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                StringBuilder body = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream(), UTF_8))) {
                    // Get up to 1000 lines for debugging
                    for (int i = 0; i < 1000; i++) {
                        String line = reader.readLine();
                        // Don't output more than 100KB
                        if (line == null || body.length() + line.length() > 100 * 1024) {
                            break;
                        }
                        body.append(line);
                    }
                }
                catch (RuntimeException | IOException e) {
                    // Ignored. Just return whatever message we were able to decode
                }
                String errorCode = response.getHeader(ERROR_CODE_HEADER);
                if (errorCode != null) {
                    throw new DataApiException(ErrorCode.valueOf(errorCode), requestErrorMessage(request, body.toString()));
                }
                throw new DataApiException(INTERNAL_ERROR, requestErrorMessage(request, "Expected response code to be 200, but was %d:%n%s".formatted(response.getStatusCode(), body)));
            }

            // invalid content type can happen when an error page is returned, but is unlikely given the above 200
            String contentType = response.getHeader(HttpHeaders.CONTENT_TYPE);
            if (contentType == null || !mediaTypeMatches(contentType, TRINO_CHUNK_DATA_TYPE)) {
                throw new DataApiException(INTERNAL_ERROR, requestErrorMessage(request, "Expected %s response from server but got %s").formatted(TRINO_CHUNK_DATA_TYPE, contentType));
            }

            try (LittleEndianDataInputStream input = new LittleEndianDataInputStream(response.getInputStream())) {
                return ChunkDataResponse.createPagesResponse(toDataPages(input, dataIntegrityVerificationEnabled));
            }
            catch (IOException e) {
                throw new DataApiException(INTERNAL_ERROR, requestErrorMessage(request, "IOException"), e);
            }
        }
    }

    public static class ChunkDataResponse
    {
        public static ChunkDataResponse createPagesResponse(Iterable<DataPage> pages)
        {
            return new ChunkDataResponse(Optional.of(pages), Optional.empty());
        }

        public static ChunkDataResponse createSpoolingFileResponse(String location, int length)
        {
            return new ChunkDataResponse(Optional.empty(), Optional.of(new SpoolingFile(location, length)));
        }

        private final Optional<List<DataPage>> pages;
        private final Optional<SpoolingFile> spoolingFile;

        private ChunkDataResponse(Optional<Iterable<DataPage>> pages, Optional<SpoolingFile> spoolingFile)
        {
            this.pages = requireNonNull(pages, "pages is null").map(ImmutableList::copyOf);
            this.spoolingFile = requireNonNull(spoolingFile, "spoolingFile is null");
            checkArgument(pages.isPresent() ^ spoolingFile.isPresent(), "Either pages or spoolingFile should be present");
        }

        public Optional<List<DataPage>> getPages()
        {
            return pages;
        }

        public Optional<SpoolingFile> getSpoolingFile()
        {
            return spoolingFile;
        }

        @Override
        public String toString()
        {
            ToStringHelper helper = toStringHelper(this);
            pages.ifPresent(pages -> helper.add("pagesSize", pages.size()));
            spoolingFile.ifPresent(spoolingFile -> helper
                    .add("spoolingFileLocation", spoolingFile.location())
                    .add("spoolingFileLength", spoolingFile.length()));
            return helper.toString();
        }
    }

    private ListenableFuture<Void> translateFailures(Request request, HttpResponseFuture<StringResponse> responseFuture)
    {
        ListenableFuture<StringResponse> catchingResponseFuture = catchAndDecorateExceptions(request, responseFuture);

        return transformAsync(catchingResponseFuture, response -> {
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                String errorCode = response.getHeader(ERROR_CODE_HEADER);
                String errorMessage = requestErrorMessage(request, response.getBody());
                return immediateFailedFuture(new DataApiException(errorCode == null ? INTERNAL_ERROR : ErrorCode.valueOf(errorCode), errorMessage));
            }
            return immediateVoidFuture();
        }, directExecutor());
    }

    private static <T> ListenableFuture<T> catchAndDecorateExceptions(Request request, HttpResponseFuture<T> responseFuture)
    {
        return catchingAsync(responseFuture, RuntimeException.class, exception -> {
            if (exception instanceof DataApiException dataApiException) {
                throw dataApiException;
            }
            // add request info to an exception
            throw new RuntimeException("Unexpected exception on %s %s".formatted(request.getMethod(), request.getUri()), exception);
        }, directExecutor());
    }

    public static boolean mediaTypeMatches(String value, MediaType range)
    {
        try {
            return MediaType.parse(value).is(range);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            return false;
        }
    }

    private static String requestErrorMessage(Request request, String message)
    {
        return "error on %s %s: %s".formatted(request.getMethod(), request.getUri(), message);
    }

    private static ByteBuffer toByteBuffer(long... numbers)
    {
        ByteBuffer byteBuffer = ByteBuffer.allocate(numbers.length * Long.BYTES).order(LITTLE_ENDIAN);
        for (long number : numbers) {
            byteBuffer.putLong(number);
        }
        return byteBuffer.rewind();
    }

    private static ByteBuffer toByteBuffer(int... numbers)
    {
        ByteBuffer byteBuffer = ByteBuffer.allocate(numbers.length * Integer.BYTES).order(LITTLE_ENDIAN);
        for (int number : numbers) {
            byteBuffer.putInt(number);
        }
        return byteBuffer.rewind();
    }
}
