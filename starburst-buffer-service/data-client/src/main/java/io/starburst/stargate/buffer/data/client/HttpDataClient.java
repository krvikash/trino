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
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import javax.ws.rs.core.UriBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.calculateChecksum;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.readSerializedPages;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA_TYPE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

public class HttpDataClient
        implements DataApi
{
    public static final String ERROR_CODE_HEADER = "X-trino-buffer-error-code";
    public static final int SERIALIZED_PAGES_MAGIC = 0xfea4f001;

    private static final JsonCodec<ChunkList> CHUNK_LIST_JSON_CODEC = jsonCodec(ChunkList.class);

    private final URI baseUri;
    private final HttpClient httpClient;
    private final boolean dataIntegrityVerificationEnabled;

    public HttpDataClient(URI baseUri, HttpClient httpClient, boolean dataIntegrityVerificationEnabled)
    {
        requireNonNull(baseUri, "baseUri is null");
        requireNonNull(httpClient, "httpClient is null");
        checkArgument(baseUri.getPath().isBlank(), "expected base URI with no path; got " + baseUri);
        this.baseUri = UriBuilder.fromUri(requireNonNull(baseUri, "baseUri is null"))
                .replacePath("/api/v1/buffer/data")
                .build();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
    }

    @Override
    public ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        requireNonNull(exchangeId, "exchangeId is null");
        requireNonNull(pagingId, "pagingId is null");

        UriBuilder uri = UriBuilder.fromUri(baseUri).path("%s/closedChunks".formatted(exchangeId));
        pagingId.ifPresent(id -> uri.queryParam("pagingId", id));
        Request request = prepareGet()
                .setUri(uri.build())
                .build();

        HttpResponseFuture<JsonResponse<ChunkList>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(CHUNK_LIST_JSON_CODEC));

        return transformAsync(responseFuture, response -> {
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                String errorCode = response.getHeader(ERROR_CODE_HEADER);
                if (errorCode != null) {
                    return immediateFailedFuture(new DataApiException(ErrorCode.valueOf(errorCode), response.getResponseBody()));
                }
                else {
                    return immediateFailedFuture(new DataApiException(INTERNAL_ERROR, response.getResponseBody()));
                }
            }
            return immediateFuture(response.getValue());
        }, directExecutor());
    }

    @Override
    public ListenableFuture<Void> registerExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(UriBuilder.fromUri(baseUri)
                        .path("%s/register".formatted(exchangeId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(responseFuture);
    }

    @Override
    public ListenableFuture<Void> pingExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(UriBuilder.fromUri(baseUri)
                        .path("%s/ping".formatted(exchangeId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(responseFuture);
    }

    @Override
    public ListenableFuture<Void> removeExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareDelete()
                .setUri(UriBuilder.fromUri(baseUri)
                        .path(exchangeId)
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(responseFuture);
    }

    @Override
    public ListenableFuture<Void> addDataPages(String exchangeId, int partitionId, int taskId, int attemptId, long dataPagesId, List<Slice> dataPages)
    {
        requireNonNull(exchangeId, "exchangeId is null");
        requireNonNull(dataPages, "dataPage is null");
        checkArgument(dataPages.size() == 1, "exactly one page can be added; got %d", dataPages.size()); // todo
        Slice dataPage = dataPages.get(0);

        Request request = preparePost()
                .setUri(UriBuilder.fromUri(baseUri)
                        .path("%s/addDataPages/%d/%d/%d/%d".formatted(exchangeId, partitionId, taskId, attemptId, dataPagesId))
                        .build())
                .addHeader(CONTENT_TYPE, TEXT_PLAIN)
                .setBodyGenerator(createStaticBodyGenerator(dataPage.byteArrayOffset() == 0 ? dataPage.byteArray() : dataPage.getBytes()))
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(responseFuture);
    }

    @Override
    public ListenableFuture<Void> finishTask(String exchangeId, int partitionId, int taskId, int attemptId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(UriBuilder.fromUri(baseUri)
                        .path("%s/finishTask/%d/%d/%d".formatted(exchangeId, partitionId, taskId, attemptId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(responseFuture);
    }

    @Override
    public ListenableFuture<Void> finishExchange(String exchangeId)
    {
        requireNonNull(exchangeId, "exchangeId is null");

        Request request = prepareGet()
                .setUri(UriBuilder.fromUri(baseUri)
                        .path("%s/finish".formatted(exchangeId))
                        .build())
                .build();

        HttpResponseFuture<StringResponse> responseFuture = httpClient.executeAsync(request, createStringResponseHandler());
        return translateFailures(responseFuture);
    }

    @Override
    public ListenableFuture<List<DataPage>> getChunkData(String exchangeId, ChunkHandle chunkHandle)
    {
        requireNonNull(exchangeId, "exchangeId is null");
        requireNonNull(chunkHandle, "chunkHandle is null");

        Request request = prepareGet()
                .setUri(UriBuilder.fromUri(baseUri)
                        .path("%s/pages/%d/%d".formatted(exchangeId, chunkHandle.partitionId(), chunkHandle.chunkId()))
                        .build())
                .build();

        HttpResponseFuture<PagesResponse> responseFuture = httpClient.executeAsync(request, new PageResponseHandler(exchangeId, chunkHandle, dataIntegrityVerificationEnabled));
        return transform(responseFuture, PagesResponse::getPages, directExecutor());
    }

    public static class PageResponseHandler
            implements ResponseHandler<PagesResponse, RuntimeException>
    {
        private final String exchangeId;
        private final ChunkHandle chunkHandle;
        private final boolean dataIntegrityVerificationEnabled;

        private PageResponseHandler(
                String exchangeId,
                ChunkHandle chunkHandle,
                boolean dataIntegrityVerificationEnabled)
        {
            this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
            this.chunkHandle = requireNonNull(chunkHandle, "chunkHandle is null");
            this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        }

        @Override
        public PagesResponse handleException(Request request, Exception exception)
                throws RuntimeException
        {
            throw propagate(request, exception);
        }

        @Override
        public PagesResponse handle(Request request, Response response)
                throws RuntimeException
        {
            if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                throw new DataApiException(CHUNK_NOT_FOUND, "Chunk not found for exchangeId %s chunkHandle %s".formatted(exchangeId, chunkHandle));
            }
            else if (response.getStatusCode() != HttpStatus.OK.code()) {
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
                    throw new DataApiException(ErrorCode.valueOf(errorCode), body.toString());
                }
                throw new DataApiException(INTERNAL_ERROR, "Expected response code to be 200, but was %d:%n%s".formatted(response.getStatusCode(), body));
            }

            // invalid content type can happen when an error page is returned, but is unlikely given the above 200
            String contentType = response.getHeader(HttpHeaders.CONTENT_TYPE);
            checkState(contentType != null && mediaTypeMatches(contentType, TRINO_CHUNK_DATA_TYPE),
                    "Expected %s response from server but got %s", TRINO_CHUNK_DATA_TYPE, contentType);

            try (LittleEndianDataInputStream input = new LittleEndianDataInputStream(response.getInputStream())) {
                int magic = input.readInt();
                if (magic != SERIALIZED_PAGES_MAGIC) {
                    throw new IllegalStateException(format("Invalid stream header, expected 0x%08x, but was 0x%08x", SERIALIZED_PAGES_MAGIC, magic));
                }
                long checkSum = input.readLong();
                int pagesCount = input.readInt();
                List<DataPage> pages = ImmutableList.copyOf(readSerializedPages(input));
                verifyChecksum(checkSum, pages);
                checkState(pages.size() == pagesCount, "Wrong number of pages, expected %s, but read %s", pagesCount, pages.size());
                return PagesResponse.createPagesResponse(pages);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void verifyChecksum(long readChecksum, List<DataPage> pages)
        {
            if (dataIntegrityVerificationEnabled) {
                long calculatedChecksum = calculateChecksum(pages);
                if (readChecksum != calculatedChecksum) {
                    throw new ChecksumVerificationException(format("Data corruption, read checksum: 0x%08x, calculated checksum: 0x%08x", readChecksum, calculatedChecksum));
                }
            }
            else {
                if (readChecksum != NO_CHECKSUM) {
                    throw new ChecksumVerificationException(format("Expected checksum to be NO_CHECKSUM (0x%08x) but is 0x%08x", NO_CHECKSUM, readChecksum));
                }
            }
        }

        private static boolean mediaTypeMatches(String value, MediaType range)
        {
            try {
                return MediaType.parse(value).is(range);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                return false;
            }
        }
    }

    public static class PagesResponse
    {
        public static PagesResponse createPagesResponse(Iterable<DataPage> pages)
        {
            return new PagesResponse(pages);
        }

        private final List<DataPage> pages;

        private PagesResponse(Iterable<DataPage> pages)
        {
            this.pages = ImmutableList.copyOf(requireNonNull(pages, "pages is null"));
        }

        public List<DataPage> getPages()
        {
            return pages;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pagesSize", pages.size())
                    .toString();
        }
    }

    private static class ChecksumVerificationException
            extends RuntimeException
    {
        public ChecksumVerificationException(String message)
        {
            super(requireNonNull(message, "message is null"));
        }
    }

    private ListenableFuture<Void> translateFailures(HttpResponseFuture<StringResponse> responseFuture)
    {
        return transformAsync(responseFuture, response -> {
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                String errorCode = response.getHeader(ERROR_CODE_HEADER);
                if (errorCode != null) {
                    return immediateFailedFuture(new DataApiException(ErrorCode.valueOf(errorCode), response.getBody()));
                }
                return immediateFailedFuture(new DataApiException(INTERNAL_ERROR, response.getBody()));
            }
            return immediateVoidFuture();
        }, directExecutor());
    }
}
