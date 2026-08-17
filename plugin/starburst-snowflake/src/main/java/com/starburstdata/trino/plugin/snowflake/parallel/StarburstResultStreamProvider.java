/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.google.inject.Inject;
import com.starburstdata.trino.plugin.snowflake.SnowflakeConfig;
import io.trino.spi.TrinoException;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.jdbc.RestRequest;
import net.snowflake.client.internal.jdbc.telemetry.ExecTimeTelemetryData;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.Map;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static net.snowflake.client.internal.jdbc.DefaultResultStreamProvider.detectGzipAndGetStream;

/**
 * {@link net.snowflake.client.internal.jdbc.DefaultResultStreamProvider} adapted to work with the split
 */
public class StarburstResultStreamProvider
{
    private static final int NETWORK_TIMEOUT_IN_MILLI = 0;
    private static final int AUTH_TIMEOUT_IN_SECONDS = 0;
    private static final int SOCKET_TIMEOUT_IN_MILLI = 0;
    private final CloseableHttpClient httpClient;
    private final int maxChunkRetries;

    @Inject
    public StarburstResultStreamProvider(CloseableHttpClient httpClient, SnowflakeConfig snowflakeConfig)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.maxChunkRetries = requireNonNull(snowflakeConfig, "snowflakeConfig is null").getMaxChunkRetries();
    }

    public byte[] getChunkData(Chunk chunk)
    {
        HttpResponse response;
        try {
            response = getResultChunk(chunk);
        }
        catch (URISyntaxException | SnowflakeSQLException e) {
            throw new TrinoException(
                    JDBC_ERROR,
                    "Error encountered when requesting a result chunk URL: %s %s".formatted(chunk.fileUrl(), requireNonNullElse(e.getMessage(), e)),
                    e);
        }

        // Return the raw, still-compressed chunk bytes. Decompression is deferred to decode time (see decompress)
        // so the prefetched buffer holds the compressed size rather than the larger uncompressed Arrow stream.
        try (InputStream inputStream = response.getEntity().getContent()) {
            return inputStream.readAllBytes();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Wraps raw chunk bytes in a decompressing stream. Snowflake result chunk files are gzip-compressed;
     * {@code detectGzipAndGetStream} passes through bytes that are not gzip-compressed (e.g. inline chunks).
     */
    public static InputStream decompress(byte[] chunkData)
            throws IOException
    {
        return detectGzipAndGetStream(new ByteArrayInputStream(chunkData));
    }

    private HttpResponse getResultChunk(Chunk chunk)
            throws URISyntaxException, SnowflakeSQLException
    {
        URIBuilder uriBuilder = new URIBuilder(chunk.fileUrl().orElseThrow());
        HttpGet httpRequest = new HttpGet(uriBuilder.build());

        for (Map.Entry<String, String> entry : chunk.headers().entrySet()) {
            httpRequest.addHeader(entry.getKey(), entry.getValue());
        }

        // RestRequest.execute method in snowflake since 3.25.1 has a bug with shift of input parameters,
        // where noRetry is used as unpack response, so we need to use executeWithRetries directly.
        HttpResponse response =
                RestRequest.executeWithRetries(
                                httpClient,
                                httpRequest,
                                NETWORK_TIMEOUT_IN_MILLI / 1000, // retry timeout
                                AUTH_TIMEOUT_IN_SECONDS,
                                SOCKET_TIMEOUT_IN_MILLI,
                                maxChunkRetries,
                                0, // no socket timeout injection
                                null, // no canceling
                                false, // no cookie
                                false, // no retry parameters in url
                                false, // no request_guid
                                true, // retry on HTTP403 for AWS S3
                                maxChunkRetries <= 0, // no retry on http request - noRetry = false (a.k.a. do retries) when maxChunkRetries > 0
                                false, // prevent unpacking response here
                                new ExecTimeTelemetryData(),
                                null,
                                null,
                                null,
                                false)
                        .getHttpResponse();
        if (response == null || response.getStatusLine().getStatusCode() != 200) {
            throw new TrinoException(
                    JDBC_ERROR,
                    "Error encountered when downloading a result chunk: HTTP status=%s".formatted((response != null) ? response.getStatusLine().getStatusCode() : "null response"));
        }
        return response;
    }
}
