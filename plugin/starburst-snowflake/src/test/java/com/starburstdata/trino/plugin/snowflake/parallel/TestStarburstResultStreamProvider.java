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

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.starburstdata.trino.plugin.snowflake.parallel.StarburstResultStreamProvider.readChunkData;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestStarburstResultStreamProvider
{
    private static final byte[] PAYLOAD = "chunk-bytes".getBytes(UTF_8);

    @Test
    void testRetryBodyReadFailure()
    {
        CountingRequest request = new CountingRequest(2);
        assertThat(readChunkData(request, 3)).isEqualTo(PAYLOAD);
        assertThat(request.attempts()).isEqualTo(3);
    }

    @Test
    void testBodyReadFailureWithoutRetries()
    {
        CountingRequest request = new CountingRequest(1);
        assertThatThrownBy(() -> readChunkData(request, 0))
                .isInstanceOf(UncheckedIOException.class)
                .hasRootCauseInstanceOf(SocketException.class);
        assertThat(request.attempts()).isEqualTo(1);
    }

    @Test
    void testBodyReadFailureExhaustsRetries()
    {
        CountingRequest request = new CountingRequest(Integer.MAX_VALUE);
        assertThatThrownBy(() -> readChunkData(request, 2))
                .isInstanceOf(UncheckedIOException.class)
                .hasRootCauseInstanceOf(SocketException.class);
        assertThat(request.attempts()).isEqualTo(3);
    }

    @Test
    void testNoRetryWhenInterrupted()
    {
        CountingRequest request = new CountingRequest(Integer.MAX_VALUE);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> readChunkData(request, 3))
                    .isInstanceOf(UncheckedIOException.class);
            assertThat(request.attempts()).isEqualTo(1);
        }
        finally {
            // Clear the interrupt flag so it does not leak into other tests
            assertThat(Thread.interrupted()).isTrue();
        }
    }

    // Returns responses whose body read fails with "Connection reset" for the first failures attempts
    private static class CountingRequest
            implements Supplier<HttpResponse>
    {
        private final int failures;
        private final AtomicInteger attempts = new AtomicInteger();

        CountingRequest(int failures)
        {
            this.failures = failures;
        }

        @Override
        public HttpResponse get()
        {
            InputStream body = attempts.getAndIncrement() < failures ? new ResetInputStream() : new ByteArrayInputStream(PAYLOAD);
            BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
            response.setEntity(new InputStreamEntity(body));
            return response;
        }

        int attempts()
        {
            return attempts.get();
        }
    }

    private static class ResetInputStream
            extends InputStream
    {
        @Override
        public int read()
                throws IOException
        {
            throw new SocketException("Connection reset");
        }
    }
}
