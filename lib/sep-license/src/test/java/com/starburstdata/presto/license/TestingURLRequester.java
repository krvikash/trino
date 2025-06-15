/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingURLRequester
        implements URLRequester
{
    static TestingURLRequesterBuilder builder()
    {
        return new TestingURLRequesterBuilder();
    }

    private final Map<String, byte[]> downloadResponses;
    private final IOException exceptionOnDownloadPathMismatch;
    private final IOException exceptionOnDownload;
    private final byte[] putResponse;
    private final IOException exceptionOnPut;

    private TestingURLRequester(
            Map<String, byte[]> downloadResponses,
            IOException exceptionOnDownloadPathMismatch,
            IOException exceptionOnDownload,
            byte[] putResponse,
            IOException exceptionOnPut)
    {
        this.downloadResponses = downloadResponses;
        this.exceptionOnDownloadPathMismatch = exceptionOnDownloadPathMismatch;
        this.exceptionOnDownload = exceptionOnDownload;
        this.putResponse = putResponse;
        this.exceptionOnPut = exceptionOnPut;
    }

    @Override
    public byte[] get(URL url, Duration connectTimeout, Map<String, String> headers)
            throws IOException
    {
        if (exceptionOnDownload != null) {
            throw exceptionOnDownload;
        }

        Optional<byte[]> response = downloadResponses.entrySet().stream()
                .filter(pathEntry -> pathEntry.getKey().equals(url.getPath()))
                .findFirst()
                .map(Map.Entry::getValue);

        if (response.isPresent()) {
            return response.get();
        }

        if (exceptionOnDownloadPathMismatch != null) {
            throw exceptionOnDownloadPathMismatch;
        }

        throw new RuntimeException(format("Test configured incorrectly - no response for path %s found, and no Exception configured", url.getPath()));
    }

    @Override
    public byte[] put(URL url, Duration connectTimeout, Map<String, String> headers)
            throws IOException
    {
        if (exceptionOnPut != null) {
            throw exceptionOnPut;
        }

        return putResponse;
    }

    static class TestingURLRequesterBuilder
    {
        private final Map<String, byte[]> downloadResponses = new HashMap<>();
        private IOException exceptionOnDownloadPathMismatch;
        private IOException exceptionOnDownload;
        private byte[] putResponse = new byte[0];
        private IOException exceptionOnPut;

        public TestingURLRequesterBuilder withDownloadResponse(String endpointPath, byte[] response)
        {
            downloadResponses.put(
                    requireNonNull(endpointPath, "endpointPath is null"),
                    requireNonNull(response, "response is null"));
            return this;
        }

        public TestingURLRequesterBuilder withExceptionOnDownloadPathMismatch(IOException exceptionOnDownloadPathMismatch)
        {
            this.exceptionOnDownloadPathMismatch = requireNonNull(exceptionOnDownloadPathMismatch, "exceptionOnDownloadPathMismatch is null");
            return this;
        }

        public TestingURLRequesterBuilder withExceptionOnDownload(IOException exceptionOnDownload)
        {
            this.exceptionOnDownload = requireNonNull(exceptionOnDownload, "exceptionOnDownload is null");
            return this;
        }

        public TestingURLRequesterBuilder withPutResponse(byte[] putResponse)
        {
            this.putResponse = requireNonNull(putResponse, "putResponse is null");
            return this;
        }

        public TestingURLRequesterBuilder withExceptionOnPut(IOException exceptionOnPut)
        {
            this.exceptionOnPut = requireNonNull(exceptionOnPut, "exceptionOnPut is null");
            return this;
        }

        public TestingURLRequester build()
        {
            return new TestingURLRequester(
                    downloadResponses,
                    exceptionOnDownloadPathMismatch,
                    exceptionOnDownload,
                    putResponse,
                    exceptionOnPut);
        }
    }
}
