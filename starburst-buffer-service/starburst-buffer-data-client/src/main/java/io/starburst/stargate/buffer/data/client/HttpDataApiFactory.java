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

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HttpDataApiFactory
        implements DataApiFactory
{
    private final HttpClient httpClient;
    private final Duration httpIdleTimeout;
    private final SpooledChunkReader spooledChunkReader;
    private final boolean dataIntegrityVerificationEnabled;
    private final JsonCodec<Span> spanJsonCodec;

    @Inject
    public HttpDataApiFactory(
            @ForBufferDataClient HttpClient httpClient,
            @ForBufferDataClient HttpClientConfig dataHttpClientConfig,
            SpooledChunkReader spooledChunkReader,
            DataApiConfig config,
            JsonCodec<Span> spanJsonCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.httpIdleTimeout = dataHttpClientConfig.getIdleTimeout();
        this.spooledChunkReader = requireNonNull(spooledChunkReader, "spooledChunkReader is null");
        requireNonNull(config, "config is null");
        dataIntegrityVerificationEnabled = config.isDataIntegrityVerificationEnabled();
        this.spanJsonCodec = requireNonNull(spanJsonCodec, "spanJsonCodec is null");
    }

    @Override
    public DataApi createDataApi(URI baseUri, long targetBufferNodeId)
    {
        return new HttpDataClient(baseUri, targetBufferNodeId, httpClient, httpIdleTimeout, spooledChunkReader, dataIntegrityVerificationEnabled, Optional.empty(), spanJsonCodec);
    }
}
