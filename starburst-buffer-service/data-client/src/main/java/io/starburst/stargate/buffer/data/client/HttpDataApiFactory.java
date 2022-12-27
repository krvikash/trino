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

import io.airlift.http.client.HttpClient;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;

import javax.inject.Inject;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class HttpDataApiFactory
        implements DataApiFactory
{
    private final HttpClient dataHttpClient;
    private final SpooledChunkReader spooledChunkReader;
    private final boolean dataIntegrityVerificationEnabled;

    @Inject
    public HttpDataApiFactory(
            @ForBufferDataClient HttpClient dataHttpClient,
            SpooledChunkReader spooledChunkReader,
            DataApiConfig config)
    {
        this.dataHttpClient = requireNonNull(dataHttpClient, "dataHttpClient is null");
        this.spooledChunkReader = requireNonNull(spooledChunkReader, "spooledChunkReader is null");
        requireNonNull(config, "config is null");
        dataIntegrityVerificationEnabled = config.isDataIntegrityVerificationEnabled();
    }

    @Override
    public DataApi createDataApi(URI baseUri, long targetBufferNodeId)
    {
        return new HttpDataClient(baseUri, targetBufferNodeId, dataHttpClient, spooledChunkReader, dataIntegrityVerificationEnabled);
    }
}
