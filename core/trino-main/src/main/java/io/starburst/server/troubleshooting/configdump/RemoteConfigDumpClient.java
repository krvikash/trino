/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.configdump;

import com.google.inject.Inject;
import io.starburst.server.troubleshooting.DownloadResult;
import io.starburst.server.troubleshooting.ForTroubleshooting;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.trino.node.InternalNode;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.Future;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static java.util.Objects.requireNonNull;

public class RemoteConfigDumpClient
{
    private final HttpClient httpClient;

    @Inject
    public RemoteConfigDumpClient(@ForTroubleshooting HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public DownloadResult download(InternalNode node)
    {
        Request request = prepareGet()
                .setUri(uriBuilderFrom(node.getInternalUri())
                        .appendPath("/api/v1/troubleshooting/config")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        Future<InputStream> future = httpClient.executeAsync(request, new InputStreamResponseHandler());
        try {
            return DownloadResult.ofInputStream(future.get());
        }
        catch (Exception e) {
            return DownloadResult.ofException(e);
        }
    }

    private static class InputStreamResponseHandler
            implements ResponseHandler<InputStream, RuntimeException>
    {
        @Override
        public InputStream handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public InputStream handle(Request request, Response response)
        {
            try {
                if (!(response.getStatusCode() == HttpStatus.OK.code())) {
                    throw new TrinoException(
                            StandardErrorCode.GENERIC_INTERNAL_ERROR,
                            "request failed with http status code: %s, request %s, response: %s".formatted(response.getStatusCode(), request, response));
                }
                return response.getInputStream();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to read response from worker", e);
            }
        }
    }
}
