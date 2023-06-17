/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.stargate.buffer.discovery.client;

import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;
import io.starburst.stargate.buffer.BufferNodeInfo;

import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.OK;
import static java.util.Objects.requireNonNull;

public class HttpDiscoveryClient
        implements DiscoveryApi
{
    private static final JsonCodec<BufferNodeInfo> BUFFER_NODE_INFO_CODEC = jsonCodec(BufferNodeInfo.class);
    private static final JsonCodec<BufferNodeInfoResponse> BUFFER_NODE_INFO_RESPONSE_CODEC = jsonCodec(BufferNodeInfoResponse.class);

    private final URI baseUri;
    private final HttpClient httpClient;

    public HttpDiscoveryClient(URI baseUri, HttpClient httpClient)
    {
        requireNonNull(baseUri, "baseUri is null");
        requireNonNull(httpClient, "httpClient is null");
        checkArgument(baseUri.getPath().isBlank(), "expected base URI with no path; got " + baseUri);
        this.baseUri = uriBuilderFrom(requireNonNull(baseUri, "baseUri is null"))
                .replacePath("/api/v1/buffer/discovery")
                .build();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
    {
        requireNonNull(bufferNodeInfo, "bufferNodeInfo is null");

        Request request = preparePost()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("nodes/update")
                        .build())
                .addHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(BUFFER_NODE_INFO_CODEC, bufferNodeInfo))
                .build();

        StringResponse response = httpClient.execute(request, createStringResponseHandler());

        if (response.getStatusCode() == BAD_REQUEST.getStatusCode()) {
            throw new InvalidBufferNodeUpdateException(response.getBody());
        }

        if (response.getStatusCode() != OK.getStatusCode()) {
            throw new RuntimeException("Unexpected response: " + response.getStatusCode() + "; " + response.getBody());
        }
    }

    @Override
    public BufferNodeInfoResponse getBufferNodes()
    {
        Request request = prepareGet()
                .setUri(uriBuilderFrom(baseUri)
                        .appendPath("nodes")
                        .build())
                .build();

        JsonResponse<BufferNodeInfoResponse> response =
                httpClient.execute(request, FullJsonResponseHandler.createFullJsonResponseHandler(BUFFER_NODE_INFO_RESPONSE_CODEC));

        if (response.getStatusCode() != OK.getStatusCode()) {
            throw new RuntimeException("Unexpected response: " + response.getStatusCode() + "; " + response.getResponseBody());
        }

        if (!response.hasValue()) {
            throw new RuntimeException("Response does not contain a JSON value", response.getException());
        }

        return response.getValue();
    }
}
