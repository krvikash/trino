/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.stargate.buffer.discovery.client.failures;

import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;

import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.OK;
import static java.util.Objects.requireNonNull;

public class HttpFailureTrackingClient
        implements FailureTrackingApi
{
    private static final JsonCodec<FailureInfo> FAILURE_INFO_CODEC = jsonCodec(FailureInfo.class);
    private static final JsonCodec<FailuresStatusInfo> FAILURES_STATUS_INFO_RESPONSE_CODEC = jsonCodec(FailuresStatusInfo.class);

    private final URI baseUri;
    private final HttpClient httpClient;

    public HttpFailureTrackingClient(URI baseUri, HttpClient httpClient)
    {
        requireNonNull(baseUri, "baseUri is null");
        checkArgument(baseUri.getPath().isBlank(), "expected base URI with no path; got " + baseUri);
        this.baseUri = HttpUriBuilder.uriBuilderFrom(baseUri)
                .replacePath("/api/v1/buffer/failures")
                .build();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public void registerFailure(FailureInfo failureInfo)
    {
        requireNonNull(failureInfo, "failureInfo is null");

        Request request = preparePost()
                .setUri(HttpUriBuilder.uriBuilderFrom(baseUri)
                        .appendPath("register")
                        .build())
                .addHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(FAILURE_INFO_CODEC, failureInfo))
                .build();

        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        if (response.getStatusCode() != OK.getStatusCode()) {
            throw new RuntimeException("Unexpected response: " + response.getStatusCode() + "; " + response.getBody());
        }
    }

    @Override
    public FailuresStatusInfo getFailuresStatus()
    {
        Request request = prepareGet()
                .setUri(baseUri)
                .build();

        FullJsonResponseHandler.JsonResponse<FailuresStatusInfo> response =
                httpClient.execute(request, FullJsonResponseHandler.createFullJsonResponseHandler(FAILURES_STATUS_INFO_RESPONSE_CODEC));

        if (response.getStatusCode() != OK.getStatusCode()) {
            throw new RuntimeException("Unexpected response: " + response.getStatusCode() + "; " + response.getResponseBody());
        }

        if (!response.hasValue()) {
            throw new RuntimeException("Response does not contain a JSON value", response.getException());
        }

        return response.getValue();
    }
}
