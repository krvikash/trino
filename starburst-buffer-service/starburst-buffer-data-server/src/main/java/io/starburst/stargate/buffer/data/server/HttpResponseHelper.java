/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import java.util.Map;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;

public final class HttpResponseHelper
{
    private HttpResponseHelper() {}

    public static Response okResponse(Map<String, String> headers)
    {
        Response.ResponseBuilder responseBuilder = Response
                .status(Status.OK)
                .header(CONTENT_TYPE, TEXT_PLAIN);
        headers.forEach(responseBuilder::header);
        return responseBuilder.build();
    }

    public static Response errorResponse(Throwable throwable)
    {
        return errorResponse(throwable, ImmutableMap.of());
    }

    public static Response errorResponse(Throwable throwable, Map<String, String> headers)
    {
        Response.ResponseBuilder responseBuilder = Response
                .status(Status.INTERNAL_SERVER_ERROR)
                .header(CONTENT_TYPE, TEXT_PLAIN);

        if (throwable instanceof DataServerException dataServerException) {
            responseBuilder
                    .header(ERROR_CODE_HEADER, dataServerException.getErrorCode())
                    .entity(throwable.getMessage());
        }
        else {
            responseBuilder
                    .header(ERROR_CODE_HEADER, INTERNAL_ERROR)
                    .entity(throwable.getMessage());
        }

        headers.forEach(responseBuilder::header);
        return responseBuilder.build();
    }

    public static Response errorResponse(ErrorCode errorCode, String message, Map<String, String> headers)
    {
        Response.ResponseBuilder responseBuilder = Response
                .status(Status.INTERNAL_SERVER_ERROR)
                .header(CONTENT_TYPE, TEXT_PLAIN)
                .header(ERROR_CODE_HEADER, errorCode)
                .entity(message);
        headers.forEach(responseBuilder::header);
        return responseBuilder.build();
    }
}
