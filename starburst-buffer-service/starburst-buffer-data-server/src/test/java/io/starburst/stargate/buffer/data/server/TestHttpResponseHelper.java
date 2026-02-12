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
import io.starburst.stargate.buffer.data.exception.DataServerException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.starburst.stargate.buffer.data.client.ErrorCode.DRAINING;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHttpResponseHelper
{
    @Test
    public void testOkResponseWithHeaders()
    {
        Map<String, String> headers = ImmutableMap.of(
                "X-Custom-Header", "custom-value",
                "X-Another-Header", "another-value");

        Response response = HttpResponseHelper.okResponse(headers);

        assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
        assertThat(response.getHeaderString("Content-Type")).isEqualTo("text/plain");
        assertThat(response.getHeaderString("X-Custom-Header")).isEqualTo("custom-value");
        assertThat(response.getHeaderString("X-Another-Header")).isEqualTo("another-value");
    }

    @Test
    public void testErrorResponseWithDataServerException()
    {
        DataServerException exception = new DataServerException(DRAINING, "Node is draining");

        Response response = HttpResponseHelper.errorResponse(exception);

        assertThat(response.getStatus()).isEqualTo(INTERNAL_SERVER_ERROR.getStatusCode());
        assertThat(response.getHeaderString("Content-Type")).isEqualTo("text/plain");
        assertThat(response.getHeaderString(ERROR_CODE_HEADER)).isEqualTo(DRAINING.toString());
        assertThat(response.getEntity()).isEqualTo("Node is draining");
    }

    @Test
    public void testErrorResponseWithGenericException()
    {
        RuntimeException exception = new RuntimeException("Generic error");

        Response response = HttpResponseHelper.errorResponse(exception);

        assertThat(response.getStatus()).isEqualTo(INTERNAL_SERVER_ERROR.getStatusCode());
        assertThat(response.getHeaderString("Content-Type")).isEqualTo("text/plain");
        assertThat(response.getHeaderString(ERROR_CODE_HEADER)).isEqualTo(INTERNAL_ERROR.toString());
        assertThat(response.getEntity()).isEqualTo("Generic error");
    }

    @Test
    public void testErrorResponseWithHeaders()
    {
        RuntimeException exception = new RuntimeException("Error with headers");
        Map<String, String> headers = ImmutableMap.of("X-Rate-Limit", "100");

        Response response = HttpResponseHelper.errorResponse(exception, headers);

        assertThat(response.getStatus()).isEqualTo(INTERNAL_SERVER_ERROR.getStatusCode());
        assertThat(response.getHeaderString(ERROR_CODE_HEADER)).isEqualTo(INTERNAL_ERROR.toString());
        assertThat(response.getHeaderString("X-Rate-Limit")).isEqualTo("100");
    }

    @Test
    public void testErrorResponseWithErrorCodeAndMessage()
    {
        Map<String, String> headers = ImmutableMap.of("X-Custom", "value");

        Response response = HttpResponseHelper.errorResponse(DRAINING, "Custom message", headers);

        assertThat(response.getStatus()).isEqualTo(INTERNAL_SERVER_ERROR.getStatusCode());
        assertThat(response.getHeaderString("Content-Type")).isEqualTo("text/plain");
        assertThat(response.getHeaderString(ERROR_CODE_HEADER)).isEqualTo(DRAINING.toString());
        assertThat(response.getEntity()).isEqualTo("Custom message");
        assertThat(response.getHeaderString("X-Custom")).isEqualTo("value");
    }
}
