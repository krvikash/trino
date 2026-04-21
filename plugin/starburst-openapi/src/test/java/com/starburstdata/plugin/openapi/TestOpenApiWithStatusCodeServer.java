/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpenApiWithStatusCodeServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        StatusCodeServer statusCodeServer = closeAfterClass(new StatusCodeServer(2));
        statusCodeServer.start();
        String specificationLocation = requireNonNull(
                OpenApiQueryRunner.class.getClassLoader().getResource("java_server/status_codes.3.0.4.json"),
                "Expected java_server specification was present")
                .getFile();
        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", specificationLocation)
                        .put("openapi.base-uri", statusCodeServer.getBaseUri().toString())
                        .buildOrThrow())
                .build();
    }

    @Test
    void testOkReturnsData()
    {
        assertThat(query("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '200'))"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    void testNotFoundSurfacesError()
    {
        assertThatThrownBy(() -> computeActual("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '404'))"))
                .hasMessageContaining("Not found (status 404)");
    }

    @Test
    void testBadRequestSurfacesError()
    {
        assertThatThrownBy(() -> computeActual("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '400'))"))
                .hasMessageContaining("Bad request (status 400)");
    }

    @Test
    void testUnauthorizedSurfacesError()
    {
        assertThatThrownBy(() -> computeActual("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '401'))"))
                .hasMessageContaining("Unauthorized (status 401)")
                .hasMessageContaining("WWW-Authenticate: Bearer realm=\"test\"");
    }

    @Test
    void testForbiddenSurfacesError()
    {
        assertThatThrownBy(() -> computeActual("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '403'))"))
                .hasMessageContaining("Forbidden (status 403)");
    }

    @Test
    void testTooManyRequestsRetriesAndSucceeds()
    {
        assertThat(query("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '429'))"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    void testUnavailableRetriesAndSucceeds()
    {
        assertThat(query("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '503'))"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    void testRedirectIsFollowed()
    {
        assertThat(query("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => '301'))"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    void testRedirectLoopSurfacesError()
    {
        assertThatThrownBy(() -> computeActual("SELECT id FROM TABLE(openapi.default.status_status_code(status_code => 'redirect_loop'))"))
                .hasMessageContaining("Too many redirects")
                .hasMessageContaining("possible redirect loop");
    }
}
