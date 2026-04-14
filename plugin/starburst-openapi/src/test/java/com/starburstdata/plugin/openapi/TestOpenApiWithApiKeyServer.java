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
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiWithApiKeyServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("ApiKeyServer"),
                new JaxrsModule(),
                binder -> jaxrsBinder(binder).bind(Server.class));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        TestingHttpServer server = injector.getInstance(TestingHttpServer.class);
        server.start();

        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", "java_server/apikey.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.security-scheme.type", "APIKEY")
                        .put("openapi.security-scheme.secret", Server.API_KEY)
                        .put("openapi.security-scheme.in", "HEADER")
                        .put("openapi.security-scheme.name", Server.HEADER_NAME)
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testAuthenticated()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.authenticated())"))
                .matches("VALUES TRUE");
    }

    @Test
    public void testUnauthorized()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.unauthorized())",
                "\\QNon-200 response status (401)\\E");
    }

    @Test
    public void testForbidden()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.forbidden())",
                "\\QNon-200 response status (403)\\E");
    }

    @Path("/")
    public static class Server
    {
        public static final String HEADER_NAME = "X-API-KEY";
        public static final String API_KEY = "MY_SECRET_KEY";

        @Path("/authenticated")
        @GET
        @Produces(APPLICATION_JSON)
        public Response authenticated(@HeaderParam(HEADER_NAME) String authorizationHeader)
        {
            if (authorizationHeader == null) {
                return Response.status(UNAUTHORIZED)
                        .header(
                                WWW_AUTHENTICATE,
                                "Key realm=\"ApiKeyServer\", description=\"Header %s required\"".formatted(HEADER_NAME))
                        .build();
            }
            if (!authorizationHeader.equals(API_KEY)) {
                return Response.status(FORBIDDEN).build();
            }
            return Response.ok().entity("true").build();
        }

        @Path("/unauthorized")
        @GET
        @Produces(APPLICATION_JSON)
        public Response unauthorized()
        {
            return Response
                    .status(UNAUTHORIZED)
                    .header(
                            WWW_AUTHENTICATE,
                            "Key realm=\"ApiKeyServer\", description=\"Header ZZZ required\"")
                    .build();
        }

        @Path("/forbidden")
        @GET
        @Produces(APPLICATION_JSON)
        public Response forbidden()
        {
            return Response.status(FORBIDDEN).build();
        }
    }
}
