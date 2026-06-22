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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.jetty.JettyHttpClient;
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
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;

import static com.google.common.net.UrlEscapers.urlFormParameterEscaper;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiWithOAuth2Server
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        KeycloakServer keycloakServer = closeAfterClass(new KeycloakServer());

        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("OAuth2Server"),
                new JaxrsModule(),
                binder -> {
                    jaxrsBinder(binder).bind(Server.class);
                    binder.bind(URI.class).annotatedWith(Names.named("introspectionUrl"))
                            .toInstance(keycloakServer.getIntrospectionUrl());
                    binder.bind(HttpClient.class).to(JettyHttpClient.class).in(Scopes.SINGLETON);
                });
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        TestingHttpServer server = injector.getInstance(TestingHttpServer.class);
        server.start();

        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.description-location", "java_server/oauth2.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.security-scheme.type", "OAUTH2")
                        .put("openapi.security-scheme.client-id", "sample-client-id")
                        .put("openapi.security-scheme.client-secret", "secret")
                        .put("openapi.security-scheme.token-url", keycloakServer.getTokenUrl().toString())
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
                ".*Unauthorized \\(status 401\\).*");
    }

    @Test
    public void testForbidden()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.forbidden())",
                ".*Forbidden \\(status 403\\).*");
    }

    @Path("/")
    public static class Server
    {
        private final URI introspectionUrl;
        private final HttpClient httpClient;
        private final ObjectMapper objectMapper;

        @Inject
        public Server(@Named("introspectionUrl") URI introspectionUrl, ObjectMapper objectMapper, HttpClient httpClient)
        {
            this.introspectionUrl = introspectionUrl;
            this.objectMapper = objectMapper;
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
        }

        @Path("/authenticated")
        @GET
        @Produces(APPLICATION_JSON)
        public Response authenticated(@HeaderParam("Authorization") String authorization)
        {
            if (authorization == null || !authorization.startsWith("Bearer ")) {
                return Response.status(UNAUTHORIZED)
                        .header(WWW_AUTHENTICATE, "Bearer realm=\"OAuth2Server\"")
                        .build();
            }
            String token = authorization.substring("Bearer ".length());
            if (isTokenActive(token)) {
                return Response.ok().entity("true").build();
            }
            return Response.status(UNAUTHORIZED).build();
        }

        @Path("/unauthorized")
        @GET
        @Produces(APPLICATION_JSON)
        public Response unauthorized()
        {
            return Response.status(UNAUTHORIZED)
                    .header(WWW_AUTHENTICATE, "Bearer realm=\"OAuth2Server\"")
                    .build();
        }

        @Path("/forbidden")
        @GET
        @Produces(APPLICATION_JSON)
        public Response forbidden()
        {
            return Response.status(FORBIDDEN).build();
        }

        private boolean isTokenActive(String token)
        {
            String credentials = Base64.getEncoder()
                    .encodeToString("sample-client-id:secret".getBytes(UTF_8));
            String body = "token=" + urlFormParameterEscaper().escape(token);
            Request request = Request.builder()
                    .setMethod("POST")
                    .setUri(introspectionUrl)
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(body, UTF_8))
                    .setHeader(AUTHORIZATION, "Basic " + credentials)
                    .setHeader(CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED)
                    .build();
            return httpClient.execute(request, new ResponseHandler<>()
            {
                @Override
                public Boolean handleException(Request request, Exception exception)
                {
                    throw new RuntimeException(
                            "Unexpected failure calling keycloak introspection endpoint (%s)".formatted(
                                    exception.getMessage()),
                            exception);
                }

                @Override
                public Boolean handle(Request request, io.airlift.http.client.Response response)
                {
                    if (response.getStatusCode() != HttpStatus.OK.code()) {
                        throw new RuntimeException(
                                "Unexpected status code from keycloak server (%s)".formatted(response.getStatusCode()));
                    }
                    try {
                        return objectMapper.readTree(response.getInputStream()).path("active").asBoolean(false);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(
                                "Unexpected failure reading keycloak server response (%s)".formatted(e.getMessage()),
                                e);
                    }
                }
            });
        }
    }
}
