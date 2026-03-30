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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.UriInfo;

import java.io.Closeable;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Map;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Runs a server that responds to parameters.
 */
public class ParametersServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;
    private final URI baseUrl;

    public ParametersServer()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("TypesServer"),
                new JaxrsModule(),
                binder -> jaxrsBinder(binder).bind(Server.class));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        server = injector.getInstance(TestingHttpServer.class);
        baseUrl = server.getBaseUrl();
    }

    public void start()
            throws Exception
    {
        server.start();
    }

    public URI getBaseUri()
    {
        return baseUrl;
    }

    @Override
    public void close()
    {
        lifeCycleManager.stop();
    }

    @Path("/")
    public static class Server
    {
        private final ObjectMapper objectMapper;

        @Inject
        public Server(ObjectMapper objectMapper)
        {
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        }

        @Path("/repeat/queryParams")
        @GET
        @Produces(APPLICATION_JSON)
        public String repeatQueryParams(@Context UriInfo uriInfo)
                throws JsonProcessingException
        {
            ImmutableList.Builder<Map<String, String>> queryParameters = ImmutableList.builder();
            uriInfo.getQueryParameters().forEach((name, values) -> values.forEach(value ->
                    queryParameters.add(ImmutableMap.of("name", name, "value", value))));
            return objectMapper.writeValueAsString(queryParameters.build());
        }

        @Path("/repeat/unexplodedQueryParams")
        @GET
        @Produces(APPLICATION_JSON)
        public String repeatUnexplodedQueryParams(@Context UriInfo uriInfo)
                throws JsonProcessingException
        {
            return objectMapper.writeValueAsString(
                    Arrays.stream(uriInfo
                                    .getQueryParameters()
                                    .getFirst("input")
                                    .split(","))
                            .map(value -> URLDecoder.decode(value, UTF_8))
                            .toList());
        }

        @Path("/repeat/path/{paramOne}/{paramTwo}")
        @GET
        @Produces(APPLICATION_JSON)
        public String repeatPathParams(@PathParam("paramOne") String paramOne, @PathParam("paramTwo") String paramTwo)
                throws JsonProcessingException
        {
            return objectMapper.writeValueAsString(ImmutableMap.of("paramOne", paramOne, "paramTwo", paramTwo));
        }
    }

    static void main()
            throws Exception
    {
        ParametersServer server = new ParametersServer();
        server.start();
        System.out.println("== SERVER STARTED ==");
        System.out.println(server.getBaseUri());
        System.out.println("====================");
    }
}
