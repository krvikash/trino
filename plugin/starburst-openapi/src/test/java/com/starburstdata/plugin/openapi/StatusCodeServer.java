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
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * Runs a server that returns configurable HTTP status codes for testing error handling.
 */
public class StatusCodeServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;
    private final URI baseUrl;

    public StatusCodeServer(int retryableFailureCount)
    {
        RetryCounters retryCounters = new RetryCounters(
                new AtomicInteger(retryableFailureCount),
                new AtomicInteger(retryableFailureCount));
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("StatusCodeServer"),
                new JaxrsModule(),
                binder -> {
                    jaxrsBinder(binder).bind(Server.class);
                    binder.bind(RetryCounters.class).toInstance(retryCounters);
                });
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

    record RetryCounters(AtomicInteger tooManyRequests, AtomicInteger unavailable) {}

    @Path("/")
    public static class Server
    {
        private final RetryCounters retryCounters;

        @Inject
        public Server(RetryCounters retryCounters)
        {
            this.retryCounters = requireNonNull(retryCounters, "retryCounters is null");
        }

        @Path("/status/200")
        @GET
        @Produces(APPLICATION_JSON)
        public String ok()
        {
            return "[{\"id\": 1}]";
        }

        @Path("/status/404")
        @GET
        public Response notFound()
        {
            return Response.status(404).build();
        }

        @Path("/status/400")
        @GET
        public Response badRequest()
        {
            return Response.status(400).build();
        }

        @Path("/status/401")
        @GET
        public Response unauthorized()
        {
            return Response.status(401)
                    .header("WWW-Authenticate", "Bearer realm=\"test\"")
                    .build();
        }

        @Path("/status/403")
        @GET
        public Response forbidden()
        {
            return Response.status(403).build();
        }

        @Path("/status/429")
        @GET
        @Produces(APPLICATION_JSON)
        public Response tooManyRequests()
        {
            if (retryCounters.tooManyRequests().getAndDecrement() > 0) {
                return Response.status(429).header("Retry-After", "1").build();
            }
            return Response.ok("[{\"id\": 1}]").type(MediaType.APPLICATION_JSON).build();
        }

        @Path("/status/503")
        @GET
        @Produces(APPLICATION_JSON)
        public Response unavailable()
        {
            if (retryCounters.unavailable().getAndDecrement() > 0) {
                return Response.status(503).header("Retry-After", "1").build();
            }
            return Response.ok("[{\"id\": 1}]").type(MediaType.APPLICATION_JSON).build();
        }

        @Path("/status/301")
        @GET
        public Response redirect(@Context UriInfo uriInfo)
        {
            URI target = uriInfo.getBaseUriBuilder().path("status/200").build();
            return Response.status(301).location(target).build();
        }

        @Path("/status/redirect_loop")
        @GET
        public Response redirectLoop(@Context UriInfo uriInfo)
        {
            URI target = uriInfo.getBaseUriBuilder().path("status/redirect_loop").build();
            return Response.status(301).location(target).build();
        }
    }
}
