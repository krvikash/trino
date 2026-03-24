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

import java.io.Closeable;
import java.net.URI;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * Runs a server that returns static strings to test against.
 */
public class StaticJavaServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;
    private final URI baseUrl;

    public StaticJavaServer()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("StaticJavaServer"),
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
        @Path("/static/boolean")
        @GET
        @Produces(APPLICATION_JSON)
        public String staticBoolean()
        {
            return "true";
        }
    }

    static void main()
            throws Exception
    {
        StaticJavaServer server = new StaticJavaServer();
        server.start();
        System.out.println("== SERVER STARTED ==");
        System.out.println(server.getBaseUri());
        System.out.println("====================");
    }
}
