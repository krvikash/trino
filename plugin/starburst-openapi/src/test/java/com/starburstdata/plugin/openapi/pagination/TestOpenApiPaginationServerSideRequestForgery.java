/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.pagination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.starburstdata.plugin.openapi.OpenApiQueryRunner;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * A malicious or compromised API can name any host in a pagination link or a redirect. The
 * connector authenticates requests as it executes them, so following one would hand the catalog's
 * credentials to that host and reach whatever the worker's network can reach.
 */
@Execution(SAME_THREAD)
public class TestOpenApiPaginationServerSideRequestForgery
        extends AbstractTestQueryFramework
{
    private static final InternalServer INTERNAL_SERVER = new InternalServer();

    private static PaginationServer paginationServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingHttpServer internalServer = startServer("InternalServer", binder -> jaxrsBinder(binder).bindInstance(INTERNAL_SERVER));
        closeAfterClass(internalServer::stop);
        paginationServer = new PaginationServer(new ObjectMapper(), internalServer.getBaseUrl());
        TestingHttpServer apiServer = startServer("SsrfPaginationServer", binder -> jaxrsBinder(binder).bindInstance(paginationServer));
        closeAfterClass(apiServer::stop);
        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.description-location", "java_server/ssrf_pagination.3.0.4.json")
                        .put("openapi.base-uri", apiServer.getBaseUrl().toString())
                        .put("openapi.pagination", "LINK_HEADER")
                        .buildOrThrow())
                .build();
    }

    private static TestingHttpServer startServer(String name, Module resourceModule)
            throws Exception
    {
        Injector injector = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule(name),
                new JaxrsModule(),
                resourceModule)
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        TestingHttpServer server = injector.getInstance(TestingHttpServer.class);
        server.start();
        return server;
    }

    @BeforeEach
    public void resetServers()
    {
        INTERNAL_SERVER.reset();
        paginationServer.reset();
    }

    @Test
    public void testOffHostLinkHeaderIsNotFollowed()
    {
        assertThatThrownBy(() -> computeActual("SELECT id, name FROM TABLE(openapi.api.items_off_host_link())"))
                .hasMessageContaining("Refusing to request");
        assertThat(INTERNAL_SERVER.requestCount()).isEqualTo(0);
    }

    @Test
    public void testOffHostLinkIsNotCheckedUntilItWouldBeRequested()
    {
        assertThat(query("SELECT id FROM TABLE(openapi.api.items_off_host_link()) LIMIT 2"))
                .matches("VALUES BIGINT '1', BIGINT '2'");
        assertThat(INTERNAL_SERVER.requestCount()).isEqualTo(0);
    }

    @Test
    public void testOffHostRedirectIsNotFollowed()
    {
        assertThatThrownBy(() -> computeActual("SELECT id, name FROM TABLE(openapi.api.items_off_host_redirect())"))
                .hasMessageContaining("Refusing to request");
        assertThat(INTERNAL_SERVER.requestCount()).isEqualTo(0);
    }

    @Test
    public void testRedirectLoopIsCapped()
    {
        assertThatThrownBy(() -> computeActual("SELECT id, name FROM TABLE(openapi.api.items_redirect_loop())"))
                .hasMessageContaining("Too many redirects");
        // The original request plus MAX_REDIRECTS hops, resulting in 9 hops total.
        assertThat(paginationServer.redirectLoopRequestCount()).isEqualTo(9);
    }

    @Test
    public void testSameHostPaginationStillWorks()
    {
        assertThat(query("SELECT id, name FROM TABLE(openapi.api.items_paged(per_page => 2))"))
                .matches(
                        """
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR))
                        """);
        assertThat(INTERNAL_SERVER.requestCount()).isEqualTo(0);
    }

    @Test
    public void testSameHostRedirectIsFollowed()
    {
        assertThat(query("SELECT id, name FROM TABLE(openapi.api.items_same_host_redirect())"))
                .matches(
                        """
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR))
                        """);
        assertThat(INTERNAL_SERVER.requestCount()).isEqualTo(0);
    }

    @Path("/")
    public static class InternalServer
    {
        private final AtomicInteger requestCount = new AtomicInteger();

        // Matches any path, so that a request to any target on this server is recorded, indicating a successful SSRF attack.
        @Path("{path:.*}")
        @GET
        @Produces(APPLICATION_JSON)
        public String anything()
        {
            requestCount.incrementAndGet();
            return "[]";
        }

        int requestCount()
        {
            return requestCount.get();
        }

        void reset()
        {
            requestCount.set(0);
        }
    }

    @Path("/")
    public static class PaginationServer
    {
        private static final List<Map<String, Object>> ITEMS = IntStream.rangeClosed(1, 5)
                .mapToObj(i -> ImmutableMap.<String, Object>of("id", i, "name", "item-" + i))
                .collect(toImmutableList());

        private final AtomicInteger redirectLoopRequestCount = new AtomicInteger();
        private final ObjectMapper objectMapper;
        private final URI internalServerUri;

        public PaginationServer(ObjectMapper objectMapper, URI internalServerUri)
        {
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
            this.internalServerUri = requireNonNull(internalServerUri, "internalServerUri is null");
        }

        @Path("/items/paged")
        @GET
        @Produces(APPLICATION_JSON)
        public Response pagedItems(
                @Context UriInfo uriInfo,
                @QueryParam("page") @DefaultValue("1") int page,
                @QueryParam("per_page") @DefaultValue("2") int perPage)
                throws JsonProcessingException
        {
            int start = (page - 1) * perPage;
            int end = Math.min(start + perPage, ITEMS.size());
            Response.ResponseBuilder builder = Response.ok(objectMapper.writeValueAsString(ITEMS.subList(start, end)));
            if (end < ITEMS.size()) {
                builder.link(
                        uriInfo.getAbsolutePathBuilder()
                                .queryParam("page", page + 1)
                                .queryParam("per_page", perPage)
                                .build(),
                        "next");
            }
            return builder.build();
        }

        @Path("/items/off-host-link")
        @GET
        @Produces(APPLICATION_JSON)
        public Response offHostLink()
                throws JsonProcessingException
        {
            return Response.ok(objectMapper.writeValueAsString(ITEMS.subList(0, 3)))
                    .link(internalServerUri.resolve("/latest/meta-data/"), "next")
                    .build();
        }

        @Path("/items/off-host-redirect")
        @GET
        @Produces(APPLICATION_JSON)
        public Response offHostRedirect()
        {
            return Response.status(302)
                    .location(internalServerUri.resolve("/latest/meta-data/"))
                    .build();
        }

        @Path("/items/redirect-loop")
        @GET
        @Produces(APPLICATION_JSON)
        public Response redirectLoop(@Context UriInfo uriInfo)
        {
            redirectLoopRequestCount.incrementAndGet();
            return Response.status(302)
                    .location(uriInfo.getAbsolutePath())
                    .build();
        }

        @Path("/items/same-host-redirect")
        @GET
        @Produces(APPLICATION_JSON)
        public Response sameHostRedirect(@Context UriInfo uriInfo)
        {
            return Response.status(302)
                    .location(uriInfo.getBaseUriBuilder().path("/items/all").build())
                    .build();
        }

        @Path("/items/all")
        @GET
        @Produces(APPLICATION_JSON)
        public String allItems()
                throws JsonProcessingException
        {
            return objectMapper.writeValueAsString(ITEMS);
        }

        int redirectLoopRequestCount()
        {
            return redirectLoopRequestCount.get();
        }

        void reset()
        {
            redirectLoopRequestCount.set(0);
        }
    }
}
