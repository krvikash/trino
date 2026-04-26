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
import com.google.inject.Inject;
import com.google.inject.Injector;
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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiWithLinkHeaderPaginationServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("LinkHeaderPaginationServer"),
                new JaxrsModule(),
                binder -> jaxrsBinder(binder).bind(PaginationServer.class));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        TestingHttpServer server = injector.getInstance(TestingHttpServer.class);
        server.start();

        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", "java_server/link_header_pagination.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.pagination", "LINK_HEADER")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testLinkHeaderFetchesAllData()
    {
        assertThat(query("SELECT id, name FROM TABLE(openapi.default.items_paged(per_page => 2))"))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR))
                        """);
    }

    @Test
    public void testLinkHeaderRespectsLimit()
    {
        assertThat(query("SELECT id, name FROM TABLE(openapi.default.items_paged(per_page => 2)) LIMIT 3"))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR))
                        """);
    }

    @Test
    public void testOperationWithoutLinkHeader()
    {
        // The /items/all endpoint never returns a Link header — the strategy finishes after
        // the first response and all items are returned in a single request.
        assertThat(query("SELECT id, name FROM TABLE(openapi.default.items_all())"))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR))
                        """);
    }

    @Path("/")
    public static class PaginationServer
    {
        static final List<Map<String, Object>> ITEMS = IntStream.rangeClosed(1, 5)
                .mapToObj(i -> ImmutableMap.<String, Object>of("id", i, "name", "item-" + i))
                .collect(toImmutableList());

        private final ObjectMapper objectMapper;

        @Inject
        public PaginationServer(ObjectMapper objectMapper)
        {
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
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
            List<Map<String, Object>> pageItems = ITEMS.subList(start, end);

            Response.ResponseBuilder builder = Response.ok(objectMapper.writeValueAsString(pageItems));
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

        @Path("/items/all")
        @GET
        @Produces(APPLICATION_JSON)
        public String allItems()
                throws JsonProcessingException
        {
            return objectMapper.writeValueAsString(ITEMS);
        }
    }
}
