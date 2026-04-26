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

public class TestOpenApiWithNextUrlFieldPaginationServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("NextUrlFieldPaginationServer"),
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
                        .put("openapi.spec-location", "java_server/next_url_field_pagination.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.pagination", "NEXT_URL_FIELD")
                        .put("openapi.pagination.next-url-cursor.next-url-field-json-pointer", "/metadata/next_url")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testMetadataNextUrlCursorFetchesAllData()
    {
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_paged(per_page => 2))
                CROSS JOIN UNNEST(items) AS t(id, name)
                """))
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
    public void testMetadataNextUrlCursorRespectsLimit()
    {
        // The /items/all endpoint response has no next_url column — the strategy's
        // requiredResponseColumnsPaths check fails so ReadOnce is used. All items are returned in one request.
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_paged(per_page => 2))
                CROSS JOIN UNNEST(items) AS t(id, name)
                LIMIT 3
                """))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR))
                        """);
    }

    @Test
    public void testOperationWithoutNextUrlField()
    {
        // The /items/all endpoint response has no next_url column — the strategy's
        // requiredResponseColumnsPaths check fails so ReadOnce is used. All items are returned in one request.
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

        // Maps cursor value to the 0-based index of the item to return.
        // An empty cursor means the first page has not yet been fetched.
        private static final Map<String, Integer> CURSOR_TO_INDEX = ImmutableMap.of(
                "", 0,
                "cursor-2", 1,
                "cursor-3", 2,
                "cursor-4", 3,
                "cursor-5", 4);

        private final ObjectMapper objectMapper;

        @Inject
        public PaginationServer(ObjectMapper objectMapper)
        {
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        }

        @Path("/items/paged")
        @GET
        @Produces(APPLICATION_JSON)
        public String pagedItems(
                @Context UriInfo uriInfo,
                @QueryParam("cursor") @DefaultValue("") String cursor,
                @QueryParam("per_page") @DefaultValue("2") int perPage)
                throws JsonProcessingException
        {
            int index = CURSOR_TO_INDEX.getOrDefault(cursor, 0);
            int end = Math.min(index + perPage, ITEMS.size());
            List<Map<String, Object>> pageItems = ITEMS.subList(index, end);
            String nextCursor = end < ITEMS.size() ? "cursor-" + (end + 1) : "";

            String nextUrl = "";
            if (end < ITEMS.size()) {
                nextUrl = uriInfo.getBaseUriBuilder()
                        .path("/items/paged")
                        .queryParam("cursor", nextCursor)
                        .queryParam("limit", perPage)
                        .build()
                        .toString();
            }

            Map<String, Object> response = ImmutableMap.of(
                    "items", pageItems,
                    "metadata", ImmutableMap.of("next_url", nextUrl));
            return objectMapper.writeValueAsString(response);
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
