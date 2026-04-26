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
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiWithNextCursorFieldPaginationServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("NextFieldCursorPaginationServer"),
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
                        .put("openapi.spec-location", "java_server/next_field_cursor_pagination.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.pagination", "NEXT_CURSOR_FIELD")
                        .put("openapi.pagination.next-field-cursor.cursor-field-json-pointer", "/metadata/next_cursor")
                        .put("openapi.pagination.next-field-cursor.cursor-parameter-name", "cursor")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testNextFieldCursorFetchesAllData()
    {
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_cursor(per_page => 3))
                CROSS JOIN UNNEST(data) AS t(id, name)
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
    public void testNextFieldCursorRespectsLimit()
    {
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_cursor(per_page => 3))
                CROSS JOIN UNNEST(data) AS t(id, name)
                LIMIT 4
                """))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR))
                        """);
    }

    @Test
    public void testExplicitCursorParameterDisablesPagination()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.items_cursor(cursor => 'cursor-2', per_page => 3))"))
                .matches("""
                        VALUES (
                            ARRAY[
                                CAST(ROW(BIGINT '2', CAST('item-2' AS VARCHAR)) AS row("id" bigint, "name" varchar)),
                                CAST(ROW(BIGINT '3', CAST('item-3' AS VARCHAR)) AS row("id" bigint, "name" varchar)),
                                CAST(ROW(BIGINT '4', CAST('item-4' AS VARCHAR)) AS row("id" bigint, "name" varchar))
                            ],
                            CAST(ROW(CAST('cursor-5' AS VARCHAR)) AS row("next_cursor" varchar))
                        )
                        """);
    }

    @Test
    public void testOperationWithoutPaginationParameters()
    {
        // The /items/all endpoint has no cursor/limit parameters in the spec — the strategy's
        // containsAll check fails so ReadOnce is used. All items are returned in one request.
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

        @Path("/items/cursor")
        @GET
        @Produces(APPLICATION_JSON)
        public String cursorItems(
                @QueryParam("cursor") @DefaultValue("") String cursor,
                @QueryParam("per_page") Integer perPage)
                throws JsonProcessingException
        {
            if (perPage == null) {
                throw new BadRequestException("per_page parameter is required");
            }
            int index = CURSOR_TO_INDEX.getOrDefault(cursor, 0);
            int end = Math.min(index + perPage, ITEMS.size());
            List<Map<String, Object>> pageItems = ITEMS.subList(index, end);
            String nextCursor = end < ITEMS.size() ? "cursor-" + (end + 1) : "";

            Map<String, Object> response = ImmutableMap.of(
                    "data", pageItems,
                    "metadata", ImmutableMap.of("next_cursor", nextCursor));
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
