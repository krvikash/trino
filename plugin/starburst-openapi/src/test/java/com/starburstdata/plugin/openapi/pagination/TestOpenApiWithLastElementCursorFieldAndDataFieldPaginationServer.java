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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiWithLastElementCursorFieldAndDataFieldPaginationServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("LastElementCursorFieldPaginationServer"),
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
                        .put("openapi.spec-location", "java_server/last_element_cursor_field_and_data_field_pagination.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.pagination", "LAST_ELEMENT_CURSOR_FIELD")
                        .put("openapi.pagination.last-element-cursor-field.data-field-json-pointer", "/data")
                        .put("openapi.pagination.last-element-cursor-field.cursor-field-json-pointer", "/id")
                        .put("openapi.pagination.last-element-cursor-field.cursor-parameter-name", "starting_after")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testLastElementFieldCursorFetchesAllData()
    {
        assertThat(query(
                """
                SELECT id, name FROM TABLE(openapi.default.items_after(per_page => 2))
                CROSS JOIN UNNEST(data) AS t(id, name)
                """))
                .matches(
                        """
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR))
                        """);
    }

    @Test
    public void testExplicitCursorParameterDisablesPagination()
    {
        // When the user explicitly provides the cursor parameter, the pagination strategy falls back
        // to ReadOnce — only the single page starting after that cursor position is returned.
        assertThat(query(
                """
                SELECT id, name FROM TABLE(openapi.default.items_after(starting_after => '1', per_page => 3))
                CROSS JOIN UNNEST(data) AS t(id, name)
                """))
                .matches(
                        """
                        VALUES
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR))
                        """);
    }

    @Test
    public void testOperationWithoutPaginationParameters()
    {
        // The /items/all endpoint has no starting_after/per_page parameters in the spec — the strategy's
        // containsAll check fails so ReadOnce is used. All items are returned in one request.
        assertThat(query("SELECT id, name FROM TABLE(openapi.default.items_all())"))
                .matches(
                        """
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

        @Path("/items/after")
        @GET
        @Produces(APPLICATION_JSON)
        public String afterItems(
                @QueryParam("starting_after") @DefaultValue("") String startingAfter,
                @QueryParam("per_page") @DefaultValue("2") int perPage)
                throws JsonProcessingException
        {
            // startingAfter holds the id of the last seen item; items with id > startingAfter are returned.
            // Since ids are 1-indexed and stored at index id-1, startingAfter == the 0-based start index.
            int startIndex = startingAfter.isEmpty() ? 0 : Integer.parseInt(startingAfter);
            int end = Math.min(startIndex + perPage, ITEMS.size());
            List<Map<String, Object>> pageItems = ITEMS.subList(startIndex, end);

            return objectMapper.writeValueAsString(ImmutableMap.of("data", pageItems));
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
