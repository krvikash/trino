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

public class TestOpenApiWithPagePaginationServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("PageNumberPaginationServer"),
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
                        .put("openapi.spec-location", "java_server/page_number_pagination.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.pagination", "PAGE_NUMBER")
                        .put("openapi.pagination.page-number.page-parameter-name", "page")
                        .put("openapi.pagination.page-number.is-last-page-field-json-pointer", "/metadata/isLastPage")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testPagePaginationFetchesAllData()
    {
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_paged(per_page => 3))
                CROSS JOIN UNNEST(items) AS t(id, name)
                """))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR)),
                            (BIGINT '6', CAST('item-6' AS VARCHAR)),
                            (BIGINT '7', CAST('item-7' AS VARCHAR))
                        """);
    }

    @Test
    public void testPagePaginationWithLimit()
    {
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
    public void testPageExplicitPagination()
    {
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_paged(page => 1, per_page => 3))
                CROSS JOIN UNNEST(items) AS t(id, name)
                """))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR))
                        """);
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_paged(page => 2, per_page => 3))
                CROSS JOIN UNNEST(items) AS t(id, name)
                """))
                .matches("""
                        VALUES
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR)),
                            (BIGINT '6', CAST('item-6' AS VARCHAR))
                        """);
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_paged(page => 3, per_page => 3))
                CROSS JOIN UNNEST(items) AS t(id, name)
                """))
                .matches("VALUES (BIGINT '7', CAST('item-7' AS VARCHAR))");
    }

    @Test
    public void testOperationWithoutPaginationParameters()
    {
        // The /items/all endpoint has no page parameter in the spec — the PAGE_NUMBER strategy's
        // containsAll check fails so ReadOnce is used. All items are returned in one request.
        assertThat(query("SELECT id, name FROM TABLE(openapi.default.items_all())"))
                .matches("""
                        VALUES
                            (BIGINT '1', CAST('item-1' AS VARCHAR)),
                            (BIGINT '2', CAST('item-2' AS VARCHAR)),
                            (BIGINT '3', CAST('item-3' AS VARCHAR)),
                            (BIGINT '4', CAST('item-4' AS VARCHAR)),
                            (BIGINT '5', CAST('item-5' AS VARCHAR)),
                            (BIGINT '6', CAST('item-6' AS VARCHAR)),
                            (BIGINT '7', CAST('item-7' AS VARCHAR))
                        """);
    }

    @Test
    public void testEmptyResultsImplicitPagination()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.items_empty())"))
                .matches("VALUES (CAST(ARRAY[] AS array(row(\"id\" bigint, \"name\" varchar))), CAST(ROW(true) AS row(\"isLastPage\" boolean)))");
    }

    @Test
    public void testEmptyResultsExplicitPagination()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.items_empty(page => 1, per_page => 3))"))
                .matches("VALUES (CAST(ARRAY[] AS array(row(\"id\" bigint, \"name\" varchar))), CAST(ROW(true) AS row(\"isLastPage\" boolean)))");
    }

    @Path("/")
    public static class PaginationServer
    {
        // 7 items — with page-size=3 this produces 3 pages: [1-3], [4-6], [7]
        static final List<Map<String, Object>> ITEMS = IntStream.rangeClosed(1, 7)
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
        public String pagedItems(
                @QueryParam("page") @DefaultValue("1") int page,
                @QueryParam("per_page") @DefaultValue("3") int perPage)
                throws JsonProcessingException
        {
            int start = (page - 1) * perPage;
            int end = Math.min(start + perPage, ITEMS.size());
            List<Map<String, Object>> pageItems = ITEMS.subList(start, end);

            Map<String, Object> response = ImmutableMap.of(
                    "items", pageItems,
                    "metadata", ImmutableMap.of("isLastPage", end == ITEMS.size()));
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

        @Path("/items/empty")
        @GET
        @Produces(APPLICATION_JSON)
        public String emptyItems()
                throws JsonProcessingException
        {
            Map<String, Object> response = ImmutableMap.of(
                    "items", List.of(),
                    "metadata", ImmutableMap.of("isLastPage", true));
            return objectMapper.writeValueAsString(response);
        }
    }
}
