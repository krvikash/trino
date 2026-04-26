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

public class TestOpenApiWithOffsetAndDataFieldPaginationServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("OffsetPaginationServer"),
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
                        .put("openapi.spec-location", "java_server/offset_pagination_data_field.3.0.4.json")
                        .put("openapi.base-uri", server.getBaseUrl().toString())
                        .put("openapi.pagination", "OFFSET")
                        .put("openapi.pagination.offset.offset-parameter-name", "offset")
                        .put("openapi.pagination.offset.data-field-json-pointer", "/data")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testLastElementFieldCursorFetchesAllData()
    {
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_offset(per_page => 2))
                CROSS JOIN UNNEST(data) AS t(id, name)
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
    public void testExplicitCursorParameterDisablesPagination()
    {
        // When the user explicitly provides the cursor parameter, the pagination strategy falls back
        // to ReadOnce — only the single page starting after that cursor position is returned.
        assertThat(query("""
                SELECT id, name FROM TABLE(openapi.default.items_offset(offset => 1, per_page => 3))
                CROSS JOIN UNNEST(data) AS t(id, name)
                """))
                .matches("""
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

    @Path("/")
    public static class PaginationServer
    {
        static final List<Map<String, Object>> ITEMS = IntStream.rangeClosed(1, 7)
                .mapToObj(i -> ImmutableMap.<String, Object>of("id", i, "name", "item-" + i))
                .collect(toImmutableList());

        private final ObjectMapper objectMapper;

        @Inject
        public PaginationServer(ObjectMapper objectMapper)
        {
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        }

        @Path("/items/offset")
        @GET
        @Produces(APPLICATION_JSON)
        public String offsetItems(
                @QueryParam("offset") @DefaultValue("0") int offset,
                @QueryParam("per_page") @DefaultValue("3") int perPage)
                throws JsonProcessingException
        {

            int start = Math.min(offset, ITEMS.size());
            int end = Math.min(offset + perPage, ITEMS.size());
            List<Map<String, Object>> pageItems = ITEMS.subList(start, end);

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
