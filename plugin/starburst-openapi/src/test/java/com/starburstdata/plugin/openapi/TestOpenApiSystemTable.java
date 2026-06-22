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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiSystemTable
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TypesServer server = closeAfterClass(new TypesServer());
        server.start();
        String descriptionLocation = Resources.getResource("petstore.yaml").getFile();
        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.description-location", descriptionLocation)
                        .put("openapi.base-uri", server.getBaseUri().toString())
                        .buildOrThrow())
                .build();
    }

    @Test
    void testTableFunctionsTableExists()
    {
        assertQuerySucceeds("SELECT * FROM openapi.system.table_functions");
    }

    @Test
    void testTableFunctionsTableColumns()
    {
        MaterializedResult result = computeActual("SELECT * FROM openapi.system.table_functions LIMIT 1");
        assertThat(result.getColumnNames())
                .containsExactly("function_name", "api_path", "description", "input_columns", "output_columns");
    }

    @Test
    void testTableFunctionsTableContainsAllFunctions()
    {
        MaterializedResult result = computeActual(
                "SELECT function_name FROM openapi.system.table_functions ORDER BY function_name");
        assertThat(result.getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        "pet_find_by_status",
                        "pet_find_by_tags",
                        "pet_pet_id",
                        "store_inventory",
                        "store_order_order_id",
                        "user_login",
                        "user_username");
    }

    @Test
    void testTableFunctionsTableApiPaths()
    {
        assertThat(query(
                """
                SELECT function_name, api_path
                FROM openapi.system.table_functions
                WHERE function_name = 'pet_find_by_status'"""))
                .matches("VALUES (CAST('pet_find_by_status' AS VARCHAR), CAST('/pet/findByStatus' AS VARCHAR))");
    }

    @Test
    void testTableFunctionsTableDescriptions()
    {
        assertThat(query(
                """
                SELECT description
                FROM openapi.system.table_functions
                WHERE function_name = 'pet_find_by_status'"""))
                .matches("VALUES CAST('Finds Pets by status.' AS VARCHAR)");

        assertThat(query(
                """
                SELECT description
                FROM openapi.system.table_functions
                WHERE function_name = 'pet_pet_id'"""))
                .matches("VALUES CAST('Find pet by ID.' AS VARCHAR)");
    }

    @Test
    void testTableFunctionsTableInputColumns()
    {
        assertThat(query(
                """
                SELECT col_type, col_required
                FROM openapi.system.table_functions
                CROSS JOIN UNNEST(input_columns) AS t(col_name, col_type, col_required)
                WHERE function_name = 'pet_find_by_status'
                AND col_name = 'status'"""))
                .matches("VALUES (CAST('varchar' AS VARCHAR), false)");
    }

    @Test
    void testTableFunctionsTableInputColumnsNullWhenNoParameters()
    {
        // store_inventory has no query parameters — input_columns must be NULL
        assertThat(query(
                """
                SELECT input_columns
                FROM openapi.system.table_functions
                WHERE function_name = 'store_inventory'"""))
                .matches("VALUES CAST(NULL AS ARRAY(ROW(name VARCHAR, type VARCHAR, required BOOLEAN)))");
    }

    @Test
    void testTableFunctionsTableOutputColumns()
    {
        assertThat(query(
                """
                SELECT cardinality(output_columns) > 0
                FROM openapi.system.table_functions
                WHERE function_name = 'store_inventory'"""))
                .matches("VALUES true");

        assertThat(query(
                """
                SELECT output_columns[1].name IS NOT NULL
                FROM openapi.system.table_functions
                WHERE function_name = 'store_inventory'"""))
                .matches("VALUES true");
    }

    @Test
    void testTableFunctionsTableFilterByApiPath()
    {
        assertThat(query(
                """
                SELECT function_name
                FROM openapi.system.table_functions
                WHERE api_path LIKE '/pet%'"""))
                .matches(
                        """
                        VALUES
                            CAST('pet_find_by_status' AS VARCHAR),
                            CAST('pet_find_by_tags' AS VARCHAR),
                            CAST('pet_pet_id' AS VARCHAR)""");
    }
}
