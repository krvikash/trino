/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class TestOpenApiQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        KeycloakServer keycloakServer = closeAfterClass(new KeycloakServer());
        PetStoreServer petStoreServer = closeAfterClass(new PetStoreServer(keycloakServer));
        FastApiServer fastApiServer = closeAfterClass(new FastApiServer());

        Map<String, String> petStoreProperties = ImmutableMap.<String, String>builder()
                .put("openapi.spec-location", petStoreServer.getSpecUrl())
                .put("openapi.base-uri", petStoreServer.getApiUrl())
                .put("openapi.authentication.type", "oauth")
                .put("openapi.authentication.scheme", "basic")
                .put("openapi.authentication.username", "user")
                .put("openapi.authentication.password", "user")
                .put("openapi.authentication.api-key-name", "api_key")
                .put("openapi.authentication.api-key-value", "special-key")
                .put("openapi.authentication.client-id", "sample-client-id")
                .put("openapi.authentication.client-secret", "secret")
                .buildOrThrow();

        Map<String, String> fastApiProperties = ImmutableMap.<String, String>builder()
                .put("openapi.spec-location", fastApiServer.getSpecUrl())
                .put("openapi.base-uri", fastApiServer.getApiUrl())
                .buildOrThrow();

        return OpenApiQueryRunner.builder(Map.of(
                "openmeteo", Map.of(
                        "openapi.spec-location", "https://raw.githubusercontent.com/open-meteo/open-meteo/main/openapi.yml",
                        "openapi.base-uri", "https://api.open-meteo.com"),
                "petstore", petStoreProperties,
                "fastapi", fastApiProperties))
                .build();
    }

    @Test
    void testShowPetStoreTables()
    {
        assertQuery("SHOW SCHEMAS FROM petstore",
                "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM petstore.default",
                "VALUES 'pet_find_by_status', 'pet_find_by_tags', 'store_inventory', 'store_order', 'pet', 'user', 'user_create_with_list', 'user_login', 'pet_upload_image'");
    }

    @Test
    void testSelectFromPetTable()
    {
        assertQuery("SELECT name FROM petstore.default.pet_find_by_status WHERE status = 'available' AND id != 100",
                "VALUES ('Cat 1'), ('Cat 2'), ('Dog 1'), ('Lion 1'), ('Lion 2'), ('Lion 3'), ('Rabbit 1')");
        assertQuery("SELECT name FROM petstore.default.pet WHERE pet_id = 1",
                "VALUES ('Cat 1')");
    }

    @Test
    void testInsertPet()
    {
        assertQueryReturnsEmptyResult("SELECT name FROM petstore.default.pet WHERE pet_id = 100");
        assertQuerySucceeds("INSERT INTO petstore.default.pet (id, name, photo_urls, status) VALUES (100, 'Cat X', ARRAY[], 'available')");
        assertQuery("SELECT name FROM petstore.default.pet WHERE pet_id = 100",
                "VALUES ('Cat X')");
        assertUpdate("UPDATE petstore.default.pet SET name = 'Cat Y' WHERE pet_id = 100", 1);
        assertQuery("SELECT name FROM petstore.default.pet WHERE pet_id = 100",
                "VALUES ('Cat Y')");
        assertUpdate("DELETE FROM petstore.default.pet WHERE pet_id = 100", 1);
        assertQueryReturnsEmptyResult("SELECT name FROM petstore.default.pet WHERE pet_id = 100");
    }

    @Test
    void testSelectFromForecastTable()
    {
        assertQuery("SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openmeteo.default.v1_forecast WHERE latitude_req = 53.1325 AND longitude_req = 23.1688",
                "VALUES (135.0, 'GMT', null)");
        assertQuery("SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openmeteo.default.v1_forecast WHERE latitude_req = 53.1325 AND longitude_req = 23.1688 AND current_weather_req = true",
                "VALUES (135.0, 'GMT', true)");
    }

    @Test
    void testSelectItems()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name, description, price, tax, tags, map_entries(properties), created_at, valid_until, revised_at FROM fastapi.default.items WHERE item_id = 1").getMaterializedRows();
        // can't use assertQuery, because array of dates read from H2 as not using LocalDate
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getFields()).containsExactly(
                "Portal Gun",
                null,
                BigDecimal.valueOf(4200000000L, 8),
                null,
                List.of("sci-fi"),
                List.of(),
                null,
                null,
                List.of(LocalDate.of(2007, 10, 10), LocalDate.of(2022, 12, 8)));
    }

    @Test
    void testSearchItemsWithInPhrase()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (ARRAY['2'])").getMaterializedRows();
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getFields()).first().isEqualTo("Plumbus");

        rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (ARRAY['1', '2'])").getMaterializedRows();
        assertThat(rows).hasSize(2);
    }

    @Test
    void testSearchItemsWithSubQuery10k()
    {
        try (TestTable table = generateDataset("memory.default.test_items_10k", 10000)) {
            List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (select array_agg(item_id) from %s)".formatted(table.getName())).getMaterializedRows();
            assertThat(rows)
                    .extracting(row -> row.getFields().getFirst())
                    .containsExactly("Portal Gun", "Plumbus");
        }
    }

    @Test
    void testSearchItemsWithSubQuery100k()
    {
        try (TestTable table = generateDataset("memory.default.test_items_100k", 100000)) {
            List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (select array_agg(item_id) from %s)".formatted(table.getName())).getMaterializedRows();
            assertThat(rows)
                    .extracting(row -> row.getFields().getFirst())
                    .containsExactly("Portal Gun", "Plumbus");
        }
    }

    @Test
    void testItemCategories()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.item_categories").getMaterializedRows();
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getFields()).first().isEqualTo("main");
    }

    @Test
    public void testItems()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.items").getMaterializedRows();
        assertThat(rows)
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    void testErrors()
    {
        assertQueryFails("SELECT * FROM fastapi.default.error", "Server responded with error 418: \"Oops! Inevitable error happened. There goes a rainbow...\"");
    }

    private TestTable generateDataset(String namePrefix, int elements)
    {
        return new TestTable(new TrinoSqlExecutor(getQueryRunner()), namePrefix, "AS SELECT CAST(sequential_number AS VARCHAR) AS item_id FROM TABLE(sequence(start=>0, stop=>%d))".formatted(elements - 1));
    }
}
