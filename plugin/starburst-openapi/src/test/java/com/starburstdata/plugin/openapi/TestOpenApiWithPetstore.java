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
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

final class TestOpenApiWithPetstore
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        KeycloakServer keycloakServer = closeAfterClass(new KeycloakServer());
        PetStoreServer petStoreServer = closeAfterClass(new PetStoreServer(keycloakServer));

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

        return OpenApiQueryRunner.builder()
                .addConnectorProperties(petStoreProperties)
                .build();
    }

    @Test
    void testShowPetStoreTables()
    {
        assertQuery("SHOW SCHEMAS FROM openapi",
                "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM openapi.default",
                "VALUES 'pet_find_by_status', 'pet_find_by_tags', 'store_inventory', 'store_order', 'pet', 'user', 'user_create_with_list', 'user_login', 'pet_upload_image'");
    }

    @Test
    void testSelectFromPetTable()
    {
        assertQuery("SELECT name FROM openapi.default.pet_find_by_status WHERE status = 'available' AND id != 100",
                "VALUES ('Cat 1'), ('Cat 2'), ('Dog 1'), ('Lion 1'), ('Lion 2'), ('Lion 3'), ('Rabbit 1')");
        assertQuery("SELECT name FROM openapi.default.pet WHERE pet_id = 1",
                "VALUES ('Cat 1')");
    }

    @Test
    void testInsertPet()
    {
        assertQueryReturnsEmptyResult("SELECT name FROM openapi.default.pet WHERE pet_id = 100");
        assertQuerySucceeds("INSERT INTO openapi.default.pet (id, name, photo_urls, status) VALUES (100, 'Cat X', ARRAY[], 'available')");
        assertQuery("SELECT name FROM openapi.default.pet WHERE pet_id = 100",
                "VALUES ('Cat X')");
        assertUpdate("UPDATE openapi.default.pet SET name = 'Cat Y' WHERE pet_id = 100", 1);
        assertQuery("SELECT name FROM openapi.default.pet WHERE pet_id = 100",
                "VALUES ('Cat Y')");
        assertUpdate("DELETE FROM openapi.default.pet WHERE pet_id = 100", 1);
        assertQueryReturnsEmptyResult("SELECT name FROM openapi.default.pet WHERE pet_id = 100");
    }
}
