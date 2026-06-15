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
package io.starburst.stargate.icehouse.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.icehouse.catalog.rest.RestCatalogConfig.Security;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRestCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RestCatalogConfig.class)
                .setUri(null)
                .setWarehouse(null)
                .setSecurity(Security.NONE)
                .setOauth2Credential(null)
                .setOauth2Scope(null)
                .setSigningName(null)
                .setSigningRegion(null));
    }

    @Test
    void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest-catalog.uri", "https://polaris.example.com/api/catalog")
                .put("iceberg.rest-catalog.warehouse", "my-warehouse")
                .put("iceberg.rest-catalog.security", "OAUTH2")
                .put("iceberg.rest-catalog.oauth2.credential", "client-id:client-secret")
                .put("iceberg.rest-catalog.oauth2.scope", "PRINCIPAL_ROLE:ALL")
                .put("iceberg.rest-catalog.signing-name", "s3tables")
                .put("iceberg.rest-catalog.signing-region", "us-east-1")
                .buildOrThrow();

        RestCatalogConfig expected = new RestCatalogConfig()
                .setUri(URI.create("https://polaris.example.com/api/catalog"))
                .setWarehouse("my-warehouse")
                .setSecurity(Security.OAUTH2)
                .setOauth2Credential("client-id:client-secret")
                .setOauth2Scope("PRINCIPAL_ROLE:ALL")
                .setSigningName("s3tables")
                .setSigningRegion("us-east-1");

        assertFullMapping(properties, expected);
    }
}
