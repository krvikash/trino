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
import io.airlift.configuration.ConfigurationFactory;
import io.starburst.stargate.icehouse.catalog.CatalogKind;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalog;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalogFactory;
import io.starburst.stargate.icehouse.spi.maintenance.PlaintextTrinoProperties;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;

import java.util.Map;

/**
 * Builds a {@link RestIcehouseCatalog} from already-translated plaintext properties.
 */
public final class RestIcehouseCatalogFactory
        implements IcehouseCatalogFactory
{
    private static final String CATALOG_NAME = "icehouse-rest";
    private static final String ICEHOUSE_MAINTENANCE_USER = "icehouse-maintenance";

    @Override
    public CatalogKind kind()
    {
        return CatalogKind.REST_CATALOG;
    }

    @Override
    public IcehouseCatalog create(AccountId accountId, CatalogId catalogId, PlaintextTrinoProperties properties, TrinoFileSystemFactory fileSystemFactory)
    {
        RestCatalogConfig config;
        try (ConfigurationFactory configFactory = new ConfigurationFactory(properties.properties())) {
            config = configFactory.build(RestCatalogConfig.class);
        }

        RESTSessionCatalog restCatalog = new RESTSessionCatalog(
                clientConfig -> HTTPClient.builder(clientConfig)
                        .uri(clientConfig.get(CatalogProperties.URI))
                        .withHeaders(RESTUtil.configHeaders(clientConfig))
                        .build(),
                (_, _) -> new ForwardingFileIo(
                        fileSystemFactory.create(ConnectorIdentity.ofUser(ICEHOUSE_MAINTENANCE_USER)),
                        true));
        restCatalog.initialize(CATALOG_NAME, buildIcebergProperties(config));
        return new RestIcehouseCatalog(restCatalog);
    }

    private static Map<String, String> buildIcebergProperties(RestCatalogConfig config)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, config.getUri().toString());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.getWarehouse());

        switch (config.getSecurity()) {
            case OAUTH2 -> {
                properties.put("credential", config.getOauth2Credential()
                        .orElseThrow(() -> new IllegalArgumentException(
                                "iceberg.rest-catalog.oauth2.credential is required for OAUTH2 security")));
                config.getOauth2Scope().ifPresent(scope -> properties.put("scope", scope));
            }
            case SIGV4 -> {
                properties.put("rest.auth.type", "sigv4");
                properties.put("rest.signing-name", config.getSigningName()
                        .orElseThrow(() -> new IllegalArgumentException(
                                "iceberg.rest-catalog.signing-name is required for SIGV4 security")));
                properties.put("rest.signing-region", config.getSigningRegion()
                        .orElseThrow(() -> new IllegalArgumentException(
                                "iceberg.rest-catalog.signing-region is required for SIGV4 security")));
            }
            case NONE -> {}
        }
        return properties.buildOrThrow();
    }
}
