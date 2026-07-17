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
package io.trino.server.starburst.accesscontrol;

import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.server.starburst.GalaxyEnabledConfig;
import io.trino.server.starburst.catalogs.CatalogResolver;
import io.trino.spi.security.Identity;
import io.trino.transaction.TransactionId;

import java.util.Optional;

import static io.trino.server.starburst.accesscontrol.MetadataAccessControllerSupplier.extractTransactionId;
import static java.util.Objects.requireNonNull;

public class LazyGalaxyAccessControllerSupplier
        implements GalaxyAccessControllerSupplier
{
    private final TrinoSecurityApi accessControlClient;
    private final CatalogResolver catalogResolver;
    private final GalaxyPermissionsCache galaxyPermissionsCache;
    private final GalaxyAccountPermissionsCache accountPermissionsCache;
    private final GalaxySystemAccessControlConfig systemAccessControlConfig;
    private final boolean useSharedPermissionsCache;

    @Inject
    public LazyGalaxyAccessControllerSupplier(
            TrinoSecurityApi accessControlClient,
            CatalogResolver catalogResolver,
            GalaxyPermissionsCache galaxyPermissionsCache,
            GalaxyAccountPermissionsCache accountPermissionsCache,
            GalaxySystemAccessControlConfig systemAccessControlConfig,
            GalaxyEnabledConfig config)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogResolver = requireNonNull(catalogResolver, "catalogResolver is null");
        this.galaxyPermissionsCache = requireNonNull(galaxyPermissionsCache, "galaxyPermissionsCache is null");
        this.accountPermissionsCache = requireNonNull(accountPermissionsCache, "accountPermissionsCache is null");
        this.systemAccessControlConfig = requireNonNull(systemAccessControlConfig, "systemAccessControlConfig is null");
        this.useSharedPermissionsCache = requireNonNull(config, "config is null").isUseSharedPermissionsCache();
    }

    @Override
    public GalaxyAccessControllerApi apply(Identity identity)
    {
        Optional<TransactionId> transactionId = extractTransactionId(identity);
        if (useSharedPermissionsCache && transactionId.isPresent()) {
            if (accountPermissionsCache.isTransactionIdInAnalysis(transactionId.get())) {
                return new GalaxySharedCacheAccessController(accountPermissionsCache, transactionId.get(), systemAccessControlConfig);
            }
            else {
                accountPermissionsCache.incrementTransactionNotInAnalysis();
            }
        }
        return new GalaxySystemAccessController(accessControlClient, catalogResolver, galaxyPermissionsCache, systemAccessControlConfig, transactionId);
    }
}
