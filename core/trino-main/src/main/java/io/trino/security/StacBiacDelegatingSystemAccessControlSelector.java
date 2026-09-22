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
package io.trino.security;

import com.google.inject.Inject;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControl;
import io.trino.spi.catalog.CatalogGovernance;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.SystemAccessControl;

import java.util.Optional;

import static io.trino.spi.catalog.CatalogGovernance.GOVERNED;
import static java.util.Objects.requireNonNull;

/**
 * Routes a catalog to {@link GalaxyAccessControl} when the catalog is {@link CatalogGovernance#GOVERNED},
 * and to every other configured access control when it is {@link CatalogGovernance#DELEGATED}.
 * <p>
 * A catalog the {@link CatalogManager} does not know, including a name that is not a valid
 * {@link CatalogName}, is treated as {@code GOVERNED}, which is the governance a catalog carries by
 * default. Operations that name no catalog are governed by {@link GalaxyAccessControl} alone.
 */
public class StacBiacDelegatingSystemAccessControlSelector
        implements SystemAccessControlSelector
{
    private final CatalogManager catalogManager;

    @Inject
    public StacBiacDelegatingSystemAccessControlSelector(CatalogManager catalogManager)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    @Override
    public boolean governsCatalog(SystemAccessControl accessControl, String catalogName)
    {
        return switch (governanceOf(catalogName)) {
            case GOVERNED -> isGalaxyAccessControl(accessControl);
            case DELEGATED -> !isGalaxyAccessControl(accessControl);
        };
    }

    @Override
    public boolean governsOperation(SystemAccessControl accessControl, SystemAccessControlOperation operation)
    {
        return isGalaxyAccessControl(accessControl);
    }

    private CatalogGovernance governanceOf(String catalogName)
    {
        return catalogName(catalogName)
                .flatMap(catalogManager::getCatalog)
                .map(Catalog::getGovernance)
                .orElse(GOVERNED);
    }

    private static Optional<CatalogName> catalogName(String catalogName)
    {
        try {
            return Optional.of(new CatalogName(catalogName));
        }
        catch (IllegalArgumentException _) {
            return Optional.empty();
        }
    }

    private static boolean isGalaxyAccessControl(SystemAccessControl accessControl)
    {
        return accessControl instanceof GalaxyAccessControl;
    }
}
