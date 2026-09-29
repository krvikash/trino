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
import io.trino.plugin.base.security.ForwardingSystemAccessControl;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogGovernance;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.SystemAccessControl;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.catalog.CatalogGovernance.GOVERNED;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

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

    private static boolean isGalaxyAccessControl(SystemAccessControl accessControl)
    {
        // Galaxy may be registered behind a delegating wrapper; classify by what the wrapper forwards to,
        // unless the wrapper hides its delegate by unwrapping to itself
        if (accessControl instanceof ForwardingSystemAccessControl forwarding) {
            SystemAccessControl unwrapped = forwarding.unwrapped();
            if (unwrapped != accessControl) {
                return isGalaxyAccessControl(unwrapped);
            }
        }
        return accessControl instanceof GalaxyAccessControl;
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

    @Override
    public void verifySystemAccessControls(List<SystemAccessControl> accessControls)
    {
        // Check every access control to make sure we don't encounter any misconfigured access controls
        boolean galaxyAccessControlExists = false;
        String configuredAccessControls = accessControls.stream()
                .map(accessControl -> accessControl.getClass().getName())
                .collect(joining(", "));
        for (SystemAccessControl accessControl : accessControls) {
            boolean thisIsGalaxy = isGalaxyAccessControl(accessControl);
            if (galaxyAccessControlExists && thisIsGalaxy) {
                throw new TrinoException(CONFIGURATION_INVALID, "Multiple instances of STAC access control found. Configured access controls: %s".formatted(configuredAccessControls));
            }
            galaxyAccessControlExists |= thisIsGalaxy;
        }
        if (!galaxyAccessControlExists) {
            throw new TrinoException(CONFIGURATION_INVALID, "No STAC access control is configured, so no access control would govern governed catalogs or catalog management. Configured access controls: %s".formatted(configuredAccessControls));
        }
    }
}
