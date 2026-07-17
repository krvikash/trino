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

import io.trino.spi.security.SystemSecurityContext;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.security.AccessDeniedException.denyCreateCatalog;
import static io.trino.spi.security.AccessDeniedException.denyDropCatalog;
import static io.trino.spi.security.AccessDeniedException.denyRenameCatalog;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogProperties;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateCatalog;

/**
 * Galaxy-mode stub: catalogs are managed at the platform level and never through Trino DDL.
 */
public class GalaxyCatalogManagementAccessControl
        implements CatalogManagementAccessControl
{
    @Override
    public void checkCanShowCreateCatalog(SystemSecurityContext context, String catalog)
    {
        denyShowCreateCatalog(catalog);
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        // Galaxy manages catalogs for users so we do not intend to support CREATE CATALOG.
        denyCreateCatalog(catalog);
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        // Galaxy manages catalogs for users so we do not intend to support DROP CATALOG.
        denyDropCatalog(catalog);
    }

    @Override
    public void checkCanRenameCatalog(SystemSecurityContext context, String catalog, String newCatalog)
    {
        denyRenameCatalog(catalog, newCatalog);
    }

    @Override
    public void checkCanSetCatalogProperties(SystemSecurityContext context, String catalog, Map<String, Optional<String>> properties)
    {
        denySetCatalogProperties(catalog);
    }
}
