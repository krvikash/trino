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

/**
 * Governs catalog DDL (CREATE/DROP/RENAME/SET PROPERTIES/SHOW CREATE), which differs between
 * Galaxy-managed and standalone SEP deployments.
 *
 * @see GalaxyCatalogManagementAccessControl
 * @see StacCatalogManagementAccessControl
 */
public interface CatalogManagementAccessControl
{
    void checkCanShowCreateCatalog(SystemSecurityContext context, String catalog);

    void checkCanCreateCatalog(SystemSecurityContext context, String catalog);

    void checkCanDropCatalog(SystemSecurityContext context, String catalog);

    void checkCanRenameCatalog(SystemSecurityContext context, String catalog, String newCatalog);

    void checkCanSetCatalogProperties(SystemSecurityContext context, String catalog, Map<String, Optional<String>> properties);
}
