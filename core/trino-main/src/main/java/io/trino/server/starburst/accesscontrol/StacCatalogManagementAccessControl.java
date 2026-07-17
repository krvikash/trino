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

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.EntityId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_CATALOG;
import static io.trino.server.starburst.accesscontrol.GalaxyAccessControl.isSystemCatalog;
import static io.trino.server.starburst.security.GalaxyIdentity.getContextRoleId;
import static io.trino.server.starburst.security.GalaxyIdentity.toAccountId;
import static io.trino.spi.security.AccessDeniedException.denyCreateCatalog;
import static io.trino.spi.security.AccessDeniedException.denyDropCatalog;
import static io.trino.spi.security.AccessDeniedException.denyRenameCatalog;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogProperties;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateCatalog;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Standalone SEP implementation of {@link CatalogManagementAccessControl}: catalog DDL is
 * permitted via real owner/privilege checks, since there is no external platform managing
 * catalogs the way Galaxy does.
 */
public class StacCatalogManagementAccessControl
        implements CatalogManagementAccessControl
{
    private final GalaxyAccessControllerSupplier controllerSupplier;

    public StacCatalogManagementAccessControl(GalaxyAccessControllerSupplier controllerSupplier)
    {
        this.controllerSupplier = requireNonNull(controllerSupplier, "controllerSupplier is null");
    }

    @Override
    public void checkCanShowCreateCatalog(SystemSecurityContext context, String catalog)
    {
        if (!isCatalogVisible(context, catalog)) {
            denyShowCreateCatalog(catalog, entityIsNotVisible(context, "Catalog", catalog));
        }
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        if (!hasAccountPrivilege(context.getIdentity(), CREATE_CATALOG)) {
            denyCreateCatalog(catalog, roleLacksPrivilege(context, CREATE_CATALOG, "account", catalog));
        }
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        checkIsCatalogOwner(context, catalog, explanation -> denyDropCatalog(catalog, explanation));
    }

    @Override
    public void checkCanRenameCatalog(SystemSecurityContext context, String catalog, String newCatalog)
    {
        checkIsCatalogOwner(context, catalog, explanation -> denyRenameCatalog(catalog, newCatalog, explanation));
    }

    @Override
    public void checkCanSetCatalogProperties(SystemSecurityContext context, String catalog, Map<String, Optional<String>> properties)
    {
        checkIsCatalogOwner(context, catalog, explanation -> denySetCatalogProperties(catalog, explanation));
    }

    private void checkIsCatalogOwner(SystemSecurityContext context, String catalogName, Consumer<String> denier)
    {
        if (!isCatalogOwner(context, catalogName)) {
            denier.accept(format("Role %s does not own the catalog", contextRoleName(context)));
        }
    }

    private boolean isCatalogOwner(SystemSecurityContext context, String catalogName)
    {
        GalaxyAccessControllerApi controller = getSystemAccessController(context);
        return isEntityOwner(controller, context, controller.getCatalogId(catalogName));
    }

    private boolean isCatalogVisible(SystemSecurityContext context, String catalog)
    {
        if (isSystemCatalog(catalog)) {
            return true;
        }
        GalaxyAccessControllerApi controller = getSystemAccessController(context);
        return controller.hasImpliedCatalogVisibility(context, catalog)
                || controller.getCatalogVisibility(context, ImmutableSet.of(catalog)).test(catalog);
    }

    private boolean isEntityOwner(GalaxyAccessControllerApi controller, SystemSecurityContext context, Optional<? extends EntityId> entity)
    {
        if (entity.isEmpty()) {
            return false;
        }
        EntityPrivileges entityPrivileges = controller.getEntityPrivileges(context, entity.get());
        return entityPrivileges.isOwnerInActiveRoleSet();
    }

    private boolean hasAccountPrivilege(Identity identity, Privilege privilege)
    {
        GalaxyAccessControllerApi controller = getSystemAccessController(identity);
        AccountId accountId = toAccountId(identity);
        EntityPrivileges entityPrivileges = controller.getEntityPrivileges(identity, accountId);
        return GalaxyAccessControl.hasEntityPrivilege(accountId, entityPrivileges, privilege, false);
    }

    private String contextRoleName(SystemSecurityContext context)
    {
        return getSystemAccessController(context).getRoleDisplayName(context.getIdentity(), getContextRoleId(context.getIdentity()));
    }

    private String roleLacksPrivilege(SystemSecurityContext context, Privilege privilege, String kind, String entity)
    {
        return format("Role %s does not have the privilege %s on the %s %s", contextRoleName(context), privilege, kind, entity);
    }

    private String entityIsNotVisible(SystemSecurityContext context, String description, String entityName)
    {
        return format("%s %s is not visible to the role %s", entityName, description, contextRoleName(context));
    }

    private GalaxyAccessControllerApi getSystemAccessController(SystemSecurityContext context)
    {
        return controllerSupplier.apply(context.getIdentity());
    }

    private GalaxyAccessControllerApi getSystemAccessController(Identity identity)
    {
        return controllerSupplier.apply(identity);
    }
}
