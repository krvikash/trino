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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.security.ForwardingSystemAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static io.trino.metadata.MetadataManager.ENTITY_KINDS_WITH_CATALOG;
import static io.trino.security.SystemAccessControlOperation.CATALOG_MANAGEMENT;
import static io.trino.security.SystemAccessControlOperation.ENTITY_AUTHORIZATION;
import static io.trino.security.SystemAccessControlOperation.ENTITY_PRIVILEGES;
import static io.trino.security.SystemAccessControlOperation.QUERY_MANAGEMENT;
import static io.trino.security.SystemAccessControlOperation.SYSTEM_INFORMATION;
import static io.trino.security.SystemAccessControlOperation.SYSTEM_ROLES;
import static io.trino.security.SystemAccessControlOperation.SYSTEM_SESSION_PROPERTIES;
import static io.trino.security.SystemAccessControlOperation.USER_IMPERSONATION;
import static java.util.Objects.requireNonNull;

public class GovernedAwareSystemAccessControl
        extends ForwardingSystemAccessControl
{
    private final SystemAccessControl delegate;
    private final SystemAccessControlSelector accessControlSelector;

    public GovernedAwareSystemAccessControl(SystemAccessControl delegate, SystemAccessControlSelector accessControlSelector)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.accessControlSelector = requireNonNull(accessControlSelector, "accessControlSelector is null");
    }

    private boolean isCatalogGoverned(String catalog)
    {
        return accessControlSelector.governsCatalog(delegate, catalog);
    }

    private boolean isOperationGoverned(SystemAccessControlOperation operation)
    {
        return accessControlSelector.governsOperation(delegate, operation);
    }

    private boolean isEntityAuthorizationGoverned(EntityKindAndName entityKindAndName)
    {
        if (ENTITY_KINDS_WITH_CATALOG.contains(entityKindAndName.entityKind())) {
            return isCatalogGoverned(entityKindAndName.name().getFirst());
        }
        return isOperationGoverned(ENTITY_AUTHORIZATION);
    }

    @Override
    public SystemAccessControl delegate()
    {
        return delegate;
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return delegate.getEventListeners();
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        if (isOperationGoverned(USER_IMPERSONATION)) {
            delegate.checkCanImpersonateUser(identity, userName);
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        if (isOperationGoverned(USER_IMPERSONATION)) {
            delegate.checkCanSetUser(principal, userName);
        }
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        if (isOperationGoverned(SYSTEM_INFORMATION)) {
            delegate.checkCanReadSystemInformation(identity);
        }
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        if (isOperationGoverned(SYSTEM_INFORMATION)) {
            delegate.checkCanWriteSystemInformation(identity);
        }
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        if (isOperationGoverned(QUERY_MANAGEMENT)) {
            delegate.checkCanExecuteQuery(identity, queryId);
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (isOperationGoverned(QUERY_MANAGEMENT)) {
            delegate.checkCanViewQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        if (!isOperationGoverned(QUERY_MANAGEMENT)) {
            return queryOwners;
        }
        return delegate.filterViewQueryOwnedBy(identity, queryOwners);
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (isOperationGoverned(QUERY_MANAGEMENT)) {
            delegate.checkCanKillQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        if (isOperationGoverned(SYSTEM_SESSION_PROPERTIES)) {
            delegate.checkCanSetSystemSessionProperty(identity, queryId, propertyName);
        }
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return !isCatalogGoverned(catalogName) || delegate.canAccessCatalog(context, catalogName);
    }

    @Override
    public void checkCanShowCreateCatalog(SystemSecurityContext context, String catalog)
    {
        if (isCatalogGoverned(catalog)) {
            delegate.checkCanShowCreateCatalog(context, catalog);
        }
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        if (isOperationGoverned(CATALOG_MANAGEMENT)) {
            delegate.checkCanCreateCatalog(context, catalog);
        }
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        if (isCatalogGoverned(catalog)) {
            delegate.checkCanDropCatalog(context, catalog);
        }
    }

    @Override
    public void checkCanRenameCatalog(SystemSecurityContext context, String catalog, String newCatalog)
    {
        if (isCatalogGoverned(catalog)) {
            delegate.checkCanRenameCatalog(context, catalog, newCatalog);
        }
    }

    @Override
    public void checkCanSetCatalogProperties(SystemSecurityContext context, String catalog, Map<String, Optional<String>> properties)
    {
        if (isCatalogGoverned(catalog)) {
            delegate.checkCanSetCatalogProperties(context, catalog, properties);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        Set<String> governedCatalogs = catalogs.stream()
                .filter(this::isCatalogGoverned)
                .collect(toImmutableSet());
        if (governedCatalogs.isEmpty()) {
            return catalogs;
        }
        Set<String> filteredCatalogs = delegate.filterCatalogs(context, governedCatalogs);
        if (governedCatalogs.size() == catalogs.size()) {
            return filteredCatalogs;
        }
        return ImmutableSet.<String>builder()
                .addAll(filteredCatalogs)
                .addAll(difference(catalogs, governedCatalogs))
                .build();
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanCreateSchema(context, schema, properties);
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanDropSchema(context, schema);
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanRenameSchema(context, schema, newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanSetSchemaAuthorization(context, schema, principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        if (isCatalogGoverned(catalogName)) {
            delegate.checkCanShowSchemas(context, catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        if (!isCatalogGoverned(catalogName)) {
            return schemaNames;
        }
        return delegate.filterSchemas(context, catalogName, schemaNames);
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        if (isCatalogGoverned(schemaName.getCatalogName())) {
            delegate.checkCanShowCreateSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanShowCreateTable(context, table);
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanCreateTable(context, table, properties);
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanDropTable(context, table);
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanRenameTable(context, table, newTable);
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanSetTableProperties(context, table, properties);
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanSetTableComment(context, table);
        }
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (isCatalogGoverned(view.getCatalogName())) {
            delegate.checkCanSetViewComment(context, view);
        }
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanSetColumnComment(context, table);
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanShowTables(context, schema);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!isCatalogGoverned(catalogName)) {
            return tableNames;
        }
        return delegate.filterTables(context, catalogName, tableNames);
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        if (isCatalogGoverned(tableName.getCatalogName())) {
            delegate.checkCanShowColumns(context, tableName);
        }
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName tableName, Set<String> columns)
    {
        if (!isCatalogGoverned(tableName.getCatalogName())) {
            return columns;
        }
        return delegate.filterColumns(context, tableName, columns);
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        if (!isCatalogGoverned(catalogName)) {
            return tableColumns;
        }
        return delegate.filterColumns(context, catalogName, tableColumns);
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanAddColumn(context, table);
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanDropColumn(context, table);
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanRenameColumn(context, table);
        }
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanAlterColumn(context, table);
        }
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanSetTableAuthorization(context, table, principal);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Optional<String> branch, Set<String> columns)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanSelectFromColumns(context, table, branch, columns);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanSelectFromColumns(context, table, columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table, Optional<String> branch)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanInsertIntoTable(context, table, branch);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanInsertIntoTable(context, table);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table, Optional<String> branch)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanDeleteFromTable(context, table, branch);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanDeleteFromTable(context, table);
        }
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanTruncateTable(context, table);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext context, CatalogSchemaTableName table, Optional<String> branch, Set<String> updatedColumnNames)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanUpdateTableColumns(context, table, branch, updatedColumnNames);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanUpdateTableColumns(context, table, updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (isCatalogGoverned(view.getCatalogName())) {
            delegate.checkCanCreateView(context, view);
        }
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        if (isCatalogGoverned(view.getCatalogName())) {
            delegate.checkCanRenameView(context, view, newView);
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        if (isCatalogGoverned(view.getCatalogName())) {
            delegate.checkCanSetViewAuthorization(context, view, principal);
        }
    }

    @Override
    public void checkCanRefreshView(SystemSecurityContext context, CatalogSchemaTableName viewName)
    {
        if (isCatalogGoverned(viewName.getCatalogName())) {
            delegate.checkCanRefreshView(context, viewName);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (isCatalogGoverned(view.getCatalogName())) {
            delegate.checkCanDropView(context, view);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Optional<String> branch, Set<String> columns)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanCreateViewWithSelectFromColumns(context, table, branch, columns);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanCreateViewWithSelectFromColumns(context, table, columns);
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        if (isCatalogGoverned(materializedView.getCatalogName())) {
            delegate.checkCanCreateMaterializedView(context, materializedView, properties);
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (isCatalogGoverned(materializedView.getCatalogName())) {
            delegate.checkCanRefreshMaterializedView(context, materializedView);
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (isCatalogGoverned(materializedView.getCatalogName())) {
            delegate.checkCanDropMaterializedView(context, materializedView);
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        if (isCatalogGoverned(view.getCatalogName())) {
            delegate.checkCanRenameMaterializedView(context, view, newView);
        }
    }

    @Override
    public void checkCanSetMaterializedViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        if (isCatalogGoverned(view.getCatalogName())) {
            delegate.checkCanSetMaterializedViewAuthorization(context, view, principal);
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        if (isCatalogGoverned(materializedView.getCatalogName())) {
            delegate.checkCanSetMaterializedViewProperties(context, materializedView, properties);
        }
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return !isCatalogGoverned(functionName.getCatalogName()) || delegate.canExecuteFunction(systemSecurityContext, functionName);
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return !isCatalogGoverned(functionName.getCatalogName()) || delegate.canCreateViewWithExecuteFunction(systemSecurityContext, functionName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        if (isCatalogGoverned(catalogName)) {
            delegate.checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanGrantSchemaPrivilege(context, privilege, schema, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanDenySchemaPrivilege(context, privilege, schema, grantee);
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanRevokeSchemaPrivilege(context, privilege, schema, revokee, grantOption);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanGrantTablePrivilege(context, privilege, table, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanDenyTablePrivilege(context, privilege, table, grantee);
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanRevokeTablePrivilege(context, privilege, table, revokee, grantOption);
        }
    }

    @Override
    public void checkCanGrantEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption)
    {
        if (isOperationGoverned(ENTITY_PRIVILEGES)) {
            delegate.checkCanGrantEntityPrivilege(context, privilege, entity, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee)
    {
        if (isOperationGoverned(ENTITY_PRIVILEGES)) {
            delegate.checkCanDenyEntityPrivilege(context, privilege, entity, grantee);
        }
    }

    @Override
    public void checkCanRevokeEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption)
    {
        if (isOperationGoverned(ENTITY_PRIVILEGES)) {
            delegate.checkCanRevokeEntityPrivilege(context, privilege, entity, revokee, grantOption);
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        if (isOperationGoverned(SYSTEM_ROLES)) {
            delegate.checkCanShowRoles(context);
        }
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        if (isOperationGoverned(SYSTEM_ROLES)) {
            delegate.checkCanCreateRole(context, role, grantor);
        }
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        if (isOperationGoverned(SYSTEM_ROLES)) {
            delegate.checkCanDropRole(context, role);
        }
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (isOperationGoverned(SYSTEM_ROLES)) {
            delegate.checkCanGrantRoles(context, roles, grantees, adminOption, grantor);
        }
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (isOperationGoverned(SYSTEM_ROLES)) {
            delegate.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        if (isOperationGoverned(SYSTEM_ROLES)) {
            delegate.checkCanShowCurrentRoles(context);
        }
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        if (isOperationGoverned(SYSTEM_ROLES)) {
            delegate.checkCanShowRoleGrants(context);
        }
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        if (isCatalogGoverned(procedure.getCatalogName())) {
            delegate.checkCanExecuteProcedure(systemSecurityContext, procedure);
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanExecuteTableProcedure(systemSecurityContext, table, procedure);
        }
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (isCatalogGoverned(schema.getCatalogName())) {
            delegate.checkCanShowFunctions(context, schema);
        }
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        if (!isCatalogGoverned(catalogName)) {
            return functionNames;
        }
        return delegate.filterFunctions(context, catalogName, functionNames);
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        if (isCatalogGoverned(functionName.getCatalogName())) {
            delegate.checkCanCreateFunction(systemSecurityContext, functionName);
        }
    }

    @Override
    public void checkCanDropFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        if (isCatalogGoverned(functionName.getCatalogName())) {
            delegate.checkCanDropFunction(systemSecurityContext, functionName);
        }
    }

    @Override
    public void checkCanShowCreateFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        if (isCatalogGoverned(functionName.getCatalogName())) {
            delegate.checkCanShowCreateFunction(systemSecurityContext, functionName);
        }
    }

    @Override
    public void checkCanShowBranches(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName tableName)
    {
        if (isCatalogGoverned(tableName.getCatalogName())) {
            delegate.checkCanShowBranches(systemSecurityContext, tableName);
        }
    }

    @Override
    public void checkCanCreateBranch(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName tableName, String branchName)
    {
        if (isCatalogGoverned(tableName.getCatalogName())) {
            delegate.checkCanCreateBranch(systemSecurityContext, tableName, branchName);
        }
    }

    @Override
    public void checkCanDropBranch(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName tableName, String branchName)
    {
        if (isCatalogGoverned(tableName.getCatalogName())) {
            delegate.checkCanDropBranch(systemSecurityContext, tableName, branchName);
        }
    }

    @Override
    public void checkCanFastForwardBranch(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName tableName, String sourceBranchName, String targetBranchName)
    {
        if (isCatalogGoverned(tableName.getCatalogName())) {
            delegate.checkCanFastForwardBranch(systemSecurityContext, tableName, sourceBranchName, targetBranchName);
        }
    }

    @Override
    public void checkCanGrantTableBranchPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, String branchName, TrinoPrincipal grantee, boolean grantOption)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanGrantTableBranchPrivilege(context, privilege, table, branchName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyTableBranchPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, String branchName, TrinoPrincipal grantee)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanDenyTableBranchPrivilege(context, privilege, table, branchName, grantee);
        }
    }

    @Override
    public void checkCanRevokeTableBranchPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, String branchName, TrinoPrincipal revokee, boolean grantOption)
    {
        if (isCatalogGoverned(table.getCatalogName())) {
            delegate.checkCanRevokeTableBranchPrivilege(context, privilege, table, branchName, revokee, grantOption);
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        if (!isCatalogGoverned(tableName.getCatalogName())) {
            return ImmutableList.of();
        }
        return delegate.getRowFilters(context, tableName);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        if (!isCatalogGoverned(tableName.getCatalogName())) {
            return Optional.empty();
        }
        return delegate.getColumnMask(context, tableName, columnName, type);
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(SystemSecurityContext context, CatalogSchemaTableName tableName, List<ColumnSchema> columns)
    {
        if (!isCatalogGoverned(tableName.getCatalogName())) {
            return ImmutableMap.of();
        }
        return delegate.getColumnMasks(context, tableName, columns);
    }

    @Override
    public void checkCanSetEntityAuthorization(SystemSecurityContext context, EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        if (isEntityAuthorizationGoverned(entityKindAndName)) {
            delegate.checkCanSetEntityAuthorization(context, entityKindAndName, principal);
        }
    }
}
