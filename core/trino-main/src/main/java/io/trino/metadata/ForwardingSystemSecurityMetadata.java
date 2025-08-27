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
package io.trino.metadata;

import io.trino.Session;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.security.FunctionAuthorization;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SchemaAuthorization;
import io.trino.spi.security.TableAuthorization;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;
import java.util.Set;

public abstract class ForwardingSystemSecurityMetadata
        implements SystemSecurityMetadata
{
    protected abstract SystemSecurityMetadata delegate();

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        return delegate().listEnabledRoles(identity);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal)
    {
        return delegate().listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listRoles(Session session)
    {
        return delegate().listRoles(session);
    }

    @Override
    public boolean roleExists(Session session, String role)
    {
        return delegate().roleExists(session, role);
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor)
    {
        delegate().createRole(session, role, grantor);
    }

    @Override
    public void dropRole(Session session, String role)
    {
        delegate().dropRole(session, role);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, TrinoPrincipal principal)
    {
        return delegate().listRoleGrants(session, principal);
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        delegate().grantRoles(session, roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        delegate().revokeRoles(session, roles, grantees, adminOption, grantor);
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        delegate().denySchemaPrivileges(session, schemaName, privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        delegate().denyTablePrivileges(session, tableName, privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        return delegate().listTablePrivileges(session, prefix);
    }

    @Override
    public void grantTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().grantTableBranchPrivileges(session, tableName, branchName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        delegate().denyTableBranchPrivileges(session, tableName, branchName, privileges, grantee);
    }

    @Override
    public void revokeTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().revokeTableBranchPrivileges(session, tableName, branchName, privileges, grantee, grantOption);
    }

    @Override
    public Set<EntityPrivilege> getAllEntityKindPrivileges(String entityKind)
    {
        return delegate().getAllEntityKindPrivileges(entityKind);
    }

    @Override
    public void grantEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().grantEntityPrivileges(session, entity, privileges, grantee, grantOption);
    }

    @Override
    public void denyEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee)
    {
        delegate().denyEntityPrivileges(session, entity, privileges, grantee);
    }

    @Override
    public void revokeEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().revokeEntityPrivileges(session, entity, privileges, grantee, grantOption);
    }

    @Override
    public void validateEntityKindAndPrivileges(Session session, String entityKind, Set<String> privileges)
    {
        delegate().validateEntityKindAndPrivileges(session, entityKind, privileges);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schema)
    {
        return delegate().getSchemaOwner(session, schema);
    }

    @Override
    public Optional<Identity> getViewRunAsIdentity(Session session, CatalogSchemaTableName viewName)
    {
        return delegate().getViewRunAsIdentity(session, viewName);
    }

    @Override
    public Optional<Identity> getFunctionRunAsIdentity(Session session, CatalogSchemaFunctionName functionName)
    {
        return delegate().getFunctionRunAsIdentity(session, functionName);
    }

    @Override
    public void catalogCreated(Session session, CatalogName catalog)
    {
        delegate().catalogCreated(session, catalog);
    }

    @Override
    public void catalogDropped(Session session, CatalogName catalog)
    {
        delegate().catalogDropped(session, catalog);
    }

    @Override
    public void functionCreated(Session session, CatalogSchemaFunctionName function)
    {
        delegate().functionCreated(session, function);
    }

    @Override
    public void functionDropped(Session session, CatalogSchemaFunctionName function)
    {
        delegate().functionDropped(session, function);
    }

    @Override
    public void schemaCreated(Session session, CatalogSchemaName schema)
    {
        delegate().schemaCreated(session, schema);
    }

    @Override
    public void schemaRenamed(Session session, CatalogSchemaName sourceSchema, CatalogSchemaName targetSchema)
    {
        delegate().schemaRenamed(session, sourceSchema, targetSchema);
    }

    @Override
    public void schemaDropped(Session session, CatalogSchemaName schema)
    {
        delegate().schemaDropped(session, schema);
    }

    @Override
    public void tableCreated(Session session, CatalogSchemaTableName table)
    {
        delegate().tableCreated(session, table);
    }

    @Override
    public void tableRenamed(Session session, CatalogSchemaTableName sourceTable, CatalogSchemaTableName targetTable)
    {
        delegate().tableRenamed(session, sourceTable, targetTable);
    }

    @Override
    public void tableDropped(Session session, CatalogSchemaTableName table)
    {
        delegate().tableDropped(session, table);
    }

    @Override
    public void columnCreated(Session session, CatalogSchemaTableName table, String column)
    {
        delegate().columnCreated(session, table, column);
    }

    @Override
    public void columnRenamed(Session session, CatalogSchemaTableName table, String oldName, String newName)
    {
        delegate().columnRenamed(session, table, oldName, newName);
    }

    @Override
    public void columnDropped(Session session, CatalogSchemaTableName table, String column)
    {
        delegate().columnDropped(session, table, column);
    }

    @Override
    public void columnTypeChanged(Session session, CatalogSchemaTableName table, String column, String oldType, String newType)
    {
        delegate().columnTypeChanged(session, table, column, oldType, newType);
    }

    @Override
    public void columnNotNullConstraintDropped(Session session, CatalogSchemaTableName table, String column)
    {
        delegate().columnNotNullConstraintDropped(session, table, column);
    }

    @Override
    public void setEntityOwner(Session session, EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        delegate().setEntityOwner(session, entityKindAndName, principal);
    }

    @Override
    public Set<SchemaAuthorization> getSchemasAuthorizationInfo(Session session, QualifiedSchemaPrefix prefix)
    {
        return delegate().getSchemasAuthorizationInfo(session, prefix);
    }

    @Override
    public Set<TableAuthorization> getTablesAuthorizationInfo(Session session, QualifiedTablePrefix prefix)
    {
        return delegate().getTablesAuthorizationInfo(session, prefix);
    }

    @Override
    public Set<FunctionAuthorization> getFunctionsAuthorizationInfo(Session session, QualifiedObjectPrefix prefix)
    {
        return delegate().getFunctionsAuthorizationInfo(session, prefix);
    }
}
