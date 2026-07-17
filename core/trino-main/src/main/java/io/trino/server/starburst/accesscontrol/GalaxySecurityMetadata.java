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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.BadAccessControlRequestException;
import io.starburst.stargate.accesscontrol.client.CreateEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.CreateRoleGrant;
import io.starburst.stargate.accesscontrol.client.EntityAlreadyExistsException;
import io.starburst.stargate.accesscontrol.client.EntityNotFoundException;
import io.starburst.stargate.accesscontrol.client.GalaxyPrincipal;
import io.starburst.stargate.accesscontrol.client.OperationNotAllowedException;
import io.starburst.stargate.accesscontrol.client.PrincipalType;
import io.starburst.stargate.accesscontrol.client.RevokeEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.TableGrant;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.ClusterId;
import io.starburst.stargate.id.ColumnId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SchemaId;
import io.starburst.stargate.id.StorageLocation;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedObjectPrefix;
import io.trino.metadata.QualifiedSchemaPrefix;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.server.starburst.catalogs.CatalogResolver;
import io.trino.server.starburst.security.EntityPropertyManager;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.security.FunctionAuthorization;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.PrivilegeInfo;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SchemaAuthorization;
import io.trino.spi.security.TableAuthorization;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.transaction.TransactionId;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.DENY;
import static io.trino.server.starburst.accesscontrol.EntityPrivilegeTranslator.translateEntityKindAndPrivileges;
import static io.trino.server.starburst.accesscontrol.MetadataAccessControllerSupplier.extractTransactionId;
import static io.trino.server.starburst.security.GalaxyIdentity.createViewOrFunctionOwnerIdentity;
import static io.trino.server.starburst.security.GalaxyIdentity.maybeGetEmbeddedEnabledRoles;
import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.ROLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.CREATE;
import static io.trino.spi.security.Privilege.CREATE_BRANCH;
import static io.trino.spi.security.Privilege.DELETE;
import static io.trino.spi.security.Privilege.INSERT;
import static io.trino.spi.security.Privilege.MANAGE_DATA_OBSERVABILITY;
import static io.trino.spi.security.Privilege.SELECT;
import static io.trino.spi.security.Privilege.UPDATE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class GalaxySecurityMetadata
        implements SystemSecurityMetadata
{
    public static final BiMap<Privilege, io.starburst.stargate.accesscontrol.privilege.Privilege> PRIVILEGE_TRANSLATIONS = ImmutableBiMap.<Privilege, io.starburst.stargate.accesscontrol.privilege.Privilege>builder()
            .put(SELECT, io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT)
            .put(INSERT, io.starburst.stargate.accesscontrol.privilege.Privilege.INSERT)
            .put(DELETE, io.starburst.stargate.accesscontrol.privilege.Privilege.DELETE)
            .put(UPDATE, io.starburst.stargate.accesscontrol.privilege.Privilege.UPDATE)
            .put(CREATE, io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_TABLE)
            .put(MANAGE_DATA_OBSERVABILITY, io.starburst.stargate.accesscontrol.privilege.Privilege.MANAGE_DATA_OBSERVABILITY)
            .buildOrThrow();

    private static final TrinoPrincipal SYSTEM_ROLE = new TrinoPrincipal(ROLE, "_system");
    private static final TrinoPrincipal PUBLIC_ROLE = new TrinoPrincipal(ROLE, "public");

    private final TrinoSecurityApi accessControlClient;
    private final CatalogResolver catalogResolver;
    private final GalaxyAccessControllerSupplier controllerSupplier;
    private final FunctionScopeResolver functionScopeResolver;
    private final CatalogDdlObserver catalogDdlObserver;

    @Inject
    public GalaxySecurityMetadata(TrinoSecurityApi accessControlClient, CatalogResolver catalogResolver, GalaxyAccessControllerSupplier controllerSupplier, FunctionScopeResolver functionScopeResolver, CatalogDdlObserver catalogDdlObserver)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogResolver = requireNonNull(catalogResolver, "catalogResolver is null");
        this.controllerSupplier = requireNonNull(controllerSupplier, "controllerSupplier is null");
        this.functionScopeResolver = requireNonNull(functionScopeResolver, "functionScopeResolver is null");
        this.catalogDdlObserver = requireNonNull(catalogDdlObserver, "catalogDdlObserver is null");
    }

    @Override
    public boolean roleExists(Session session, String role)
    {
        return handleRoleClientError(() -> accessControlClient.roleExists(toDispatchSession(session), new RoleName(role)), role);
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor)
    {
        if (grantor.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support creating a role with an explicit grantor");
        }
        handleRoleClientError(() -> accessControlClient.createRole(toDispatchSession(session), new RoleName(role)), Optional.of(role));
    }

    @Override
    public void dropRole(Session session, String role)
    {
        handleRoleClientError(() -> accessControlClient.dropRole(toDispatchSession(session), new RoleName(role)), Optional.of(role));
    }

    @Override
    public Set<String> listRoles(Session session)
    {
        Identity identity = session.getIdentity();
        return handleClientError(() -> controllerSupplier.apply(identity).listRoles(identity).keySet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet()));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, TrinoPrincipal principal)
    {
        GalaxyPrincipal galaxyPrincipal = toGalaxyPrincipal(principal);
        return toTrinoRoleGrants(handleClientError(() -> accessControlClient.listRoleGrants(toDispatchSession(session), galaxyPrincipal, false)));
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (grantor.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support GRANT with the GRANTED BY clause");
        }

        DispatchSession dispatchSession = toDispatchSession(session);
        ImmutableSet.Builder<CreateRoleGrant> roleGrants = ImmutableSet.builder();
        for (String role : roles) {
            for (TrinoPrincipal grantee : grantees) {
                if (adminOption && grantee.getType() == USER) {
                    throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE for GRANT with ADMIN OPTION");
                }
                RoleName roleName = throwIfInvalidRoleName(role);
                roleGrants.add(new CreateRoleGrant(roleName, toGalaxyPrincipal(grantee), adminOption));
            }
        }

        handleRoleClientError(() -> accessControlClient.grantRoles(dispatchSession, roleGrants.build()), Optional.empty());
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (grantor.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support REVOKE with the GRANTED BY clause");
        }
        ImmutableSet.Builder<io.starburst.stargate.accesscontrol.client.RoleGrant> roleGrants = ImmutableSet.builder();
        for (String role : roles) {
            for (TrinoPrincipal grantee : grantees) {
                RoleName roleName = throwIfInvalidRoleName(role);
                roleGrants.add(new io.starburst.stargate.accesscontrol.client.RoleGrant(toGalaxyPrincipal(grantee), roleName, adminOption));
            }
        }
        handleClientError(() -> accessControlClient.revokeRoles(toDispatchSession(session), roleGrants.build()));
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal)
    {
        GalaxyPrincipal galaxyPrincipal = toGalaxyPrincipal(principal);
        return toTrinoRoleGrants(handleClientError(() -> accessControlClient.listRoleGrants(toDispatchSession(session), galaxyPrincipal, true)));
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        return handleClientError(() -> maybeGetEmbeddedEnabledRoles(identity)
                .orElseGet(() -> controllerSupplier.apply(identity).listEnabledRoles(identity).keySet().stream()
                        .map(RoleName::getName)
                        .collect(toImmutableSet())));
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        addEntityPrivilege(session, toSchemaEntity(session.getTransactionId(), schemaName), privileges, ALLOW, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        addEntityPrivilege(session, toSchemaEntity(session.getTransactionId(), schemaName), privileges, DENY, grantee, false);
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        revokeEntityPrivileges(session, toSchemaEntity(session.getTransactionId(), schemaName), privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        addEntityPrivilege(session, toTableEntity(session.getTransactionId(), tableName), privileges, ALLOW, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        addEntityPrivilege(session, toTableEntity(session.getTransactionId(), tableName), privileges, DENY, grantee, false);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        revokeEntityPrivileges(session, toTableEntity(session.getTransactionId(), tableName), privileges, grantee, grantOption);
    }

    // TODO: Are these methods candidates for inclusion in SystemSecurityMetatadata?

    public void grantColumnPrivileges(Session session, QualifiedObjectName tableName, String columnName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        addEntityPrivilege(session, toColumnEntity(session.getTransactionId(), tableName, columnName), privileges, ALLOW, grantee, grantOption);
    }

    public void addEntityPrivilege(Session session, EntityId entityId, Set<Privilege> privileges, GrantKind allow, TrinoPrincipal grantee, boolean grantOption)
    {
        addEntityPrivileges(session, entityId, () -> privileges.stream()
                .flatMap(privilege -> {
                    // TODO; filter out CREATE_BRANCH for now as not supported by Galaxy (https://github.com/starburstdata/trino-fork-log/issues/176)
                    if (privilege == CREATE_BRANCH) {
                        return Stream.empty();
                    }
                    return Stream.of(toGalaxyPrivilege(privilege));
                })
                .collect(toImmutableSet()), grantee, allow, grantOption);
    }

    public void revokeEntityPrivileges(Session session, EntityId entityId, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        revokeEntityPrivileges(session, entityId, () -> privileges.stream()
                .flatMap(privilege -> {
                    // TODO; filter out CREATE_BRANCH for now as not supported by Galaxy (https://github.com/starburstdata/trino-fork-log/issues/176)
                    if (privilege == CREATE_BRANCH) {
                        return Stream.empty();
                    }
                    return Stream.of(toGalaxyPrivilege(privilege));
                })
                .collect(toImmutableSet()), grantee, grantOption);
    }

    @Override
    public Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        if (isSystemCatalog(prefix.getCatalogName())) {
            return ImmutableSet.of(new GrantInfo(
                    new PrivilegeInfo(SELECT, false),
                    PUBLIC_ROLE,
                    schemaTableName("*", "*"),
                    Optional.empty(),
                    Optional.empty()));
        }
        CatalogId catalogId = translateCatalogNameToId(session.getTransactionId(), prefix.getCatalogName());
        EntityId entityId;
        if (prefix.getTableName().isPresent()) {
            entityId = new TableId(catalogId, prefix.getSchemaName().orElseThrow(), prefix.getTableName().get());
        }
        else if (prefix.getSchemaName().isPresent()) {
            entityId = new SchemaId(catalogId, prefix.getSchemaName().get());
        }
        else {
            entityId = catalogId;
        }

        return handleClientError(() -> accessControlClient.listTableGrants(toDispatchSession(session), entityId)).stream()
                // Trino's GrantInfo does not contain grantKind.  For now, only show ALLOW privileges,
                // pending a change to Trino to pass GrantKind through
                .filter(details -> details.getGrantKind() == ALLOW)
                .filter(details -> PRIVILEGE_TRANSLATIONS.inverse().containsKey(details.getPrivilege()))
                .map(details -> toGrantInfo(details, new SchemaTableName(details.getTableId().getSchemaName(), details.getTableId().getTableName())))
                .collect(toImmutableSet());
    }

    private static GrantInfo toGrantInfo(TableGrant tableGrant, SchemaTableName tableName)
    {
        Privilege privilege = PRIVILEGE_TRANSLATIONS.inverse().get(tableGrant.getPrivilege());
        checkArgument(privilege != null, "Could not find Trino privilege for Galaxy privilege %s, in PrivilegeDetails %s", tableGrant.getPrivilege(), tableGrant);
        return new GrantInfo(
                new PrivilegeInfo(privilege, tableGrant.isGrantOption()),
                new TrinoPrincipal(ROLE, tableGrant.getGrantee().getName()),
                tableName,
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public void grantTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void denyTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schema)
    {
        if (isSystemCatalog(schema.getCatalogName())) {
            return Optional.of(SYSTEM_ROLE);
        }
        Identity identity = session.getIdentity();
        RoleName owner = handleClientError(() -> controllerSupplier.apply(identity).getEntityPrivileges(identity, toSchemaEntity(session.getTransactionId(), schema)).getOwner());
        return Optional.of(new TrinoPrincipal(ROLE, owner.getName()));
    }

    @Override
    public void setEntityOwner(Session session, EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        RoleName roleName = validateOwnerPrincipal(principal);
        List<String> name = entityKindAndName.name();
        String entityKindString = entityKindAndName.entityKind();
        switch (entityKindAndName.entityKind()) {
            case "SCHEMA" -> setSchemaOwner(session, new CatalogSchemaName(name.getFirst(), name.get(1)), principal);
            case "TABLE" -> setTableOwner(session, new CatalogSchemaTableName(name.getFirst(), name.get(1), name.get(2)), principal);
            case "VIEW" -> setViewOwner(session, new CatalogSchemaTableName(name.getFirst(), name.get(1), name.get(2)), principal);
            case "FUNCTION" -> setFunctionOwner(session, new CatalogSchemaFunctionName(name.getFirst(), name.get(1), name.get(2)), principal);
            default -> {
                Optional<EntityKind> optionalEntityKind = EntityPrivilegeTranslator.getEntityKind(entityKindString);
                if (optionalEntityKind.isEmpty()) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Galaxy does not support setting the owner of entity kind %s".formatted(entityKindString));
                }
                EntityKind entityKind = optionalEntityKind.get();
                if (entityKind.entityKindDoesNotAllowOwnership() || entityKind.isContainedKind()) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Galaxy does not support setting the owner of entity kind %s".formatted(entityKindString));
                }

                handleClientError(() -> accessControlClient.setUncontainedEntityOwner(
                        toDispatchSession(session),
                        entityKind,
                        entityKindAndName.name().getFirst(),
                        roleName));
            }
        }
    }

    @VisibleForTesting
    void setSchemaOwner(Session session, CatalogSchemaName schema, TrinoPrincipal owner)
    {
        throwIfSystemCatalog(schema);
        RoleName roleName = validateOwnerPrincipal(owner);
        handleClientError(() -> accessControlClient.setEntityOwner(
                toDispatchSession(session),
                toSchemaEntity(session.getTransactionId(), schema),
                roleName));
    }

    @VisibleForTesting
    void setTableOwner(Session session, CatalogSchemaTableName table, TrinoPrincipal owner)
    {
        throwIfSystemCatalog(table);
        RoleName roleName = validateOwnerPrincipal(owner);
        handleClientError(() -> accessControlClient.setEntityOwner(
                toDispatchSession(session),
                toTableEntity(session.getTransactionId(), new QualifiedObjectName(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName())),
                roleName));
    }

    void setFunctionOwner(Session session, CatalogSchemaFunctionName function, TrinoPrincipal owner)
    {
        throwIfSystemCatalog(function);
        RoleName roleName = validateOwnerPrincipal(owner);
        handleClientError(() -> accessControlClient.setEntityOwner(
                toDispatchSession(session),
                toFunctionEntity(session.getTransactionId(), function),
                roleName));
    }

    /**
     * Create an identity with the synthetic user name, the owner role in the credentials, and
     * enabled roles set consisting only of the owner role.
     */
    @Override
    public Optional<Identity> getViewRunAsIdentity(Session session, CatalogSchemaTableName viewName)
    {
        Identity identity = session.getIdentity();
        EntityPrivileges privileges = handleClientError(() -> controllerSupplier.apply(identity).getEntityPrivileges(identity, toTableEntity(session.getTransactionId(), viewName)));
        if (!privileges.isExplicitOwner()) {
            throw new TrinoException(
                    INVALID_VIEW,
                    format("View '%s' does not have an explicit owner role, which is not allowed. Please define an explicit owner with an 'ALTER VIEW %s SET AUTHORIZATION ROLE' command. For more information, visit docs.starburst.io/starburst-galaxy/security/privileges.html", viewName, viewName));
        }
        return Optional.of(createViewOrFunctionOwnerIdentity(session.getIdentity(), privileges.getOwner(), privileges.getOwnerId()));
    }

    /**
     * Create an identity with the synthetic user name, the owner role in the credentials, and
     * enabled roles set consisting only of the owner role IFF the view has an owner.
     * <p>
     * Only to be used for metadata queries, as this could result in a privilege escalation when used for actual querying.
     */
    public Optional<Identity> getMetadataViewRunAsIdentity(Session session, CatalogSchemaTableName viewName)
    {
        EntityPrivileges privileges = handleClientError(() -> controllerSupplier.apply(session.getIdentity()).getEntityPrivileges(session.getIdentity(), toTableEntity(session.getTransactionId(), viewName)));
        if (!privileges.isExplicitOwner()) {
            return Optional.empty();
        }
        return Optional.of(createViewOrFunctionOwnerIdentity(session.getIdentity(), privileges.getOwner(), privileges.getOwnerId()));
    }

    @VisibleForTesting
    void setViewOwner(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        setTableOwner(session, view, principal);
    }

    @Override
    public Optional<Identity> getFunctionRunAsIdentity(Session session, CatalogSchemaFunctionName functionName)
    {
        // This method is only called for SECURITY DEFINER functions, so return the identity of the function owner
        if (!functionScopeResolver.isFunctionCorrectlyScoped(new CatalogSchemaRoutineName(functionName.catalogName(), functionName.schemaName(), functionName.functionName()))) {
            throw new TrinoException(FUNCTION_NOT_FOUND, "SECURITY DEFINER Function %s must be in schema galaxy.functions".formatted(functionName));
        }
        EntityPrivileges privileges = handleClientError(() -> controllerSupplier.apply(session.getIdentity()).getEntityPrivileges(session.getIdentity(), toFunctionEntity(session.getTransactionId(), functionName)));
        if (!privileges.isExplicitOwner()) {
            return Optional.empty();
        }
        return Optional.of(createViewOrFunctionOwnerIdentity(session.getIdentity(), privileges.getOwner(), privileges.getOwnerId()));
    }

    @Override
    public void catalogCreated(Session session, CatalogName catalog)
    {
        Optional<TransactionId> transactionId = extractTransactionId(session.getIdentity());
        CatalogId catalogId = translateCatalogNameToId(transactionId, catalog.toString());
        handleClientError(() -> catalogDdlObserver.onCatalogCreated(toDispatchSession(session), catalogId));
    }

    @Override
    public void catalogDropped(Session session, CatalogName catalog)
    {
        Optional<TransactionId> transactionId = extractTransactionId(session.getIdentity());
        CatalogId catalogId = translateCatalogNameToId(transactionId, catalog.toString());
        handleClientError(() -> catalogDdlObserver.onCatalogDropped(toDispatchSession(session), catalogId));
    }

    @Override
    public void catalogAltered(Session session, CatalogName catalog)
    {
        // No-op
    }

    @Override
    public void catalogRenamed(Session session, CatalogName sourceCatalog, CatalogName targetCatalog)
    {
        // No changes required since catalog is referred in privileges and ownership by its ID
    }

    @Override
    public void functionCreated(Session session, CatalogSchemaFunctionName function)
    {
        // Galaxy is already alerted to UDF creation via the galaxy catalog connector, and does not support UDFs in other catalogs
        // so nothing needs to happen here
    }

    @Override
    public void functionDropped(Session session, CatalogSchemaFunctionName function)
    {
        // Galaxy is already alerted to UDF deletion via the galaxy catalog connector, and does not support UDFs in other catalogs
        // so nothing needs to happen here
    }

    @Override
    public void schemaCreated(Session session, CatalogSchemaName schema)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(schema);
        handleClientError(() -> accessControlClient.entityCreated(toDispatchSession(session), toSchemaEntity(session.getTransactionId(), schema)));
    }

    @Override
    public void schemaRenamed(Session session, CatalogSchemaName sourceSchema, CatalogSchemaName targetSchema)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(sourceSchema);
        throwIfSystemCatalog(targetSchema);
        handleClientError(() -> accessControlClient.entityRenamed(toDispatchSession(session), toSchemaEntity(session.getTransactionId(), sourceSchema), toSchemaEntity(session.getTransactionId(), targetSchema)));
    }

    @Override
    public void schemaDropped(Session session, CatalogSchemaName schema)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(schema);
        handleClientError(() -> accessControlClient.entityDropped(toDispatchSession(session), toSchemaEntity(session.getTransactionId(), schema)));
    }

    @Override
    public void tableCreated(Session session, CatalogSchemaTableName table)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        handleClientError(() -> accessControlClient.entityCreated(toDispatchSession(session), toTableEntity(session.getTransactionId(), table)));
    }

    @Override
    public void tableRenamed(Session session, CatalogSchemaTableName sourceTable, CatalogSchemaTableName targetTable)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(sourceTable);
        throwIfSystemCatalog(targetTable);
        handleClientError(() -> accessControlClient.entityRenamed(toDispatchSession(session), toTableEntity(session.getTransactionId(), sourceTable), toTableEntity(session.getTransactionId(), targetTable)));
    }

    @Override
    public void tableDropped(Session session, CatalogSchemaTableName table)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        handleClientError(() -> accessControlClient.entityDropped(toDispatchSession(session), toTableEntity(session.getTransactionId(), table)));
    }

    @Override
    public void columnCreated(Session session, CatalogSchemaTableName table, String column)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        handleClientError(() -> accessControlClient.entityCreated(toDispatchSession(session), toColumnEntity(session.getTransactionId(), table, column)));
    }

    @Override
    public void columnRenamed(Session session, CatalogSchemaTableName table, String oldName, String newName)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        handleClientError(() -> accessControlClient.entityRenamed(toDispatchSession(session), toColumnEntity(session.getTransactionId(), table, oldName), toColumnEntity(session.getTransactionId(), table, newName)));
    }

    @Override
    public void columnDropped(Session session, CatalogSchemaTableName table, String column)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        handleClientError(() -> accessControlClient.entityDropped(toDispatchSession(session), toColumnEntity(session.getTransactionId(), table, column)));
    }

    @Override
    public void columnTypeChanged(Session session, CatalogSchemaTableName table, String column, String oldType, String newType)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        handleClientError(() -> accessControlClient.columnTypeChanged(toDispatchSession(session), toColumnEntity(session.getTransactionId(), table, column), oldType, newType));
    }

    @Override
    public void finishStatisticsCollection(Session session, CatalogSchemaTableName table)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        handleClientError(() -> accessControlClient.analyzeTableFinished(toDispatchSession(session), toTableEntity(session.getTransactionId(), table)));
    }

    @Override
    public void columnNotNullConstraintDropped(Session session, CatalogSchemaTableName table, String column)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        accessControlClient.columnNotNullDropped(toDispatchSession(session), toColumnEntity(session.getTransactionId(), table, column));
    }

    @Override
    public Set<EntityPrivilege> getAllEntityKindPrivileges(String entityKind)
    {
        return EntityPrivilegeTranslator.getPrivilegesForEntityKind(entityKind);
    }

    @Override
    public void grantEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        EntityKindAndPrivileges entityKindAndPrivileges = translateEntityKindAndPrivileges(entity.entityKind(), privileges);
        EntityId entityId = translateQualifiedNameToEntityId(session, entity);
        addEntityPrivileges(session, entityId, entityKindAndPrivileges::privileges, grantee, ALLOW, grantOption);
    }

    @Override
    public void denyEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee)
    {
        EntityKindAndPrivileges entityKindAndPrivileges = translateEntityKindAndPrivileges(entity.entityKind(), privileges);
        EntityId entityId = translateQualifiedNameToEntityId(session, entity);
        addEntityPrivileges(session, entityId, entityKindAndPrivileges::privileges, grantee, DENY, false);
    }

    @Override
    public void revokeEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        EntityKindAndPrivileges entityKindAndPrivileges = translateEntityKindAndPrivileges(entity.entityKind(), privileges);
        EntityId entityId = translateQualifiedNameToEntityId(session, entity);
        revokeEntityPrivileges(session, entityId, entityKindAndPrivileges::privileges, grantee, grantOption);
    }

    @Override
    public void validateEntityKindAndPrivileges(Session session, String entityKind, Set<String> privileges)
    {
        Set<EntityPrivilege> entityKindPrivileges = EntityPrivilegeTranslator.getPrivilegesForEntityKind(entityKind);
        for (String privilege : privileges) {
            if (!entityKindPrivileges.contains(new EntityPrivilege(privilege))) {
                throw new TrinoException(INVALID_PRIVILEGE, "Privilege %s does not exist for entity kind %s".formatted(privilege, entityKind));
            }
        }
    }

    @Override
    public Set<SchemaAuthorization> getSchemasAuthorizationInfo(Session session, QualifiedSchemaPrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<TableAuthorization> getTablesAuthorizationInfo(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<FunctionAuthorization> getFunctionsAuthorizationInfo(Session session, QualifiedObjectPrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    // Helper methods
    private static <V> V handleRoleClientError(Supplier<V> routine, String roleName)
    {
        throwIfInvalidRoleName(roleName);
        try {
            return routine.get();
        }
        catch (EntityAlreadyExistsException e) {
            throw new TrinoException(ROLE_ALREADY_EXISTS, "Role %s already exists".formatted(roleName));
        }
        catch (EntityNotFoundException e) {
            throw new TrinoException(ROLE_NOT_FOUND, "Role %s not found".formatted(roleName));
        }
        catch (Exception e) {
            throw toTrinoException(e);
        }
    }

    private static void handleRoleClientError(Runnable routine, Optional<String> roleName)
    {
        roleName.ifPresent(GalaxySecurityMetadata::throwIfInvalidRoleName);
        try {
            routine.run();
        }
        catch (EntityAlreadyExistsException e) {
            throw new TrinoException(ROLE_ALREADY_EXISTS, "Role already exists%s".formatted(roleName.map(": %s"::formatted).orElse("")));
        }
        catch (EntityNotFoundException e) {
            throw new TrinoException(ROLE_NOT_FOUND, "Role not found%s".formatted(roleName.map(": %s"::formatted).orElse("")));
        }
        catch (Exception e) {
            throw toTrinoException(e);
        }
    }

    private static TrinoException toTrinoException(Exception exception)
    {
        List<Throwable> causalChain = getCausalChain(exception);
        if (causalChain.stream().anyMatch(e -> e instanceof OperationNotAllowedException)) {
            throw new TrinoException(PERMISSION_DENIED, exception);
        }
        else if (causalChain.stream().anyMatch(e -> e instanceof EntityNotFoundException)) {
            throw new TrinoException(NOT_FOUND, exception);
        }
        else if (causalChain.stream().anyMatch(e -> e instanceof BadAccessControlRequestException)) {
            throw new TrinoException(INVALID_ARGUMENTS, exception);
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing Galaxy Access Control: " + requireNonNullElse(exception.getMessage(), exception), exception);
    }

    private void addEntityPrivileges(Session session, EntityId entityId, Supplier<Set<io.starburst.stargate.accesscontrol.privilege.Privilege>> galaxyPrivilegesSupplier, TrinoPrincipal grantee, GrantKind grantKind, boolean grantOption)
    {
        if (grantee.getType() != ROLE) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE as a grantee");
        }
        RoleName granteeRole = throwIfInvalidRoleName(grantee.getName());

        Set<CreateEntityPrivilege> entityPrivileges = galaxyPrivilegesSupplier.get().stream()
                .map(privilege -> new CreateEntityPrivilege(privilege, grantKind, granteeRole, grantOption))
                .collect(toImmutableSet());
        handleClientError(() -> accessControlClient.addEntityPrivileges(toDispatchSession(session), entityId, entityPrivileges));
    }

    private void revokeEntityPrivileges(Session session, EntityId entityId, Supplier<Set<io.starburst.stargate.accesscontrol.privilege.Privilege>> galaxyPrivilegesSupplier, TrinoPrincipal grantee, boolean grantOption)
    {
        if (grantee.getType() != ROLE) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE as a grantee");
        }
        RoleName granteeRole = throwIfInvalidRoleName(grantee.getName());

        Set<RevokeEntityPrivilege> entityPrivileges = galaxyPrivilegesSupplier.get().stream()
                .map(privilege -> new RevokeEntityPrivilege(privilege, granteeRole, grantOption))
                .collect(toImmutableSet());
        handleClientError(() -> accessControlClient.revokeEntityPrivileges(toDispatchSession(session), entityId, entityPrivileges));
    }

    private static void handleClientError(Runnable routine)
    {
        try {
            routine.run();
        }
        catch (Exception e) {
            throw toTrinoException(e);
        }
    }

    private static <V> V handleClientError(Supplier<V> routine)
    {
        try {
            return routine.get();
        }
        catch (Exception e) {
            throw toTrinoException(e);
        }
    }

    private static GalaxyPrincipal toGalaxyPrincipal(TrinoPrincipal principal)
    {
        return switch (principal.getType()) {
            case ROLE -> {
                throwIfInvalidRoleName(principal.getName());
                yield new GalaxyPrincipal(PrincipalType.ROLE, principal.getName());
            }
            case USER -> new GalaxyPrincipal(PrincipalType.USER, principal.getName());
        };
    }

    private static TrinoPrincipal toTrinoPrincipal(GalaxyPrincipal principal)
    {
        return switch (principal.getType()) {
            case ROLE -> new TrinoPrincipal(ROLE, principal.getName());
            case USER -> new TrinoPrincipal(USER, principal.getName());
            default -> throw new TrinoException(INVALID_ARGUMENTS, "Unknown Galaxy PrincipalType in principal " + principal);
        };
    }

    private static Set<RoleGrant> toTrinoRoleGrants(Set<io.starburst.stargate.accesscontrol.client.RoleGrant> grants)
    {
        return grants.stream()
                .map(grant -> new RoleGrant(toTrinoPrincipal(grant.getGrantee()), grant.getRoleName().getName(), grant.isGrantable()))
                .collect(toImmutableSet());
    }

    private SchemaId toSchemaEntity(Optional<TransactionId> transactionId, CatalogSchemaName schema)
    {
        return new SchemaId(translateCatalogNameToId(transactionId, schema.getCatalogName()), schema.getSchemaName());
    }

    private TableId toTableEntity(Optional<TransactionId> transactionId, QualifiedObjectName table)
    {
        return toTableEntity(transactionId, new CatalogSchemaTableName(table.catalogName(), table.schemaName(), table.objectName()));
    }

    public TableId toTableEntity(Optional<TransactionId> transactionId, CatalogSchemaTableName table)
    {
        return new TableId(translateCatalogNameToId(transactionId, table.getCatalogName()), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName());
    }

    private FunctionId toFunctionEntity(Optional<TransactionId> transactionId, CatalogSchemaFunctionName table)
    {
        return new FunctionId(translateCatalogNameToId(transactionId, table.catalogName()), table.schemaName(), table.functionName());
    }

    private FunctionId toFunctionEntity(Session session, CatalogSchemaTableName table)
    {
        return new FunctionId(translateCatalogNameToId(session.getTransactionId(), table.getCatalogName()), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName());
    }

    private ColumnId toColumnEntity(Optional<TransactionId> transactionId, QualifiedObjectName table, String columnName)
    {
        return toColumnEntity(transactionId, new CatalogSchemaTableName(table.catalogName(), table.schemaName(), table.objectName()), columnName);
    }

    public ColumnId toColumnEntity(Optional<TransactionId> transactionId, CatalogSchemaTableName table, String columnName)
    {
        return new ColumnId(translateCatalogNameToId(transactionId, table.getCatalogName()), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), columnName);
    }

    private EntityId translateQualifiedNameToEntityId(Session session, EntityKindAndName entityKindAndName)
    {
        EntityKind entityKind = EntityPrivilegeTranslator.translateEntityKind(entityKindAndName.entityKind());
        List<String> parts = entityKindAndName.name();
        Optional<TransactionId> transactionId = session.getTransactionId();
        parts = EntityPropertyManager.fillInMissingNameElements(session, entityKind, parts);
        return switch (entityKind) {
            case CLUSTER -> translateClusterNameToId(session, parts.get(0));
            case CATALOG -> translateCatalogNameToId(transactionId, parts.get(0));
            case LOCATION -> new StorageLocation(parts.get(0));
            case SCHEMA -> toSchemaEntity(transactionId, new CatalogSchemaName(parts.get(0), parts.get(1)));
            case TABLE -> toTableEntity(transactionId, makeCatalogSchemaTableNameFromParts(parts));
            case FUNCTION -> toFunctionEntity(session, makeCatalogSchemaTableNameFromParts(parts));
            case COLUMN -> toColumnEntity(transactionId, makeCatalogSchemaTableNameFromParts(parts), parts.get(3));
            case ACCOUNT -> toDispatchSession(session).getAccountId();
            default -> throw new UnsupportedOperationException("Granting privileges on entity kind %s is not supported".formatted(entityKind));
        };
    }

    private static CatalogSchemaTableName makeCatalogSchemaTableNameFromParts(List<String> parts)
    {
        checkArgument(parts.size() >= 3, "parts %s size %s is > 3", parts, parts.size());
        return new CatalogSchemaTableName(parts.get(0), parts.get(1), parts.get(2));
    }

    private ClusterId translateClusterNameToId(Session session, String clusterName)
    {
        return accessControlClient.getAccountClusterNamesAndIds(toDispatchSession(session)).get(clusterName);
    }

    private CatalogId translateCatalogNameToId(Optional<TransactionId> transactionId, String catalogName)
    {
        if (isSystemCatalog(catalogName)) {
            throw new TrinoException(NOT_SUPPORTED, "System catalog is read-only");
        }
        return catalogResolver.getCatalogId(transactionId, catalogName)
                .orElseThrow(() -> new TrinoException(CATALOG_NOT_FOUND, format("Catalog '%s' does not exist", catalogName)));
    }

    private static io.starburst.stargate.accesscontrol.privilege.Privilege toGalaxyPrivilege(Privilege privilege)
    {
        io.starburst.stargate.accesscontrol.privilege.Privilege obj = PRIVILEGE_TRANSLATIONS.get(privilege);
        return requireNonNull(obj, "Could not find privilege translation");
    }

    private static boolean isSystemCatalog(String catalogName)
    {
        return catalogName.equalsIgnoreCase("system");
    }

    private static void throwIfSystemCatalog(CatalogSchemaTableName table)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            throw new TrinoException(NOT_SUPPORTED, "System catalog is read-only");
        }
    }

    private static void throwIfSystemCatalog(CatalogSchemaFunctionName function)
    {
        if (isSystemCatalog(function.catalogName())) {
            throw new TrinoException(NOT_SUPPORTED, "System catalog is read-only");
        }
    }

    private static void throwIfSystemCatalog(CatalogSchemaName table)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            throw new TrinoException(NOT_SUPPORTED, "System catalog is read-only");
        }
    }

    private static RoleName validateOwnerPrincipal(TrinoPrincipal owner)
    {
        if (owner.getType() != ROLE) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE as an owner");
        }

        return throwIfInvalidRoleName(owner.getName());
    }

    private static RoleName throwIfInvalidRoleName(String role)
    {
        try {
            return new RoleName(role);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_ARGUMENTS, e.getMessage());
        }
    }
}
