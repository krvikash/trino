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
import io.starburst.stargate.accesscontrol.client.ColumnMaskExpression;
import io.starburst.stargate.accesscontrol.client.ColumnMaskType;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionDetails;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.TypedColumnMaskExpression;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GalaxyPrivilegeInfo;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.server.starburst.accesscontrol.GalaxyPermissionsCache.GalaxyQueryPermissions;
import io.trino.server.starburst.catalogs.CatalogResolver;
import io.trino.server.starburst.security.GalaxyIdentity;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.ViewExpression;
import io.trino.transaction.TransactionId;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.EXECUTE;
import static io.trino.server.starburst.security.GalaxyIdentity.getContextRoleId;
import static io.trino.server.starburst.security.GalaxyIdentity.getRowFilterAndColumnMaskUserString;
import static io.trino.server.starburst.security.GalaxyIdentity.isDelegateIdentityEncoded;
import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class GalaxySystemAccessController
        implements GalaxyAccessControllerApi
{
    private final TrinoSecurityApi accessControlClient;
    private final CatalogResolver catalogResolver;
    private final GalaxyPermissionsCache galaxyPermissionsCache;
    private final GalaxySystemAccessControlConfig systemAccessControlConfig;
    private final Optional<TransactionId> transactionId;

    @Inject
    public GalaxySystemAccessController(TrinoSecurityApi accessControlClient, CatalogResolver catalogResolver, GalaxyPermissionsCache galaxyPermissionsCache, GalaxySystemAccessControlConfig systemAccessControlConfig)
    {
        this(accessControlClient, catalogResolver, galaxyPermissionsCache, systemAccessControlConfig, Optional.empty());
    }

    public GalaxySystemAccessController(TrinoSecurityApi accessControlClient, CatalogResolver catalogResolver, GalaxyPermissionsCache galaxyPermissionsCache, GalaxySystemAccessControlConfig systemAccessControlConfig, Optional<TransactionId> transactionId)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogResolver = requireNonNull(catalogResolver, "catalogResolver is null");
        this.galaxyPermissionsCache = requireNonNull(galaxyPermissionsCache, "galaxyPermissionsCache is null");
        this.systemAccessControlConfig = requireNonNull(systemAccessControlConfig, "systemAccessControlConfig is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
    }

    @Override
    public Optional<CatalogId> getCatalogId(String catalogName)
    {
        return catalogResolver.getCatalogId(transactionId, catalogName);
    }

    @Override
    public boolean isReadOnlyCatalog(String catalogName)
    {
        return catalogResolver.isReadOnlyCatalog(transactionId, catalogName);
    }

    @Override
    public Optional<SharedSchemaNameAndAccepted> getSharedCatalogSchemaName(String catalogName)
    {
        return catalogResolver.getSharedSchemaForCatalog(transactionId, catalogName);
    }

    /**
     * @see #getEntityPrivileges(Identity, EntityId)
     */
    @Override
    public EntityPrivileges getEntityPrivileges(SystemSecurityContext context, EntityId entity)
    {
        boolean isDelegateRequest = isDelegateIdentityEncoded(context.getIdentity());
        return getCache(context).getEntityPrivileges(getContextRoleId(context.getIdentity()), entity, isDelegateRequest);
    }

    /**
     * Equivalent of {@link #getEntityPrivileges(SystemSecurityContext, EntityId)} but without caching.
     * To be used when caching is not possible due to lack of query ID.
     *
     * @see #getEntityPrivileges(SystemSecurityContext, EntityId)
     */
    @Override
    public EntityPrivileges getEntityPrivileges(Identity identity, EntityId entity)
    {
        return getEntityPrivileges(identity, entity, isDelegateIdentityEncoded(identity));
    }

    private EntityPrivileges getEntityPrivileges(Identity identity, EntityId entity, boolean delegateRequest)
    {
        return handleClientError(() -> accessControlClient.getEntityPrivileges(toDispatchSession(identity), getContextRoleId(identity), entity, delegateRequest));
    }

    @Override
    public Map<RoleName, RoleId> listEnabledRoles(Identity identity)
    {
        return listEnabledRoles(identity, GalaxyIdentity::toDispatchSession);
    }

    @Override
    public Map<RoleName, RoleId> listEnabledRoles(Identity identity, Function<Identity, DispatchSession> sessionCreator)
    {
        return handleClientError(() -> accessControlClient.listEnabledRoles(sessionCreator.apply(identity)));
    }

    @Override
    public Map<RoleName, RoleId> listRoles(Identity identity)
    {
        return accessControlClient.listRoles(toDispatchSession(identity));
    }

    @Override
    public Predicate<String> getCatalogVisibility(SystemSecurityContext context, Set<String> requestedCatalogs)
    {
        Set<CatalogId> requestedCatalogIds = requestedCatalogs.stream()
                .flatMap(catalogName -> catalogResolver.getCatalogId(transactionId, catalogName).stream())
                .collect(toImmutableSet());
        Predicate<CatalogId> catalogVisibility = getCache(context).getCatalogVisibility(requestedCatalogIds);
        return catalogName -> {
            checkArgument(requestedCatalogs.contains(catalogName), "Unexpected catalog checked for visibility: %s, expected one of: %s", catalogName, requestedCatalogs);
            return catalogResolver.getCatalogId(transactionId, catalogName)
                    .map(catalogVisibility::test)
                    .orElse(false);
        };
    }

    @Override
    public void implyCatalogVisibility(SystemSecurityContext context, String catalogName)
    {
        getCache(context).implyCatalogVisibility(catalogName);
    }

    @Override
    public boolean hasImpliedCatalogVisibility(SystemSecurityContext context, String catalogName)
    {
        return getCache(context).hasImpliedCatalogVisibility(catalogName);
    }

    @Override
    public AccountId getAccountId(Identity identity)
    {
        return toDispatchSession(identity).getAccountId();
    }

    @Override
    public Predicate<String> getVisibilityForSchemas(SystemSecurityContext context, CatalogId catalogId, Set<String> schemaNames)
    {
        // This is only called once per query, so no need to cache  TODO: I wonder if this is true?
        checkArgument(!schemaNames.contains("information_schema"), "Unexpected schema names: %s", schemaNames);
        if (schemaNames.isEmpty()) {
            return schemaName -> {
                throw new UnsupportedOperationException("Cannot provide visibility for schema when no schema names were provided: " + schemaName);
            };
        }
        ContentsVisibility visibility = handleClientError(() -> accessControlClient.getVisibilityForSchemas(toDispatchSession(context.getIdentity()), catalogId, schemaNames));
        return schemaName -> {
            checkArgument(schemaNames.contains(schemaName), "Invalid schema name consulted in predicate constructed for %s: %s", schemaNames, schemaName);
            return visibility.isVisible(schemaName);
        };
    }

    @Override
    public ContentsVisibility getVisibilityForTables(SystemSecurityContext context, CatalogId catalogId, String schemaName, Set<String> tableNames)
    {
        return getCache(context).getVisibilityForTables(catalogId, schemaName, tableNames);
    }

    @Override
    public String getRoleDisplayName(Identity identity, RoleId roleId)
    {
        // Not cached because this is used for error messages only, so at most once per query.
        return handleClientError(() -> accessControlClient.listRoles(toDispatchSession(identity))).entrySet().stream()
                .filter(entry -> roleId.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .findFirst()
                .map(RoleName::toString)
                .orElse(roleId.toString());
    }

    @Override
    public boolean shouldFilterOtherUsers()
    {
        return systemAccessControlConfig.getSystemRuntimeFilterOtherUsers();
    }

    @Override
    public boolean canUseLocation(SystemSecurityContext context, String location)
    {
        return accessControlClient.canUseLocation(toDispatchSession(context.getIdentity()), location);
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext context, FunctionId functionId)
    {
        return getAvailableFunctions(context).stream()
                .anyMatch(function -> functionId.equals(function.functionId()) &&
                        function.galaxyPrivileges().stream().map(GalaxyPrivilegeInfo::getPrivilege).anyMatch(privilege -> privilege == EXECUTE));
    }

    @Override
    public boolean isUserDefinedFunctionVisible(SystemSecurityContext context, FunctionId functionId)
    {
        return getAvailableFunctions(context).stream().anyMatch(function -> functionId.equals(function.functionId()));
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, TableId tableId)
    {
        return getEntityPrivileges(context, tableId).getRowFilters().stream()
                .map(filter -> {
                    ViewExpression.Builder builder = ViewExpression.builder();

                    getRowFilterAndColumnMaskUserString(context.getIdentity(), filter.owningRoleId()).ifPresent(builder::identity);
                    catalogResolver.getCatalogName(transactionId, tableId.getCatalogId()).ifPresent(builder::catalog);

                    return builder.schema(tableId.getSchemaName())
                            .expression(filter.expression())
                            .build();
                })
                .collect(toImmutableList());
    }

    /**
     * Return the ViewExpression for the column mask corresponding to the columnName,
     * or Optional.empty() if none exists.  If the specific columnName isn't found,
     * look up the wildcard columnName "*".  Right now Trino supports at most one
     * column mask for any column.
     */
    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, ColumnSchema columnSchema, TableId tableId)
    {
        return getColumnMask(
                context,
                columnSchema,
                tableId,
                () -> getEntityPrivileges(context, tableId),
                () -> catalogResolver.getCatalogName(transactionId, tableId.getCatalogId()));
    }

    @Override
    public Set<GalaxyLanguageFunctionDetails> getAvailableFunctions(SystemSecurityContext context)
    {
        return getCache(context).getAvailableFunctions(getContextRoleId(context.getIdentity()));
    }

    @Override
    public Optional<Boolean> isLiveTableStopped(SystemSecurityContext context, TableId tableId)
    {
        return galaxyPermissionsCache.isLiveTableStopped(accessControlClient, tableId, toDispatchSession(context.getIdentity()));
    }

    @Override
    public boolean isGalaxyEntityPrivilegesEnabled()
    {
        return systemAccessControlConfig.isGalaxyEntityPrivilegesEnabled();
    }

    @Override
    public Set<SchemaTableName> getAlwaysVisibleSystemTables(String catalogName)
    {
        return catalogResolver.getAlwaysVisibleSystemTables(transactionId, catalogName);
    }

    private GalaxyQueryPermissions getCache(SystemSecurityContext context)
    {
        return galaxyPermissionsCache.getCache(accessControlClient, context.getQueryId(), toDispatchSession(context.getIdentity()));
    }

    private static <V> V handleClientError(Supplier<V> routine)
    {
        try {
            return routine.get();
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing Galaxy Access Control: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    static Optional<ViewExpression> getColumnMask(
            SystemSecurityContext context,
            ColumnSchema columnSchema,
            TableId tableId,
            Supplier<EntityPrivileges> entityPrivilegesSupplier,
            Supplier<Optional<String>> catalogNameSupplier)
    {
        EntityPrivileges privileges = entityPrivilegesSupplier.get();
        Map<String, Set<TypedColumnMaskExpression>> masks = privileges.getTypedColumnMasks();
        Set<TypedColumnMaskExpression> typedMasks = masks.getOrDefault(columnSchema.getName(), masks.get("*"));
        if (typedMasks == null || typedMasks.isEmpty()) {
            return Optional.empty();
        }

        Optional<TypedColumnMaskExpression> exactMatchingExpression = Optional.empty();
        Optional<TypedColumnMaskExpression> adjacentMatchingExpression = Optional.empty();
        Iterator<TypedColumnMaskExpression> iterator = typedMasks.iterator();

        // Choosing the right column mask:
        // 1. If the types match exactly, we know we want that one.
        // 2. If the types are known to be coercible, then we can use that mask.
        // 3. ANY acts as a catch-all in case there are no types that are known to match.
        while (exactMatchingExpression.isEmpty() && iterator.hasNext()) {
            TypedColumnMaskExpression expression = iterator.next();
            Set<String> coercibleTypes = expression.columnMaskType().getCompatibleWith()
                    .stream().map(ColumnMaskType::getDisplayName)
                    .collect(toImmutableSet());

            // if the type matches exactly, choose it. We know we want that one.
            if (columnSchema.getType().getBaseName().equalsIgnoreCase(expression.columnMaskType().getDisplayName())) {
                exactMatchingExpression = Optional.of(expression);
            }
            // pick a coercible type as the second priority
            else if (coercibleTypes.contains(columnSchema.getType().getBaseName())
                    && (adjacentMatchingExpression.isEmpty() || adjacentMatchingExpression.get().columnMaskType() == ColumnMaskType.ANY)) {
                adjacentMatchingExpression = Optional.of(expression);
            }
            // ANY is only applicable if all else fails
            else if (adjacentMatchingExpression.isEmpty() && expression.columnMaskType() == ColumnMaskType.ANY) {
                adjacentMatchingExpression = Optional.of(expression);
            }
        }

        Optional<TypedColumnMaskExpression> finalAdjacentMatchingExpression = adjacentMatchingExpression;
        Optional<TypedColumnMaskExpression> expressionForType = exactMatchingExpression.or(() -> finalAdjacentMatchingExpression);
        if (expressionForType.isEmpty()) {
            return Optional.empty();
        }

        ColumnMaskExpression columnMask = expressionForType.get().toColumnMaskExpression();

        ViewExpression.Builder builder = ViewExpression.builder();

        getRowFilterAndColumnMaskUserString(context.getIdentity(), columnMask.owningRoleId()).ifPresent(builder::identity);
        catalogNameSupplier.get().ifPresent(builder::catalog);

        return Optional.of(builder.schema(tableId.getSchemaName())
                .expression(String.format("try_cast(%s as %s)", columnMask.expression(), columnSchema.getType().getDisplayName()))
                .build());
    }
}
