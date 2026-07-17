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

import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionDetails;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.server.starburst.accesscontrol.GalaxyAccountPermissionsCache.CacheKeyAndResultInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.ViewExpression;
import io.trino.sql.analyzer.Analysis;
import io.trino.transaction.TransactionId;

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
import static io.trino.server.starburst.accesscontrol.GalaxyAccountPermissionsCache.maybeLog;
import static io.trino.server.starburst.security.GalaxyIdentity.getContextRoleId;
import static io.trino.server.starburst.security.GalaxyIdentity.getRowFilterAndColumnMaskUserString;
import static io.trino.server.starburst.security.GalaxyIdentity.isDelegateIdentityEncoded;
import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

/**
 * Instances of this class are created for each transaction.  The instances
 * share the GalaxyAccountPermissionsCache
 */
public class GalaxySharedCacheAccessController
        implements GalaxyAccessControllerApi
{
    private static final Logger log = Logger.get(GalaxySharedCacheAccessController.class);
    private final TransactionId transactionId;

    private final GalaxyAccountPermissionsCache cache;
    private final GalaxySystemAccessControlConfig systemAccessControlConfig;

    public GalaxySharedCacheAccessController(GalaxyAccountPermissionsCache cache, TransactionId transactionId, GalaxySystemAccessControlConfig systemAccessControlConfig)
    {
        this.cache = requireNonNull(cache, "cache is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.systemAccessControlConfig = requireNonNull(systemAccessControlConfig, "systemAccessControlConfig is null");
    }

    /**
     * Run analysis and validate references to the shared cache.  If validation returned
     * no invalid results, return the Analysis object.  Otherwise, insert the new values
     * in the cache and run analysis again, regardless of whether analysis raised an exception.
     * <p>
     * If there was an exception during the first pass of analysis, and validation shows
     * that all cached results were up-to-date, throw the exception without repeating analysis.
     * <p>
     * After running the second pass of analysis, don't validate the cache references,
     * since they were known to be correct after the first pass validation.
     * <p>
     * If there was an exception during the second pass of analysis, throw it.
     * Otherwise, return the Analysis object.
     */
    public Analysis performStatementAnalysis(Session session, Supplier<Analysis> analyzer)
    {
        DispatchSession dispatchSession = toDispatchSession(session);
        Analysis analysis = null;
        RuntimeException exceptionDuringAnalysis = null;
        boolean isTrinoException = false;

        try {
            try {
                maybeLog("Calling analyzer.get() for transactionId %s.", transactionId);
                analysis = analyzer.get();
            }
            // TODO: Can we be more restrictive?  What exceptions can be raised due toz
            //       obsolete cached access control results?
            catch (RuntimeException e) {
                isTrinoException = Throwables.getCausalChain(e).stream()
                        .anyMatch(TrinoException.class::isInstance);
                log.warn("%s during statement analysis: %s", e.getClass().getName(), e.getMessage());
                cache.incrementAnalysisExceptionsFirstPass();
                exceptionDuringAnalysis = e;
            }

            List<CacheKeyAndResultInfo> cacheReferences = cache.getTransactionCacheReferences(transactionId);
            List<CacheKeyAndResultInfo> updatedReferences = validateCachedResultsAndApplyUpdates(transactionId, dispatchSession, cacheReferences, exceptionDuringAnalysis != null);

            // If the cache was up-to-date
            if (updatedReferences.isEmpty()) {
                if (exceptionDuringAnalysis != null) {
                    // An exception thrown with an up-to-date cache must be propagated out the user
                    cache.incrementAnalysisExceptionsWithUpToDateCache();
                    throw exceptionDuringAnalysis;
                }
                return analysis;
            }

            if (isTrinoException) {
                // informational log only for TrinoExceptions to understand how often
                // we will re-run USER_ERROR
                log.info("TrinoException with code: %d type: %s is being re-run.",
                        ((TrinoException) exceptionDuringAnalysis).getErrorCode().getCode(),
                        ((TrinoException) exceptionDuringAnalysis).getErrorCode().getType().name());
            }

            cache.clearImpliedCatalogVisibility(transactionId);

            if (exceptionDuringAnalysis != null) {
                // Since there was an exception during analysis, and analysis made references
                // to cached elements, don't read from the cache for the second time we run
                // analysis
                cache.dontReadFromCacheForTransactionId(transactionId);
            }
            // Since the cache wasn't up-to-date, repeat analysis and return the result.  There is no need to check
            // that the cached values are valid, since that was checked after the first call to analyzer.get().
            // Any exception should be propagated back to the user.
            cache.incrementAnalysisReruns();
            maybeLog("Calling analyzer.get() again for transaction %s", transactionId);
            return analyzer.get();
        }
        finally {
            // We must explicitly remove the TransactionId history
            cache.removeTransactionCacheReferences(transactionId);
        }
    }

    private List<CacheKeyAndResultInfo> validateCachedResultsAndApplyUpdates(
            TransactionId transactionId,
            DispatchSession session,
            List<CacheKeyAndResultInfo> cacheReferences,
            boolean exceptionDuringAnalysis)
    {
        if (cacheReferences.isEmpty()) {
            return cacheReferences;
        }
        return cache.validateCachedResultsAndApplyUpdates(transactionId, session, cacheReferences, exceptionDuringAnalysis);
    }

    @Override
    public Optional<CatalogId> getCatalogId(String catalogName)
    {
        return cache.getCatalogId(transactionId, catalogName);
    }

    @Override
    public boolean isReadOnlyCatalog(String catalogName)
    {
        return cache.isReadOnlyCatalog(transactionId, catalogName);
    }

    @Override
    public Optional<SharedSchemaNameAndAccepted> getSharedCatalogSchemaName(String catalogName)
    {
        return cache.getCatalogResolver().getSharedSchemaForCatalog(Optional.of(transactionId), catalogName);
    }

    @Override
    public EntityPrivileges getEntityPrivileges(SystemSecurityContext context, EntityId entity)
    {
        return getEntityPrivileges(transactionId, context.getIdentity(), entity);
    }

    public EntityPrivileges getEntityPrivileges(TransactionId transactionId, Identity identity, EntityId entity)
    {
        boolean isDelegateRequest = isDelegateIdentityEncoded(identity);
        return cache.getEntityPrivileges(transactionId, toDispatchSession(identity), getContextRoleId(identity), entity, isDelegateRequest);
    }

    @Override
    public EntityPrivileges getEntityPrivileges(Identity identity, EntityId entity)
    {
        return getEntityPrivileges(transactionId, identity, entity);
    }

    public Map<RoleName, RoleId> listEnabledRoles(TransactionId transactionId, Identity identity)
    {
        DispatchSession session = toDispatchSession(identity);
        return cache.listEnabledRoles(transactionId, session);
    }

    @Override
    public Map<RoleName, RoleId> listEnabledRoles(Identity identity)
    {
        return cache.listEnabledRoles(transactionId, toDispatchSession(identity));
    }

    @Override
    public Map<RoleName, RoleId> listEnabledRoles(Identity identity, Function<Identity, DispatchSession> sessionCreator)
    {
        return cache.listEnabledRoles(transactionId, sessionCreator.apply(identity));
    }

    @Override
    public Map<RoleName, RoleId> listRoles(Identity identity)
    {
        return cache.listRoles(transactionId, toDispatchSession(identity));
    }

    @Override
    public Predicate<String> getCatalogVisibility(SystemSecurityContext context, Set<String> requestedCatalogs)
    {
        Set<CatalogId> requestedCatalogIds = requestedCatalogs.stream()
                .flatMap(catalogName -> cache.getCatalogId(transactionId, catalogName).stream())
                .collect(toImmutableSet());
        DispatchSession session = toDispatchSession(context.getIdentity());
        ContentsVisibility catalogVisibility = cache.getCatalogVisibility(transactionId, session, requestedCatalogIds);
        return catalogName -> {
            checkArgument(requestedCatalogs.contains(catalogName), "Unexpected catalog checked for visibility: %s, expected one of: %s", catalogName, requestedCatalogs);
            return cache.getCatalogId(transactionId, catalogName)
                    .map(catalogId -> catalogVisibility.isVisible(catalogId.getBaseEntityIdString()))
                    .orElse(false);
        };
    }

    @Override
    public void implyCatalogVisibility(SystemSecurityContext context, String catalogName)
    {
        cache.implyCatalogVisibility(transactionId, catalogName);
    }

    @Override
    public boolean hasImpliedCatalogVisibility(SystemSecurityContext context, String catalogName)
    {
        return cache.hasImpliedCatalogVisibility(transactionId, catalogName);
    }

    @Override
    public AccountId getAccountId(Identity identity)
    {
        return toDispatchSession(identity).getAccountId();
    }

    @Override
    public Predicate<String> getVisibilityForSchemas(SystemSecurityContext context, CatalogId catalogId, Set<String> schemaNames)
    {
        DispatchSession session = toDispatchSession(context.getIdentity());
        return cache.getVisibilityForSchemas(transactionId, session, catalogId, schemaNames)::isVisible;
    }

    @Override
    public ContentsVisibility getVisibilityForTables(SystemSecurityContext context, CatalogId catalogId, String schemaName, Set<String> tableNames)
    {
        return cache.getVisibilityForTables(transactionId, toDispatchSession(context.getIdentity()), catalogId, schemaName, tableNames);
    }

    @Override
    public String getRoleDisplayName(Identity identity, RoleId roleId)
    {
        return cache.listRoles(transactionId, toDispatchSession(identity)).entrySet().stream()
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
        DispatchSession session = toDispatchSession(context.getIdentity());
        return cache.canUseLocation(transactionId, session, location);
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext context, FunctionId functionId)
    {
        DispatchSession session = toDispatchSession(context.getIdentity());
        return cache.canExecuteFunction(transactionId, session, functionId);
    }

    @Override
    public boolean isUserDefinedFunctionVisible(SystemSecurityContext context, FunctionId functionId)
    {
        DispatchSession session = toDispatchSession(context.getIdentity());
        return cache.getAvailableFunctions(transactionId, session).stream()
                .anyMatch(functionDetails -> functionDetails.functionId().equals(functionId));
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, TableId tableId)
    {
        return getEntityPrivileges(context, tableId).getRowFilters().stream()
                .map(filter -> {
                    ViewExpression.Builder builder = ViewExpression.builder();

                    getRowFilterAndColumnMaskUserString(context.getIdentity(), filter.owningRoleId()).ifPresent(builder::identity);
                    cache.getCatalogName(transactionId, tableId.getCatalogId()).ifPresent(builder::catalog);

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
        return GalaxySystemAccessController.getColumnMask(
                context,
                columnSchema,
                tableId,
                () -> getEntityPrivileges(context, tableId),
                () -> cache.getCatalogName(transactionId, tableId.getCatalogId()));
    }

    @Override
    public Set<GalaxyLanguageFunctionDetails> getAvailableFunctions(SystemSecurityContext context)
    {
        return cache.getAvailableFunctions(transactionId, toDispatchSession(context.getIdentity()));
    }

    @Override
    public Optional<Boolean> isLiveTableStopped(SystemSecurityContext context, TableId tableId)
    {
        return cache.isLiveTableStopped(tableId, toDispatchSession(context.getIdentity()));
    }

    @Override
    public boolean isGalaxyEntityPrivilegesEnabled()
    {
        return systemAccessControlConfig.isGalaxyEntityPrivilegesEnabled();
    }

    @Override
    public Set<SchemaTableName> getAlwaysVisibleSystemTables(String catalogName)
    {
        return cache.getAlwaysVisibleSystemTables(transactionId, catalogName);
    }
}
