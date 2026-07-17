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

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.stargate.accesscontrol.cache.CacheKeyAndResult;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.CanExecuteFunction;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.CanUseLocation;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.GetAvailableFunctions;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.GetCatalogVisibility;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.GetEntityPrivileges;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.GetVisibilityForSchemas;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.GetVisibilityForTables;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.ListEnabledRoles;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.ListRoles;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.RoleExists;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeResult;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeResult.GetVisibilityForTablesResult;
import io.starburst.stargate.accesscontrol.cache.TrinoSecurityCacheKey;
import io.starburst.stargate.accesscontrol.cache.UserIdAndRoleId;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionDetails;
import io.starburst.stargate.accesscontrol.client.OperationNotAllowedException;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.NonKeyEvictableCache;
import io.trino.server.starburst.catalogs.CatalogResolver;
import io.trino.spi.connector.SchemaTableName;
import io.trino.transaction.TransactionId;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static io.trino.server.starburst.accesscontrol.GalaxySystemAccessControlConfig.DEFAULT_CACHE_EXPIRATION;
import static java.util.Objects.requireNonNull;

/**
 * Holds cached values for a single AccountId
 */
public class GalaxyAccountPermissionsCache
{
    /**
     * This controls logging only for local debugging.  It should never be set to true
     * in a commit.
     */
    private static final Logger log = Logger.get(GalaxyAccountPermissionsCache.class);
    private final CatalogResolver catalogResolver;
    private final TrinoSecurityApi client;
    private final NonKeyEvictableCache<TrinoSecurityCacheKey, TrinoPrivilegeResultInfo> accountPermissionsCache;
    private final Set<TransactionId> transactionIdsNotUsingTheCache = ConcurrentHashMap.newKeySet();
    private final Map<TransactionId, Instant> transactionIdsPerformingAnalysis = new ConcurrentHashMap<>();

    private final Map<TransactionId, TransactionIdResults> cachedTransactionIdResults = new ConcurrentHashMap<>();

    private final Object validationStatisticsLock = new Object();
    @GuardedBy("validationStatisticsLock")
    private final ValidationStatistics validationStatistics = new ValidationStatistics();
    private final IsIcehouseLiveTableCache isLiveTableCache;

    public record TrinoPrivilegeResultInfo(TrinoPrivilegeResult result, Instant fetchedTime)
    {
        public TrinoPrivilegeResultInfo
        {
            requireNonNull(result, "result is null");
            requireNonNull(fetchedTime, "fetchedTime is null");
        }

        static TrinoPrivilegeResultInfo toPrivilegeResultInfo(TrinoPrivilegeResult result)
        {
            return toPrivilegeResultInfo(result, Instant.now());
        }

        static TrinoPrivilegeResultInfo toPrivilegeResultInfo(TrinoPrivilegeResult result, Instant fetchedTime)
        {
            return new TrinoPrivilegeResultInfo(result, fetchedTime);
        }
    }

    @Inject
    public GalaxyAccountPermissionsCache(CatalogResolver catalogResolver, GalaxySystemAccessControlConfig accessControlConfig, TrinoSecurityApi client)
    {
        this(accessControlConfig.getPermissionsCacheExpireAfterWriteDuration().orElse(DEFAULT_CACHE_EXPIRATION), catalogResolver, client, accessControlConfig.getAccessControlMode());
    }

    public CatalogResolver getCatalogResolver()
    {
        return catalogResolver;
    }

    public GalaxyAccountPermissionsCache(Duration expireAfterWriteDuration, CatalogResolver catalogResolver, TrinoSecurityApi client, GalaxySystemAccessControlConfig.AccessControlMode accessControlMode)
    {
        this.catalogResolver = requireNonNull(catalogResolver, "catalogResolver is null");
        this.client = requireNonNull(client, "client is null");
        accountPermissionsCache = buildNonEvictableCacheWithWeakInvalidateAll(
                CacheBuilder.newBuilder()
                        .recordStats()
                        .expireAfterWrite(expireAfterWriteDuration));
        isLiveTableCache = IsIcehouseLiveTableCache.create(accessControlMode);
    }

    public List<CacheKeyAndResultInfo> getTransactionCacheReferences(TransactionId transactionId)
    {
        TransactionIdResults transactionIdResults = getTransactionIdResults(transactionId);
        if (transactionIdResults == null) {
            maybeLog("getTransactionCacheReferences for transactionId %s, results %s", transactionId, ImmutableList.of());
            return ImmutableList.of();
        }
        List<CacheKeyAndResultInfo> keysAndResults = withExistingTransactionIdResults(transactionIdResults, () ->
                ImmutableList.copyOf(transactionIdResults.cacheReferences()));
        maybeLog("getTransactionCacheReferences for transactionId %s, results %s", transactionId, keysAndResults);
        return keysAndResults;
    }

    public List<CacheKeyAndResultInfo> applyCacheUpdates(List<CacheKeyAndResult> updates)
    {
        Instant fetchedTime = Instant.now();
        ImmutableList.Builder<CacheKeyAndResultInfo> infos = ImmutableList.builder();
        updates.forEach(cacheKeyAndResult -> {
            CacheKeyAndResultInfo info = new CacheKeyAndResultInfo(
                    cacheKeyAndResult.cacheKey(),
                    TrinoPrivilegeResultInfo.toPrivilegeResultInfo(cacheKeyAndResult.result(), fetchedTime));
            accountPermissionsCache.put(cacheKeyAndResult.cacheKey(), info.result());
            infos.add(info);
        });
        return infos.build();
    }

    private TrinoPrivilegeResultInfo runAndCacheOperation(AtomicBoolean fromCache, DispatchSession session, TrinoSecurityCacheKey cacheKey)
    {
        TrinoPrivilegeResult result = runOperation(fromCache, session, cacheKey);
        TrinoPrivilegeResultInfo info = TrinoPrivilegeResultInfo.toPrivilegeResultInfo(result);
        accountPermissionsCache.put(cacheKey, info);
        return info;
    }

    private <T> T get(TransactionId transactionId, DispatchSession session, TrinoSecurityCacheKey cacheKey)
    {
        AtomicBoolean fromCache = new AtomicBoolean(true);

        // If this transactionId should not use the cache, run the operation without reading
        // from the cache, but add the result to the cache
        if (transactionIdsNotUsingTheCache.contains(transactionId)) {
            TrinoPrivilegeResultInfo resultInfo = runAndCacheOperation(fromCache, session, cacheKey);
            T realResult = (T) resultInfo.result().getResult();
            maybeLog("Adding to cache for transactionId %s, roleId %s, key %s, result %s", transactionId, session.roleId(), cacheKey, realResult);
            return realResult;
        }

        // Otherwise we try to read the result from the cache.
        TrinoPrivilegeResultInfo resultInfo = accountPermissionsCache.getIfPresent(cacheKey);
        if (resultInfo == null) {
            resultInfo = runAndCacheOperation(fromCache, session, cacheKey);
        }
        T realResult = (T) resultInfo.result().getResult();
        boolean reallyFromCache = fromCache.get();
        maybeLog("Result %sfrom cache for transactionId %s, roleId %s, key %s, result %s", reallyFromCache ? "" : "not ", transactionId, session.roleId(), cacheKey, realResult);
        if (reallyFromCache) {
            addQueryCacheReference(transactionId, cacheKey, resultInfo);
        }
        return realResult;
    }

    private void addQueryCacheReference(TransactionId transactionId, TrinoSecurityCacheKey cacheKey, TrinoPrivilegeResultInfo result)
    {
        useTransactionIdResults(transactionId, transactionIdResults -> {
            if (transactionIdResults.cacheReferences().stream().noneMatch(cacheKeyAndResult -> cacheKey.equals(cacheKeyAndResult.cacheKey()))) {
                maybeLog("Adding cache reference for transactionId %s, key %s, result %s", transactionId, cacheKey, result);
                transactionIdResults.cacheReferences().add(new CacheKeyAndResultInfo(cacheKey, result));
            }
        });
    }

    public Map<RoleName, RoleId> listEnabledRoles(TransactionId transactionId, DispatchSession session)
    {
        return get(transactionId, session, cacheKey(session, new ListEnabledRoles()));
    }

    public Map<RoleName, RoleId> listRoles(TransactionId transactionId, DispatchSession session)
    {
        return get(transactionId, session, cacheKey(session, new ListRoles()));
    }

    public boolean roleExists(TransactionId transactionId, DispatchSession session, RoleName role)
            throws OperationNotAllowedException
    {
        return get(transactionId, session, cacheKey(session, new RoleExists(role)));
    }

    public EntityPrivileges getEntityPrivileges(TransactionId transactionId, DispatchSession session, RoleId roleId, EntityId entityId, boolean delegateRequest)
    {
        return get(transactionId, session, cacheKey(session, roleId, new GetEntityPrivileges(entityId, delegateRequest)));
    }

    public ContentsVisibility getCatalogVisibility(TransactionId transactionId, DispatchSession session, Set<CatalogId> catalogIds)
    {
        ContentsVisibility contentsVisibility = get(transactionId, session, cacheKey(session, new GetCatalogVisibility(Optional.of(catalogIds))));
        catalogIds.forEach(catalogId -> {
            if (contentsVisibility.isVisible(catalogId.getBaseEntityIdString())) {
                implyCatalogVisibility(transactionId, catalogId);
            }
        });
        return contentsVisibility;
    }

    public void clearImpliedCatalogVisibility(TransactionId transactionId)
    {
        useTransactionIdResults(transactionId, transactionIdResults -> transactionIdResults.impliedCatalogVisibility.clear());
    }

    public void implyCatalogVisibility(TransactionId transactionId, CatalogId catalogId)
    {
        getCatalogName(transactionId, catalogId).ifPresent(catalogName ->
                withTransactionIdResults(transactionId, transactionIdResults -> transactionIdResults.impliedCatalogVisibility.add(catalogName)));
    }

    public void implyCatalogVisibility(TransactionId transactionId, String catalogName)
    {
        withTransactionIdResults(transactionId, transactionIdResults -> transactionIdResults.impliedCatalogVisibility.add(catalogName));
    }

    public boolean hasImpliedCatalogVisibility(TransactionId transactionId, String catalogName)
    {
        return withTransactionIdResults(transactionId, transactionIdResults -> transactionIdResults.impliedCatalogVisibility().contains(catalogName));
    }

    private <T> T withTransactionIdResults(TransactionId transactionId, Function<TransactionIdResults, T> function)
    {
        TransactionIdResults transactionIdResults = getTransactionIdResults(transactionId);
        return withExistingTransactionIdResults(transactionIdResults, () -> function.apply(transactionIdResults));
    }

    private <T> T withExistingTransactionIdResults(TransactionIdResults transactionIdResults, Supplier<T> function)
    {
        synchronized (transactionIdResults) {
            return function.get();
        }
    }

    private void useTransactionIdResults(TransactionId transactionId, Consumer<TransactionIdResults> consumer)
    {
        TransactionIdResults transactionIdResults = getTransactionIdResults(transactionId);
        useExistingTransactionIdResults(transactionIdResults, () -> consumer.accept(transactionIdResults));
    }

    private void useExistingTransactionIdResults(TransactionIdResults transactionIdResults, Runnable consumer)
    {
        synchronized (transactionIdResults) {
            consumer.run();
        }
    }

    public ContentsVisibility getVisibilityForSchemas(TransactionId transactionId, DispatchSession session, CatalogId catalogId, Set<String> schemaNames)
    {
        ContentsVisibility contentsVisibility = get(transactionId, session, cacheKey(session, new GetVisibilityForSchemas(catalogId, schemaNames)));
        maybeLog("In getVisibilityForSchemas, transactionId %s, roleId %s, catalogId %s, schemaNames %s, contentsVisibility %s", transactionId, session, catalogId, schemaNames, contentsVisibility);
        implyCatalogIfAnyAreVisible(transactionId, catalogId, contentsVisibility, schemaNames);
        return contentsVisibility;
    }

    private void implyCatalogIfAnyAreVisible(TransactionId transactionId, CatalogId catalogId, ContentsVisibility contentsVisibility, Set<String> names)
    {
        for (String name : names) {
            if (contentsVisibility.isVisible(name)) {
                implyCatalogVisibility(transactionId, catalogId);
                break;
            }
        }
    }

    public ContentsVisibility getVisibilityForTables(TransactionId transactionId, DispatchSession session, CatalogId catalogId, String schemaName, Set<String> tableNames)
    {
        if (tableNames.isEmpty()) {
            return ContentsVisibility.DENY_ALL;
        }

        // Check if the specific visibility result is already in the cache.  We don't want to force
        // loading of the result, since we might already have visibility for some of the tables in the
        // transactionId cache.
        TrinoSecurityCacheKey cacheKey = cacheKey(session, new GetVisibilityForTables(catalogId, schemaName, tableNames));
        TrinoPrivilegeResultInfo result = accountPermissionsCache.getIfPresent(cacheKey);
        if (result != null) {
            addQueryCacheReference(transactionId, cacheKey, result);
            GetVisibilityForTablesResult visibilityForTablesResult = (GetVisibilityForTablesResult) result.result();
            return visibilityForTablesResult.getResult();
        }

        AtomicBoolean catalogImpliedVisibility = new AtomicBoolean();

        // Since the specific result wasn't already in the cache, try to find the visibility results for each table in the query cache
        Set<VisibilityForTablesKey> allKeys = tableNames.stream().map(tableName -> new VisibilityForTablesKey(catalogId, schemaName, tableName)).collect(toImmutableSet());
        Map<VisibilityForTablesKey, Boolean> cachedTableVisibility = new HashMap<>();
        TransactionIdResults transactionIdResults = getTransactionIdResults(transactionId);
        useExistingTransactionIdResults(transactionIdResults, () -> {
            for (VisibilityForTablesKey key : allKeys) {
                Boolean visibility = transactionIdResults.visibilityForTables().get(key);
                if (visibility != null) {
                    if (visibility && !catalogImpliedVisibility.get()) {
                        implyCatalogVisibility(transactionId, catalogId);
                        catalogImpliedVisibility.set(true);
                    }
                    cachedTableVisibility.put(key, visibility);
                }
            }
        });
        Map<String, Boolean> tableVisibility = new HashMap<>();
        cachedTableVisibility.forEach((key, visibility) -> tableVisibility.put(key.tableName(), visibility));

        if (cachedTableVisibility.size() < tableNames.size()) {
            // We didn't find all the keys in the transactionId cache, so make a request to get them.
            Set<String> remainingTables = ImmutableSet.copyOf(Sets.difference(tableNames, tableVisibility.keySet()));
            TrinoSecurityCacheKey remainingTablesCacheKey = cacheKey(session, new GetVisibilityForTables(catalogId, schemaName, remainingTables));
            GetVisibilityForTablesResult remainingTablesResult = (GetVisibilityForTablesResult) remainingTablesCacheKey.operation().fetchResult(session, session.roleId(), client);
            ContentsVisibility remainingTableVisibility = remainingTablesResult.visibility();
            // Add the results to the response
            Map<VisibilityForTablesKey, Boolean> remainingResultsMap = new HashMap<>();
            for (String tableName : remainingTables) {
                boolean isVisible = remainingTableVisibility.isVisible(tableName);
                if (isVisible && !catalogImpliedVisibility.get()) {
                    implyCatalogVisibility(transactionId, catalogId);
                    catalogImpliedVisibility.set(true);
                }
                tableVisibility.put(tableName, isVisible);
                remainingResultsMap.put(new VisibilityForTablesKey(catalogId, schemaName, tableName), isVisible);
            }

            // Add the results to the transactionId cache
            useExistingTransactionIdResults(transactionIdResults, () ->
                    transactionIdResults.visibilityForTables().putAll(remainingResultsMap));
        }

        return new ContentsVisibility(GrantKind.DENY, tableVisibility.entrySet().stream()
                .filter(entry -> entry.getValue())
                .map(Map.Entry::getKey)
                .collect(toImmutableSet()));
    }

    public boolean canUseLocation(TransactionId transactionId, DispatchSession session, String location)
    {
        return get(transactionId, session, cacheKey(session, new CanUseLocation(location)));
    }

    public boolean canExecuteFunction(TransactionId transactionId, DispatchSession session, FunctionId functionId)
    {
        return get(transactionId, session, cacheKey(session, new CanExecuteFunction(functionId)));
    }

    public Optional<Boolean> isLiveTableStopped(TableId tableId, DispatchSession session)
    {
        return isLiveTableCache.isLiveTableStopped(client, tableId, session);
    }

    public Set<GalaxyLanguageFunctionDetails> getAvailableFunctions(TransactionId transactionId, DispatchSession session)
    {
        return get(transactionId, session, cacheKey(session, new GetAvailableFunctions()));
    }

    public TrinoPrivilegeResult runOperation(AtomicBoolean fromCache, DispatchSession session, TrinoSecurityCacheKey cacheKey)
    {
        fromCache.set(false);
        return cacheKey.operation().fetchResult(session, cacheKey.userIdAndRoleId().roleId(), client);
    }

    public void dontReadFromCacheForTransactionId(TransactionId transactionId)
    {
        transactionIdsNotUsingTheCache.add(transactionId);
    }

    public void removeTransactionCacheReferences(TransactionId transactionId)
    {
        cachedTransactionIdResults.remove(transactionId);
        transactionIdsNotUsingTheCache.remove(transactionId);
    }

    public record CacheKeyAndResultInfo(TrinoSecurityCacheKey cacheKey, TrinoPrivilegeResultInfo result)
    {
        public CacheKeyAndResultInfo
        {
            requireNonNull(cacheKey, "cacheKey is null");
            requireNonNull(result, "result is null");
        }
    }

    private record TransactionIdResults(
            // TODO: We don't actually need ordering, and the portal-server may sort it anyway,
            //       so maybe this should be a set rather than a list?
            List<CacheKeyAndResultInfo> cacheReferences,
            Set<String> impliedCatalogVisibility,
            Map<VisibilityForTablesKey, Boolean> visibilityForTables)
    {
        public TransactionIdResults
        {
            requireNonNull(cacheReferences, "cacheReferences is null");
            requireNonNull(impliedCatalogVisibility, "impliedCatalogVisibility is null");
            requireNonNull(visibilityForTables, "visibilityForTables is null");
        }
    }

    private TransactionIdResults makeTransactionIdResults()
    {
        return new TransactionIdResults(new ArrayList<>(), new HashSet<>(), new HashMap<>());
    }

    public boolean isReadOnlyCatalog(TransactionId transactionId, String catalogName)
    {
        return catalogResolver.isReadOnlyCatalog(Optional.of(transactionId), catalogName);
    }

    public Optional<CatalogId> getCatalogId(TransactionId transactionId, String catalogName)
    {
        return catalogResolver.getCatalogId(Optional.of(transactionId), catalogName);
    }

    public Optional<String> getCatalogName(TransactionId transactionId, CatalogId catalogId)
    {
        return catalogResolver.getCatalogName(transactionId, catalogId);
    }

    public Set<SchemaTableName> getAlwaysVisibleSystemTables(TransactionId transactionId, String catalogName)
    {
        return catalogResolver.getAlwaysVisibleSystemTables(Optional.of(transactionId), catalogName);
    }

    public List<CacheKeyAndResultInfo> validateCachedResultsAndApplyUpdates(
            TransactionId transactionId,
            DispatchSession session,
            List<CacheKeyAndResultInfo> cacheReferences,
            boolean exceptionDuringAnalysis)
    {
        Optional<Instant> transactionStartTime = Optional.ofNullable(transactionIdsPerformingAnalysis.get(transactionId));
        List<CacheKeyAndResultInfo> referencesToValidate = cacheReferences;
        if (transactionStartTime.isPresent()) {
            referencesToValidate = cacheReferences.stream()
                    // if the cache reference was retrieved any time after the start of the transaction,
                    // consider it a valid reference that does not need to be validated
                    // this will prevent needing to validate results redundantly if something was fetched (e.g. entity privileges)
                    // and used multiple times throughout the transaction (e.g. getting column masks, row filters, and privileges)
                    .filter(info -> {
                        Instant actualTxnStart = transactionStartTime.get();
                        if (info.result().fetchedTime().compareTo(actualTxnStart) > 0) {
                            maybeLog("Not validating %s because it was fetched at %s, after the start of the transaction %s", info.cacheKey(), info.result().fetchedTime(), actualTxnStart);
                            return false;
                        }
                        return true;
                    })
                    .collect(toImmutableList());
        }
        List<CacheKeyAndResult> cacheUpdates = ImmutableList.of();
        if (!referencesToValidate.isEmpty()) {
            cacheUpdates = client.validateCachedResults(session, cacheReferences.stream()
                    .map(info -> new CacheKeyAndResult(info.cacheKey(), info.result().result()))
                    .collect(toImmutableList()));
            maybeLog("Validated cache references %s resulting in cacheUpdates %s", cacheReferences, cacheUpdates);
        }
        List<CacheKeyAndResultInfo> updatedResults = applyCacheUpdates(cacheUpdates);
        incrementValidationStatistics(cacheReferences, referencesToValidate, cacheUpdates);
        incrementAnalysisCounters(exceptionDuringAnalysis, cacheReferences.isEmpty(), cacheUpdates.isEmpty());
        return updatedResults;
    }

    private static TrinoSecurityCacheKey cacheKey(DispatchSession session, TrinoPrivilegeOperation operation)
    {
        return new TrinoSecurityCacheKey(new UserIdAndRoleId(session.userId(), session.roleId()), operation);
    }

    private static TrinoSecurityCacheKey cacheKey(DispatchSession session, RoleId roleId, TrinoPrivilegeOperation operation)
    {
        return new TrinoSecurityCacheKey(new UserIdAndRoleId(session.userId(), roleId), operation);
    }

    @Managed
    @Nested
    public CacheStatsMBean getStatistics()
    {
        return new CacheStatsMBean(accountPermissionsCache);
    }

    public void incrementValidationStatistics(List<CacheKeyAndResultInfo> allCacheRefrences, List<CacheKeyAndResultInfo> validatedCacheReferences, List<CacheKeyAndResult> invalidReferences)
    {
        synchronized (validationStatisticsLock) {
            if (!validatedCacheReferences.isEmpty()) {
                validationStatistics.validationCalls++;
                validationStatistics.resultValidationCalls += validatedCacheReferences.size();
            }

            if (invalidReferences.isEmpty()) {
                validationStatistics.allValidValidationCount++;
            }
            else {
                validationStatistics.someInvalidValidationCount++;
            }
            validationStatistics.validButNotValidatedCount += allCacheRefrences.size() - validatedCacheReferences.size();
            validationStatistics.validResultCount += validatedCacheReferences.size() - invalidReferences.size();
            validationStatistics.invalidResultCount += invalidReferences.size();

            Set<TrinoSecurityCacheKey> invalidResultKeys = invalidReferences.stream().map(CacheKeyAndResult::cacheKey).collect(toImmutableSet());
            for (CacheKeyAndResultInfo keyAndResult : validatedCacheReferences) {
                String type = keyAndResult.cacheKey().operation().type();
                validationStatistics.resultCountsByType.computeIfAbsent(type, _ -> new MutableLong()).increment();
                if (invalidResultKeys.contains(keyAndResult.cacheKey())) {
                    validationStatistics.invalidResultCountsByType.computeIfAbsent(type, _ -> new MutableLong()).increment();
                }
                else {
                    validationStatistics.validResultCountsByType.computeIfAbsent(type, _ -> new MutableLong()).increment();
                }
            }
        }
    }

    public void incrementAnalysisCounters(boolean exceptionDuringAnalysis, boolean noCacheReferences, boolean noCacheUpdates)
    {
        synchronized (validationStatisticsLock) {
            if (exceptionDuringAnalysis) {
                validationStatistics.analysisExceptions++;
            }
            if (noCacheReferences) {
                validationStatistics.noCacheReferences++;
            }
            if (noCacheUpdates) {
                validationStatistics.noCacheUpdates++;
            }
        }
    }

    public void incrementAnalysisExceptionsWithUpToDateCache()
    {
        synchronized (validationStatisticsLock) {
            validationStatistics.analysisExceptionsWithUpToDateCache++;
        }
    }

    public void incrementAnalysisExceptionsFirstPass()
    {
        synchronized (validationStatisticsLock) {
            validationStatistics.analysisExceptionsFirstPass++;
        }
    }

    public void incrementAnalysisReruns()
    {
        synchronized (validationStatisticsLock) {
            validationStatistics.analysisReruns++;
        }
    }

    public void incrementTransactionNotInAnalysis()
    {
        synchronized (validationStatisticsLock) {
            validationStatistics.transactionNotInAnalysis++;
        }
    }

    private TransactionIdResults getTransactionIdResults(TransactionId transactionId)
    {
        return cachedTransactionIdResults.computeIfAbsent(transactionId, _ -> makeTransactionIdResults());
    }

    @Managed
    @Nested
    public PublishedValidationStatistics getValidationStatistics()
    {
        synchronized (validationStatisticsLock) {
            return new PublishedValidationStatistics(validationStatistics);
        }
    }

    public boolean isTransactionIdInAnalysis(TransactionId transactionId)
    {
        return transactionIdsPerformingAnalysis.containsKey(transactionId);
    }

    public void setTransactionIdInAnalysis(TransactionId transactionId, boolean inAnalysis)
    {
        if (inAnalysis) {
            Instant startTime = Instant.now();
            transactionIdsPerformingAnalysis.put(transactionId, startTime);
        }
        else {
            transactionIdsPerformingAnalysis.remove(transactionId);
        }
    }

    public static class ValidationStatistics
    {
        private long validationCalls;
        private long allValidValidationCount;
        private long someInvalidValidationCount;
        private long resultValidationCalls;
        private long validButNotValidatedCount;
        private long validResultCount;
        private long invalidResultCount;
        private long analysisExceptions;
        private long analysisExceptionsWithUpToDateCache;
        private long analysisExceptionsFirstPass;
        private long analysisReruns;
        private long noCacheReferences;
        private long noCacheUpdates;
        private long transactionNotInAnalysis;
        private final Map<String, MutableLong> resultCountsByType = new HashMap<>();
        private final Map<String, MutableLong> validResultCountsByType = new HashMap<>();
        private final Map<String, MutableLong> invalidResultCountsByType = new HashMap<>();
    }

    public static class PublishedValidationStatistics
    {
        private final long validationCalls;
        private final long allValidValidationCount;
        private final long someInvalidValidateCount;
        private final long resultValidationCalls;
        private final long validButNotValidatedCount;
        private final long validResultCount;
        private final long invalidResultCount;
        private final long analysisExceptions;
        private final long analysisExceptionsWithUpToDateCache;
        private final long analysisExceptionsFirstPass;
        private final long analysisReruns;
        private final long noCacheReferences;
        private final long noCacheUpdates;
        private final long transactionNotInAnalysis;

        private final Map<String, Long> resultsByType;
        private final Map<String, Long> validResultsByType;
        private final Map<String, Long> invalidResultsByType;

        public PublishedValidationStatistics(ValidationStatistics stats)
        {
            validationCalls = stats.validationCalls;
            allValidValidationCount = stats.allValidValidationCount;
            someInvalidValidateCount = stats.someInvalidValidationCount;
            validButNotValidatedCount = stats.validButNotValidatedCount;
            validResultCount = stats.validResultCount;
            resultValidationCalls = stats.resultValidationCalls;
            invalidResultCount = stats.invalidResultCount;
            analysisExceptions = stats.analysisExceptions;
            analysisExceptionsWithUpToDateCache = stats.analysisExceptionsWithUpToDateCache;
            analysisExceptionsFirstPass = stats.analysisExceptionsFirstPass;
            analysisReruns = stats.analysisReruns;
            noCacheReferences = stats.noCacheReferences;
            noCacheUpdates = stats.noCacheUpdates;
            resultsByType = makeImmutableCopy(stats.resultCountsByType);
            validResultsByType = makeImmutableCopy(stats.validResultCountsByType);
            invalidResultsByType = makeImmutableCopy(stats.invalidResultCountsByType);
            transactionNotInAnalysis = stats.transactionNotInAnalysis;
        }

        @Managed
        public long getValidationCalls()
        {
            return validationCalls;
        }

        @Managed
        public long getAllValidValidationCount()
        {
            return allValidValidationCount;
        }

        @Managed
        public long getSomeInvalidValidateCount()
        {
            return someInvalidValidateCount;
        }

        @Managed
        public long getResultValidationCalls()
        {
            return resultValidationCalls;
        }

        @Managed
        public long getValidButNotValidatedCount()
        {
            return validButNotValidatedCount;
        }

        @Managed
        public long getValidResultCount()
        {
            return validResultCount;
        }

        @Managed
        public long getInvalidResultCount()
        {
            return invalidResultCount;
        }

        @Managed
        public long getAnalysisExceptions()
        {
            return analysisExceptions;
        }

        @Managed
        public long getAnalysisExceptionsWithUpToDateCache()
        {
            return analysisExceptionsWithUpToDateCache;
        }

        @Managed
        public long getAnalysisExceptionsFirstPass()
        {
            return analysisExceptionsFirstPass;
        }

        @Managed
        public long getAnalysisReruns()
        {
            return analysisReruns;
        }

        @Managed
        public long getNoCacheReferences()
        {
            return noCacheReferences;
        }

        @Managed
        public long getNoCacheUpdates()
        {
            return noCacheUpdates;
        }

        @Managed
        public long getTransactionNotInAnalysis()
        {
            return transactionNotInAnalysis;
        }

        @Managed
        public Map<String, Long> getResultsByType()
        {
            return resultsByType;
        }

        @Managed
        public Map<String, Long> getValidResultsByType()
        {
            return validResultsByType;
        }

        @Managed
        public Map<String, Long> getInvalidResultsByType()
        {
            return invalidResultsByType;
        }

        private Map<String, Long> makeImmutableCopy(Map<String, MutableLong> map)
        {
            return map.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getValue()));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("validationCalls", validationCalls)
                    .add("validCalls", allValidValidationCount)
                    .add("invalidCalls", someInvalidValidateCount)
                    .add("validResults", validResultCount)
                    .add("resultValidationCalls", resultValidationCalls)
                    .add("invalidResults", invalidResultCount)
                    .add("analysisExceptions", analysisExceptions)
                    .add("analysisExceptionsWithUpToDateCache", analysisExceptionsWithUpToDateCache)
                    .add("analysisReruns", analysisReruns)
                    .add("noCacheReferences", noCacheReferences)
                    .add("noCacheUpdates", noCacheUpdates)
                    .add("resultsByType", resultsByType)
                    .add("validResultsByType", validResultsByType)
                    .add("invalidResultsByType", invalidResultsByType)
                    .toString();
        }
    }

    private static class MutableLong
    {
        private long value;

        public MutableLong()
        {
            this.value = 0;
        }

        public long getValue()
        {
            return value;
        }

        public void increment()
        {
            value++;
        }
    }

    @FormatMethod
    public static void maybeLog(final @FormatString String format, Object... args)
    {
        if (log.isDebugEnabled()) {
            log.info(format, args);
        }
    }
}
