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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionDetails;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.cache.CacheStatsMBean;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;

@ThreadSafe
public class GalaxyPermissionsCache
{
    // per-query caches
    private final LoadingCache<QueryId, Map<DispatchSession, GalaxyQueryPermissions>> queryPermissionsCache;
    private final IsIcehouseLiveTableCache isLiveTableCache;

    @Inject
    public GalaxyPermissionsCache(GalaxySystemAccessControlConfig accessControlConfig)
    {
        this(accessControlConfig.getExpectedQueryParallelism(), accessControlConfig.getAccessControlMode());
    }

    public GalaxyPermissionsCache(int expectedQueryParallelism, GalaxySystemAccessControlConfig.AccessControlMode accessControlMode)
    {
        queryPermissionsCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .maximumSize(expectedQueryParallelism),
                CacheLoader.from(_ -> new ConcurrentHashMap<>()));
        isLiveTableCache = IsIcehouseLiveTableCache.create(accessControlMode);
    }

    public GalaxyQueryPermissions getCache(TrinoSecurityApi trinoSecurityApi, QueryId queryId, DispatchSession session)
    {
        return queryPermissionsCache.getUnchecked(queryId)
                .computeIfAbsent(session, _ -> new GalaxyQueryPermissions(trinoSecurityApi, session));
    }

    record SessionAndTableId(DispatchSession session, TableId tableId) {}

    @Managed
    @Nested
    public CacheStatsMBean getQueryPermissionsCacheStats()
    {
        return new CacheStatsMBean(queryPermissionsCache);
    }

    public Optional<Boolean> isLiveTableStopped(TrinoSecurityApi trinoSecurityApi, TableId tableId, DispatchSession session)
    {
        return isLiveTableCache.isLiveTableStopped(trinoSecurityApi, tableId, session);
    }

    /**
     * Permission cache for given query and identity ({@link DispatchSession}).
     */
    @ThreadSafe
    public static class GalaxyQueryPermissions
    {
        // It's a cache to support concurrent loads while also preventing multiple loads for the same key
        private final LoadingCache<EntityPrivilegesKey, EntityPrivileges> entityPrivileges;
        // It's a cache only for convenience to use loading cache's bulk loading capability
        private final LoadingCache<CatalogId, ContentsVisibility> catalogVisibility;
        @GuardedBy("this")
        private final Set<String> impliedCatalogVisibility = new HashSet<>();
        // It's a cache only for convenience to use loading cache's bulk loading capability
        private final LoadingCache<VisibilityForTablesKey, Boolean> visibilityForTables;
        private final LoadingCache<RoleId, Set<GalaxyLanguageFunctionDetails>> functions;

        public GalaxyQueryPermissions(TrinoSecurityApi trinoSecurityApi, DispatchSession session)
        {
            entityPrivileges = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    CacheLoader.from(key -> handleClientError(() -> trinoSecurityApi.getEntityPrivileges(session, key.roleId(), key.entity(), key.isDelegateRequest()))));

            catalogVisibility = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    BulkOnlyLoader.of(catalogIds -> {
                        Set<CatalogId> catalogIdsSet = ImmutableSet.copyOf(catalogIds);
                        ContentsVisibility catalogVisibility = handleClientError(() -> trinoSecurityApi.getCatalogVisibility(session, catalogIdsSet));
                        return catalogIdsSet.stream()
                                .collect(toImmutableMap(identity(), _ -> catalogVisibility));
                    }));

            visibilityForTables = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    BulkOnlyLoader.of(keys -> {
                        CatalogId catalogId = stream(keys)
                                .map(VisibilityForTablesKey::catalogId)
                                .distinct()
                                // The cache is never invoked for different catalogs at once
                                .collect(onlyElement());
                        String schemaName = stream(keys)
                                .map(VisibilityForTablesKey::schemaName)
                                .distinct()
                                // The cache is never invoked for different schemas at once
                                .collect(onlyElement());
                        Set<String> tableNames = stream(keys)
                                .map(VisibilityForTablesKey::tableName)
                                .collect(toImmutableSet());
                        ContentsVisibility visibility = handleClientError(() -> trinoSecurityApi.getVisibilityForTables(session, catalogId, schemaName, tableNames));
                        return tableNames.stream()
                                .collect(toImmutableMap(tableName -> new VisibilityForTablesKey(catalogId, schemaName, tableName), tableName -> visibility.isVisible(tableName)));
                    }));

            functions = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    CacheLoader.from(_ -> handleClientError(() -> trinoSecurityApi.getAvailableFunctions(session))));
        }

        public EntityPrivileges getEntityPrivileges(RoleId roleId, EntityId entity)
        {
            return getEntityPrivileges(roleId, entity, false);
        }

        public EntityPrivileges getEntityPrivileges(RoleId roleId, EntityId entity, boolean isDelegate)
        {
            return entityPrivileges.getUnchecked(new EntityPrivilegesKey(roleId, entity, isDelegate));
        }

        public Predicate<CatalogId> getCatalogVisibility(Set<CatalogId> requestedCatalogs)
        {
            if (requestedCatalogs.isEmpty()) {
                return catalogId -> {
                    throw new IllegalArgumentException("Unexpected catalog checked for visibility: %s, none was expected".formatted(catalogId));
                };
            }
            try {
                Map<CatalogId, ContentsVisibility> visibility = catalogVisibility.getAll(requestedCatalogs);
                return catalogId -> {
                    // The ContentsVisibility, values in this map, may be all the same, but also may have been loaded at different times and may be different.
                    // For example, they may have different values of ContentsVisibility.defaultVisibility.
                    // The ContentsVisibility at the catalogId is authoritative for given catalogId.
                    ContentsVisibility contentsVisibility = visibility.get(catalogId);
                    checkArgument(contentsVisibility != null, "Unexpected catalog checked for visibility: %s, expected one of: %s", catalogId, requestedCatalogs);
                    return contentsVisibility.isVisible(catalogId.toString());
                };
            }
            catch (ExecutionException e) {
                // Impossible, loader does not throw checked exceptions
                throw new RuntimeException(e);
            }
        }

        public synchronized void implyCatalogVisibility(String catalogName)
        {
            impliedCatalogVisibility.add(catalogName);
        }

        public synchronized boolean hasImpliedCatalogVisibility(String catalogName)
        {
            return impliedCatalogVisibility.contains(catalogName);
        }

        public ContentsVisibility getVisibilityForTables(CatalogId catalogId, String schemaName, Set<String> tableNames)
        {
            if (tableNames.isEmpty()) {
                return ContentsVisibility.DENY_ALL;
            }

            List<VisibilityForTablesKey> cacheKeys = tableNames.stream()
                    .map(tableName -> new VisibilityForTablesKey(catalogId, schemaName, tableName))
                    .collect(toImmutableList());

            Map<VisibilityForTablesKey, Boolean> loaded;
            // Otherwise check the visibilityForTables cache.

            try {
                loaded = visibilityForTables.getAll(cacheKeys);
            }
            catch (ExecutionException e) { // Impossible, the cache loader does not currently throw checked exceptions
                throw new RuntimeException(e);
            }
            return new ContentsVisibility(GrantKind.DENY, loaded.entrySet().stream()
                    .filter(entry -> entry.getValue())
                    .map(entry -> entry.getKey().tableName())
                    .collect(toImmutableSet()));
        }

        public Set<GalaxyLanguageFunctionDetails> getAvailableFunctions(RoleId roleId)
        {
            try {
                return functions.get(roleId);
            }
            catch (ExecutionException e) { // Impossible, the cache loader does not currently throw checked exceptions
                throw new RuntimeException(e);
            }
        }

        private record EntityPrivilegesKey(RoleId roleId, EntityId entity, boolean isDelegateRequest)
        {
            EntityPrivilegesKey
            {
                requireNonNull(roleId, "roleId is null");
                requireNonNull(entity, "entity is null");
            }
        }

        private record VisibilityForTablesKey(CatalogId catalogId, String schemaName, String tableName)
        {
            VisibilityForTablesKey
            {
                requireNonNull(catalogId, "catalogId is null");
                requireNonNull(schemaName, "schemaName is null");
                requireNonNull(tableName, "tableName is null");
            }
        }
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
}
