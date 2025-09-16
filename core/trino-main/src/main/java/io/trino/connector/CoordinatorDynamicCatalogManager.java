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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.CacheManagerRegistry;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.ForStartup;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.metadata.Catalog.failedCatalog;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.util.Executors.executeUntilFailure;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

@ThreadSafe
public class CoordinatorDynamicCatalogManager
        implements CatalogManager, ConnectorServicesProvider
{
    private static final Logger log = Logger.get(CoordinatorDynamicCatalogManager.class);

    private enum State
    {
        CREATED, INITIALIZED, STOPPED
    }

    private final CatalogStoreWithBuiltInCatalogs catalogStore;
    private final CatalogFactory catalogFactory;
    private final Set<CatalogFailureHandler> catalogFailureHandler;
    private final CacheManagerRegistry cacheManagerRegistry;
    private final Executor executor;
    private final CatalogMetricsService catalogMetricsService;

    private final Object catalogsUpdateLock = new Object();

    /**
     * Active catalogs that have been created and not dropped.
     */
    private final ConcurrentMap<CatalogName, Catalog> activeCatalogs = new ConcurrentHashMap<>();

    /**
     * All catalogs including those that have been dropped.
     */
    private final ConcurrentMap<CatalogHandle, RegisteredCatalog> allCatalogs = new ConcurrentHashMap<>();

    @GuardedBy("catalogsUpdateLock")
    private State state = State.CREATED;

    @Inject
    public CoordinatorDynamicCatalogManager(
            CatalogStore catalogStore,
            CatalogFactory catalogFactory,
            BuiltInCatalogsProvider builtInCatalogsProvider,
            Set<CatalogFailureHandler> catalogFailureHandler,
            CacheManagerRegistry cacheManagerRegistry,
            @ForStartup Executor executor,
            CatalogMetricsService catalogMetricsService)
    {
        this.catalogStore = new CatalogStoreWithBuiltInCatalogs(
                requireNonNull(catalogStore, "catalogStore is null"),
                requireNonNull(builtInCatalogsProvider, "builtInCatalogsProvider is null"));
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.catalogFailureHandler = requireNonNull(catalogFailureHandler, "catalogFailureHandler is null");
        this.cacheManagerRegistry = requireNonNull(cacheManagerRegistry, "cacheManagerRegistry is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.catalogMetricsService = requireNonNull(catalogMetricsService, "catalogMetricsService is null");
    }

    @PreDestroy
    public void stop()
    {
        List<RegisteredCatalog> catalogs;

        synchronized (catalogsUpdateLock) {
            if (state == State.STOPPED) {
                return;
            }
            state = State.STOPPED;

            catalogs = ImmutableList.copyOf(allCatalogs.values());
            allCatalogs.clear();
            activeCatalogs.clear();
        }

        for (RegisteredCatalog connector : catalogs) {
            connector.catalog().shutdown();
        }
    }

    @Override
    public void loadInitialCatalogs()
    {
        synchronized (catalogsUpdateLock) {
            if (state == State.INITIALIZED) {
                return;
            }
            checkState(state != State.STOPPED, "ConnectorManager is stopped");
            state = State.INITIALIZED;

            executeUntilFailure(
                    executor,
                    catalogStore.getCatalogs().stream()
                            .map(storedCatalog -> (Callable<?>) () -> {
                                CatalogProperties catalog = null;
                                try {
                                    catalog = storedCatalog.loadProperties();
                                    verify(catalog.name().equals(storedCatalog.name()), "Catalog name does not match catalog properties");
                                    CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                                    activeCatalogs.put(storedCatalog.name(), newCatalog.getCatalog());
                                    allCatalogs.put(newCatalog.getCatalogHandle(), new RegisteredCatalog(new RegistrationToken(), newCatalog));
                                    log.debug("-- Added catalog %s using connector %s --", storedCatalog.name(), catalog.connectorName());
                                    catalogMetricsService.addCatalog();
                                }
                                catch (Throwable e) {
                                    CatalogVersion catalogVersion = catalog != null ? catalog.version() : new CatalogVersion("failed");
                                    ConnectorName connectorName = catalog != null ? catalog.connectorName() : new ConnectorName("unknown");
                                    Catalog failedCatalog = failedCatalog(storedCatalog.name(), catalogVersion, connectorName);
                                    activeCatalogs.put(storedCatalog.name(), failedCatalog);
                                    log.error(e, "-- Failed to load catalog %s using connector %s --", storedCatalog.name(), connectorName);
                                    handleFailure(failedCatalog, e);
                                }
                                return null;
                            })
                            .collect(toImmutableList()));
        }
    }

    private void handleFailure(Catalog failedCatalog, Throwable cause)
    {
        for (CatalogFailureHandler failureHandler : catalogFailureHandler) {
            try {
                failureHandler.handleCatalogFailure(failedCatalog, cause);
            }
            catch (Throwable e) {
                cause.addSuppressed(e);
                log.error(e, "Uncaught exception from CatalogFailureHandler");
            }
        }
    }

    @Override
    public Set<CatalogName> getCatalogNames()
    {
        return ImmutableSet.copyOf(activeCatalogs.keySet());
    }

    @Override
    public Optional<Catalog> getCatalog(CatalogName catalogName)
    {
        return Optional.ofNullable(activeCatalogs.get(catalogName));
    }

    @Override
    public Set<CatalogHandle> getReachableDynamicCatalogs()
    {
        return activeCatalogs.values().stream()
                .map(Catalog::getCatalogHandle)
                .collect(toImmutableSet());
    }

    @Override
    public void ensureCatalogsLoaded(List<CatalogProperties> catalogs)
    {
        List<CatalogName> missingCatalogs = catalogs.stream()
                .map(catalog -> createRootCatalogHandle(catalog.name(), catalog.version()))
                .filter(not(allCatalogs::containsKey))
                .map(CatalogHandle::getCatalogName)
                .collect(toImmutableList());

        if (!missingCatalogs.isEmpty()) {
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "Missing catalogs: " + missingCatalogs);
        }
    }

    @Override
    public PrunableState getPrunableState()
    {
        return new PrunableStateImpl(allCatalogs.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey(), entry -> entry.getValue().registrationToken())));
    }

    @Override
    public void pruneCatalogs(PrunableState opaquePrunableState, Set<CatalogHandle> catalogsInUse)
    {
        PrunableStateImpl prunableState = (PrunableStateImpl) opaquePrunableState;
        List<CatalogConnector> removedCatalogs = new ArrayList<>();
        synchronized (catalogsUpdateLock) {
            if (state == State.STOPPED) {
                return;
            }
            Iterator<Entry<CatalogHandle, RegisteredCatalog>> iterator = allCatalogs.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<CatalogHandle, RegisteredCatalog> entry = iterator.next();
                CatalogHandle catalogHandle = entry.getKey();
                RegistrationToken registrationToken = entry.getValue().registrationToken();

                Catalog activeCatalog = activeCatalogs.get(catalogHandle.getCatalogName());
                if (activeCatalog != null && activeCatalog.getCatalogHandle().equals(catalogHandle)) {
                    // catalog is registered with a name, and therefor is available for new queries, and should not be removed
                    continue;
                }

                if (registrationToken.equals(prunableState.prunableCatalogs().get(catalogHandle))
                        && !catalogsInUse.contains(catalogHandle)) {
                    iterator.remove();
                    removedCatalogs.add(entry.getValue().catalog());
                }
            }
        }

        // todo do this in a background thread
        for (CatalogConnector removedCatalog : removedCatalogs) {
            try {
                removedCatalog.shutdown();
            }
            catch (Throwable e) {
                log.error(e, "Error shutting down catalog: %s".formatted(removedCatalog));
            }
        }

        if (!removedCatalogs.isEmpty()) {
            List<String> sortedHandles = removedCatalogs.stream().map(connector -> connector.getCatalogHandle().toString()).sorted().toList();
            log.debug("Pruned catalogs: %s", sortedHandles);
        }
    }

    @Override
    public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
    {
        return Optional.ofNullable(allCatalogs.get(catalogHandle.getRootCatalogHandle()))
                .map(RegisteredCatalog::catalog)
                .flatMap(CatalogConnector::getCatalogProperties);
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        RegisteredCatalog registeredCatalog = allCatalogs.get(catalogHandle.getRootCatalogHandle());
        checkArgument(registeredCatalog != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return registeredCatalog.catalog().getMaterializedConnector(catalogHandle.getType());
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogName catalogName)
    {
        for (Entry<CatalogHandle, RegisteredCatalog> entry : allCatalogs.entrySet()) {
            if (entry.getKey().getCatalogName().equals(catalogName)) {
                return entry.getValue().catalog().getMaterializedConnector(entry.getKey().getType());
            }
        }
        throw new IllegalArgumentException("No catalog '%s'".formatted(catalogName));
    }

    @Override
    public void createCatalog(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        synchronized (catalogsUpdateLock) {
            checkState(state != State.STOPPED, "ConnectorManager is stopped");

            if (activeCatalogs.containsKey(catalogName)) {
                if (!notExists) {
                    throw new TrinoException(ALREADY_EXISTS, format("Catalog '%s' already exists", catalogName));
                }
                return;
            }

            CatalogProperties catalogProperties = catalogStore.createCatalogProperties(catalogName, connectorName, properties);

            // get or create catalog for the handle
            RegisteredCatalog registeredCatalog = allCatalogs.computeIfAbsent(
                    createRootCatalogHandle(catalogName, catalogProperties.version()),
                    _ -> new RegisteredCatalog(new RegistrationToken(), catalogFactory.createCatalog(catalogProperties)));
            CatalogConnector catalog = registeredCatalog.catalog();
            catalogStore.addOrReplaceCatalog(catalogProperties);
            activeCatalogs.put(catalogName, catalog.getCatalog());

            log.debug("Added catalog: %s", catalog.getCatalogHandle());
            catalogMetricsService.catalogCreated(catalog.getConnectorName().toString());
        }
    }

    @Override
    public void renameCatalog(CatalogName catalogName, CatalogName newCatalogName)
    {
        requireNonNull(catalogName, "catalogName is null");
        catalogStore.verifyBuiltInCatalog(catalogName, "Renaming");

        synchronized (catalogsUpdateLock) {
            checkState(state != State.STOPPED, "ConnectorManager is stopped");

            if (!activeCatalogs.containsKey(catalogName)) {
                throw new TrinoException(NOT_FOUND, format("Catalog '%s' does not exist", catalogName));
            }
            if (activeCatalogs.containsKey(newCatalogName)) {
                throw new TrinoException(ALREADY_EXISTS, format("Catalog '%s' already exists", newCatalogName));
            }

            createCatalogLikeInternal(catalogName, newCatalogName, ImmutableMap.of());
            catalogStore.removeCatalog(catalogName);
            activeCatalogs.remove(catalogName);

            log.info("Renamed catalog from: '%s' to: '%s'", catalogName, newCatalogName);
            catalogMetricsService.catalogRenamed(activeCatalogs.get(newCatalogName).getConnectorName().toString());
        }
    }

    @Override
    public void alterCatalog(CatalogName catalogName, Map<String, Optional<String>> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        catalogStore.verifyBuiltInCatalog(catalogName, "Altering");

        synchronized (catalogsUpdateLock) {
            checkState(state != State.STOPPED, "ConnectorManager is stopped");

            if (!activeCatalogs.containsKey(catalogName)) {
                throw new TrinoException(NOT_FOUND, format("Catalog '%s' does not exist", catalogName));
            }

            createCatalogLikeInternal(catalogName, catalogName, properties);
            log.info("Updated catalog: " + catalogName);
            catalogMetricsService.catalogAltered(activeCatalogs.get(catalogName).getConnectorName().toString());
        }
    }

    private RegisteredCatalog createCatalogLikeInternal(CatalogName oldCatalogName, CatalogName catalogName, Map<String, Optional<String>> properties)
    {
        CatalogHandle catalogHandle = activeCatalogs.get(oldCatalogName).getCatalogHandle();
        CatalogProperties oldCatalogProperties = allCatalogs.get(catalogHandle).catalog().getCatalogProperties().orElseThrow();
        Map<String, String> newProperties = updateProperties(oldCatalogProperties, properties);
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(catalogName, oldCatalogProperties.connectorName(), newProperties);

        // get or create catalog for the handle
        RegisteredCatalog registeredCatalog = allCatalogs.computeIfAbsent(
                createRootCatalogHandle(catalogName, catalogProperties.version()),
                _ -> new RegisteredCatalog(new RegistrationToken(), catalogFactory.createCatalog(catalogProperties)));
        catalogStore.addOrReplaceCatalog(catalogProperties);
        activeCatalogs.put(catalogName, registeredCatalog.catalog().getCatalog());
        return registeredCatalog;
    }

    private static Map<String, String> updateProperties(CatalogProperties existingProperties, Map<String, Optional<String>> changedProperties)
    {
        ImmutableSet.Builder<String> removedPropertiesBuilder = ImmutableSet.builder();
        ImmutableMap.Builder<String, String> updatedPropertiesBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Optional<String>> property : changedProperties.entrySet()) {
            if (property.getValue().isEmpty()) {
                removedPropertiesBuilder.add(property.getKey());
            }
            else {
                updatedPropertiesBuilder.put(property.getKey(), property.getValue().get());
            }
        }
        Set<String> removedProperties = removedPropertiesBuilder.build();
        Map<String, String> updatedProperties = updatedPropertiesBuilder.buildOrThrow();

        Map<String, String> oldProperties = Maps.filterKeys(existingProperties.properties(), key -> !removedProperties.contains(key));

        return ImmutableMap.<String, String>builder()
                .putAll(oldProperties)
                .putAll(updatedProperties)
                .buildKeepingLast();
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        requireNonNull(connector, "connector is null");

        synchronized (catalogsUpdateLock) {
            if (state == State.STOPPED) {
                return;
            }

            CatalogConnector catalog = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), connector);
            if (activeCatalogs.putIfAbsent(new CatalogName(GlobalSystemConnector.NAME), catalog.getCatalog()) != null) {
                throw new IllegalStateException("Global system catalog already registered");
            }
            allCatalogs.put(GlobalSystemConnector.CATALOG_HANDLE, new RegisteredCatalog(new RegistrationToken(), catalog));
        }
    }

    @Override
    public void dropCatalog(CatalogName catalogName, boolean exists)
    {
        requireNonNull(catalogName, "catalogName is null");
        catalogStore.verifyBuiltInCatalog(catalogName, "Dropping");

        boolean removed;
        synchronized (catalogsUpdateLock) {
            checkState(state != State.STOPPED, "ConnectorManager is stopped");

            catalogStore.removeCatalog(catalogName);
            Catalog removedCatalog = activeCatalogs.remove(catalogName);
            removed = removedCatalog != null;
            if (removed) {
                catalogMetricsService.catalogDropped(removedCatalog.getConnectorName().toString());
                cacheManagerRegistry.drop(catalogName);
            }
        }

        if (!removed && !exists) {
            throw new TrinoException(CATALOG_NOT_FOUND, format("Catalog '%s' not found", catalogName));
        }
        // Do not shut down the catalog, because there may still be running queries using this catalog.
        // Catalog shutdown logic will be added later.
    }

    record PrunableStateImpl(Map<CatalogHandle, RegistrationToken> prunableCatalogs)
            implements PrunableState
    {
        public PrunableStateImpl
        {
            prunableCatalogs = ImmutableMap.copyOf(requireNonNull(prunableCatalogs, "prunableCatalogs is null"));
        }
    }

    static class RegistrationToken
    {
        // identity-based equality
        @Override
        public boolean equals(Object obj)
        {
            return super.equals(obj);
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }
    }

    private record RegisteredCatalog(RegistrationToken registrationToken, CatalogConnector catalog)
    {
        RegisteredCatalog
        {
            requireNonNull(registrationToken, "registrationToken is null");
            requireNonNull(catalog, "catalog is null");
        }
    }

    /*
     * A catalog store that is aware of buil-in catalogs (catalogs that do not exist in the underlying catalog store
     * and cannot be dropped or altered).
     */
    private static class CatalogStoreWithBuiltInCatalogs
            implements CatalogStore
    {
        private final CatalogStore catalogStore;
        private final Map<String, CatalogStore.StoredCatalog> builtInCatalogs;

        CatalogStoreWithBuiltInCatalogs(CatalogStore catalogStore, BuiltInCatalogsProvider builtInCatalogsProvider)
        {
            this.catalogStore = requireNonNull(catalogStore, "catalogStore is null");
            this.builtInCatalogs = requireNonNull(builtInCatalogsProvider, "builtInCatalogsProvider is null")
                    .getBuiltInCatalogs()
                    .stream()
                    .collect(toImmutableMap(
                            storedCatalog -> normalizedName(storedCatalog.name()),
                            catalog -> catalog));
        }

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            List<String> duplicateCatalogNames = catalogStore.getCatalogs().stream()
                    .map(StoredCatalog::name)
                    .map(CatalogStoreWithBuiltInCatalogs::normalizedName)
                    .filter(builtInCatalogs::containsKey)
                    .collect(toImmutableList());
            if (!duplicateCatalogNames.isEmpty()) {
                String message;
                if (duplicateCatalogNames.size() > 1) {
                    message = "Catalog names %s are reserved by Starburst".formatted(duplicateCatalogNames);
                }
                else {
                    message = "Catalog name %s is reserved by Starburst".formatted(duplicateCatalogNames.getFirst());
                }
                throw new TrinoException(ALREADY_EXISTS, message);
            }

            return ImmutableList.copyOf(Iterables.concat(builtInCatalogs.values(), catalogStore.getCatalogs()));
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            return catalogStore.createCatalogProperties(catalogName, connectorName, properties);
        }

        @Override
        public void addOrReplaceCatalog(CatalogProperties catalogProperties)
        {
            verifyBuiltInCatalog(catalogProperties.name(), "Creating or replacing");
            catalogStore.addOrReplaceCatalog(catalogProperties);
        }

        @Override
        public void removeCatalog(CatalogName catalogName)
        {
            verifyBuiltInCatalog(catalogName, "Dropping");
            catalogStore.removeCatalog(catalogName);
        }

        private static String normalizedName(CatalogName catalogName)
        {
            return catalogName.toString().toLowerCase(Locale.ROOT);
        }

        private void verifyBuiltInCatalog(CatalogName catalogName, String operation)
        {
            String normalizedName = normalizedName(catalogName);
            if (builtInCatalogs.containsKey(normalizedName)) {
                throw new TrinoException(NOT_SUPPORTED, operation + " built-in catalog " + normalizedName + " is not allowed");
            }
        }
    }
}
