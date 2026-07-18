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
package io.trino.plugin.objectstore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.airlift.bootstrap.LifeCycleManager;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorCacheMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSubstitutionMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.objectstore.substitution.ObjectStoreSubstitutionMetadata;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TypeManager;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.immutableEnumSet;
import static com.google.common.collect.Sets.symmetricDifference;
import static com.google.common.collect.Streams.forEachPair;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.objectstore.FeatureExposure.UNDEFINED;
import static io.trino.plugin.objectstore.FeatureExposures.tableProcedureExposureDecisions;
import static io.trino.plugin.objectstore.MethodHandles.translateArguments;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.connector.ConnectorCapabilities.ALWAYS_VISIBLE_SYSTEM_TABLES;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_GRACE_PERIOD;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_WHEN_STALE_BEHAVIOR;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toSet;

public class ObjectStoreConnector
        implements Connector
{
    private final Injector injector;
    private final DelegateConnectors delegates;
    private final Connector hiveConnector;
    private final Connector icebergConnector;
    private final Connector deltaConnector;
    private final Connector hudiConnector;
    private final LifeCycleManager lifeCycleManager;
    private final TypeManager typeManager;
    private final FeatureExposures featureExposures;
    private final ObjectStoreSplitManager splitManager;
    private final ObjectStorePageSourceProviderFactory pageSourceProviderFactory;
    private final ObjectStorePageSinkProvider pageSinkProvider;
    private final ObjectStoreNodePartitioningProvider nodePartitioningProvider;
    private final List<PropertyMetadata<?>> schemaProperties;
    private final ObjectStoreTableProperties tableProperties;
    private final List<PropertyMetadata<?>> branchProperties;
    private final List<PropertyMetadata<?>> columnProperties;
    private final ObjectStoreMaterializedViewProperties materializedViewProperties;
    private final ObjectStoreSessionProperties sessionProperties;
    private final List<PropertyMetadata<?>> analyzeProperties;
    private final Set<Procedure> procedures;
    private final Set<TableProcedureMetadata> tableProcedures;
    private final Set<SystemTable> systemTables;
    private final Procedure flushMetadataCache;
    private final Optional<Procedure> migrateHiveToIcebergProcedure;
    private final boolean hiveRecursiveDirWalkerEnabled;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final FunctionProvider functionProvider;
    private final Tracer tracer;
    private final CatalogName catalogName;

    private final IcebergFileFormat defaultIcebergFileFormat;
    private final RelationTypeCache relationTypeCache = new RelationTypeCache();
    private final ExecutorService parallelInformationSchemaQueryingExecutor;
    private final boolean isIcebergRestCatalogUsed;

    @Inject
    public ObjectStoreConnector(
            Injector injector,
            DelegateConnectors delegates,
            LifeCycleManager lifeCycleManager,
            TypeManager typeManager,
            ObjectStoreSplitManager splitManager,
            ObjectStorePageSourceProviderFactory pageSourceProviderFactory,
            ObjectStorePageSinkProvider pageSinkProvider,
            ObjectStoreNodePartitioningProvider nodePartitioningProvider,
            ObjectStoreTableProperties tableProperties,
            ObjectStoreMaterializedViewProperties materializedViewProperties,
            ObjectStoreSessionProperties sessionProperties,
            Set<Procedure> objectStoreProcedures,
            Set<ConnectorTableFunction> tableFunctions,
            FunctionProvider functionProvider,
            FeatureExposures featureExposures,
            ObjectStoreConfig objectStoreConfig,
            HiveConfig hiveConfig,
            Tracer tracer,
            CatalogName catalogName)
    {
        this.injector = requireNonNull(injector, "injector is null");
        this.delegates = requireNonNull(delegates, "delegates is null");
        this.hiveConnector = delegates.hiveConnector();
        this.icebergConnector = delegates.icebergConnector();
        this.deltaConnector = delegates.deltaConnector();
        this.hudiConnector = delegates.hudiConnector();
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.featureExposures = requireNonNull(featureExposures, "featureExposures is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.schemaProperties = schemaProperties();
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.branchProperties = branchProperties(delegates);
        boolean hivePartitionProjectionEnabled = hiveConfig.isPartitionProjectionEnabled();
        this.columnProperties = columnProperties(delegates, hivePartitionProjectionEnabled);
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.analyzeProperties = analyzeProperties(delegates);
        this.isIcebergRestCatalogUsed = objectStoreConfig.isIcebergRestCatalogUsed();
        this.procedures = procedures(delegates, sessionProperties, objectStoreProcedures);
        this.tableProcedures = tableProcedures(delegates);
        this.systemTables = systemTables(delegates);
        this.flushMetadataCache = objectStoreProcedures.stream()
                .filter(procedure -> procedure.getName().equals("flush_metadata_cache"))
                .collect(onlyElement());
        this.migrateHiveToIcebergProcedure = isIcebergRestCatalogUsed
                ? Optional.empty()
                : Optional.of(icebergConnector.getProcedures().stream().filter(procedure -> procedure.getName().equals("migrate")).collect(onlyElement()));
        this.hiveRecursiveDirWalkerEnabled = hiveConfig.getRecursiveDirWalkerEnabled();
        this.parallelInformationSchemaQueryingExecutor = newFixedThreadPool(objectStoreConfig.getMaxMetadataQueriesProcessingThreads(), daemonThreadsNamed("osc-information-schema-%s"));
        this.tableFunctions = ImmutableSet.copyOf(requireNonNull(tableFunctions, "tableFunctions is null"));
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.defaultIcebergFileFormat = objectStoreConfig.getDefaultIcebergFileFormat();
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @VisibleForTesting
    Injector getInjector()
    {
        return injector;
    }

    @VisibleForTesting
    DelegateConnectors getDelegates()
    {
        return delegates;
    }

    private static List<PropertyMetadata<?>> schemaProperties()
    {
        return ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        "location",
                        "Base file system location URI",
                        null,
                        false))
                .build();
    }

    private static List<PropertyMetadata<?>> columnProperties(DelegateConnectors delegates, boolean partitionProjectionEnabled)
    {
        verify(delegates.hiveConnector().getColumnProperties().stream().allMatch(
                property -> property.getName().startsWith("partition_projection_")), "Unexpected Hive column properties");
        verify(delegates.icebergConnector().getColumnProperties().isEmpty(), "Unexpected Iceberg column properties");
        verify(delegates.deltaConnector().getColumnProperties().isEmpty(), "Unexpected Delta Lake column properties");
        verify(delegates.hudiConnector().getColumnProperties().isEmpty(), "Unexpected Hudi column properties");

        ImmutableList.Builder<PropertyMetadata<?>> columnProperties = ImmutableList.builder();
        if (partitionProjectionEnabled) {
            columnProperties.addAll(delegates.hiveConnector().getColumnProperties());
        }
        return columnProperties.build();
    }

    private static List<PropertyMetadata<?>> analyzeProperties(DelegateConnectors delegates)
    {
        Map<String, PropertyMetadata<?>> properties = new HashMap<>();
        for (Connector connector : delegates.asList()) {
            for (PropertyMetadata<?> property : connector.getAnalyzeProperties()) {
                PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
                if (existing != null) {
                    verifyPropertyMetadata(property, existing);
                }
            }
        }
        return ImmutableList.copyOf(properties.values());
    }

    private static List<PropertyMetadata<?>> branchProperties(DelegateConnectors delegates)
    {
        Map<String, PropertyMetadata<?>> properties = new HashMap<>();
        for (Connector connector : delegates.asList()) {
            for (PropertyMetadata<?> property : connector.getBranchProperties()) {
                PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
                if (existing != null) {
                    verifyPropertyMetadata(property, existing);
                }
            }
        }
        return ImmutableList.copyOf(properties.values());
    }

    private Set<Procedure> procedures(DelegateConnectors delegates, ObjectStoreSessionProperties sessionProperties, Set<Procedure> objectStoreProcedures)
    {
        Map<String, Procedure> procedures = new HashMap<>();
        objectStoreProcedures.forEach(procedure -> procedures.put(procedure.getName(), procedure));
        Table<TableType, String, FeatureExposure> procedureExposures = HashBasedTable.create(featureExposures.procedureExposureDecisions());
        delegates.byType().forEach((type, connector) -> {
            for (Procedure procedure : connector.getProcedures()) {
                String name = procedure.getName();
                switch (requireNonNullElse(procedureExposures.remove(type, name), UNDEFINED)) {
                    case INACCESSIBLE -> {
                        /* skipped */
                    }
                    case UNDEFINED -> throw new IllegalStateException("Unknown procedure provided by %s: %s".formatted(type, name));
                    case EXPOSED -> {
                        Procedure boundProcedure = translateArguments(procedure.getMethodHandle(), ConnectorSession.class, session -> sessionProperties.unwrap(type, session))
                                .map(methodHandle -> new Procedure(
                                        procedure.getSchema(),
                                        procedure.getName(),
                                        procedure.getArguments(),
                                        methodHandle,
                                        procedure.requiresNamedArguments()))
                                .orElse(procedure);
                        Procedure existing = procedures.putIfAbsent(name, boundProcedure);
                        if (existing != null) {
                            throw new VerifyException("Duplicate procedure: " + name);
                        }
                    }
                }
            }
        });

        // `migrate` Iceberg procedure should be removed as Iceberg REST catalog doesn't support it.
        if (isIcebergRestCatalogUsed) {
            procedureExposures.remove(ICEBERG, "migrate");
        }
        if (!procedureExposures.isEmpty()) {
            throw new IllegalStateException("Procedures no longer provided: " + Maps.transformValues(procedureExposures.rowMap(), Map::keySet));
        }

        return ImmutableSet.copyOf(procedures.values());
    }

    private static Set<TableProcedureMetadata> tableProcedures(DelegateConnectors delegates)
    {
        // Table procedures are currently defined on per-Connector basis. TODO Let engine ask for table procedures via ConnectorMetadata, for given table, so that we don't have to resolve collisions.

        Map<String, TableProcedureMetadata> tableProcedures = new HashMap<>();
        Table<TableType, String, FeatureExposure> featureExposures = HashBasedTable.create(tableProcedureExposureDecisions());
        delegates.byType().forEach((type, connector) -> {
            for (TableProcedureMetadata procedure : connector.getTableProcedures()) {
                String name = procedure.getName();
                verify(name.equals(name.toUpperCase(Locale.ROOT)), "Procedure name is not uppercase: %s", name);

                switch (requireNonNullElse(featureExposures.remove(type, name), UNDEFINED)) {
                    case INACCESSIBLE -> {
                        /* skipped */
                    }
                    case UNDEFINED -> throw new IllegalStateException("Unknown table procedure provided by %s: %s".formatted(type, name));
                    case EXPOSED -> {
                        TableProcedureMetadata existing = tableProcedures.putIfAbsent(name, procedure);
                        if (existing == null) {
                            continue;
                        }

                        verify(procedure.getExecutionMode().isReadsData() == existing.getExecutionMode().isReadsData(),
                                "Procedure uses different execution mode for reads data: %s",
                                name);
                        verify(procedure.getExecutionMode().supportsFilter() == existing.getExecutionMode().supportsFilter(),
                                "Procedure uses different execution mode for supports filter: %s",
                                name);

                        Set<String> difference = symmetricDifference(
                                procedure.getProperties().stream()
                                        .map(PropertyMetadata::getName)
                                        .collect(toSet()),
                                existing.getProperties().stream()
                                        .map(PropertyMetadata::getName)
                                        .collect(toSet()));
                        verify(difference.isEmpty(), "Procedure '%s' has different properties: %s", name, difference);

                        forEachPair(
                                procedure.getProperties().stream().sorted(comparing(PropertyMetadata::getName)),
                                existing.getProperties().stream().sorted(comparing(PropertyMetadata::getName)),
                                PropertyMetadataValidation::verifyPropertyMetadata);
                    }
                }
            }
        });

        if (!featureExposures.isEmpty()) {
            throw new IllegalStateException("Procedures no longer provided: " + Maps.transformValues(featureExposures.rowMap(), Map::keySet));
        }

        return ImmutableSet.copyOf(tableProcedures.values());
    }

    private Set<SystemTable> systemTables(DelegateConnectors delegates)
    {
        Map<SchemaTableName, SystemTable> systemTables = new HashMap<>();
        Table<TableType, SchemaTableName, FeatureExposure> systemTableExposures = HashBasedTable.create(featureExposures.systemTableExposureDecisions());
        delegates.byType().forEach((type, connector) -> {
            for (SystemTable systemTable : connector.getSystemTables()) {
                SchemaTableName name = systemTable.getTableMetadata().getTable();

                switch (requireNonNullElse(systemTableExposures.remove(type, name), UNDEFINED)) {
                    case INACCESSIBLE -> {
                        /* skipped */
                    }
                    case UNDEFINED -> throw new IllegalStateException("Unknown system table provided by %s: %s".formatted(type, name));
                    case EXPOSED -> {
                        SystemTable existing = systemTables.putIfAbsent(name, systemTable);
                        if (existing == null) {
                            continue;
                        }

                        verify(systemTable.getDistribution() == existing.getDistribution(),
                                "System table uses different distribution: %s",
                                name);
                        verify(systemTable.getTableMetadata().getTable().equals(existing.getTableMetadata().getTable()),
                                "System table uses different table: %s",
                                name);
                        verify(systemTable.getTableMetadata().getCheckConstraints().equals(existing.getTableMetadata().getCheckConstraints()),
                                "System table uses different check constraints: %s",
                                name);
                        verify(systemTable.getTableMetadata().getColumns().equals(existing.getTableMetadata().getColumns()),
                                "System table uses different columns: %s",
                                name);
                        verify(systemTable.getTableMetadata().getComment().equals(existing.getTableMetadata().getComment()),
                                "System table uses different comment: %s",
                                name);

                        Set<String> propertiesDiff = symmetricDifference(
                                systemTable.getTableMetadata().getProperties().keySet(),
                                existing.getTableMetadata().getProperties().keySet());
                        verify(propertiesDiff.isEmpty(),
                                "System table '%s' uses different properties: %s",
                                name,
                                propertiesDiff);

                        Set<Map.Entry<String, Object>> propertyValuesDiff = symmetricDifference(
                                systemTable.getTableMetadata().getProperties().entrySet(),
                                existing.getTableMetadata().getProperties().entrySet());
                        List<String> propertyNamesWithValuesDiff = propertyValuesDiff.stream()
                                .map(Map.Entry::getKey)
                                .collect(toImmutableList());
                        verify(propertyValuesDiff.isEmpty(),
                                "System table '%s' uses different properties values for: %s",
                                name,
                                propertyNamesWithValuesDiff);
                    }
                }
            }
        });

        if (!systemTableExposures.isEmpty()) {
            throw new IllegalStateException("System tables no longer provided: " + Maps.transformValues(systemTableExposures.rowMap(), Map::keySet));
        }

        return ImmutableSet.copyOf(systemTables.values());
    }

    @Override
    public ObjectStoreTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);

        HiveTransactionHandle hiveHandle = (HiveTransactionHandle) hiveConnector.beginTransaction(isolationLevel, readOnly, true);
        HiveTransactionHandle icebergHandle = (HiveTransactionHandle) icebergConnector.beginTransaction(isolationLevel, readOnly, true);
        HiveTransactionHandle deltaHandle = (HiveTransactionHandle) deltaConnector.beginTransaction(isolationLevel, readOnly, true);
        HiveTransactionHandle hudiHandle = (HiveTransactionHandle) hudiConnector.beginTransaction(isolationLevel, readOnly, true);

        return new ObjectStoreTransactionHandle(hiveHandle, icebergHandle, deltaHandle, hudiHandle);
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        ObjectStoreTransactionHandle handle = (ObjectStoreTransactionHandle) transactionHandle;

        // only one of the connectors will be used
        hiveConnector.commit(handle.getHiveHandle());
        icebergConnector.commit(handle.getIcebergHandle());
        deltaConnector.commit(handle.getDeltaHandle());
        hudiConnector.commit(handle.getHudiHandle());
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        ObjectStoreTransactionHandle handle = (ObjectStoreTransactionHandle) transactionHandle;

        // only one of the connectors will be used
        hiveConnector.rollback(handle.getHiveHandle());
        icebergConnector.rollback(handle.getIcebergHandle());
        deltaConnector.rollback(handle.getDeltaHandle());
        hudiConnector.rollback(handle.getHudiHandle());
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
        hiveConnector.shutdown();
        icebergConnector.shutdown();
        deltaConnector.shutdown();
        hudiConnector.shutdown();
        parallelInformationSchemaQueryingExecutor.shutdown();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        ObjectStoreTransactionHandle handle = (ObjectStoreTransactionHandle) transactionHandle;

        ConnectorMetadata hiveMetadata = hiveConnector.getMetadata(session, handle.getHiveHandle());
        ConnectorMetadata icebergMetadata = icebergConnector.getMetadata(session, handle.getIcebergHandle());
        ConnectorMetadata deltaMetadata = deltaConnector.getMetadata(session, handle.getDeltaHandle());
        ConnectorMetadata hudiMetadata = hudiConnector.getMetadata(session, handle.getHudiHandle());

        return new ClassLoaderSafeConnectorMetadata(
                new ObjectStoreMetadata(
                        typeManager,
                        // It's enough to have ClassLoaderSafeConnectorMetadata once (outside of ObjectStoreMetadata). All delegates share same classloader.
                        new TracingObjectStoreConnectorMetadata<>(tracer, catalogName, (TransactionalMetadata) ((ClassLoaderSafeConnectorMetadata) hiveMetadata).unwrap()),
                        new TracingObjectStoreConnectorMetadata<>(tracer, catalogName, (IcebergMetadata) ((ClassLoaderSafeConnectorMetadata) icebergMetadata).unwrap()),
                        new TracingObjectStoreConnectorMetadata<>(tracer, catalogName, (DeltaLakeMetadata) ((ClassLoaderSafeConnectorMetadata) deltaMetadata).unwrap()),
                        new TracingObjectStoreConnectorMetadata<>(tracer, catalogName, ((ClassLoaderSafeConnectorMetadata) hudiMetadata).unwrap()),
                        tableProperties,
                        sessionProperties,
                        flushMetadataCache,
                        migrateHiveToIcebergProcedure,
                        hiveRecursiveDirWalkerEnabled,
                        defaultIcebergFileFormat,
                        relationTypeCache,
                        parallelInformationSchemaQueryingExecutor,
                        isIcebergRestCatalogUsed),
                getClass().getClassLoader());
    }

    @Override
    public ConnectorSubstitutionMetadata getSubstitutionMetadata()
    {
        return new ClassLoaderSafeConnectorSubstitutionMetadata(
                new ObjectStoreSubstitutionMetadata(
                        icebergConnector.getSubstitutionMetadata(),
                        hiveConnector.getSubstitutionMetadata()),
                getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorCacheMetadata getCacheMetadata()
    {
        ConnectorCacheMetadata hiveMetadata = hiveConnector.getCacheMetadata();
        ConnectorCacheMetadata icebergMetadata = icebergConnector.getCacheMetadata();
        ConnectorCacheMetadata deltaMetadata = deltaConnector.getCacheMetadata();
        return new ClassLoaderSafeConnectorCacheMetadata(
                new ObjectStoreCacheMetadata(
                        ((ClassLoaderSafeConnectorCacheMetadata) hiveMetadata).unwrap(),
                        ((ClassLoaderSafeConnectorCacheMetadata) icebergMetadata).unwrap(),
                        ((ClassLoaderSafeConnectorCacheMetadata) deltaMetadata).unwrap()),
                getClass().getClassLoader());
    }

    @Override
    public ConnectorPageSourceProviderFactory getPageSourceProviderFactory()
    {
        return pageSourceProviderFactory;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(
                DEFAULT_COLUMN_VALUE,
                NOT_NULL_COLUMN_CONSTRAINT,
                MATERIALIZED_VIEW_GRACE_PERIOD,
                MATERIALIZED_VIEW_WHEN_STALE_BEHAVIOR,
                ALWAYS_VISIBLE_SYSTEM_TABLES);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getBranchProperties()
    {
        return branchProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties.getProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return tableProcedures;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(functionProvider);
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }
}
