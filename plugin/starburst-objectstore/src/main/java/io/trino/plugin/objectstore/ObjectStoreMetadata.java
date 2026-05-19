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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.trino.metastore.Table;
import io.trino.plugin.base.util.MaybeLazy;
import io.trino.plugin.deltalake.CorruptedDeltaLakeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeInsertTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMergeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeOutputTableHandle;
import io.trino.plugin.deltalake.DeltaLakePartitioningHandle;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesTableFunctionHandle;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveOutputTableHandle;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hive.HiveTableExecuteHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveViewNotSupportedException;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.ViewReaderUtil;
import io.trino.plugin.hive.functions.Unload.UnloadFunctionHandle;
import io.trino.plugin.hive.procedure.OptimizeTableProcedure;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.CorruptedIcebergTableHandle;
import io.trino.plugin.iceberg.IcebergExceptions;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergMaterializedViewDefinition;
import io.trino.plugin.iceberg.IcebergMergeTableHandle;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.IcebergPartitioningHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergTableName;
import io.trino.plugin.iceberg.IcebergWritableTableHandle;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ApplyPartialTopNResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorWritableTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.UnificationResult;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.toSpiMaterializedViewColumns;
import static io.trino.plugin.objectstore.RelationType.MATERIALIZED_VIEW;
import static io.trino.plugin.objectstore.RelationType.VIEW;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class ObjectStoreMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(ObjectStoreMetadata.class);

    private final TypeManager typeManager;
    private final TracingObjectStoreConnectorMetadata<TransactionalMetadata> hiveMetadata;
    private final TracingObjectStoreConnectorMetadata<IcebergMetadata> icebergMetadata;
    private final TracingObjectStoreConnectorMetadata<DeltaLakeMetadata> deltaMetadata;
    private final TracingObjectStoreConnectorMetadata<ConnectorMetadata> hudiMetadata;
    private final ObjectStoreTableProperties tableProperties;
    private final ObjectStoreSessionProperties sessionProperties;
    private final PropertyMetadata<?> hiveFormatProperty;
    private final PropertyMetadata<?> hiveSortedByProperty;
    private final PropertyMetadata<?> icebergFormatProperty;
    private final Procedure flushMetadataCache;
    private final Optional<Procedure> migrateHiveToIcebergProcedure;
    private final boolean hiveRecursiveDirWalkerEnabled;
    private final IcebergFileFormat defaultIcebergFileFormat;
    private final RelationTypeCache relationTypeCache;
    private final ExecutorService parallelInformationSchemaQueryingExecutor;
    private final boolean isIcebergRestCatalogUsed;

    public ObjectStoreMetadata(
            TypeManager typeManager,
            TracingObjectStoreConnectorMetadata<TransactionalMetadata> hiveMetadata,
            TracingObjectStoreConnectorMetadata<IcebergMetadata> icebergMetadata,
            TracingObjectStoreConnectorMetadata<DeltaLakeMetadata> deltaMetadata,
            TracingObjectStoreConnectorMetadata<ConnectorMetadata> hudiMetadata,
            ObjectStoreTableProperties tableProperties,
            ObjectStoreSessionProperties sessionProperties,
            Procedure flushMetadataCache,
            Optional<Procedure> migrateHiveToIcebergProcedure,
            boolean hiveRecursiveDirWalkerEnabled,
            IcebergFileFormat defaultIcebergFileFormat,
            RelationTypeCache relationTypeCache,
            ExecutorService parallelInformationSchemaQueryingExecutor,
            boolean isIcebergRestCatalogUsed)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hiveMetadata = requireNonNull(hiveMetadata, "hiveMetadata is null");
        this.icebergMetadata = requireNonNull(icebergMetadata, "icebergMetadata is null");
        this.deltaMetadata = requireNonNull(deltaMetadata, "deltaMetadata is null");
        this.hudiMetadata = requireNonNull(hudiMetadata, "hudiMetadata is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.hiveFormatProperty = tableProperties.getHiveFormatProperty();
        this.hiveSortedByProperty = tableProperties.getHiveSortedByProperty();
        this.icebergFormatProperty = tableProperties.getIcebergFormatProperty();
        this.flushMetadataCache = requireNonNull(flushMetadataCache, "flushMetadataCache is null");
        this.migrateHiveToIcebergProcedure = requireNonNull(migrateHiveToIcebergProcedure, "migrateHiveToIcebergProcedure is null");
        this.hiveRecursiveDirWalkerEnabled = hiveRecursiveDirWalkerEnabled;
        this.defaultIcebergFileFormat = requireNonNull(defaultIcebergFileFormat, "defaultIcebergFileFormat is null");
        this.relationTypeCache = requireNonNull(relationTypeCache, "relationTypeCache is null");
        this.parallelInformationSchemaQueryingExecutor = requireNonNull(parallelInformationSchemaQueryingExecutor, "parallelInformationSchemaQueryingExecutor is null");
        this.isIcebergRestCatalogUsed = isIcebergRestCatalogUsed;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.schemaExists(unwrap(HIVE, session), schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return hiveMetadata.listSchemaNames(unwrap(HIVE, session));
    }

    @Nullable
    private ConnectorTableHandle getTableHandleInOrder(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion,
            List<TableType> candidateTypes)
    {
        checkArgument(candidateTypes.size() == 4);
        TrinoException deferredException = null;
        for (TableType candidateType : candidateTypes) {
            try {
                return delegate(candidateType).getTableHandle(unwrap(candidateType, session), tableName, startVersion, endVersion);
            }
            catch (TrinoException e) {
                if (!isError(e, UNSUPPORTED_TABLE_TYPE) && !isVersionedTableUnsupported(e)) {
                    throw e;
                }
                deferredException = e;
            }
        }

        // Propagate last exception
        throw deferredException;
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isEmpty() && endVersion.isEmpty()) {
            if (IcebergTableName.isIcebergTableName(tableName.getTableName()) && IcebergTableName.isMaterializedViewStorage(tableName.getTableName())) {
                return delegate(ICEBERG).getTableHandle(unwrap(ICEBERG, session), tableName, Optional.empty(), Optional.empty());
            }

            // Typically, getTableHandle is called after getMaterializedView and getView, so no point in checking cached RelationCategory
            ConnectorTableHandle tableHandle = getTableHandleInOrder(session, tableName, Optional.empty(), Optional.empty(), relationTypeCache.getTableTypeAffinity(tableName));
            if (tableHandle != null) {
                relationTypeCache.record(tableName, tableType(tableHandle));
            }
            return tableHandle;
        }

        try {
            return getTableHandleInOrder(session, tableName, startVersion, endVersion, relationTypeCache.getTableTypeAffinity(tableName));
        }
        catch (TrinoException e) {
            if (!isError(e, UNSUPPORTED_TABLE_TYPE) && !isVersionedTableUnsupported(e)) {
                throw e;
            }
        }
        throw new TrinoException(NOT_SUPPORTED, "Versioning is only supported for Iceberg and Delta Lake tables");
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getStatisticsCollectionMetadata(unwrap(tableType, session), tableHandle, analyzeProperties);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorAccessControl accessControl, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HIVE && procedureName.equals(OptimizeTableProcedure.NAME)) {
            throw new TrinoException(NOT_SUPPORTED, "Executing OPTIMIZE on Hive tables is not supported");
        }
        return delegate(tableType).getTableHandleForExecute(unwrap(tableType, session), accessControl, tableHandle, procedureName, executeProperties, retryMode);
    }

    @Override
    public Set<ColumnHandle> getColumnHandlesForTableExecute(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableExecuteHandle connectorTableExecuteHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getColumnHandlesForTableExecute(unwrap(tableType, session), tableHandle, connectorTableExecuteHandle);
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        TableType tableType = tableType(tableExecuteHandle);
        return delegate(tableType).getLayoutForTableExecute(unwrap(tableType, session), tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
    {
        TableType tableType = tableType(tableExecuteHandle);
        return delegate(tableType).beginTableExecute(unwrap(tableType, session), tableExecuteHandle, updatedSourceTableHandle);
    }

    @Override
    public Map<String, Long> finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        TableType tableType = tableType(tableExecuteHandle);
        return delegate(tableType).finishTableExecute(unwrap(tableType, session), tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        TableType tableType = tableType(tableExecuteHandle);
        return delegate(tableType).executeTableExecute(unwrap(tableType, session), tableExecuteHandle);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getHiveSystemTable(session, tableName)
                .or(() -> getIcebergSystemTable(session, tableName))
                .or(() -> getDeltaLakeSystemTable(session, tableName))
                .or(() -> getHudiSystemTable(session, tableName));
    }

    private Optional<SystemTable> getHiveSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return hiveMetadata.getSystemTable(unwrap(HIVE, session), tableName);
        }
        catch (TrinoException e) {
            if (isError(e, HIVE_UNSUPPORTED_FORMAT)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private Optional<SystemTable> getIcebergSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return icebergMetadata.getSystemTable(unwrap(ICEBERG, session), tableName);
        }
        catch (TrinoException e) {
            if (isError(e, UNSUPPORTED_TABLE_TYPE, NOT_SUPPORTED)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private Optional<SystemTable> getDeltaLakeSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return deltaMetadata.getSystemTable(unwrap(DELTA, session), tableName);
        }
        catch (TrinoException e) {
            if (isError(e, UNSUPPORTED_TABLE_TYPE, NOT_SUPPORTED)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private Optional<SystemTable> getHudiSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return hudiMetadata.getSystemTable(unwrap(HUDI, session), tableName);
        }
        catch (TrinoException e) {
            if (isError(e, UNSUPPORTED_TABLE_TYPE, NOT_SUPPORTED)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @SuppressWarnings("removal")
    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            boolean hasForeignSourceTables,
            RetryMode retryMode,
            RefreshType refreshType)
    {
        if (!tableHandle.getClass().equals(IcebergTableHandle.class)) {
            throw new TrinoException(NOT_SUPPORTED, "Refreshing materialized views with a target table that is not an Iceberg table is not supported");
        }
        return icebergMetadata.beginRefreshMaterializedView(unwrap(ICEBERG, session), tableHandle, sourceTableHandles, hasForeignSourceTables, retryMode, refreshType);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            boolean hasForeignSourceTables,
            boolean hasSourceTableFunctions,
            boolean hasNonDeterministicFunctions)
    {
        return icebergMetadata.finishRefreshMaterializedView(
                unwrap(ICEBERG, session),
                tableHandle,
                insertHandle,
                fragments,
                computedStatistics,
                sourceTableHandles,
                hasForeignSourceTables,
                hasSourceTableFunctions,
                hasNonDeterministicFunctions);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting)
    {
        icebergMetadata.createMaterializedView(
                unwrap(ICEBERG, session),
                viewName,
                definition,
                ObjectStoreMaterializedViewProperties.addIcebergPropertyOverrides(materializedViewProperties),
                replace,
                ignoreExisting);
        relationTypeCache.record(viewName, MATERIALIZED_VIEW);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorSession icebergSession = unwrap(ICEBERG, session);
        Optional<ConnectorMaterializedViewDefinition> materializedView = icebergMetadata.getMaterializedView(icebergSession, viewName);
        icebergMetadata.dropMaterializedView(icebergSession, viewName);
        flushMetadataCache(session, viewName);
        materializedView
                .flatMap(ConnectorMaterializedViewDefinition::getStorageTable)
                .map(CatalogSchemaTableName::getSchemaTableName)
                .ifPresent(view -> flushMetadataCache(session, view));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.listMaterializedViews(unwrap(ICEBERG, session), schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.getMaterializedViews(unwrap(ICEBERG, session), schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        RelationType relationType = relationTypeCache.getRelationType(viewName).orElse(null);
        if (relationType != null) {
            switch (relationType) {
                case MATERIALIZED_VIEW -> {
                    // Do nothing.
                }

                case VIEW -> {
                    if (doGetView(session, viewName).isPresent()) {
                        // This is a view, so not a materialized view
                        return Optional.empty();
                    }
                }

                case HIVE_TABLE, ICEBERG_TABLE, DELTA_TABLE, HUDI_TABLE -> {
                    if (getTableHandle(session, viewName, Optional.empty(), Optional.empty()) != null) {
                        // This is a table, so not a materialized view
                        return Optional.empty();
                    }
                }
            }
        }

        return doGetMaterializedView(session, viewName);
    }

    private Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = icebergMetadata.getMaterializedView(unwrap(ICEBERG, session), viewName);
        if (materializedViewDefinition.isPresent()) {
            relationTypeCache.record(viewName, MATERIALIZED_VIEW);
        }
        return materializedViewDefinition;
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
    {
        return ObjectStoreMaterializedViewProperties.removeOverriddenOrRemovedIcebergProperties(
                icebergMetadata.getMaterializedViewProperties(unwrap(ICEBERG, session), viewName, materializedViewDefinition));
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name, boolean considerGracePeriod)
    {
        return icebergMetadata.getMaterializedViewFreshness(unwrap(ICEBERG, session), name, considerGracePeriod);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        icebergMetadata.renameMaterializedView(unwrap(ICEBERG, session), source, target);
        relationTypeCache.record(target, MATERIALIZED_VIEW);
        flushMetadataCache(session, source);
        flushMetadataCache(session, target);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        icebergMetadata.setMaterializedViewProperties(unwrap(ICEBERG, session), viewName, properties);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        icebergMetadata.setMaterializedViewColumnComment(unwrap(ICEBERG, session), viewName, columnName, comment);
        flushMetadataCache(session, viewName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorPartitioningHandle> partitioningHandle, List<ColumnHandle> columns)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyPartitioning(unwrap(tableType, session), tableHandle, partitioningHandle, columns);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        TableType leftType = tableType(left);
        if (leftType == tableType(right)) {
            return Optional.empty();
        }
        return delegate(leftType).getCommonPartitioningHandle(unwrap(leftType, session), left, right);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableName(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        ConnectorTableMetadata tableMetadata = delegate(tableType).getTableMetadata(unwrap(tableType, session), tableHandle);
        return withProperties(tableMetadata, ImmutableMap.<String, Object>builder()
                .putAll(wrap(tableType, tableMetadata.getProperties()))
                .put("type", tableType)
                .buildOrThrow());
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return icebergMetadata.listFunctions(unwrap(ICEBERG, session), schemaName);
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return icebergMetadata.getFunctions(unwrap(ICEBERG, session), name);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return icebergMetadata.getFunctionMetadata(unwrap(ICEBERG, session), functionId);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return icebergMetadata.getFunctionDependencies(unwrap(ICEBERG, session), functionId, boundSignature);
    }

    @Override
    public void createBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String branch, Optional<String> fromBranch, SaveMode saveMode, Map<String, Object> properties)
    {
        icebergMetadata.createBranch(unwrap(ICEBERG, session), tableHandle, branch, fromBranch, saveMode, properties);
    }

    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String branch)
    {
        icebergMetadata.dropBranch(unwrap(ICEBERG, session), tableHandle, branch);
    }

    @Override
    public void fastForwardBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String sourceBranch, String targetBranch)
    {
        icebergMetadata.fastForwardBranch(unwrap(ICEBERG, session), tableHandle, sourceBranch, targetBranch);
    }

    @Override
    public Collection<String> listBranches(ConnectorSession session, SchemaTableName tableName)
    {
        return icebergMetadata.listBranches(unwrap(ICEBERG, session), tableName);
    }

    @Override
    public boolean branchExists(ConnectorSession session, SchemaTableName tableName, String branch)
    {
        return icebergMetadata.branchExists(unwrap(ICEBERG, session), tableName, branch);
    }

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getInfo(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Metrics getMetrics(ConnectorSession session)
    {
        // TODO: support other type catalog metrics
        return hiveMetadata.getMetrics(unwrap(HIVE, session));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.listTables(unwrap(HIVE, session), schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getColumnHandles(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getColumnMetadata(unwrap(tableType, session), tableHandle, columnHandle);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("listTableColumns should not be called when streamRelationColumns is implemented");
    }

    @Override
    @SuppressWarnings("deprecation")
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("streamTableColumns should not be called when streamRelationColumns is implemented");
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        ConnectorSession hiveSession = unwrap(HIVE, session);
        ConnectorSession icebergSession = unwrap(ICEBERG, session);
        ConnectorSession deltaSession = unwrap(DELTA, session);

        return streamRelationMetadata(
                session,
                schemaName,
                relationFilter,
                "columns",
                (relation, table) -> {
                    boolean trinoMaterializedView = isTrinoMaterializedView(table);
                    boolean trinoView = isTrinoView(table);
                    boolean hiveView = isHiveView(table);

                    if (trinoView) {
                        ConnectorViewDefinition viewDefinition = ViewReaderUtil.PrestoViewReader.decodeViewData(table.getViewOriginalText().orElseThrow());
                        return Optional.of(MaybeLazy.ofValue(RelationColumnsMetadata.forView(relation, viewDefinition.getColumns())));
                    }
                    if (trinoMaterializedView) {
                        IcebergMaterializedViewDefinition materializedViewDefinition = decodeMaterializedViewData(table.getViewOriginalText().orElseThrow());
                        return Optional.of(MaybeLazy.ofValue(RelationColumnsMetadata.forMaterializedView(relation, toSpiMaterializedViewColumns(materializedViewDefinition.columns()))));
                    }
                    if (hiveView) {
                        try {
                            Optional<ConnectorViewDefinition> viewDefinition = hiveMetadata.getView(hiveSession, relation);
                            if (viewDefinition.isEmpty()) {
                                // Disappeared
                                return Optional.empty();
                            }
                            return Optional.of(MaybeLazy.ofValue(RelationColumnsMetadata.forView(relation, viewDefinition.get().getColumns())));
                        }
                        catch (HiveViewNotSupportedException ignore) {
                            // Hive view translation is not enabled, treat it as if it was a table
                        }
                    }

                    boolean icebergTable = isIcebergTable(table);
                    boolean deltaLakeTable = isDeltaLakeTable(table);
                    boolean hudiTable = isHudiTable(table);
                    boolean hiveTable = !(hiveView || icebergTable || deltaLakeTable || hudiTable);

                    MaybeLazy<List<ColumnMetadata>> tableColumnsMetadata;
                    if (hiveTable || hiveView || hudiTable) {
                        // TODO: this is probably incorrect for Hudi wrt timestamp precision, but that's what we used to be doing so far.
                        tableColumnsMetadata = MaybeLazy.ofValue(HiveUtil.getTableColumnMetadata(hiveSession, table, typeManager));
                    }
                    else if (icebergTable) {
                        tableColumnsMetadata = icebergMetadata.unwrap().getTableColumnMetadata(icebergSession, table);
                    }
                    else if (deltaLakeTable) {
                        tableColumnsMetadata = deltaMetadata.unwrap().getTableColumnMetadata(deltaSession, table);
                    }
                    else {
                        throw new UnsupportedOperationException("Unreachable");
                    }

                    return Optional.of(tableColumnsMetadata.transform(columnMetadata -> RelationColumnsMetadata.forTable(relation, columnMetadata)));
                });
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        ConnectorSession icebergSession = unwrap(ICEBERG, session);
        ConnectorSession deltaSession = unwrap(DELTA, session);

        return streamRelationMetadata(
                session,
                schemaName,
                relationFilter,
                "comments",
                (relation, table) -> {
                    boolean trinoMaterializedView = isTrinoMaterializedView(table);
                    boolean trinoView = isTrinoView(table);
                    boolean hiveView = isHiveView(table);

                    if (trinoView) {
                        ConnectorViewDefinition viewDefinition = ViewReaderUtil.PrestoViewReader.decodeViewData(table.getViewOriginalText().orElseThrow());
                        return Optional.of(MaybeLazy.ofValue(RelationCommentMetadata.forRelation(relation, viewDefinition.getComment())));
                    }
                    if (trinoMaterializedView) {
                        IcebergMaterializedViewDefinition materializedViewDefinition = decodeMaterializedViewData(table.getViewOriginalText().orElseThrow());
                        return Optional.of(MaybeLazy.ofValue(RelationCommentMetadata.forRelation(relation, materializedViewDefinition.comment())));
                    }
                    if (hiveView) {
                        return Optional.of(MaybeLazy.ofValue(RelationCommentMetadata.forRelation(relation, Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)))));
                    }

                    boolean icebergTable = isIcebergTable(table);
                    boolean deltaLakeTable = isDeltaLakeTable(table);
                    boolean hudiTable = isHudiTable(table);
                    boolean hiveTable = !(icebergTable || deltaLakeTable || hudiTable);

                    MaybeLazy<Optional<String>> tableComment;
                    if (hiveTable || hudiTable) {
                        tableComment = MaybeLazy.ofValue(Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)));
                    }
                    else if (icebergTable) {
                        tableComment = icebergMetadata.unwrap().getTableComment(icebergSession, table);
                    }
                    else if (deltaLakeTable) {
                        tableComment = deltaMetadata.unwrap().getTableComment(deltaSession, table);
                    }
                    else {
                        throw new UnsupportedOperationException("Unreachable");
                    }

                    return Optional.of(tableComment.transform(comment -> RelationCommentMetadata.forRelation(relation, comment)));
                });
    }

    private <MetadataType> Iterator<MetadataType> streamRelationMetadata(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            String metadataTypeName,
            BiFunction<SchemaTableName, Table, Optional<MaybeLazy<MetadataType>>> metadataReader)
    {
        ConnectorSession hiveSession = unwrap(HIVE, session);

        ImmutableMap.Builder<SchemaTableName, MetadataType> unfilteredResult = ImmutableMap.builder();
        ImmutableList.Builder<MetadataType> filteredResult = ImmutableList.builder();
        Map<SchemaTableName, Supplier<MetadataType>> unprocessedTables = new HashMap<>();

        List<String> schemas = schemaName.map(List::of)
                // TODO filter schemas: limit to those accessible to the user
                .orElseGet(() -> listSchemaNames(session));

        for (String schema : schemas) {
            Iterator<Table> iterator = hiveMetadata.unwrap().getMetastore().streamTables(hiveSession, schema).orElseGet(() ->
                    hiveMetadata.unwrap().getMetastore().getTables(schema).stream()
                            .map(tableInfo -> {
                                String schemaInfo = tableInfo.tableName().getSchemaName();
                                String tableName = tableInfo.tableName().getTableName();
                                try {
                                    // Attempt to retrieve the table
                                    return hiveMetadata.unwrap().getMetastore().getTable(schemaInfo, tableName);
                                }
                                catch (RuntimeException e) {
                                    // E.g. a table's output format is not supported and throws Table StorageDescriptor is null for table
                                    log.warn(e, "Failed to process metadata of table %s during streaming table for %s", schemaInfo, tableName);
                                    return Optional.<Table>empty();
                                }
                            })
                            .flatMap(Optional::stream) // Only keep successfully retrieved tables
                            .iterator());
            iterator.forEachRemaining(table -> {
                SchemaTableName relation = new SchemaTableName(schema, table.getTableName());
                Optional<MaybeLazy<MetadataType>> optionalRelationMetadata;
                try {
                    optionalRelationMetadata = metadataReader.apply(relation, table);
                }
                catch (RuntimeException e) {
                    // E.g. a table declared as Iceberg table but lacking metadata_location, etc.
                    log.warn(e, "Failed to process metadata of table %s [%s] during streaming table %s for %s", relation, table, metadataTypeName, schemaName);
                    return; // Exclude from response
                }
                optionalRelationMetadata.ifPresent(relationMetadata ->
                        relationMetadata.value().ifPresentOrElse(
                                value -> unfilteredResult.put(relation, value),
                                // lazy computation ties up little memory (basically table/metadata location), so no need for explicit bound on unprocessedTables
                                () -> unprocessedTables.put(relation, relationMetadata.lazy().orElseThrow())));
            });
        }

        Context context = Context.current();
        relationFilter.apply(unprocessedTables.keySet()).stream()
                .map(tableName -> {
                    Supplier<MetadataType> metadataSupplier = unprocessedTables.get(tableName);
                    return parallelInformationSchemaQueryingExecutor.submit(() -> {
                        try (Scope ignore = context.makeCurrent()) {
                            return Optional.of(metadataSupplier.get());
                        }
                        catch (Exception e) {
                            boolean silent = false;
                            if (IcebergExceptions.isNotFoundException(e)) {
                                silent = true;
                            }
                            else if (e instanceof TrinoException trinoException) {
                                ErrorCode errorCode = trinoException.getErrorCode();
                                // e.g. table migrated to a different format concurrently
                                silent = errorCode.equals(UNSUPPORTED_TABLE_TYPE.toErrorCode()) ||
                                        // e.g. table deleted concurrently
                                        errorCode.equals(NOT_FOUND.toErrorCode()) ||
                                        // e.g. Iceberg/Delta table being deleted concurrently resulting in failure to load metadata from filesystem
                                        errorCode.getType() == EXTERNAL;
                            }
                            if (silent) {
                                log.debug(e, "Failed to access metadata of table %s during streaming table %s for %s", tableName, metadataTypeName, schemaName);
                            }
                            else {
                                log.warn(e, "Failed to access metadata of table %s during streaming table %s for %s", tableName, metadataTypeName, schemaName);
                            }
                            return Optional.<MetadataType>empty();
                        }
                    });
                })
                .collect(toImmutableList())
                .forEach(future -> getFutureValue(future).ifPresent(filteredResult::add));

        Map<SchemaTableName, MetadataType> unfilteredResultMap = unfilteredResult.buildOrThrow();
        Set<SchemaTableName> availableNames = relationFilter.apply(unfilteredResultMap.keySet());

        return Stream.concat(
                        unfilteredResultMap.entrySet().stream()
                                .filter(entry -> availableNames.contains(entry.getKey()))
                                .map(Map.Entry::getValue),
                        filteredResult.build().stream())
                .iterator();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableStatistics(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        hiveMetadata.createSchema(unwrap(HIVE, session), schemaName, properties, owner);
        flushMetadataCache();
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            // List all objects first because such operations after adding/dropping/altering tables/views in a transaction is disallowed
            List<SchemaTableName> views = listViews(session, Optional.of(schemaName));
            Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = getMaterializedViews(session, Optional.of(schemaName));
            Set<SchemaTableName> storageTables = materializedViews.values().stream()
                    .filter(view -> view.getStorageTable().isPresent())
                    .map(view -> view.getStorageTable().get().getSchemaTableName())
                    .collect(toImmutableSet());
            List<SchemaTableName> tables = listTables(session, Optional.of(schemaName)).stream()
                    .filter(table -> !views.contains(table) && !materializedViews.containsKey(table) && !storageTables.contains(table))
                    .collect(toImmutableList());

            // Drop views and materialized views first because tables might be used from them
            for (SchemaTableName viewName : views) {
                try {
                    dropView(session, viewName);
                }
                catch (ViewNotFoundException e) {
                    log.debug("View disappeared during DROP SCHEMA CASCADE: %s", viewName);
                }
            }
            for (SchemaTableName materializedViewName : materializedViews.keySet()) {
                try {
                    dropMaterializedView(session, materializedViewName);
                }
                catch (MaterializedViewNotFoundException e) {
                    log.debug("Materialized view disappeared during DROP SCHEMA CASCADE: %s", materializedViewName);
                }
            }
            for (SchemaTableName tableName : tables) {
                try {
                    ConnectorTableHandle table = getTableHandle(session, tableName, Optional.empty(), Optional.empty());
                    if (table == null) {
                        throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Unexpected table present: " + tableName);
                    }
                    dropTable(session, table);
                }
                catch (TableNotFoundException e) {
                    log.debug("Table disappeared during DROP SCHEMA CASCADE: %s", tableName);
                }
            }
            // Commit and then drop database with raw metastore because exclusive operation after dropping object is disallowed in SemiTransactionalHiveMetastore
            hiveMetadata.unwrap().commit();
            boolean deleteData = hiveMetadata.unwrap().getMetastore().shouldDeleteDatabaseData(session, schemaName);
            hiveMetadata.unwrap().getMetastore().unsafeGetRawHiveMetastore().dropDatabase(schemaName, deleteData);
        }
        else {
            hiveMetadata.dropSchema(session, schemaName, false);
        }
        flushMetadataCache();
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        hiveMetadata.renameSchema(unwrap(HIVE, session), source, target);
        flushMetadataCache();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        TableType tableType = tableType(tableMetadata);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Table creation is not supported for Hudi");
        }
        if ((tableType != ICEBERG && tableType != DELTA) && tableMetadata.getColumns().stream().anyMatch(column -> !column.isNullable())) {
            throw new TrinoException(NOT_SUPPORTED, "%s tables do not support NOT NULL columns".formatted(tableType.displayName()));
        }
        if ((tableType != ICEBERG) && tableMetadata.getColumns().stream().anyMatch(column -> column.getDefaultValue().isPresent())) {
            throw new TrinoException(NOT_SUPPORTED, "%s tables do not support DEFAULT columns".formatted(tableType.displayName()));
        }
        tableMetadata = unwrap(tableType, tableMetadata);
        delegate(tableType).createTable(unwrap(tableType, session), tableMetadata, saveMode);
        relationTypeCache.record(tableMetadata.getTableSchema().getTable(), tableType);
        flushMetadataCache(session, tableMetadata.getTable());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping Hudi tables is not supported");
        }
        delegate(tableType).dropTable(unwrap(tableType, session), tableHandle);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).truncateTable(unwrap(tableType, session), tableHandle);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming Hudi tables is not supported");
        }
        delegate(tableType).renameTable(unwrap(tableType, session), tableHandle, newTableName);
        relationTypeCache.record(newTableName, tableType);
        flushMetadataCache(session, tableName(tableHandle));
        flushMetadataCache(session, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        TableType tableType = tableType(tableHandle);
        if (properties.containsKey("type")) {
            if (properties.size() > 1) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot set table properties when changing table type");
            }
            TableType newTableType = (TableType) properties.get("type")
                    .orElseThrow(() -> new IllegalArgumentException("The type property cannot be empty"));
            migrateTable(session, tableHandle, newTableType);
            flushMetadataCache(session, tableName(tableHandle));
            return;
        }

        Map<String, Object> nullableProperties = properties.entrySet().stream()
                .collect(HashMap::new, (accumulator, entry) -> accumulator.put(entry.getKey(), entry.getValue().orElse(null)), HashMap::putAll);
        nullableProperties = unwrap(tableType, nullableProperties);
        properties = nullableProperties.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> Optional.ofNullable(entry.getValue())));
        delegate(tableType).setTableProperties(unwrap(tableType, session), tableHandle, properties);
        flushMetadataCache(session, tableName(tableHandle));
    }

    private void migrateTable(ConnectorSession session, ConnectorTableHandle tableHandle, TableType newTableType)
    {
        TableType sourceTableType = tableType(tableHandle);
        if (sourceTableType == HIVE && newTableType == ICEBERG) {
            HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
            try {
                String recursiveDirectory = hiveRecursiveDirWalkerEnabled ? "TRUE" : "FALSE";
                if (!isIcebergRestCatalogUsed) {
                    migrateHiveToIcebergProcedure.orElseThrow().getMethodHandle().invoke(unwrap(ICEBERG, session), hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), recursiveDirectory);
                }
                else {
                    throw new TrinoException(NOT_SUPPORTED, "Migrate procedure is not supported");
                }
                return;
            }
            catch (Throwable e) {
                throw new TrinoException(NOT_SUPPORTED, "Failed to migrate table", e);
            }
        }
        throw new TrinoException(NOT_SUPPORTED, "Changing table type from '%s' to '%s' is not supported".formatted(sourceTableType, newTableType));
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Setting comments for Hudi tables is not supported");
        }
        delegate(tableType).setTableComment(unwrap(tableType, session), tableHandle, comment);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Setting column comments in Hudi tables is not supported");
        }
        delegate(tableType).setColumnComment(unwrap(tableType, session), tableHandle, column, comment);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Adding columns to Hudi tables is not supported");
        }
        if ((tableType != ICEBERG && tableType != DELTA) && !column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "%s tables do not support NOT NULL columns".formatted(tableType.displayName()));
        }
        delegate(tableType).addColumn(unwrap(tableType, session), tableHandle, column, position);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        TableType tableType = tableType(tableHandle);
        try {
            delegate(tableType).addField(unwrap(tableType, session), tableHandle, parentPath, fieldName, type, ignoreExisting);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED)) {
                throw new TrinoException(NOT_SUPPORTED, "Adding fields to %s tables is not supported".formatted(tableType.displayName()), e);
            }
            throw e;
        }
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void setDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, String defaultValue)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).setDefaultValue(unwrap(tableType, session), tableHandle, column, defaultValue);
    }

    @Override
    public void dropDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).dropDefaultValue(unwrap(tableType, session), tableHandle, columnHandle);
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        TableType tableType = tableType(tableHandle);
        try {
            delegate(tableType).setColumnType(unwrap(tableType, session), tableHandle, column, type);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED) && "This connector does not support setting column types".equals(e.getMessage())) {
                throw new TrinoException(NOT_SUPPORTED, "Setting column type on %s tables is not supported".formatted(tableType.displayName()), e);
            }
            throw e;
        }
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, Type type)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType != ICEBERG) {
            throw new TrinoException(NOT_SUPPORTED, "Setting field type in %s tables is not supported".formatted(tableType.displayName()));
        }
        delegate(tableType).setFieldType(unwrap(tableType, session), tableHandle, fieldPath, type);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        TableType tableType = tableType(tableHandle);
        switch (tableType) {
            case HIVE, HUDI -> throw new TrinoException(NOT_SUPPORTED, "Dropping NOT NULL constraint in %s tables is not supported".formatted(tableType.displayName()));
            default -> {
                // handled below
            }
        }
        delegate(tableType).dropNotNullConstraint(unwrap(tableType, session), tableHandle, column);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming columns in Hudi tables is not supported");
        }
        delegate(tableType).renameColumn(unwrap(tableType, session), tableHandle, source, target);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType != ICEBERG) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming fields in %s tables is not supported".formatted(tableType.displayName()));
        }
        delegate(tableType).renameField(unwrap(tableType, session), tableHandle, fieldPath, target);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping columns from Hudi tables is not supported");
        }
        delegate(tableType).dropColumn(unwrap(tableType, session), tableHandle, column);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        TableType tableType = tableType(tableHandle);
        switch (tableType) {
            case HIVE, DELTA, HUDI -> throw new TrinoException(NOT_SUPPORTED, "Dropping fields from %s tables is not supported".formatted(tableType.displayName()));
            default -> {
                // handled below
            }
        }
        delegate(tableType).dropField(unwrap(tableType, session), tableHandle, column, fieldPath);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        TableType tableType = tableType(tableMetadata);
        tableMetadata = unwrap(tableType, tableMetadata);
        return delegate(tableType).getNewTableLayout(unwrap(tableType, session), tableMetadata);
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
    {
        TableType tableType = tableType(tableProperties);
        if (tableType == HUDI) {
            return Optional.empty();
        }
        tableProperties = unwrap(tableType, tableProperties);
        return delegate(tableType).getSupportedType(unwrap(tableType, session), tableProperties, type);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getInsertLayout(unwrap(tableType, session), tableHandle);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
    {
        TableType tableType = tableType(tableMetadata);
        tableMetadata = unwrap(tableType, tableMetadata);
        return delegate(tableType).getStatisticsCollectionMetadataForWrite(unwrap(tableType, session), tableMetadata, tableReplace);
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).beginStatisticsCollection(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).finishStatisticsCollection(unwrap(tableType, session), tableHandle, computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        TableType tableType = tableType(tableMetadata);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Table creation is not supported for Hudi");
        }
        tableMetadata = unwrap(tableType, tableMetadata);
        return delegate(tableType).beginCreateTable(unwrap(tableType, session), tableMetadata, layout, retryMode, replace);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle outputHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType;
        SchemaTableName schemaTableName;
        if (outputHandle instanceof HiveOutputTableHandle hiveOutputTableHandle) {
            tableType = HIVE;
            schemaTableName = hiveOutputTableHandle.getSchemaTableName();
        }
        else if (outputHandle instanceof IcebergWritableTableHandle icebergWritableTableHandle) {
            tableType = ICEBERG;
            schemaTableName = icebergWritableTableHandle.name();
        }
        else if (outputHandle instanceof DeltaLakeOutputTableHandle deltaLakeOutputTableHandle) {
            tableType = DELTA;
            schemaTableName = new SchemaTableName(deltaLakeOutputTableHandle.schemaName(), deltaLakeOutputTableHandle.tableName());
        }
        else {
            throw new UnsupportedOperationException("Unhandled class: " + outputHandle.getClass().getName());
        }
        Optional<ConnectorOutputMetadata> outputMetadata = delegate(tableType).finishCreateTable(unwrap(tableType, session), outputHandle, fragments, computedStatistics);
        relationTypeCache.record(schemaTableName, tableType);
        flushMetadataCache(session, tableName(outputHandle));
        return outputMetadata;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Writes are not supported for Hudi tables");
        }
        return delegate(tableType).beginInsert(unwrap(tableType, session), tableHandle, columns, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType = tableType(insertHandle);
        return delegate(tableType).finishInsert(unwrap(tableType, session), insertHandle, sourceTableHandles, fragments, computedStatistics);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        try {
            return delegate(tableType).getRowChangeParadigm(unwrap(tableType, session), tableHandle);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED) && MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE.equals(e.getMessage())) {
                throw new TrinoException(NOT_SUPPORTED, "Modifying Hive table rows is constrained to deletes of whole partitions");
            }
            throw e;
        }
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Writes are not supported for Hudi tables");
        }
        return delegate(tableType).getMergeRowIdColumnHandle(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getUpdateLayout(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        TableType tableType = tableType(tableHandle);
        try {
            return delegate(tableType).beginMerge(unwrap(tableType, session), tableHandle, updateCaseColumns, retryMode);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED) && MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE.equals(e.getMessage())) {
                throw new TrinoException(NOT_SUPPORTED, "Modifying Hive table rows is constrained to deletes of whole partitions");
            }
            throw e;
        }
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType = tableType(mergeTableHandle);
        delegate(tableType).finishMerge(unwrap(tableType, session), mergeTableHandle, sourceTableHandles, fragments, computedStatistics);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        hiveMetadata.createView(unwrap(HIVE, session), viewName, definition, viewProperties, replace);
        relationTypeCache.record(viewName, VIEW);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void refreshView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition viewDefinition)
    {
        hiveMetadata.refreshView(unwrap(HIVE, session), viewName, viewDefinition);
        relationTypeCache.record(viewName, VIEW);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        hiveMetadata.renameView(unwrap(HIVE, session), source, target);
        relationTypeCache.record(target, VIEW);
        flushMetadataCache(session, source);
        flushMetadataCache(session, target);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        hiveMetadata.dropView(unwrap(HIVE, session), viewName);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        hiveMetadata.setViewComment(unwrap(HIVE, session), viewName, comment);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        hiveMetadata.setViewColumnComment(unwrap(HIVE, session), viewName, columnName, comment);
        flushMetadataCache(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        // stop gap solution to deal with shared hive metastore between iceberg and hive
        // iceberg stores materialized view with type virtual view and hive lists them without filtering with properties
        Set<SchemaTableName> materializedViews = icebergMetadata.listMaterializedViews(unwrap(ICEBERG, session), schemaName).stream().collect(toImmutableSet());
        return hiveMetadata.listViews(session, schemaName)
                .stream()
                .filter(view -> !materializedViews.contains(view))
                .collect(toImmutableList());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.getViews(unwrap(HIVE, session), schemaName);
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        RelationType relationType = relationTypeCache.getRelationType(viewName).orElse(null);
        if (relationType != null) {
            switch (relationType) {
                case MATERIALIZED_VIEW -> {
                    if (doGetMaterializedView(session, viewName).isPresent()) {
                        // This is a materialized view, so not a view
                        return false;
                    }
                }

                case VIEW -> {
                    // Do nothing.
                }

                case HIVE_TABLE, ICEBERG_TABLE, DELTA_TABLE, HUDI_TABLE -> {
                    if (getTableHandle(session, viewName, Optional.empty(), Optional.empty()) != null) {
                        // This is a table, so not a materialized view
                        return false;
                    }
                }
            }
        }
        return hiveMetadata.isView(unwrap(HIVE, session), viewName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        RelationType relationType = relationTypeCache.getRelationType(viewName).orElse(null);
        if (relationType != null) {
            switch (relationType) {
                case MATERIALIZED_VIEW -> {
                    if (doGetMaterializedView(session, viewName).isPresent()) {
                        // This is a materialized view, so not a view
                        return Optional.empty();
                    }
                }

                case VIEW -> {
                    // Do nothing.
                }

                case HIVE_TABLE, ICEBERG_TABLE, DELTA_TABLE, HUDI_TABLE -> {
                    if (getTableHandle(session, viewName, Optional.empty(), Optional.empty()) != null) {
                        // This is a table, so not a materialized view
                        return Optional.empty();
                    }
                }
            }
        }

        return doGetView(session, viewName);
    }

    private Optional<ConnectorViewDefinition> doGetView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<ConnectorViewDefinition> view = hiveMetadata.getView(unwrap(HIVE, session), viewName);
        if (view.isPresent()) {
            relationTypeCache.record(viewName, VIEW);
        }
        return view;
    }

    @Override
    public Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMetadata.getViewProperties(unwrap(HIVE, session), viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.getSchemaProperties(unwrap(HIVE, session), schemaName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyDelete(unwrap(tableType, session), tableHandle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).executeDelete(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableProperties(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle tableHandle, long limit)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyLimit(unwrap(tableType, session), tableHandle, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyFilter(unwrap(tableType, session), tableHandle, constraint);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyProjection(unwrap(tableType, session), tableHandle, projections, assignments);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyTableScanRedirect(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).validateScan(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        hiveMetadata.beginQuery(unwrap(HIVE, session));
        icebergMetadata.beginQuery(unwrap(ICEBERG, session));
        deltaMetadata.beginQuery(unwrap(DELTA, session));
        hudiMetadata.beginQuery(unwrap(HUDI, session));
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        hiveMetadata.cleanupQuery(unwrap(HIVE, session));
        icebergMetadata.cleanupQuery(unwrap(ICEBERG, session));
        deltaMetadata.cleanupQuery(unwrap(DELTA, session));
        hudiMetadata.cleanupQuery(unwrap(HUDI, session));
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).allowSplittingReadIntoMultipleSubQueries(unwrap(tableType, session), tableHandle);
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        TableType tableType = tableType(tableProperties);
        return delegate(tableType).getNewTableWriterScalingOptions(unwrap(tableType, session), tableName, unwrap(tableType, tableProperties));
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getInsertWriterScalingOptions(unwrap(tableType, session), tableHandle);
    }

    @Override
    public WriterScalingOptions getMergeWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getMergeWriterScalingOptions(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Optional<ApplyPartialTopNResult<ConnectorTableHandle>> applyPartialTopN(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<SortingProperty<ColumnHandle>> sortProperties,
            long count)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyPartialTopN(unwrap(tableType, session), tableHandle, sortProperties, count);
    }

    @Override
    public Optional<UnificationResult<ConnectorTableHandle>> unifyTables(ConnectorSession session, ConnectorTableHandle first, ConnectorTableHandle second)
    {
        TableType firstTableType = tableType(first);
        TableType secondTableType = tableType(second);
        if (firstTableType != secondTableType) {
            return Optional.empty();
        }
        return delegate(firstTableType).unifyTables(unwrap(firstTableType, session), first, second);
    }

    @Override
    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableCredentials(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorWritableTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableCredentials(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorTableFunctionHandle tableFunctionHandle)
    {
        TableType tableType = tableType(tableFunctionHandle);
        return delegate(tableType).getTableCredentials(unwrap(tableType, session), tableFunctionHandle);
    }

    private void flushMetadataCache()
    {
        try {
            flushMetadataCache.getMethodHandle().invoke(null, null, null, null, null);
        }
        catch (TrinoException | Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to flush metadata cache: " + requireNonNullElse(e.toString(), e), e);
        }
    }

    private void flushMetadataCache(ConnectorSession session, SchemaTableName table)
    {
        try {
            flushMetadataCache.getMethodHandle().invoke(session, table.getSchemaName(), table.getTableName(), null, null);
        }
        catch (TrinoException | Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to flush metadata cache: " + requireNonNullElse(e.toString(), e), e);
        }
    }

    ConnectorMetadata delegate(TableType tableType)
    {
        return switch (tableType) {
            case HIVE -> hiveMetadata;
            case ICEBERG -> icebergMetadata;
            case DELTA -> deltaMetadata;
            case HUDI -> hudiMetadata;
        };
    }

    static TableType tableType(ConnectorTableHandle handle)
    {
        if (handle instanceof HiveTableHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergTableHandle || handle instanceof CorruptedIcebergTableHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeTableHandle || handle instanceof CorruptedDeltaLakeTableHandle) {
            return DELTA;
        }
        if (handle instanceof HudiTableHandle) {
            return HUDI;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static SchemaTableName tableName(ConnectorTableHandle handle)
    {
        if (handle instanceof HiveTableHandle hiveTableHandle) {
            return hiveTableHandle.getSchemaTableName();
        }
        if (handle instanceof IcebergTableHandle icebergTableHandle) {
            return icebergTableHandle.getSchemaTableName();
        }
        if (handle instanceof CorruptedIcebergTableHandle corruptedIcebergTableHandle) {
            return corruptedIcebergTableHandle.schemaTableName();
        }
        if (handle instanceof DeltaLakeTableHandle deltaLakeTableHandle) {
            return deltaLakeTableHandle.getSchemaTableName();
        }
        if (handle instanceof CorruptedDeltaLakeTableHandle corruptedDeltaLakeTableHandle) {
            return corruptedDeltaLakeTableHandle.schemaTableName();
        }
        if (handle instanceof HudiTableHandle hudiTableHandle) {
            return hudiTableHandle.getSchemaTableName();
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private TableType tableType(ConnectorWritableTableHandle handle)
    {
        return switch (handle) {
            case HiveInsertTableHandle _, HiveOutputTableHandle _, HiveTableExecuteHandle _ -> HIVE;
            case IcebergWritableTableHandle _, IcebergTableExecuteHandle _ -> ICEBERG;
            case DeltaLakeInsertTableHandle _, DeltaLakeOutputTableHandle _, DeltaLakeTableExecuteHandle _ -> DELTA;
            default -> throw new IllegalArgumentException("Unsupported writable table handle: " + handle.getClass().getName());
        };
    }

    private static TableType tableType(ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof TableChangesFunctionHandle) {
            return ICEBERG;
        }
        if (handle instanceof TableChangesTableFunctionHandle) {
            return DELTA;
        }
        if (handle instanceof UnloadFunctionHandle) {
            return HIVE;
        }
        throw new IllegalArgumentException("Unsupported table function handle: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorInsertTableHandle handle)
    {
        if (handle instanceof HiveInsertTableHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergWritableTableHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeInsertTableHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private SchemaTableName tableName(ConnectorOutputTableHandle handle)
    {
        if (handle instanceof HiveOutputTableHandle hiveOutputTableHandle) {
            return hiveOutputTableHandle.getSchemaTableName();
        }
        if (handle instanceof IcebergWritableTableHandle icebergWritableTableHandle) {
            return icebergWritableTableHandle.name();
        }
        if (handle instanceof DeltaLakeOutputTableHandle deltaLakeOutputTableHandle) {
            return new SchemaTableName(deltaLakeOutputTableHandle.schemaName(), deltaLakeOutputTableHandle.tableName());
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorMergeTableHandle handle)
    {
        if (handle instanceof IcebergMergeTableHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeMergeTableHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private TableType tableType(ConnectorTableExecuteHandle handle)
    {
        if (handle instanceof HiveTableExecuteHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergTableExecuteHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeTableExecuteHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorPartitioningHandle handle)
    {
        if (handle instanceof HivePartitioningHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergPartitioningHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakePartitioningHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorTableMetadata tableMetadata)
    {
        return tableType(tableMetadata.getProperties());
    }

    private static TableType tableType(Map<String, Object> tableProperties)
    {
        return (TableType) tableProperties.get("type");
    }

    private Map<String, Object> wrap(TableType tableType, Map<String, Object> tableProperties)
    {
        Map<String, Object> properties = new HashMap<>(tableProperties);
        switch (tableType) {
            case HIVE -> {
                Optional.ofNullable(properties.get("format")).ifPresent(value ->
                        properties.put("format", encodeProperty(hiveFormatProperty, value)));
                Optional.ofNullable(properties.get("sorted_by")).ifPresent(value ->
                        properties.put("sorted_by", encodeProperty(hiveSortedByProperty, value)));
            }
            case ICEBERG -> Optional.ofNullable(properties.get("format")).ifPresent(value ->
                    properties.put("format", encodeProperty(hiveFormatProperty, value)));
            case DELTA, HUDI -> verify(!properties.containsKey("format"), "%s should not have 'format' property".formatted(tableType));
            default -> throw new VerifyException("Unhandled type: " + tableType);
        }
        return properties;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object encodeProperty(PropertyMetadata<?> property, Object value)
    {
        return ((PropertyMetadata) property).encode(value);
    }

    private ConnectorSession unwrap(TableType tableType, ConnectorSession session)
    {
        return sessionProperties.unwrap(tableType, session);
    }

    private ConnectorTableMetadata unwrap(TableType tableType, ConnectorTableMetadata metadata)
    {
        Map<String, Object> tableProperties = unwrap(tableType, metadata.getProperties());
        validateColumnProperties(tableType, metadata.getColumns());
        return withProperties(metadata, tableProperties);
    }

    private Map<String, Object> unwrap(TableType tableType, Map<String, Object> tableProperties)
    {
        Map<String, Object> properties = new HashMap<>(tableProperties);
        switch (tableType) {
            case HIVE -> {
                Optional.ofNullable(properties.get("format")).ifPresent(value -> {
                    if (value.equals("DEFAULT")) {
                        value = "ORC";
                    }
                    properties.put("format", decodeProperty(hiveFormatProperty, value));
                });
                Optional.ofNullable(properties.get("sorted_by")).ifPresent(value ->
                        properties.put("sorted_by", decodeProperty(hiveSortedByProperty, value)));
            }
            case ICEBERG -> Optional.ofNullable(properties.get("format")).ifPresent(value -> {
                if (value.equals("DEFAULT")) {
                    value = defaultIcebergFileFormat.name();
                }
                properties.put("format", decodeProperty(icebergFormatProperty, value));
            });
            case DELTA, HUDI -> {
                /* ignore 'format' property */
            }
            default -> throw new VerifyException("Unhandled type: " + tableType);
        }
        properties.entrySet().removeIf(entry ->
                !this.tableProperties.validProperty(tableType, entry.getKey(), entry.getValue()));
        return properties;
    }

    private static <T> T decodeProperty(PropertyMetadata<T> property, Object value)
    {
        try {
            return property.decode(value);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "%s is invalid: %s".formatted(property.getName(), value), e);
        }
    }

    private static void validateColumnProperties(TableType tableType, List<ColumnMetadata> columnMetadata)
    {
        switch (tableType) {
            case HIVE -> {
                // Hive is the only connector using column properties
            }
            case ICEBERG, DELTA, HUDI -> {
                Set<String> columnProperties = columnMetadata.stream()
                        .map(ColumnMetadata::getProperties)
                        .map(Map::keySet)
                        .reduce(ImmutableSet.of(), Sets::union);
                if (!columnProperties.isEmpty()) {
                    throw new TrinoException(INVALID_COLUMN_PROPERTY, "%s tables do not support column properties %s".formatted(tableType.displayName(), columnProperties));
                }
            }
        }
    }

    public static boolean isError(TrinoException e, ErrorCodeSupplier... errorCodes)
    {
        return Stream.of(errorCodes)
                .map(ErrorCodeSupplier::toErrorCode)
                .anyMatch(e.getErrorCode()::equals);
    }

    private static boolean isVersionedTableUnsupported(TrinoException e)
    {
        return e.getErrorCode() == NOT_SUPPORTED.toErrorCode() && "This connector does not support versioned tables".equals(e.getMessage());
    }

    private static ConnectorTableMetadata withProperties(ConnectorTableMetadata metadata, Map<String, Object> properties)
    {
        return new ConnectorTableMetadata(metadata.getTable(), metadata.getColumns(), properties, metadata.getComment());
    }
}
