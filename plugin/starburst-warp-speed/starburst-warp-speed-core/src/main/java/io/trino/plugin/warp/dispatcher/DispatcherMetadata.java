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
package io.trino.plugin.warp.dispatcher;

import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.util.ConnectorExpressionUtil.ExpressionAndAssignments;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.expression.rewrite.ExpressionService;
import io.trino.plugin.warp.expression.rewrite.WarpExpression;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.spi.RefreshType;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
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
import io.trino.spi.connector.ConnectorResolvedIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.UnificationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.trino.plugin.base.util.ConnectorExpressionUtil.or;
import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.createFixedStatKey;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.predicate.TupleDomain.columnWiseUnion;
import static java.util.Objects.requireNonNull;

public class DispatcherMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(DispatcherMetadata.class);

    private final ConnectorMetadata proxiedConnectorMetadata;
    private final ExpressionService expressionService;
    private final DispatcherStatisticsProvider dispatcherStatisticsProvider;
    private final DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider;
    private final GlobalConfig globalConfig;
    private final ShapingLogger shapingLogger;

    public DispatcherMetadata(
            ConnectorMetadata proxiedConnectorMetadata,
            ExpressionService expressionService,
            DispatcherStatisticsProvider dispatcherStatisticsProvider,
            DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider,
            GlobalConfig globalConfig,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.proxiedConnectorMetadata = requireNonNull(proxiedConnectorMetadata);
        this.expressionService = requireNonNull(expressionService);
        this.dispatcherStatisticsProvider = requireNonNull(dispatcherStatisticsProvider);
        this.dispatcherTableHandleBuilderProvider = requireNonNull(dispatcherTableHandleBuilderProvider);
        this.globalConfig = requireNonNull(globalConfig);
        this.shapingLogger = shapingLoggerFactory.getInstance(DispatcherMetadata.class);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return proxiedConnectorMetadata.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return convertTableHandle(
                session,
                proxiedConnectorMetadata.getTableHandle(session, tableName, startVersion, endVersion),
                tableName);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return proxiedConnectorMetadata.getSystemTable(session, tableName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartitioning(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Optional<ConnectorPartitioningHandle> partitioningHandle,
            List<ColumnHandle> columns)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        Optional<ConnectorTableHandle> partitioned = proxiedConnectorMetadata.applyPartitioning(
                session,
                dispatcherTableHandle.getProxyConnectorTableHandle(),
                partitioningHandle,
                columns);
        return partitioned.map(handle -> convertTableHandle(
                session,
                dispatcherTableHandle,
                handle,
                dispatcherTableHandle.getSchemaTableName()));
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        return proxiedConnectorMetadata.getCommonPartitioningHandle(session, left, right);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableName(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableSchema(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableMetadata(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getInfo(session, ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public Metrics getMetrics(ConnectorSession session)
    {
        return proxiedConnectorMetadata.getMetrics(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.listTables(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getColumnHandles(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return proxiedConnectorMetadata.getColumnMetadata(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                columnHandle);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        TableStatistics tableStatistics = getProxyTableStatistics(
                session,
                dispatcherTableHandle.getProxyConnectorTableHandle());
        dispatcherStatisticsProvider.putColumnsNotFitForDictionary(
                dispatcherTableHandle.getSchemaTableName(),
                tableStatistics.getColumnStatistics());
        return tableStatistics;
    }

    private TableStatistics getProxyTableStatistics(ConnectorSession session, ConnectorTableHandle proxiedConnectorTableHandle)
    {
        TableStatistics tableStatistics;

        if (dispatcherStatisticsProvider.isValidForTableStatistics(proxiedConnectorTableHandle)) {
            try {
                tableStatistics = proxiedConnectorMetadata.getTableStatistics(session, proxiedConnectorTableHandle);
            }
            catch (Exception e) {
                shapingLogger.warn("getTableStatistics failed tableHandle %s error %s", proxiedConnectorTableHandle, e);
                tableStatistics = TableStatistics.empty();
            }
        }
        else {
            tableStatistics = TableStatistics.empty();
        }
        return tableStatistics;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        proxiedConnectorMetadata.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        proxiedConnectorMetadata.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        proxiedConnectorMetadata.renameSchema(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal)
    {
        proxiedConnectorMetadata.setSchemaAuthorization(session, source, principal);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        proxiedConnectorMetadata.dropTable(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        proxiedConnectorMetadata.truncateTable(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        proxiedConnectorMetadata.renameTable(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        proxiedConnectorMetadata.setTableProperties(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                properties);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        proxiedConnectorMetadata.setTableComment(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        proxiedConnectorMetadata.setViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        proxiedConnectorMetadata.setViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        proxiedConnectorMetadata.setColumnComment(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                column,
                comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        proxiedConnectorMetadata.addColumn(session, ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(), column, position);
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        proxiedConnectorMetadata.setColumnType(session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                column,
                type);
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, Type type)
    {
        ConnectorTableHandle proxyConnectorTableHandle = ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle();
        proxiedConnectorMetadata.setFieldType(session, proxyConnectorTableHandle, fieldPath, type);
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        ConnectorTableHandle proxyConnectorTableHandle = ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle();
        proxiedConnectorMetadata.renameField(session, proxyConnectorTableHandle, fieldPath, target);
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return proxiedConnectorMetadata.getNewTableWriterScalingOptions(session, tableName, tableProperties);
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return proxiedConnectorMetadata.getInsertWriterScalingOptions(session, dispatcherTableHandle.getProxyConnectorTableHandle());
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartialLimit(ConnectorSession session, ConnectorTableHandle tableHandle, long limitHint)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return proxiedConnectorMetadata.applyPartialLimit(session, dispatcherTableHandle.getProxyConnectorTableHandle(), limitHint);
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        proxiedConnectorMetadata.setTableAuthorization(session, tableName, principal);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        proxiedConnectorMetadata.renameColumn(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                source,
                target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        proxiedConnectorMetadata.dropColumn(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                column);
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        DispatcherTableHandle dispatcherMergeTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.dropField(session, dispatcherMergeTableHandle.getProxyConnectorTableHandle(), column, fieldPath);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return proxiedConnectorMetadata.getNewTableLayout(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getInsertLayout(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
    {
        return proxiedConnectorMetadata.getStatisticsCollectionMetadataForWrite(session, tableMetadata, tableReplace);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        ConnectorAnalyzeMetadata proxiedResult = proxiedConnectorMetadata.getStatisticsCollectionMetadata(
                session,
                dispatcherTableHandle.getProxyConnectorTableHandle(),
                analyzeProperties);

        DispatcherTableHandle dispatcherTableHandleResult = convertTableHandle(
                session,
                dispatcherTableHandle,
                proxiedResult.getTableHandle(),
                dispatcherTableHandle.getSchemaTableName());

        return new ConnectorAnalyzeMetadata(dispatcherTableHandleResult, proxiedResult.getStatisticsMetadata());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return convertTableHandle(
                session,
                dispatcherTableHandle,
                proxiedConnectorMetadata.beginStatisticsCollection(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle()),
                dispatcherTableHandle.getSchemaTableName());
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        proxiedConnectorMetadata.finishStatisticsCollection(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        return proxiedConnectorMetadata.beginCreateTable(session, tableMetadata, layout, retryMode, replace);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        proxiedConnectorMetadata.createTable(session, tableMetadata, saveMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return proxiedConnectorMetadata.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        proxiedConnectorMetadata.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        proxiedConnectorMetadata.cleanupQuery(session);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        return proxiedConnectorMetadata.beginInsert(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                columns,
                retryMode);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return proxiedConnectorMetadata.supportsMissingColumnsOnInsert();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return proxiedConnectorMetadata.finishInsert(session, insertHandle, sourceTableHandles, fragments, computedStatistics);
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
        return proxiedConnectorMetadata.beginRefreshMaterializedView(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                sourceTableHandles.stream()
                        .map(connectorTableHandle -> {
                            if (connectorTableHandle instanceof DispatcherTableHandle dispatcherTableHandle) {
                                return dispatcherTableHandle.getProxyConnectorTableHandle();
                            }
                            return connectorTableHandle;
                        })
                        .toList(),
                hasForeignSourceTables,
                retryMode,
                refreshType);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            boolean hasForeignSourceTables,
            boolean hasSourceTableFunctions)
    {
        return proxiedConnectorMetadata.finishRefreshMaterializedView(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                insertHandle,
                fragments,
                computedStatistics,
                sourceTableHandles.stream()
                        .map(connectorTableHandle -> {
                            if (connectorTableHandle instanceof DispatcherTableHandle dispatcherTableHandle) {
                                return dispatcherTableHandle.getProxyConnectorTableHandle();
                            }
                            return connectorTableHandle;
                        })
                        .collect(Collectors.toList()),
                hasForeignSourceTables,
                hasSourceTableFunctions);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        proxiedConnectorMetadata.createView(session, viewName, definition, viewProperties, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        proxiedConnectorMetadata.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        proxiedConnectorMetadata.setViewAuthorization(session, viewName, principal);
    }

    @Override
    public void refreshView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition viewDefinition)
    {
        proxiedConnectorMetadata.refreshView(session, viewName, viewDefinition);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        proxiedConnectorMetadata.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.getView(session, viewName);
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.isView(session, viewName);
    }

    @Override
    public Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.getViewProperties(session, viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.getSchemaOwner(session, schemaName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        return proxiedConnectorMetadata.applyDelete(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle())
                .map(ret ->
                        convertTableHandle(
                                session,
                                dispatcherTableHandle,
                                ret,
                                dispatcherTableHandle.getSchemaTableName()));
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return proxiedConnectorMetadata.executeDelete(
                session,
                ((DispatcherTableHandle) handle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Set<ColumnHandle> indexableColumns,
            Set<ColumnHandle> outputColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        return proxiedConnectorMetadata.resolveIndex(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                indexableColumns,
                outputColumns,
                tupleDomain);
    }

    @Override
    public boolean roleExists(ConnectorSession session, String role)
    {
        return proxiedConnectorMetadata.roleExists(session, role);
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        proxiedConnectorMetadata.createRole(session, role, grantor);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        proxiedConnectorMetadata.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return proxiedConnectorMetadata.listRoles(session);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        proxiedConnectorMetadata.setMaterializedViewProperties(session, viewName, properties);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        return proxiedConnectorMetadata.listRoleGrants(session, principal);
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        proxiedConnectorMetadata.grantRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        proxiedConnectorMetadata.revokeRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        return proxiedConnectorMetadata.listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return proxiedConnectorMetadata.listEnabledRoles(session);
    }

    @Override
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        proxiedConnectorMetadata.denySchemaPrivileges(session, schemaName, privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        proxiedConnectorMetadata.denyTablePrivileges(session, tableName, privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return proxiedConnectorMetadata.listTablePrivileges(session, prefix);
    }

    @Override
    public void grantTableBranchPrivileges(ConnectorSession session, SchemaTableName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.grantTableBranchPrivileges(session, tableName, branchName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTableBranchPrivileges(ConnectorSession session, SchemaTableName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        proxiedConnectorMetadata.denyTableBranchPrivileges(session, tableName, branchName, privileges, grantee);
    }

    @Override
    public void revokeTableBranchPrivileges(ConnectorSession session, SchemaTableName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.revokeTableBranchPrivileges(session, tableName, branchName, privileges, grantee, grantOption);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableProperties(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return proxiedConnectorMetadata.streamRelationComments(session, schemaName, relationFilter);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;

        if (dispatcherTableHandle.getLimit().equals(OptionalLong.of(limit))) {
            return Optional.empty();
        }

        Optional<LimitApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyLimit(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        limit);

        boolean isLimitGuaranteed = false;
        SchemaTableName schemaTableName = dispatcherTableHandle.getSchemaTableName();

        if (resultOpt.isPresent()) {
            LimitApplicationResult<ConnectorTableHandle> result = resultOpt.get();
            dispatcherTableHandle = createTableHandleBuilder(
                    session,
                    Optional.of(dispatcherTableHandle),
                    result.getHandle(),
                    schemaTableName)
                    .limit(limit)
                    .build();
            isLimitGuaranteed = result.isLimitGuaranteed();
        }
        else {
            dispatcherTableHandle = createTableHandleBuilder(
                    session,
                    Optional.of(dispatcherTableHandle),
                    dispatcherTableHandle.getProxyConnectorTableHandle(),
                    schemaTableName)
                    .limit(limit)
                    .build();
        }
        return Optional.of(new LimitApplicationResult<>(dispatcherTableHandle, isLimitGuaranteed, false));
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        proxiedConnectorMetadata.setMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return proxiedConnectorMetadata.streamRelationColumns(session, schemaName, relationFilter);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Constraint constraint)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;

        if (logger.isDebugEnabled()) {
            logger.debug("ApplyFilter - Input tupleDomain: %s, expression: %s",
                    constraint.getSummary().toString(),
                    constraint.getExpression().toString());
        }
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyFilter(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        constraint);

        // TODO: VDB-5850 The new WarpExpressions should be merged with the existing ones at the table handle
        // TODO: and we should check that no changes were made to the WarpExpressions before returning Optional.empty().
        Map<String, Long> customStats = new HashMap<>();

        Optional<WarpExpression> warpExpression = dispatcherTableHandle.getWarpExpression();
        if (warpExpression.isEmpty()) {
            warpExpression = expressionService.convertToWarpExpression(session,
                    constraint.getExpression(),
                    constraint.getAssignments(),
                    customStats);
        }
        // Build and return result
        if (resultOpt.isEmpty()) {
            if (dispatcherTableHandle.getWarpExpression().equals(warpExpression)) {
                return Optional.empty();
            }
            return createConstraintApplicationResult(
                    session,
                    constraint,
                    dispatcherTableHandle,
                    constraint.getSummary(),
                    Optional.empty(),
                    dispatcherTableHandle.getProxyConnectorTableHandle(),
                    warpExpression,
                    customStats);
        }

        TupleDomain<ColumnHandle> newRemainingFilter = resultOpt.get().getAlternatives().getFirst().remainingFilter();
        Optional<ConnectorExpression> newRemainingExpression = resultOpt.get().getAlternatives().getFirst().remainingExpression();
        if (logger.isDebugEnabled()) {
            logger.debug("Will return to Trino the following remaining filter: %s and remaining expression: %s", newRemainingFilter.toString(), newRemainingExpression);
        }

        return createConstraintApplicationResult(
                session,
                constraint,
                dispatcherTableHandle,
                newRemainingFilter,
                newRemainingExpression,
                resultOpt.get().getAlternatives().getFirst().handle(),
                warpExpression,
                customStats);
    }

    private Optional<ConstraintApplicationResult<ConnectorTableHandle>> createConstraintApplicationResult(
            ConnectorSession session,
            Constraint constraint,
            DispatcherTableHandle table,
            TupleDomain<ColumnHandle> newRemainingFilter,
            Optional<ConnectorExpression> newRemainingExpression,
            ConnectorTableHandle proxiedConnectorTableHandle,
            Optional<WarpExpression> warpExpression,
            Map<String, Long> customStatsMap)
    {
        List<CustomStat> customStats = mergeCustomStats(table.getCustomStats(), customStatsMap);
        TupleDomain<ColumnHandle> fullPredicate = table.getFullPredicate().intersect(newRemainingFilter);

        DispatcherTableHandleBuilderProvider.Builder builder = createTableHandleBuilder(session, Optional.of(table), proxiedConnectorTableHandle, table.getSchemaTableName())
                .warpExpression(warpExpression)
                .customStats(customStats)
                .fullPredicate(fullPredicate);
        if (table.getWarpExpression().isEmpty()) {
            // Currently, WarpExpression is set only once
            builder.originalExpression(constraint.getExpression(), constraint.getAssignments());
        }
        DispatcherTableHandle dispatcherTableHandle = builder.build();

        List<ConstraintApplicationResult.Alternative<ConnectorTableHandle>> alternatives = List.of(
                new ConstraintApplicationResult.Alternative<>(dispatcherTableHandle, newRemainingFilter, newRemainingExpression, false));

        return Optional.of(new ConstraintApplicationResult<>(false, alternatives));
    }

    private static List<CustomStat> mergeCustomStats(List<CustomStat> first, List<CustomStat> second)
    {
        Map<String, Long> customStatsMap = second.stream()
                .collect(Collectors.toMap(CustomStat::statName, CustomStat::statValue, (a, b) -> a, HashMap::new));
        return mergeCustomStats(first, customStatsMap);
    }

    private static List<CustomStat> mergeCustomStats(List<CustomStat> customStats, Map<String, Long> customStatsMap)
    {
        Map<String, Long> allStatsMap = customStats.stream()
                .collect(Collectors.toMap(CustomStat::statName, CustomStat::statValue, (a, b) -> a, HashMap::new));
        customStatsMap.forEach((key, value) -> allStatsMap.merge(createFixedStatKey(DispatcherPageSourceStats.createKey(), key), value, Long::sum));

        return allStatsMap.entrySet().stream()
                .map(entry -> new CustomStat(entry.getKey(), entry.getValue()))
                .toList();
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyProjection(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        projections,
                        assignments);
        return resultOpt.map(result -> new ProjectionApplicationResult<>(
                convertTableHandle(session, dispatcherTableHandle, result.getHandle(), dispatcherTableHandle.getSchemaTableName()),
                result.getProjections(),
                result.getAssignments(),
                result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(
            ConnectorSession session,
            ConnectorTableHandle handle,
            SampleType sampleType,
            double sampleRatio)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        return proxiedConnectorMetadata.applySample(session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        sampleType,
                        sampleRatio)
                .map(result -> new SampleApplicationResult<>(
                        convertTableHandle(session, dispatcherTableHandle, result.getHandle(), dispatcherTableHandle.getSchemaTableName()),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        Optional<AggregationApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyAggregation(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        aggregates,
                        assignments,
                        groupingSets);
        return resultOpt.map(result -> new AggregationApplicationResult<>(
                convertTableHandle(session, dispatcherTableHandle, result.getHandle(), dispatcherTableHandle.getSchemaTableName()),
                result.getProjections(),
                result.getAssignments(),
                result.getGroupingColumnMapping(),
                result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(ConnectorSession session, JoinType joinType, ConnectorTableHandle left, ConnectorTableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        return Optional.empty();
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session, ConnectorTableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    @Override
    public Optional<UnificationResult<ConnectorTableHandle>> unifyTables(ConnectorSession session, ConnectorTableHandle first, ConnectorTableHandle second)
    {
        DispatcherTableHandle firstTable = (DispatcherTableHandle) first;
        DispatcherTableHandle secondTable = (DispatcherTableHandle) second;

        if (!Objects.equals(firstTable.getSchemaTableName(), secondTable.getSchemaTableName())) {
            return Optional.empty();
        }

        Optional<UnificationResult<ConnectorTableHandle>> unifiedProxyResult = proxiedConnectorMetadata.unifyTables(session, firstTable.getProxyConnectorTableHandle(), secondTable.getProxyConnectorTableHandle());
        if (unifiedProxyResult.isEmpty()) {
            return Optional.empty();
        }

        // union full predicates from the first and second table handles
        // it doesn't matter if the unioned TupleDomain is abundant
        // the unenforced predicate is "best effort", and there is no guarantee it is effective
        TupleDomain<ColumnHandle> unifiedFullPredicate = columnWiseUnion(firstTable.getFullPredicate(), secondTable.getFullPredicate());

        ExpressionAndAssignments unionExpression = or(firstTable.getOriginalExpression().orElseThrow(), secondTable.getOriginalExpression().orElseThrow());
        Optional<WarpExpression> unionWarpExpression = expressionService.convertToWarpExpression(
                session,
                unionExpression.expression(),
                unionExpression.assignments(),
                new HashMap<>()); // stats will be collected from the individual tables

        List<CustomStat> unifiedCustomStats = mergeCustomStats(firstTable.getCustomStats(), secondTable.getCustomStats());
        DispatcherTableHandle unified = new DispatcherTableHandle(
                firstTable.getSchemaName(),
                firstTable.getTableName(),
                OptionalLong.empty(),
                unifiedFullPredicate,
                new SimplifiedColumns(Sets.union(firstTable.getSimplifiedColumns().simplifiedColumns(), secondTable.getSimplifiedColumns().simplifiedColumns())),
                unifiedProxyResult.get().unifiedHandle(),
                unionWarpExpression,
                unifiedCustomStats,
                false,
                Sets.union(firstTable.getColumnsNotFitForDictionary(), secondTable.getColumnsNotFitForDictionary()),
                Optional.of(unionExpression));

        // union limits from the first and second table handles
        // we can do this only if we're not extracting and returning to the engine any compensating filters for the unified table handles
        // the compensating filters are applied later by the engine, which would mean that we pulled filter above limit
        if (unifiedProxyResult.get().firstCompensationFilter().isAll() &&
                TRUE.equals(unifiedProxyResult.get().firstCompensationExpression()) &&
                unifiedProxyResult.get().secondCompensationFilter().isAll() &&
                TRUE.equals(unifiedProxyResult.get().secondCompensationExpression()) &&
                firstTable.getLimit().isPresent() &&
                secondTable.getLimit().isPresent()) {
            unified = createTableHandleBuilder(
                    session,
                    Optional.of(unified),
                    unified.getProxyConnectorTableHandle(),
                    unified.getSchemaTableName())
                    .limit(Long.max(firstTable.getLimit().getAsLong(), secondTable.getLimit().getAsLong()))
                    .build();
        }

        return Optional.of(new UnificationResult<>(
                unified,
                unifiedProxyResult.get().firstCompensationFilter(),
                unifiedProxyResult.get().firstCompensationExpression(),
                unifiedProxyResult.get().firstAssignments(),
                unifiedProxyResult.get().secondCompensationFilter(),
                unifiedProxyResult.get().secondCompensationExpression(),
                unifiedProxyResult.get().secondAssignments(),
                unifiedProxyResult.get().enforcedProperties())); // All properties are the same as proxy's
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        proxiedConnectorMetadata.validateScan(
                session,
                ((DispatcherTableHandle) handle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            ConnectorTableHandle tableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        return proxiedConnectorMetadata.getTableHandleForExecute(
                session,
                accessControl,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                procedureName,
                executeProperties,
                retryMode);
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return proxiedConnectorMetadata.getLayoutForTableExecute(session, tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            ConnectorTableHandle updatedSourceTableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) updatedSourceTableHandle;
        BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecuteResult =
                proxiedConnectorMetadata.beginTableExecute(
                        session,
                        tableExecuteHandle,
                        dispatcherTableHandle.getProxyConnectorTableHandle());
        return new BeginTableExecuteResult<>(
                beginTableExecuteResult.getTableExecuteHandle(),
                convertTableHandle(
                        session,
                        dispatcherTableHandle,
                        beginTableExecuteResult.getSourceHandle(),
                        dispatcherTableHandle.getSchemaTableName()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        proxiedConnectorMetadata.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return proxiedConnectorMetadata.executeTableExecute(session, tableExecuteHandle);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> properties, boolean replace, boolean ignoreExisting)
    {
        proxiedConnectorMetadata.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
    {
        return proxiedConnectorMetadata.getMaterializedViewProperties(session, viewName, materializedViewDefinition);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        proxiedConnectorMetadata.dropMaterializedView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.listMaterializedViews(session, schemaName);
    }

    @Override
    public void setMaterializedViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        proxiedConnectorMetadata.setMaterializedViewAuthorization(session, viewName, principal);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.getMaterializedViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.getMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name, boolean considerGracePeriod)
    {
        return proxiedConnectorMetadata.getMaterializedViewFreshness(session, name, considerGracePeriod);
    }

    @SuppressWarnings("removal")
    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        throw new UnsupportedOperationException("getMaterializedViewFreshness(session, name, considerGracePeriod) should be called instead");
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        proxiedConnectorMetadata.renameMaterializedView(session, source, target);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.applyTableScanRedirect(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return proxiedConnectorMetadata.redirectTable(session, tableName);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        Optional<TableFunctionApplicationResult<ConnectorTableHandle>> proxiedResult = proxiedConnectorMetadata.applyTableFunction(session, handle);

        return proxiedResult.map(result ->
                new TableFunctionApplicationResult<>(
                        convertTableHandle(session, result.getTableHandle(), null),
                        result.getColumnHandles()));
    }

    @Override
    public OptionalInt getMaxWriterTasks(ConnectorSession session)
    {
        return proxiedConnectorMetadata.getMaxWriterTasks(session);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        ConnectorMergeTableHandle proxyConnectorMergeTableHandle = proxiedConnectorMetadata.beginMerge(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                updateCaseColumns,
                retryMode);
        return new DispatcherMergeTableHandle(
                dispatcherTableHandle,
                proxyConnectorMergeTableHandle);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getMergeRowIdColumnHandle(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<List<ColumnHandle>> getColumnHandlesForExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getColumnHandlesForExecute(
                session,
                tableExecuteHandle,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getUpdateLayout(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getRowChangeParadigm(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DispatcherMergeTableHandle dispatcherMergeTableHandle = (DispatcherMergeTableHandle) mergeTableHandle;
        proxiedConnectorMetadata.finishMerge(
                session,
                dispatcherMergeTableHandle.getProxyConnectorMergeTableHandle(),
                sourceTableHandles,
                fragments,
                computedStatistics);
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.addField(session, dispatcherTableHandle.getProxyConnectorTableHandle(), parentPath, fieldName, type, ignoreExisting);
    }

    @Override
    public void setDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, String defaultValue)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.setDefaultValue(session, dispatcherTableHandle.getProxyConnectorTableHandle(), column, defaultValue);
    }

    @Override
    public void dropDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.dropDefaultValue(session, dispatcherTableHandle.getProxyConnectorTableHandle(), columnHandle);
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.listFunctions(session, schemaName);
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return proxiedConnectorMetadata.getFunctions(session, name);
    }

    @Override
    public Optional<ConnectorTableHandle> applyUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Map<ColumnHandle, Constant> assignments)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return proxiedConnectorMetadata.applyUpdate(session, dispatcherTableHandle.getProxyConnectorTableHandle(), assignments);
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        return proxiedConnectorMetadata.executeUpdate(session, dispatcherTableHandle.getProxyConnectorTableHandle());
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return proxiedConnectorMetadata.getFunctionMetadata(session, functionId);
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return proxiedConnectorMetadata.getAggregationFunctionMetadata(session, functionId);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return proxiedConnectorMetadata.getFunctionDependencies(session, functionId, boundSignature);
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // when this optimization is enabled there is a different in the offset when the query is of aggregate of select count(col) Vs select col.
        // one of them is creating a split with offset 0 (when the metadata needs to be used) and the other is created with offset 4.
        // in warp this will endup duplicating the data stored on the disk so we prefer to remove the optimization and keep a single copy
        return false;
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> map, Type type)
    {
        return proxiedConnectorMetadata.getSupportedType(session, map, type);
    }

    @Override
    public Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.listLanguageFunctions(session, schemaName);
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return proxiedConnectorMetadata.getLanguageFunctions(session, name);
    }

    @Override
    public boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        return proxiedConnectorMetadata.languageFunctionExists(session, name, signatureToken);
    }

    @Override
    public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        proxiedConnectorMetadata.createLanguageFunction(session, name, function, replace);
    }

    @Override
    public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        proxiedConnectorMetadata.dropLanguageFunction(session, name, signatureToken);
    }

    @Override
    public void createBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String branch, Optional<String> fromBranch, SaveMode saveMode, Map<String, Object> properties)
    {
        proxiedConnectorMetadata.createBranch(session, tableHandle, branch, fromBranch, saveMode, properties);
    }

    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String branch)
    {
        proxiedConnectorMetadata.dropBranch(session, tableHandle, branch);
    }

    @Override
    public void fastForwardBranch(ConnectorSession session, ConnectorTableHandle tableHandle, String sourceBranch, String targetBranch)
    {
        proxiedConnectorMetadata.fastForwardBranch(session, tableHandle, sourceBranch, targetBranch);
    }

    @Override
    public Collection<String> listBranches(ConnectorSession session, SchemaTableName tableName)
    {
        return proxiedConnectorMetadata.listBranches(session, tableName);
    }

    @Override
    public boolean branchExists(ConnectorSession session, SchemaTableName tableName, String branch)
    {
        return proxiedConnectorMetadata.branchExists(session, tableName, branch);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.getRelationTypes(session, schemaName);
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.dropNotNullConstraint(session, dispatcherTableHandle.getProxyConnectorTableHandle(), column);
    }

    @Override
    public String getCatalogIdentity(ConnectorSession session)
    {
        return proxiedConnectorMetadata.getCatalogIdentity(session);
    }

    private DispatcherTableHandle convertTableHandle(ConnectorSession session, ConnectorTableHandle proxiedConnectorTableHandle, SchemaTableName schemaTableName)
    {
        return convertTableHandle(session, null, proxiedConnectorTableHandle, schemaTableName);
    }

    private DispatcherTableHandle convertTableHandle(
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorTableHandle proxiedConnectorTableHandle,
            SchemaTableName schemaTableName)
    {
        if (proxiedConnectorTableHandle == null) {
            return null;
        }
        return createTableHandleBuilder(session, Optional.ofNullable(dispatcherTableHandle), proxiedConnectorTableHandle, schemaTableName)
                .build();
    }

    private DispatcherTableHandleBuilderProvider.Builder createTableHandleBuilder(
            ConnectorSession session,
            Optional<DispatcherTableHandle> optionalDispatcherTableHandle,
            ConnectorTableHandle proxiedConnectorTableHandle,
            SchemaTableName schemaTableName)
    {
        int predicateThreshold = WarpSessionProperties.getPredicateSimplifyThreshold(session, globalConfig);

        return optionalDispatcherTableHandle.map(dispatcherTableHandle ->
                        dispatcherTableHandleBuilderProvider.builder(dispatcherTableHandle, predicateThreshold)
                                .proxiedConnectorTableHandle(proxiedConnectorTableHandle))
                .orElse(dispatcherTableHandleBuilderProvider.builder(predicateThreshold, proxiedConnectorTableHandle)
                        .columnsNotFitForDictionary(getColumnsNotFitForDictionary(schemaTableName, session, proxiedConnectorTableHandle)));
    }

    private Set<String> getColumnsNotFitForDictionary(SchemaTableName schemaTableName,
            ConnectorSession session,
            ConnectorTableHandle proxiedConnectorTableHandle)
    {
        if (schemaTableName == null || globalConfig.getEnableFSCacheMode()) {
            return Set.of();
        }

        Set<String> columnsNotFitForDictionary = dispatcherStatisticsProvider.getColumnsNotFitForDictionary(schemaTableName);

        if (columnsNotFitForDictionary != null) {
            return columnsNotFitForDictionary;
        }

        TableStatistics tableStatistics = getProxyTableStatistics(session, proxiedConnectorTableHandle);
        return dispatcherStatisticsProvider.putColumnsNotFitForDictionary(schemaTableName, tableStatistics.getColumnStatistics());
    }
}
