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
package io.trino.plugin.iceberg.catalog.unity;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.util.MaybeLazy;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.view.ViewMetadata;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;

// Dummy class created for Unity metastore as Galaxy doesn't support Iceberg table querying with unity metastore.
// There are three ways of handling a specific method
// 1. Throw UNSUPPORTED_TABLE_TYPE - ObjectstoreMetadata understands this error code so that particular action can be ignored.
// 2. Throw UnsupportedOperationException - Invalid code path which should never be called
// 3. Returning empty results for read operation for caller to proceed with next actions.
public class TrinoUnityCatalog
        implements TrinoCatalog
{
    private static final Logger log = Logger.get(TrinoUnityCatalog.class);

    @Override
    public MaybeLazy<List<ColumnMetadata>> getTableColumnMetadata(ConnectorSession session, Table metastoreTable)
    {
        throw new UnsupportedOperationException("getTableColumnMetadata is not supported");
    }

    @Override
    public MaybeLazy<Optional<String>> getTableComment(ConnectorSession session, Table metastoreTable)
    {
        throw new UnsupportedOperationException("getTableComment is not supported");
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        throw new UnsupportedOperationException("getTableComment is not supported");
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        throw new UnsupportedOperationException("listNamespaces is not supported");
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        throw new UnsupportedOperationException("dropNamespace is not supported");
    }

    @Override
    public Optional<String> getNamespaceSeparator()
    {
        throw new UnsupportedOperationException("getNamespaceSeparator is not supported");
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        throw new UnsupportedOperationException("loadNamespaceMetadata is not supported");
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        throw new UnsupportedOperationException("getNamespacePrincipal is not supported");
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        throw new UnsupportedOperationException("createNamespace is not supported");
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException("setNamespacePrincipal is not supported");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new UnsupportedOperationException("renameNamespace is not supported");
    }

    @Override
    public List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace)
    {
        log.debug("listTables returned empty list");
        return ImmutableList.of(); // ObjectStoreMetadata#listViews calls IcebergMetadata#listMaterializedViews which calls TrinoCatalog#listTables
    }

    @Override
    public List<SchemaTableName> listIcebergTables(ConnectorSession session, List<String> filter)
    {
        throw new UnsupportedOperationException("listIcebergTables is not supported");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        throw new UnsupportedOperationException("listViews is not supported");
    }

    @Override
    public void registerView(ConnectorSession session, SchemaTableName viewName, ViewMetadata viewMetadata)
    {
        throw new UnsupportedOperationException("registerView is not supported");
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        throw new UnsupportedOperationException("streamRelationColumns is not supported");
    }

    @Override
    public Transaction newTransaction(org.apache.iceberg.Table icebergTable)
    {
        throw new UnsupportedOperationException("streamRelationColumns is not supported");
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        throw new UnsupportedOperationException("streamRelationComments is not supported");
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, Optional<String> location, Map<String, String> properties)
    {
        throw new UnsupportedOperationException("newCreateTableTransaction is not supported");
    }

    @Override
    public Transaction newCreateOrReplaceTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        throw new UnsupportedOperationException("newCreateOrReplaceTableTransaction is not supported");
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException("registerTable is not supported");
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        throw new UnsupportedOperationException("unregisterTable is not supported");
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException("dropTable is not supported");
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException("dropCorruptedTable is not supported");
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        throw new UnsupportedOperationException("renameTable is not supported");
    }

    @Override
    public BaseTable loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Unexpected table present: %s".formatted(schemaTableName));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        throw new UnsupportedOperationException("tryGetColumnMetadata is not supported");
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("updateTableComment is not supported");
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("updateViewComment is not supported");
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("updateViewColumnComment is not supported");
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException("defaultTableLocation is not supported");
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException("setTablePrincipal is not supported");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new UnsupportedOperationException("createView is not supported");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new UnsupportedOperationException("renameView is not supported");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException("setViewPrincipal is not supported");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new UnsupportedOperationException("dropView is not supported");
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        throw new UnsupportedOperationException("getViews is not supported"); // Objectstore doesn't call IcebergMetadata#getViews
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new UnsupportedOperationException("getView is not supported");  // Objectstore doesn't call IcebergMetadata#getView
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> materializedViewProperties, boolean replace, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException("createMaterializedView is not supported");
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("updateMaterializedViewColumnComment is not supported");
    }

    @Override
    public void updateMaterializedViewRefreshSchedule(ConnectorSession session, SchemaTableName viewName, Optional<String> schedule)
    {
        throw new UnsupportedOperationException("updateMaterializedViewRefreshSchedule is not supported");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new UnsupportedOperationException("dropMaterializedView is not supported");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        log.debug("getMaterializedView returned empty result");
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        throw new UnsupportedOperationException("getMaterializedViewProperties is not supported");
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        throw new UnsupportedOperationException("getMaterializedViewProperties is not supported");
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new UnsupportedOperationException("renameMaterializedView is not supported");
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        throw new UnsupportedOperationException("updateColumnComment is not supported");
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        throw new UnsupportedOperationException("redirectTable is not supported");
    }

    @Override
    public Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }
}
