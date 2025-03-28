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
package io.trino.plugin.hive.metastore.unity;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.TableType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.databricks.sdk.core.PatCredentialsProvider.PAT;
import static com.databricks.sdk.service.catalog.DataSourceFormat.DELTA;
import static com.databricks.sdk.service.catalog.TableType.EXTERNAL;
import static com.databricks.sdk.service.catalog.TableType.MANAGED;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.META_TABLE_LOCATION;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TABLE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.CSV;
import static io.trino.plugin.hive.HiveStorageFormat.JSON;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;

public class UnityHiveMetastore
        implements HiveMetastore
{
    private static final Logger LOG = Logger.get(UnityHiveMetastore.class);

    private static final String NAMESPACE_SEPARATOR = ".";
    private static final String DELTA_PATH_PROPERTY = "path";
    private static final String DELTA_TABLE_PROVIDER_PROPERTY = "spark.sql.sources.provider";
    private static final String DELTA_TABLE_PROVIDER_VALUE = "DELTA";
    private static final Map<com.databricks.sdk.service.catalog.TableType, TableType> SUPPORTED_TABLE_TYPES_MAPPING = ImmutableMap.of(
            MANAGED, MANAGED_TABLE,
            EXTERNAL, EXTERNAL_TABLE);

    private final Set<DataSourceFormat> supportedUnityTableFormats;
    private final SchemasAPI schemasApi;
    private final TablesAPI tablesApi;
    private final String catalogName;

    public UnityHiveMetastore(String host, String catalogName, Optional<String> token, Set<DataSourceFormat> supportedUnityTableFormats)
    {
        DatabricksConfig databricksConfig = new DatabricksConfig()
                .setHost(host)
                .setAuthType(PAT);
        token.ifPresent(databricksConfig::setToken);
        ApiClient apiClient = new ApiClient(databricksConfig);
        schemasApi = new SchemasAPI(apiClient);
        tablesApi = new TablesAPI(apiClient);
        this.catalogName = catalogName;
        this.supportedUnityTableFormats = ImmutableSet.copyOf(supportedUnityTableFormats);
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        SchemaInfo schemaInfo;
        try {
            schemaInfo = schemasApi.get(catalogName + NAMESPACE_SEPARATOR + databaseName);
        }
        catch (DatabricksError e) {
            if (e.getStatusCode() == 404) {
                LOG.debug("Schema '%s' not found", databaseName);
                return Optional.empty();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        Database database = new Database(
                schemaInfo.getName(),
                Optional.ofNullable(schemaInfo.getStorageRoot()),
                Optional.ofNullable(schemaInfo.getOwner()),
                Optional.of(USER),
                Optional.ofNullable(schemaInfo.getComment()),
                firstNonNull(schemaInfo.getProperties(), ImmutableMap.of()));
        return Optional.of(database);
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return Streams.stream(schemasApi.list(catalogName).iterator())
                    .map(SchemaInfo::getName)
                    .collect(toImmutableList());
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        com.databricks.sdk.service.catalog.TableInfo tableInfo;
        try {
            tableInfo = tablesApi.get(catalogName + NAMESPACE_SEPARATOR + databaseName + NAMESPACE_SEPARATOR + tableName);
        }
        catch (DatabricksError e) {
            if (e.getStatusCode() == 404) {
                LOG.debug("Table '%s.%s' is not found", databaseName, tableName);
                return Optional.empty();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        if (tableInfo == null) {
            return Optional.empty();
        }
        return fromUnityTable(tableInfo);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return ImmutableMap.of(); // This method has to return an empty map instead of throwing an exception for Hive tables
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        throw new TrinoException(NOT_SUPPORTED, "getPartitionColumnStatistics is not supported for Unity metastore");
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        // Do not throw an exception as Hive table format requires this method to be implemented for insert path.
        // Moreover, implementation remains empty as Unity catalog doesn't support table statistics.
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        throw new TrinoException(NOT_SUPPORTED, "updatePartitionStatistics is not supported for Unity metastore");
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        try {
            return Streams.stream(tablesApi.list(catalogName, databaseName))
                    .filter(tableInfo -> {
                        DataSourceFormat dataSourceFormat = firstNonNull(tableInfo.getDataSourceFormat(), DELTA);
                        com.databricks.sdk.service.catalog.TableType tableType = firstNonNull(tableInfo.getTableType(), MANAGED);
                        if (dataSourceFormat != DELTA && tableType == MANAGED) {
                            return false;
                        }
                        return supportedUnityTableFormats.contains(dataSourceFormat)
                                && SUPPORTED_TABLE_TYPES_MAPPING.containsKey(tableType);
                    })
                    .map(table -> new TableInfo(schemaTableName(table.getSchemaName(), table.getName()), TABLE))
                    .collect(toImmutableList());
        }
        catch (DatabricksError e) {
            if (e.getStatusCode() == 404) {
                LOG.debug("Schema '%s' not found", databaseName);
                return ImmutableList.of();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, ImmutableSet<String> parameterValues)
    {
        throw new TrinoException(NOT_SUPPORTED, "getTableNamesWithParameters is not supported for Unity metastore");
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new TrinoException(NOT_SUPPORTED, "createDatabase is not supported for Unity metastore");
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropDatabase is not supported for Unity metastore");
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameDatabase is not supported for Unity metastore");
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setDatabaseOwner is not supported for Unity metastore");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new TrinoException(NOT_SUPPORTED, "createTable is not supported for Unity metastore");
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropTable is not supported for Unity metastore");
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        throw new TrinoException(NOT_SUPPORTED, "replaceTable is not supported for Unity metastore");
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameTable is not supported for Unity metastore");
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "commentTable is not supported for Unity metastore");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTableOwner is not supported for Unity metastore");
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "commentColumn is not supported for Unity metastore");
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new TrinoException(NOT_SUPPORTED, "addColumn is not supported for Unity metastore");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameColumn is not supported for Unity metastore");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropColumn is not supported for Unity metastore");
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        throw new TrinoException(NOT_SUPPORTED, "getPartition is not supported for Unity metastore");
    }

    // TODO support Hive partitioned tables https://starburstdata.atlassian.net/browse/CONNECT-426
    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        throw new TrinoException(NOT_SUPPORTED, "getPartitionNamesByFilter is not supported for Unity metastore");
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        throw new TrinoException(NOT_SUPPORTED, "getPartitionsByNames is not supported for Unity metastore");
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new TrinoException(NOT_SUPPORTED, "addPartitions is not supported for Unity metastore");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropPartition is not supported for Unity metastore");
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new TrinoException(NOT_SUPPORTED, "alterPartition is not supported for Unity metastore");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "createRole is not supported for Unity metastore");
    }

    @Override
    public void dropRole(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropRole is not supported for Unity metastore");
    }

    @Override
    public Set<String> listRoles()
    {
        throw new TrinoException(NOT_SUPPORTED, "listRoles is not supported for Unity metastore");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantRoles is not supported for Unity metastore");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeRoles is not supported for Unity metastore");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "listRoleGrants is not supported for Unity metastore");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantTablePrivileges is not supported for Unity metastore");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeTablePrivileges is not supported for Unity metastore");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "listTablePrivileges is not supported for Unity metastore");
    }

    @Override
    public void checkSupportsTransactions()
    {
        throw new TrinoException(NOT_SUPPORTED, "checkSupportsTransactions is not supported for Unity metastore");
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        throw new TrinoException(NOT_SUPPORTED, "commitTransaction is not supported for Unity metastore");
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        throw new TrinoException(NOT_SUPPORTED, "abortTransaction is not supported for Unity metastore");
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        throw new TrinoException(NOT_SUPPORTED, "sendTransactionHeartbeat is not supported for Unity metastore");
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        throw new TrinoException(NOT_SUPPORTED, "getValidWriteIds is not supported for Unity metastore");
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        throw new TrinoException(NOT_SUPPORTED, "openTransaction is not supported for Unity metastore");
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        throw new TrinoException(NOT_SUPPORTED, "acquireSharedReadLock is not supported for Unity metastore");
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        throw new TrinoException(NOT_SUPPORTED, "getConfigValue is not supported for Unity metastore");
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        throw new TrinoException(NOT_SUPPORTED, "allocateWriteId is not supported for Unity metastore");
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, AcidOperation operation, boolean isDynamicPartitionWrite)
    {
        throw new TrinoException(NOT_SUPPORTED, "acquireTableWriteLock is not supported for Unity metastore");
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateTableWriteId is not supported for Unity metastore");
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        throw new TrinoException(NOT_SUPPORTED, "addDynamicPartitions is not supported for Unity metastore");
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        throw new TrinoException(NOT_SUPPORTED, "alterTransactionalTable is not supported for Unity metastore");
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        throw new TrinoException(NOT_SUPPORTED, "functionExists is not supported for Unity metastore");
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        throw new TrinoException(NOT_SUPPORTED, "getAllFunctions is not supported for Unity metastore");
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        throw new TrinoException(NOT_SUPPORTED, "getFunctions is not supported for Unity metastore");
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new TrinoException(NOT_SUPPORTED, "createFunction is not supported for Unity metastore");
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new TrinoException(NOT_SUPPORTED, "replaceFunction is not supported for Unity metastore");
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropFunction is not supported for Unity metastore");
    }

    private Optional<Table> fromUnityTable(com.databricks.sdk.service.catalog.TableInfo tableInfo)
    {
        com.databricks.sdk.service.catalog.TableType tableType = firstNonNull(tableInfo.getTableType(), MANAGED);
        if (!SUPPORTED_TABLE_TYPES_MAPPING.containsKey(tableType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table type: " + tableType);
        }
        DataSourceFormat dataSourceFormat = firstNonNull(tableInfo.getDataSourceFormat(), DELTA);
        if (!supportedUnityTableFormats.contains(dataSourceFormat)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported data source format: " + dataSourceFormat);
        }
        if (dataSourceFormat != DELTA && tableType == MANAGED) {
            throw new TrinoException(NOT_SUPPORTED, "Only DELTA table format supports managed table type: " + dataSourceFormat);
        }

        requireNonNull(tableInfo.getColumns(), "columns is null");
        List<Column> dataColumns = tableInfo.getColumns().stream()
                .filter(column -> column.getPartitionIndex() == null)
                .map(column -> new Column(column.getName(), getHiveTypeFromUnity(column.getTypeText()), Optional.ofNullable(column.getComment()), ImmutableMap.of()))
                .toList();

        List<Column> partitionColumns = tableInfo.getColumns().stream()
                .filter(column -> column.getPartitionIndex() != null)
                .sorted(Comparator.comparing(ColumnInfo::getPartitionIndex))
                .map(column -> new Column(column.getName(), getHiveTypeFromUnity(column.getTypeText()), Optional.ofNullable(column.getComment()), ImmutableMap.of()))
                .toList();

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(tableInfo.getSchemaName())
                .setTableName(tableInfo.getName())
                .setDataColumns(dataColumns)
                .setPartitionColumns(partitionColumns)
                .setTableType(SUPPORTED_TABLE_TYPES_MAPPING.get(tableType).name())
                .setOwner(Optional.ofNullable(tableInfo.getOwner()))
                .setParameter(META_TABLE_LOCATION, tableInfo.getStorageLocation());

        if (dataSourceFormat == DataSourceFormat.DELTA) {
            tableBuilder.withStorage(storage -> storage
                    .setStorageFormat(getStorageFormat(dataSourceFormat))
                    .setLocation(tableInfo.getStorageLocation())
                    .setSerdeParameters(Map.of(DELTA_PATH_PROPERTY, requireNonNull(tableInfo.getStorageLocation(), "storage location is null"))));
            tableBuilder.setParameters(tableInfo.getProperties());
            tableBuilder.setParameter(DELTA_TABLE_PROVIDER_PROPERTY, DELTA_TABLE_PROVIDER_VALUE);
        }
        else {
            tableBuilder.withStorage(storage -> storage
                    .setStorageFormat(getStorageFormat(dataSourceFormat))
                    .setLocation(tableInfo.getStorageLocation()));
        }

        return Optional.of(tableBuilder.build().withComment(Optional.ofNullable(tableInfo.getComment())));
    }

    private static HiveType getHiveTypeFromUnity(String hiveType)
    {
        return HiveType.valueOf(hiveType);
    }

    private static StorageFormat getStorageFormat(DataSourceFormat dataSourceFormat)
    {
        return switch (dataSourceFormat) {
            case AVRO -> AVRO.toStorageFormat();
            case ORC -> ORC.toStorageFormat();
            case PARQUET, DELTA -> PARQUET.toStorageFormat();
            case CSV -> CSV.toStorageFormat();
            case JSON -> JSON.toStorageFormat();
            case TEXT -> TEXTFILE.toStorageFormat();
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported data source format: " + dataSourceFormat);
        };
    }
}
