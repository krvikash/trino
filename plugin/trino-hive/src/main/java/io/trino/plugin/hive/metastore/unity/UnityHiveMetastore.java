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
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.error.platform.BadRequest;
import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnTypeName;
import com.databricks.sdk.service.catalog.CreateSchema;
import com.databricks.sdk.service.catalog.CreateTableRequest;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.SchemaAlreadyExistsException;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.metastore.type.CharTypeInfo;
import io.trino.metastore.type.DecimalTypeInfo;
import io.trino.metastore.type.ListTypeInfo;
import io.trino.metastore.type.MapTypeInfo;
import io.trino.metastore.type.StructTypeInfo;
import io.trino.metastore.type.TypeInfo;
import io.trino.metastore.type.VarcharTypeInfo;
import io.trino.plugin.hive.TableType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.LongStream;

import static com.databricks.sdk.core.PatCredentialsProvider.PAT;
import static com.databricks.sdk.service.catalog.DataSourceFormat.DELTA;
import static com.databricks.sdk.service.catalog.TableType.EXTERNAL;
import static com.databricks.sdk.service.catalog.TableType.MANAGED;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
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
        implements UnityMetastore
{
    private static final Logger LOG = Logger.get(UnityHiveMetastore.class);

    public static final String UNITY_CATALOG_TABLE_ID = "ucTableId";

    // TODO: support azure credentials vending https://starburstdata.atlassian.net/browse/SEP-18169

    private static final String NAMESPACE_SEPARATOR = ".";
    private static final String DELTA_PATH_PROPERTY = "path";
    private static final String DELTA_TABLE_PROVIDER_PROPERTY = "spark.sql.sources.provider";
    private static final String DELTA_TABLE_PROVIDER_VALUE = "DELTA";
    private static final Map<com.databricks.sdk.service.catalog.TableType, TableType> SUPPORTED_TABLE_TYPES_MAPPING = ImmutableMap.of(
            MANAGED, MANAGED_TABLE,
            EXTERNAL, EXTERNAL_TABLE);

    private final Set<DataSourceFormat> supportedUnityTableFormats;
    private final ApiClient apiClient;
    private final SchemasAPI schemasApi;
    private final TablesAPI tablesApi;
    private final String catalogName;
    private final TemporaryCredentialsApi temporaryCredentialsApi;

    public UnityHiveMetastore(String host, String catalogName, Optional<String> token, boolean vendedCredentialsEnabled, Set<DataSourceFormat> supportedUnityTableFormats)
    {
        DatabricksConfig databricksConfig = new DatabricksConfig()
                .setHost(host)
                .setAuthType(PAT);
        token.ifPresent(databricksConfig::setToken);
        apiClient = new ApiClient(databricksConfig);
        schemasApi = new SchemasAPI(apiClient);
        tablesApi = new TablesAPI(apiClient);
        this.catalogName = catalogName;

        if (vendedCredentialsEnabled) {
            io.unitycatalog.client.ApiClient unityApiClient = new io.unitycatalog.client.ApiClient();
            unityApiClient.updateBaseUri("https://" + host + "/api/2.1/unity-catalog");
            token.ifPresent(authToken -> unityApiClient.setRequestInterceptor(request -> request.header("Authorization", "Bearer " + authToken)));
            this.temporaryCredentialsApi = new TemporaryCredentialsApi(unityApiClient);
        }
        else {
            this.temporaryCredentialsApi = null;
        }
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
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        throw new TrinoException(NOT_SUPPORTED, "getTableNamesWithParameters is not supported for Unity metastore");
    }

    @Override
    public void createDatabase(Database database)
    {
        CreateSchema createSchema = new CreateSchema();
        database.getLocation().ifPresent(createSchema::setStorageRoot);
        createSchema.setCatalogName(catalogName);
        createSchema.setName(database.getDatabaseName());
        createSchema.setProperties(database.getParameters());
        database.getComment().ifPresent(createSchema::setComment);
        try {
            schemasApi.create(createSchema);
        }
        catch (BadRequest ex) {
            if (ex.getErrorCode().equals("SCHEMA_ALREADY_EXISTS")) {
                throw new SchemaAlreadyExistsException(database.getDatabaseName(), ex);
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
        catch (DatabricksException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            schemasApi.delete(catalogName + "." + databaseName);
        }
        catch (DatabricksException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
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
        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EXTERNAL_TABLE.equals(tableType), "Invalid table type: %s, create table is supported only for external tables", tableType);

        CreateTableRequest createTable = new CreateTableRequest()
                .setCatalogName(catalogName)
                .setSchemaName(table.getDatabaseName())
                .setName(table.getTableName())
                .setTableType(EXTERNAL)
                .setStorageLocation(table.getStorage().getLocation())
                .setProperties(table.getParameters());

        if (DELTA_TABLE_PROVIDER_VALUE.equals(table.getParameters().get(DELTA_TABLE_PROVIDER_PROPERTY))) {
            createTable.setDataSourceFormat(DELTA);
        }
        else {
            createTable.setDataSourceFormat(getDataSourceFormat(table.getStorage().getStorageFormat()));
        }

        if (!table.getPartitionColumns().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Create table with partitioned columns are not supported for Unity metastore");
        }
        checkArgument(!table.getDataColumns().isEmpty(), "Cannot create table: No columns defined. Tables must have at least one column to be compatible with Databricks Unity Catalog");

        createTable.setColumns(
                LongStream.range(0, table.getDataColumns().size())
                        .mapToObj(i -> {
                            Column column = table.getDataColumns().get((int) i);
                            ColumnInfo columnInfo = new ColumnInfo();
                            columnInfo.setName(column.getName());
                            columnInfo.setPosition(i);
                            columnInfo.setTypeName(toColumTypeName(column.getType()));
                            columnInfo.setTypeText(column.getType().toString());
                            columnInfo.setTypeJson(buildColumnTypeJson(column.getName(), column.getType()));
                            column.getComment().ifPresent(columnInfo::setComment);

                            return columnInfo;
                        })
                        .collect(toImmutableList()));

        try {
            tablesApi.create(createTable);
        }
        catch (NotFound ex) {
            if (ex.getErrorCode().equals("SCHEMA_DOES_NOT_EXIST")) {
                throw new SchemaNotFoundException(table.getDatabaseName());
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
        catch (DatabricksException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Table table = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EXTERNAL_TABLE.equals(tableType), "Invalid table type: %s, drop table is supported only for external tables", tableType);
        try {
            tablesApi.delete(catalogName + "." + databaseName + "." + tableName);
        }
        catch (DatabricksException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
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

        TableType type = SUPPORTED_TABLE_TYPES_MAPPING.get(tableType);
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(tableInfo.getSchemaName())
                .setTableName(tableInfo.getName())
                .setDataColumns(dataColumns)
                .setPartitionColumns(partitionColumns)
                .setTableType(type.name())
                .setOwner(Optional.ofNullable(tableInfo.getOwner()))
                .setParameter(META_TABLE_LOCATION, tableInfo.getStorageLocation());

        if (dataSourceFormat == DataSourceFormat.DELTA) {
            tableBuilder.withStorage(storage -> storage
                    .setStorageFormat(getStorageFormat(dataSourceFormat))
                    .setLocation(tableInfo.getStorageLocation())
                    .setSerdeParameters(Map.of(DELTA_PATH_PROPERTY, requireNonNull(tableInfo.getStorageLocation(), "storage location is null"))));
            tableBuilder.setParameters(tableInfo.getProperties());
            tableBuilder.setParameter(DELTA_TABLE_PROVIDER_PROPERTY, DELTA_TABLE_PROVIDER_VALUE);
            tableBuilder.setParameter(UNITY_CATALOG_TABLE_ID, tableInfo.getTableId());
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
        return switch (hiveType) {
            case "timestamp_ntz" -> HiveType.valueOf("timestamp(6)");
            default -> HiveType.valueOf(hiveType);
        };
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

    private static ColumnTypeName toColumTypeName(HiveType hiveType)
    {
        if (hiveType.equals(HiveType.HIVE_BOOLEAN)) {
            return ColumnTypeName.BOOLEAN;
        }
        if (hiveType.equals(HiveType.HIVE_BYTE)) {
            return ColumnTypeName.BYTE;
        }
        if (hiveType.equals(HiveType.HIVE_SHORT)) {
            return ColumnTypeName.SHORT;
        }
        if (hiveType.equals(HiveType.HIVE_INT)) {
            return ColumnTypeName.INT;
        }
        if (hiveType.equals(HiveType.HIVE_LONG)) {
            return ColumnTypeName.LONG;
        }
        if (hiveType.equals(HiveType.HIVE_FLOAT)) {
            return ColumnTypeName.FLOAT;
        }
        if (hiveType.equals(HiveType.HIVE_DOUBLE)) {
            return ColumnTypeName.DOUBLE;
        }
        if (hiveType.equals(HiveType.HIVE_STRING)) {
            return ColumnTypeName.STRING;
        }
        if (hiveType.equals(HiveType.HIVE_TIMESTAMP)) {
            return ColumnTypeName.TIMESTAMP;
        }
        if (hiveType.equals(HiveType.HIVE_DATE)) {
            return ColumnTypeName.DATE;
        }
        if (hiveType.equals(HiveType.HIVE_BINARY)) {
            return ColumnTypeName.BINARY;
        }
        if (hiveType.equals(HiveType.HIVE_VARIANT)) {
            return ColumnTypeName.VARIANT;
        }
        if (hiveType.getTypeInfo() instanceof DecimalTypeInfo) {
            return ColumnTypeName.DECIMAL;
        }
        if (hiveType.getTypeInfo() instanceof CharTypeInfo) {
            return ColumnTypeName.CHAR;
        }
        if (hiveType.getTypeInfo() instanceof VarcharTypeInfo) {
            return ColumnTypeName.STRING;
        }
        if (hiveType.getTypeInfo() instanceof ListTypeInfo) {
            return ColumnTypeName.ARRAY;
        }
        if (hiveType.getTypeInfo() instanceof MapTypeInfo) {
            return ColumnTypeName.MAP;
        }
        if (hiveType.getTypeInfo() instanceof StructTypeInfo) {
            return ColumnTypeName.STRUCT;
        }
        // TODO https://starburstdata.atlassian.net/browse/CONNECT-603
        //  some types like timestamp_ntz is not supported by trino hive globally
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + hiveType);
    }

    private static String buildColumnTypeJson(String columnName, HiveType hiveType)
    {
        return "{\"name\":\"%s\",\"type\":%s,\"nullable\":true,\"metadata\":{}}".formatted(columnName, getTypeJson(hiveType));
    }

    private static String getTypeJson(HiveType hiveType)
    {
        if (hiveType.equals(HiveType.HIVE_BOOLEAN)) {
            return "\"boolean\"";
        }
        if (hiveType.equals(HiveType.HIVE_BYTE)) {
            return "\"byte\"";
        }
        if (hiveType.equals(HiveType.HIVE_SHORT)) {
            return "\"short\"";
        }
        if (hiveType.equals(HiveType.HIVE_INT)) {
            return "\"integer\"";
        }
        if (hiveType.equals(HiveType.HIVE_LONG)) {
            return "\"long\"";
        }
        if (hiveType.equals(HiveType.HIVE_FLOAT)) {
            return "\"float\"";
        }
        if (hiveType.equals(HiveType.HIVE_DOUBLE)) {
            return "\"double\"";
        }
        if (hiveType.equals(HiveType.HIVE_STRING)) {
            return "\"string\"";
        }
        if (hiveType.equals(HiveType.HIVE_TIMESTAMP)) {
            return "\"timestamp\"";
        }
        if (hiveType.equals(HiveType.HIVE_TIMESTAMPLOCALTZ)) {
            return "\"timestamp_ntz\"";
        }
        if (hiveType.equals(HiveType.HIVE_DATE)) {
            return "\"date\"";
        }
        if (hiveType.equals(HiveType.HIVE_BINARY)) {
            return "\"binary\"";
        }
        if (hiveType.equals(HiveType.HIVE_VARIANT)) {
            return "\"variant\"";
        }

        if (hiveType.getTypeInfo() instanceof DecimalTypeInfo) {
            return "\"%s\"".formatted(hiveType.toString());
        }

        if (hiveType.getTypeInfo() instanceof CharTypeInfo) {
            return "\"%s\"".formatted(hiveType.toString());
        }

        if (hiveType.getTypeInfo() instanceof VarcharTypeInfo) {
            return "\"%s\"".formatted(hiveType.toString());
        }

        if (hiveType.getTypeInfo() instanceof ListTypeInfo listTypeInfo) {
            TypeInfo elementsTypeInfo = listTypeInfo.getListElementTypeInfo();
            return "{\"type\":\"array\",\"elementType\":%s,\"containsNull\":true}".formatted(getTypeJson(HiveType.fromTypeInfo(elementsTypeInfo)));
        }
        if (hiveType.getTypeInfo() instanceof MapTypeInfo mapTypeInfo) {
            TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
            TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
            return "{\"type\":\"map\",\"keyType\":%s,\"valueType\":%s,\"valueContainsNull\":true}".formatted(getTypeJson(HiveType.fromTypeInfo(keyTypeInfo)), getTypeJson(HiveType.fromTypeInfo(valueTypeInfo)));
        }
        if (hiveType.getTypeInfo() instanceof StructTypeInfo structTypeInfo) {
            List<String> names = structTypeInfo.getAllStructFieldNames();
            List<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();

            StringJoiner fields = new StringJoiner(",");
            for (int i = 0; i < names.size(); i++) {
                String fieldName = names.get(i);
                TypeInfo fieldTypeInfo = typeInfos.get(i);
                fields.add(buildColumnTypeJson(fieldName, HiveType.fromTypeInfo(fieldTypeInfo)));
            }

            return "{\"type\":\"struct\",\"fields\":[%s]}".formatted(fields.toString());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + hiveType);
    }

    private static DataSourceFormat getDataSourceFormat(StorageFormat storageFormat)
    {
        if (AVRO.toStorageFormat().equals(storageFormat)) {
            return com.databricks.sdk.service.catalog.DataSourceFormat.AVRO;
        }
        if (ORC.toStorageFormat().equals(storageFormat)) {
            return com.databricks.sdk.service.catalog.DataSourceFormat.ORC;
        }
        if (PARQUET.toStorageFormat().equals(storageFormat)) {
            return com.databricks.sdk.service.catalog.DataSourceFormat.PARQUET;
        }
        if (CSV.toStorageFormat().equals(storageFormat)) {
            return com.databricks.sdk.service.catalog.DataSourceFormat.CSV;
        }
        if (JSON.toStorageFormat().equals(storageFormat)) {
            return com.databricks.sdk.service.catalog.DataSourceFormat.JSON;
        }
        if (TEXTFILE.toStorageFormat().equals(storageFormat)) {
            return com.databricks.sdk.service.catalog.DataSourceFormat.TEXT;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported data source format: " + storageFormat);
    }

    /// /////////////////////////////////////////
    /// Unity Catalog specific methods
    /// Below methods are only supported by Unity Catalog
    /// /////////////////////////////////////////

    @Override
    public StagedCommitsInfo loadStagedCommitsInfo(String tableId, String tableLocation, Optional<Long> startVersion, Optional<Long> endVersion)
    {
        Request request = new Request("GET", "/api/2.1/unity-catalog/delta/preview/commits")
                .withQueryParam("table_id", tableId)
                .withQueryParam("table_uri", tableLocation)
                .withQueryParam("start_version", String.valueOf(startVersion.orElse(0L)))
                .withHeader("Accept", "application/json")
                .withHeader("Content-Type", "application/json");
        endVersion.ifPresent(version -> request.withQueryParam("end_version", String.valueOf(version)));
        try {
            return apiClient.execute(request, StagedCommitsInfo.class);
        }
        catch (IOException | DatabricksException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void commitStagedCommits(CommitRequest commitStagedRequest)
    {
        Request request;
        try {
            request = new Request("POST", "/api/2.1/unity-catalog/delta/preview/commits", apiClient.serialize(commitStagedRequest))
                    .withHeader("Accept", "application/json")
                    .withHeader("Content-Type", "application/json");
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to serialize commit request", e);
        }

        try {
            apiClient.execute(request, Void.class);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        // handle databricks on the caller side
    }

    @Override
    public TemporaryCredentials getTemporaryTableCredentials(String tableId, TableOperation operation)
    {
        try {
            return temporaryCredentialsApi.generateTemporaryTableCredentials(new GenerateTemporaryTableCredential().tableId(tableId).operation(operation));
        }
        catch (ApiException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public TemporaryCredentials getTemporaryPathCredentials(String tableLocation, PathOperation operation)
    {
        try {
            return temporaryCredentialsApi.generateTemporaryPathCredentials(new GenerateTemporaryPathCredential().url(tableLocation).operation(operation));
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }
}
