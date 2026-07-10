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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.airlift.http.client.HeaderNames;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.json.JsonMapperProvider;
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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.io.IOException;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.META_TABLE_LOCATION;
import static io.trino.metastore.HiveType.HIVE_STRING;
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
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.security.PrincipalType.USER;
import static io.unitycatalog.client.model.DataSourceFormat.DELTA;
import static io.unitycatalog.client.model.TableType.EXTERNAL;
import static io.unitycatalog.client.model.TableType.MANAGED;
import static java.net.Authenticator.RequestorType.PROXY;
import static java.net.Proxy.NO_PROXY;
import static java.net.Proxy.Type.HTTP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.joining;

public class UnityHiveMetastore
        implements UnityMetastore
{
    private static final Logger LOG = Logger.get(UnityHiveMetastore.class);

    public static final String UNITY_CATALOG_TABLE_ID = "io.unitycatalog.tableId";

    // TODO: support azure credentials vending https://starburstdata.atlassian.net/browse/SEP-18169

    private static final String NAMESPACE_SEPARATOR = ".";
    private static final String DELTA_PATH_PROPERTY = "path";
    private static final String DELTA_TABLE_PROVIDER_PROPERTY = "spark.sql.sources.provider";
    private static final String DELTA_TABLE_PROVIDER_VALUE = "DELTA";
    private static final String USER_AGENT = "starburst-unity-hive-metastore";
    // Unity Catalog server caps table list size at 50, so we paginate results ourselves
    private static final int LIST_PAGE_SIZE = 50;
    private static final Map<io.unitycatalog.client.model.TableType, TableType> SUPPORTED_TABLE_TYPES_MAPPING = ImmutableMap.of(
            MANAGED, MANAGED_TABLE,
            EXTERNAL, EXTERNAL_TABLE);
    private static final ObjectMapper OBJECT_MAPPER = new JsonMapperProvider().get();
    // sometimes we see transient errors from unity api, which could be retried
    private static final RetryPolicy<Object> UNITY_API_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(failure -> failure instanceof ApiException apiException && Set.of(408, 429, 504).contains(apiException.getCode()))
            .onFailedAttempt(e -> LOG.warn(e.getLastException(), "Retrying unity api call"))
            .withMaxDuration(Duration.ofSeconds(5))
            .withBackoff(100, 500, MILLIS)
            .withMaxAttempts(3)
            .build();

    private final Set<DataSourceFormat> supportedUnityTableFormats;
    private final ApiClient apiClient;
    private final SchemasApi schemasApi;
    private final TablesApi tablesApi;
    private final String catalogName;
    private final TemporaryCredentialsApi temporaryCredentialsApi;
    private final UnityTokenProvider tokenProvider;
    private final URI stagedCommitsUri;

    public UnityHiveMetastore(
            String host,
            String catalogName,
            UnityTokenProvider tokenProvider,
            boolean vendedCredentialsEnabled,
            boolean proxyEnabled,
            Optional<String> proxyHost,
            OptionalInt proxyPort,
            Optional<String> proxyUsername,
            Optional<String> proxyPassword,
            Optional<List<String>> nonProxyHosts,
            Set<DataSourceFormat> supportedUnityTableFormats)
    {
        HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();
        if (proxyEnabled) {
            checkArgument(proxyHost.isPresent(), "Proxy host must be specified when proxy is enabled");
            checkArgument(proxyPort.isPresent(), "Proxy port must be specified when proxy is enabled");
            checkArgument(nonProxyHosts.isPresent(), "Non-proxy hosts must be specified when proxy is enabled");

            setupProxy(
                    httpClientBuilder,
                    proxyHost.get(),
                    proxyPort.getAsInt(),
                    proxyUsername,
                    proxyPassword,
                    nonProxyHosts.get());
        }
        apiClient = new ApiClient();
        apiClient.updateBaseUri("https://" + host + "/api/2.1/unity-catalog");
        apiClient.setHttpClientBuilder(httpClientBuilder);
        apiClient.setRequestInterceptor(request -> {
            request.header(HeaderNames.USER_AGENT.toString(), USER_AGENT);
            tokenProvider.getToken().ifPresent(authToken -> request.header(AUTHORIZATION.toString(), "Bearer " + authToken));
        });

        schemasApi = new SchemasApi(apiClient);
        tablesApi = new TablesApi(apiClient);
        this.catalogName = catalogName;
        this.temporaryCredentialsApi = vendedCredentialsEnabled ? new TemporaryCredentialsApi(apiClient) : null;
        this.supportedUnityTableFormats = ImmutableSet.copyOf(supportedUnityTableFormats);
        this.tokenProvider = requireNonNull(tokenProvider, "tokenProvider is null");
        this.stagedCommitsUri = URI.create("https://" + host + "/api/2.1/unity-catalog/delta/preview/commits");
    }

    private static void setupProxy(
            HttpClient.Builder httpClientBuilder,
            String proxyHost,
            int proxyPort,
            Optional<String> proxyUsername,
            Optional<String> proxyPassword,
            List<String> nonProxyHosts)
    {
        InetSocketAddress proxyAddress = InetSocketAddress.createUnresolved(proxyHost, proxyPort);
        ProxySelector proxySelector = getProxySelector(nonProxyHosts, proxyAddress);
        httpClientBuilder.proxy(proxySelector);

        if (proxyUsername.isPresent() || proxyPassword.isPresent()) {
            httpClientBuilder.authenticator(new Authenticator()
            {
                @Override
                protected PasswordAuthentication getPasswordAuthentication()
                {
                    if (getRequestorType() == PROXY) {
                        return new PasswordAuthentication(proxyUsername.orElse(""), proxyPassword.orElse("").toCharArray());
                    }
                    return null;
                }
            });
        }
    }

    private static ProxySelector getProxySelector(List<String> nonProxyHosts, InetSocketAddress proxyAddress)
    {
        return new ProxySelector()
        {
            @Override
            public List<Proxy> select(URI uri)
            {
                String host = uri.getHost();
                for (String pattern : nonProxyHosts) {
                    if (matchesNonProxyHost(host, pattern)) {
                        return ImmutableList.of(NO_PROXY);
                    }
                }
                return ImmutableList.of(new Proxy(HTTP, proxyAddress));
            }

            @Override
            public void connectFailed(URI uri, SocketAddress sa, IOException ioe)
            {
                LOG.warn(ioe, "Proxy connect failed for %s via %s", uri, sa);
            }
        };
    }

    private static boolean matchesNonProxyHost(String host, String pattern)
    {
        if (pattern.isEmpty()) {
            return false;
        }
        String regex = Arrays.stream(pattern.split("\\*", -1))
                .map(Pattern::quote)
                .collect(joining(".*"));
        return host.matches(regex);
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        SchemaInfo schemaInfo;
        try {
            schemaInfo = retry(() -> schemasApi.getSchema(catalogName + NAMESPACE_SEPARATOR + databaseName));
        }
        catch (ApiException e) {
            if (e.getCode() == 404) {
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
                requireNonNullElse(schemaInfo.getProperties(), ImmutableMap.of()));
        return Optional.of(database);
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return Streams.stream(paginate(
                            pageToken -> schemasApi.listSchemas(catalogName, LIST_PAGE_SIZE, pageToken),
                            ListSchemasResponse::getSchemas,
                            ListSchemasResponse::getNextPageToken))
                    .map(SchemaInfo::getName)
                    .collect(toImmutableList());
        }
        catch (UncheckedApiException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        io.unitycatalog.client.model.TableInfo tableInfo;
        try {
            tableInfo = retry(() -> tablesApi.getTable(catalogName + NAMESPACE_SEPARATOR + databaseName + NAMESPACE_SEPARATOR + tableName, false, false));
        }
        catch (ApiException e) {
            if (e.getCode() == 404) {
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
            return Streams.stream(paginate(
                            pageToken -> tablesApi.listTables(catalogName, databaseName, LIST_PAGE_SIZE, pageToken),
                            ListTablesResponse::getTables,
                            ListTablesResponse::getNextPageToken))
                    .filter(this::isSupportedUnityTable)
                    .map(table -> new TableInfo(schemaTableName(table.getSchemaName(), table.getName()), TABLE))
                    .collect(toImmutableList());
        }
        catch (UncheckedApiException e) {
            if (e.getCause().getCode() == 404) {
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
    public Optional<Iterator<Table>> streamTables(ConnectorSession session, String databaseName)
    {
        return Optional.of(new AbstractIterator<>()
        {
            private Iterator<io.unitycatalog.client.model.TableInfo> delegate;

            @Override
            protected Table computeNext()
            {
                if (delegate == null) {
                    delegate = Iterators.filter(
                            paginate(
                                    pageToken -> tablesApi.listTables(catalogName, databaseName, LIST_PAGE_SIZE, pageToken),
                                    ListTablesResponse::getTables,
                                    ListTablesResponse::getNextPageToken).iterator(),
                            UnityHiveMetastore.this::isSupportedUnityTable);
                }
                try {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    return fromUnityTable(delegate.next()).orElseGet(this::computeNext);
                }
                catch (UncheckedApiException e) {
                    if (e.getCause().getCode() == 404) {
                        LOG.debug("Schema '%s' not found", databaseName);
                        return endOfData();
                    }
                    throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
                }
                catch (RuntimeException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing Unity Metastore: " + requireNonNullElse(e.getMessage(), e), e);
                }
            }
        });
    }

    private boolean isSupportedUnityTable(io.unitycatalog.client.model.TableInfo tableInfo)
    {
        DataSourceFormat dataSourceFormat = requireNonNullElse(tableInfo.getDataSourceFormat(), DELTA);
        io.unitycatalog.client.model.TableType tableType = requireNonNullElse(tableInfo.getTableType(), MANAGED);
        if (dataSourceFormat != DELTA && tableType == MANAGED) {
            return false;
        }
        return supportedUnityTableFormats.contains(dataSourceFormat)
                && SUPPORTED_TABLE_TYPES_MAPPING.containsKey(tableType);
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
            schemasApi.createSchema(createSchema);
        }
        catch (ApiException ex) {
            if ("SCHEMA_ALREADY_EXISTS".equals(parseErrorCode(ex.getResponseBody()).orElse(""))) {
                throw new SchemaAlreadyExistsException(database.getDatabaseName(), ex);
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            schemasApi.deleteSchema(catalogName + "." + databaseName, false);
        }
        catch (ApiException ex) {
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

        CreateTable createTable = new CreateTable()
                .catalogName(catalogName)
                .schemaName(table.getDatabaseName())
                .name(table.getTableName())
                .tableType(EXTERNAL)
                .storageLocation(table.getStorage().getLocation())
                .properties(table.getParameters());

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
                IntStream.range(0, table.getDataColumns().size())
                        .mapToObj(i -> {
                            Column column = table.getDataColumns().get(i);
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
            tablesApi.createTable(createTable);
        }
        catch (ApiException ex) {
            if ("SCHEMA_DOES_NOT_EXIST".equals(parseErrorCode(ex.getResponseBody()).orElse(""))) {
                throw new SchemaNotFoundException(table.getDatabaseName());
            }
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
            tablesApi.deleteTable(catalogName + "." + databaseName + "." + tableName);
        }
        catch (ApiException ex) {
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
    public void flushTableCache(String databaseName, String tableName) {}

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

    private Optional<Table> fromUnityTable(io.unitycatalog.client.model.TableInfo tableInfo)
    {
        io.unitycatalog.client.model.TableType tableType = requireNonNullElse(tableInfo.getTableType(), MANAGED);
        if (!SUPPORTED_TABLE_TYPES_MAPPING.containsKey(tableType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table type: " + tableType);
        }
        DataSourceFormat dataSourceFormat = requireNonNullElse(tableInfo.getDataSourceFormat(), DELTA);
        if (!supportedUnityTableFormats.contains(dataSourceFormat)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported data source format: " + dataSourceFormat);
        }
        if (dataSourceFormat != DELTA && tableType == MANAGED) {
            throw new TrinoException(NOT_SUPPORTED, "Only DELTA table format supports managed table type: " + dataSourceFormat);
        }

        TableType type = SUPPORTED_TABLE_TYPES_MAPPING.get(tableType);
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(tableInfo.getSchemaName())
                .setTableName(tableInfo.getName())
                .setTableType(type.name())
                .setOwner(Optional.ofNullable(tableInfo.getOwner()))
                .setParameter(META_TABLE_LOCATION, tableInfo.getStorageLocation());

        if (dataSourceFormat == DataSourceFormat.DELTA) {
            tableBuilder.setDataColumns(ImmutableList.of(new Column("dummy", HIVE_STRING, Optional.empty(), ImmutableMap.of())));
            tableBuilder.withStorage(storage -> storage
                    .setStorageFormat(getStorageFormat(dataSourceFormat))
                    .setLocation(tableInfo.getStorageLocation())
                    .setSerdeParameters(Map.of(DELTA_PATH_PROPERTY, requireNonNull(tableInfo.getStorageLocation(), "storage location is null"))));
            tableBuilder.setParameters(tableInfo.getProperties());
            tableBuilder.setParameter(DELTA_TABLE_PROVIDER_PROPERTY, DELTA_TABLE_PROVIDER_VALUE);
            tableBuilder.setParameter(UNITY_CATALOG_TABLE_ID, tableInfo.getTableId());
        }
        else {
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

            tableBuilder
                    .setDataColumns(dataColumns)
                    .setPartitionColumns(partitionColumns);

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
            return DataSourceFormat.AVRO;
        }
        if (ORC.toStorageFormat().equals(storageFormat)) {
            return DataSourceFormat.ORC;
        }
        if (PARQUET.toStorageFormat().equals(storageFormat)) {
            return DataSourceFormat.PARQUET;
        }
        if (CSV.toStorageFormat().equals(storageFormat)) {
            return DataSourceFormat.CSV;
        }
        if (JSON.toStorageFormat().equals(storageFormat)) {
            return DataSourceFormat.JSON;
        }
        if (TEXTFILE.toStorageFormat().equals(storageFormat)) {
            return DataSourceFormat.TEXT;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported data source format: " + storageFormat);
    }

    private static <T> T retry(ApiCall<T> call)
            throws ApiException
    {
        try {
            return Failsafe.with(UNITY_API_RETRY_POLICY)
                    .get(call::execute);
        }
        catch (FailsafeException e) {
            throwIfInstanceOf(e.getCause(), ApiException.class);
            throw e;
        }
    }

    @FunctionalInterface
    private interface ApiCall<T>
    {
        T execute()
                throws ApiException;
    }

    private static <PageT, ItemT> Iterable<ItemT> paginate(
            PageFetcher<PageT> pageFetcher,
            Function<PageT, List<ItemT>> getItems,
            Function<PageT, String> getNextPageToken)
    {
        return () -> new AbstractIterator<>()
        {
            private Iterator<ItemT> currentPage = Collections.emptyIterator();
            private String pageToken;
            private boolean exhausted;

            @Override
            protected ItemT computeNext()
            {
                while (!currentPage.hasNext()) {
                    if (exhausted) {
                        return endOfData();
                    }

                    String currentToken = pageToken;
                    PageT page;
                    try {
                        page = retry(() -> pageFetcher.fetch(currentToken));
                    }
                    catch (ApiException e) {
                        throw new UncheckedApiException(e);
                    }

                    List<ItemT> pageItems = getItems.apply(page);
                    currentPage = pageItems == null ? Collections.emptyIterator() : pageItems.iterator();
                    pageToken = getNextPageToken.apply(page);
                    if (pageToken == null || pageToken.isEmpty()) {
                        exhausted = true;
                    }
                }
                return currentPage.next();
            }
        };
    }

    @FunctionalInterface
    private interface PageFetcher<PageT>
    {
        PageT fetch(String pageToken)
                throws ApiException;
    }

    private static Optional<String> parseErrorCode(String body)
    {
        if (body == null || body.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(OBJECT_MAPPER.readValue(body, ErrorResponse.class).errorCode());
        }
        catch (JsonProcessingException ignored) {
            return Optional.empty();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ErrorResponse(@JsonProperty("error_code") String errorCode, @JsonProperty("message") String message) {}

    private static final class UncheckedApiException
            extends RuntimeException
    {
        UncheckedApiException(ApiException cause)
        {
            super(requireNonNull(cause, "cause is null"));
        }

        @Override
        public ApiException getCause()
        {
            return (ApiException) super.getCause();
        }
    }

    /// /////////////////////////////////////////
    /// Unity Catalog specific methods
    /// Below methods are only supported by Unity Catalog
    /// /////////////////////////////////////////

    @Override
    public StagedCommitsInfo loadStagedCommitsInfo(String tableId, String tableLocation, Optional<Long> startVersion, Optional<Long> endVersion)
    {
        HttpUriBuilder uriBuilder = HttpUriBuilder.uriBuilderFrom(stagedCommitsUri)
                .addParameter("table_id", tableId)
                .addParameter("table_uri", tableLocation)
                .addParameter("start_version", String.valueOf(startVersion.orElse(0L)));
        endVersion.ifPresent(version -> uriBuilder.addParameter("end_version", String.valueOf(version)));

        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(uriBuilder.build())
                .header(ACCEPT.toString(), "application/json")
                .header(HeaderNames.USER_AGENT.toString(), USER_AGENT)
                .GET();
        tokenProvider.getToken().ifPresent(token -> request.header(AUTHORIZATION.toString(), "Bearer " + token));

        HttpResponse<String> response = sendRequest(request.build());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to load staged commits: HTTP " + response.statusCode() + " " + response.body());
        }
        try {
            return OBJECT_MAPPER.readValue(response.body(), StagedCommitsInfo.class);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to parse staged commits response", e);
        }
    }

    @Override
    public void commitStagedCommits(CommitRequest commitStagedRequest)
    {
        String body;
        try {
            body = OBJECT_MAPPER.writeValueAsString(commitStagedRequest);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to serialize commit request", e);
        }

        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(stagedCommitsUri)
                .header(ACCEPT.toString(), "application/json")
                .header(CONTENT_TYPE.toString(), "application/json")
                .header(HeaderNames.USER_AGENT.toString(), USER_AGENT)
                .POST(HttpRequest.BodyPublishers.ofString(body, UTF_8));
        tokenProvider.getToken().ifPresent(token -> request.header(AUTHORIZATION.toString(), "Bearer " + token));

        HttpResponse<String> response = sendRequest(request.build());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new UnityCatalogException(response.statusCode(), parseErrorCode(response.body()), response.body());
        }
        // handle databricks on the caller side
    }

    private HttpResponse<String> sendRequest(HttpRequest request)
    {
        try {
            return apiClient.getHttpClient().send(request, HttpResponse.BodyHandlers.ofString(UTF_8));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(HIVE_METASTORE_ERROR, "Interrupted while calling Unity Catalog", e);
        }
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
