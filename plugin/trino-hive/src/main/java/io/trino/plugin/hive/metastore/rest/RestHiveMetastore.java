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
package io.trino.plugin.hive.metastore.rest;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.starburst.stargate.metastore.client.Column;
import io.starburst.stargate.metastore.client.ColumnStatistics;
import io.starburst.stargate.metastore.client.EntityAlreadyExistsException;
import io.starburst.stargate.metastore.client.EntityNotFoundException;
import io.starburst.stargate.metastore.client.GetTablesResult;
import io.starburst.stargate.metastore.client.Metastore;
import io.starburst.stargate.metastore.client.MetastoreException;
import io.starburst.stargate.metastore.client.PartitionName;
import io.starburst.stargate.metastore.client.RestMetastore;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.SchemaAlreadyExistsException;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableAlreadyExistsException;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toPartitionName;
import static io.trino.plugin.hive.metastore.rest.RestMetastoreUtils.PUBLIC_ROLE_NAME;
import static io.trino.plugin.hive.metastore.rest.RestMetastoreUtils.fromRestTable;
import static io.trino.plugin.hive.metastore.rest.RestMetastoreUtils.toRestDatabase;
import static io.trino.plugin.hive.metastore.rest.RestMetastoreUtils.toRestPartitionWithStatistics;
import static io.trino.plugin.hive.metastore.rest.RestMetastoreUtils.toRestStatisticsUpdate;
import static io.trino.plugin.hive.metastore.rest.RestMetastoreUtils.toRestStatisticsUpdateMode;
import static io.trino.plugin.hive.metastore.rest.RestMetastoreUtils.toRestTable;
import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class RestHiveMetastore
        implements HiveMetastore
{
    private static final Logger log = Logger.get(RestHiveMetastore.class);

    private static final String DEFAULT_METASTORE_USER = "trino";

    private final Metastore metastore;
    private final TrinoFileSystem fileSystem;
    private final Location defaultDirectory;

    public RestHiveMetastore(RestHiveMetastoreConfig config, TrinoFileSystemFactory fileSystemFactory, HttpClient httpClient)
    {
        this(new RestMetastore(config.getMetastoreId(), config.getSharedSecret(), config.getServerUri(), httpClient),
                fileSystemFactory,
                config.getDefaultDataDirectory());
    }

    public RestHiveMetastore(Metastore metastore, TrinoFileSystemFactory fileSystemFactory, Location defaultDirectory)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.defaultDirectory = requireNonNull(defaultDirectory, "defaultDirectory is null");
        this.fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER));
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return metastore.getDatabase(databaseName)
                    .map(RestMetastoreUtils::fromRestDatabase);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return ImmutableList.copyOf(metastore.getDatabaseNames());
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            return metastore.getTable(databaseName, tableName)
                    .map(RestMetastoreUtils::fromRestTable);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        try {
            return metastore.getTableColumnStatistics(databaseName, tableName, columnNames)
                    .entrySet().stream()
                    .collect(toImmutableMap(
                            Entry::getKey,
                            entry -> RestMetastoreUtils.fromRestColumnStatistics(entry.getValue())));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(SchemaTableName.schemaTableName(databaseName, tableName));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        try {
            ImmutableSet.Builder<PartitionName> restPartitionNamesBuilder = ImmutableSet.builder();
            ImmutableMap.Builder<PartitionName, String> restPartitionNameToSourcePartitionNameBuilder = ImmutableMap.builder();

            partitionNames.forEach(partitionName -> {
                List<String> partitionValues = extractPartitionValues(partitionName);
                PartitionName restPartitionName = new PartitionName(partitionValues);
                restPartitionNamesBuilder.add(restPartitionName);
                restPartitionNameToSourcePartitionNameBuilder.put(restPartitionName, partitionName);
            });

            Set<PartitionName> restPartitionNames = restPartitionNamesBuilder.build();
            Map<PartitionName, String> restPartitionNameToSourcePartitionName = restPartitionNameToSourcePartitionNameBuilder.buildOrThrow();

            Map<PartitionName, Map<String, ColumnStatistics>> partitionColumnStatistics = metastore.getPartitionColumnStatistics(databaseName, tableName, restPartitionNames, columnNames);
            return partitionColumnStatistics.entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> requireNonNull(restPartitionNameToSourcePartitionName.get(entry.getKey())),
                            entry -> transformValues(entry.getValue(), RestMetastoreUtils::fromRestColumnStatistics)));
        }
        catch (EntityNotFoundException e) {
            // The server conflates table-not-found and partition-not-found into the same 404, so we
            // cannot reliably throw TableNotFoundException here without risking false positives on
            // missing partitions.
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        if (acidWriteId.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Hive ACID is not supported");
        }

        try {
            metastore.updateTableStatistics(databaseName, tableName, toRestStatisticsUpdateMode(mode), toRestStatisticsUpdate(statisticsUpdate));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> updates)
    {
        updates.forEach((partitionName, update) -> doUpdatePartitionStatistics(table, partitionName, mode, update));
    }

    private void doUpdatePartitionStatistics(Table table, String encodedPartitionName, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        PartitionName partitionName = new PartitionName(extractPartitionValues(encodedPartitionName));
        try {
            metastore.updatePartitionStatistics(table.getDatabaseName(), table.getTableName(), partitionName, toRestStatisticsUpdateMode(mode), toRestStatisticsUpdate(statisticsUpdate));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName()), partitionName.partitionValues());
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        try {
            return RestMetastoreUtils.fromRestTableInfos(databaseName, metastore.getTables(databaseName));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Iterator<Table>> streamTables(ConnectorSession session, String databaseName)
    {
        return Optional.of(new AbstractIterator<>()
        {
            private Iterator<io.starburst.stargate.metastore.client.Table> delegate;

            @Override
            protected Table computeNext()
            {
                try {
                    if (delegate == null) {
                        delegate = stream(paginateGetTables(databaseName))
                                .flatMap(List::stream)
                                .iterator();
                    }

                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    return fromRestTable(delegate.next());
                }
                catch (MetastoreException e) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
                }
                catch (RuntimeException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
                }
            }
        });
    }

    private Iterator<List<io.starburst.stargate.metastore.client.Table>> paginateGetTables(String databaseName)
    {
        return new AbstractIterator<>()
        {
            // TODO: use pagination
            private final Optional<Integer> desiredLimit = Optional.empty();
            private boolean firstRequest = true;
            private Optional<String> nextToken = Optional.empty();

            @Override
            protected List<io.starburst.stargate.metastore.client.Table> computeNext()
            {
                if (nextToken.isEmpty() && !firstRequest) {
                    return endOfData();
                }
                GetTablesResult result = metastore.getTables(databaseName, desiredLimit, nextToken);

                firstRequest = false;
                nextToken = result.nextToken();
                return result.tables();
            }
        };
    }

    @Override
    public void createDatabase(Database database)
    {
        if (database.getLocation().isEmpty()) {
            Location location = defaultDirectory.appendPath(escapeSchemaName(database.getDatabaseName()));
            database = Database.builder(database)
                    .setLocation(Optional.of(location.toString()))
                    .build();
        }
        try {
            metastore.createDatabase(toRestDatabase(database));
        }
        catch (EntityAlreadyExistsException e) {
            // Ignore SchemaAlreadyExistsException when this query has already created the database.
            // This may happen when an actually successful metastore create call is retried
            // after a timeout on our side.
            String expectedQueryId = database.getParameters().get(TRINO_QUERY_ID_NAME);
            if (expectedQueryId != null) {
                String existingQueryId = getDatabase(database.getDatabaseName())
                        .map(Database::getParameters)
                        .map(parameters -> parameters.get(TRINO_QUERY_ID_NAME))
                        .orElse(null);
                if (expectedQueryId.equals(existingQueryId)) {
                    return;
                }
            }
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }

        if (database.getLocation().isPresent()) {
            Location location = Location.of(database.getLocation().get());
            try {
                fileSystem.createDirectory(location);
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to create directory: " + location, e);
            }
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            metastore.dropDatabase(databaseName);
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        try {
            metastore.renameDatabase(databaseName, newDatabaseName);
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (EntityAlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(newDatabaseName);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            metastore.createTable(toRestTable(table));
        }
        catch (EntityAlreadyExistsException e) {
            // Do not throw TableAlreadyExistsException if this query has already created the table.
            // This may happen when an actually successful metastore create call is retried
            // after a timeout on our side.
            String expectedQueryId = table.getParameters().get(TRINO_QUERY_ID_NAME);
            if (expectedQueryId != null) {
                String existingQueryId = getTable(table.getDatabaseName(), table.getTableName())
                        .map(Table::getParameters)
                        .map(parameters -> parameters.get(TRINO_QUERY_ID_NAME))
                        .orElse(null);
                if (expectedQueryId.equals(existingQueryId)) {
                    return;
                }
            }
            throw new TableAlreadyExistsException(table.getSchemaTableName());
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(table.getDatabaseName());
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Optional<String> deleteLocation = Optional.empty();
        if (deleteData) {
            io.starburst.stargate.metastore.client.Table table;
            try {
                table = metastore.getTable(databaseName, tableName)
                        .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
            }
            catch (MetastoreException e) {
                throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
            }
            catch (RuntimeException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
            }
            if (table.tableType().equals(MANAGED_TABLE.name())) {
                deleteLocation = Optional.ofNullable(emptyToNull(table.storage().location()));
            }
        }

        try {
            metastore.dropTable(databaseName, tableName);
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }

        deleteLocation.ifPresent(location -> deleteDir(Location.of(location)));
    }

    private void deleteDir(Location path)
    {
        try {
            fileSystem.deleteDirectory(path);
        }
        catch (Exception e) {
            // don't fail if unable to delete path
            log.warn(e, "Failed to delete path: %s", path);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        if (!newTable.getDatabaseName().equals(databaseName) || !newTable.getTableName().equals(tableName)) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Replacement table must have same name");
        }
        try {
            // The server-side metastore implementation has a special check for replaceTable on Iceberg that previous_metadata_location must be equal
            // to currently stored metadata_location, so it's an atomic compare-or-replace.
            metastore.replaceTable(toRestTable(newTable));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(newTable.getSchemaTableName());
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            metastore.renameTable(databaseName, tableName, newDatabaseName, newTableName);
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (EntityAlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(newDatabaseName, newTableName));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        try {
            metastore.commentTable(databaseName, tableName, comment);
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        try {
            metastore.commentColumn(databaseName, tableName, columnName, comment);
        }
        catch (EntityNotFoundException e) {
            throw new ColumnNotFoundException(new SchemaTableName(databaseName, tableName), columnName);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        try {
            metastore.addColumn(databaseName, tableName, new Column(columnName, columnType.toString(), Optional.ofNullable(columnComment), ImmutableMap.of()));
        }
        catch (EntityNotFoundException e) {
            throw new ColumnNotFoundException(new SchemaTableName(databaseName, tableName), columnName);
        }
        catch (EntityAlreadyExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, "Column already exists: " + columnName);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        try {
            metastore.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
        }
        catch (EntityNotFoundException e) {
            throw new ColumnNotFoundException(new SchemaTableName(databaseName, tableName), oldColumnName);
        }
        catch (EntityAlreadyExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, "Column already exists: " + newColumnName);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        try {
            metastore.dropColumn(databaseName, tableName, columnName);
        }
        catch (EntityNotFoundException e) {
            throw new ColumnNotFoundException(new SchemaTableName(databaseName, tableName), columnName);
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        try {
            return metastore.getPartition(table.getDatabaseName(), table.getTableName(), new PartitionName(partitionValues))
                    .map(RestMetastoreUtils::fromRestPartition);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        try {
            return Optional.of(metastore.getPartitionNames(databaseName, tableName).stream()
                    .map(PartitionName::partitionValues)
                    .map(partitionValues -> toPartitionName(columnNames, partitionValues))
                    .collect(toImmutableList()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        List<PartitionName> names = partitionNames.stream()
                .map(HivePartitionManager::extractPartitionValues)
                .map(PartitionName::new)
                .collect(toImmutableList());
        try {
            return metastore.getPartitionsByNames(table.getDatabaseName(), table.getTableName(), names).entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> makePartitionName(table.getPartitionColumns(), entry.getKey().partitionValues()),
                            entry -> entry.getValue().map(RestMetastoreUtils::fromRestPartition)));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        if (!partitions.stream().map(PartitionWithStatistics::getPartition).allMatch(partition -> partition.getDatabaseName().equals(databaseName) && partition.getTableName().equals(tableName))) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Partition names must match table name");
        }
        try {
            metastore.addPartitions(partitions.stream()
                    .map(RestMetastoreUtils::toRestPartitionWithStatistics)
                    .collect(toImmutableList()));
        }
        catch (EntityAlreadyExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, "Partition already exists");
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        Optional<String> deleteLocation = Optional.empty();
        try {
            if (deleteData) {
                io.starburst.stargate.metastore.client.Table table = metastore.getTable(databaseName, tableName)
                        .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
                io.starburst.stargate.metastore.client.Partition partition = metastore.getPartition(databaseName, tableName, new PartitionName(partitionValues))
                        .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues));
                if (table.tableType().equals(MANAGED_TABLE.name())) {
                    deleteLocation = Optional.ofNullable(emptyToNull(partition.storage().location()));
                }
            }

            try {
                metastore.dropPartition(databaseName, tableName, new PartitionName(partitionValues));
            }
            catch (EntityNotFoundException e) {
                throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues);
            }
            catch (MetastoreException e) {
                throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }

        deleteLocation.ifPresent(location -> deleteDir(Location.of(location)));
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        if (!partition.getPartition().getDatabaseName().equals(databaseName) || !partition.getPartition().getTableName().equals(tableName)) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Partition names must match table name");
        }
        try {
            metastore.alterPartition(toRestPartitionWithStatistics(partition));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getPartition().getValues());
        }
        catch (MetastoreException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, requireNonNullElse(e.getMessage(), e).toString(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error accessing REST Metastore: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    //
    // Security is handled by system security plugins
    //

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setDatabaseOwner is not supported by REST metastore");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTableOwner is not supported by REST metastore");
    }

    @Override
    public void flushTableCache(String databaseName, String tableName)
    {
        // no caching, so nothing to do
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "createRole is not supported by REST metastore");
    }

    @Override
    public void dropRole(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropRole is not supported by REST metastore");
    }

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of(PUBLIC_ROLE_NAME);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantRoles is not supported by REST metastore");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeRoles is not supported by REST metastore");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        if (principal.getType() == USER) {
            return ImmutableSet.of(new RoleGrant(principal.toTrinoPrincipal(), PUBLIC_ROLE_NAME, false));
        }
        return ImmutableSet.of();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantTablePrivileges is not supported by REST metastore");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeTablePrivileges is not supported by REST metastore");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        // this is required for insert overwrite to work
        return ImmutableSet.of();
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        throw new TrinoException(NOT_SUPPORTED, "functionExists is not supported for REST metastore");
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        throw new TrinoException(NOT_SUPPORTED, "getFunctions is not supported for REST metastore");
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        throw new TrinoException(NOT_SUPPORTED, "getFunctions is not supported for REST metastore");
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new TrinoException(NOT_SUPPORTED, "createFunction is not supported for REST metastore");
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new TrinoException(NOT_SUPPORTED, "replaceFunction is not supported for REST metastore");
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropFunction is not supported for REST metastore");
    }
}
