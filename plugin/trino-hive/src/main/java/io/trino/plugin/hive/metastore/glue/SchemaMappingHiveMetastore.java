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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

/**
 * Routes prefixed schema names to per-rule metastore delegates, exposing each delegate's databases under the prefix.
 */
public class SchemaMappingHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastore defaultDelegate;
    private final Map<String, HiveMetastore> delegatesByPrefix;

    public SchemaMappingHiveMetastore(HiveMetastore defaultDelegate, Map<String, HiveMetastore> delegatesByPrefix)
    {
        this.defaultDelegate = requireNonNull(defaultDelegate, "defaultDelegate is null");
        this.delegatesByPrefix = ImmutableMap.copyOf(delegatesByPrefix);
    }

    /**
     * Resolves against the longest matching prefix, matching how a view body is rewritten.
     */
    private MappedDatabase resolve(String databaseName)
    {
        return delegatesByPrefix.entrySet().stream()
                .filter(entry -> databaseName.startsWith(entry.getKey()))
                .max(Map.Entry.comparingByKey(comparingInt(String::length)))
                .map(entry -> new MappedDatabase(entry.getValue(), databaseName.substring(entry.getKey().length())))
                .orElseGet(() -> new MappedDatabase(defaultDelegate, databaseName));
    }

    private MappedDatabase resolve(Table table)
    {
        return resolve(table.getDatabaseName());
    }

    private static Table withDatabaseName(Table table, String databaseName)
    {
        if (table.getDatabaseName().equals(databaseName)) {
            return table;
        }
        return Table.builder(table).setDatabaseName(databaseName).build();
    }

    private static Partition withDatabaseName(Partition partition, String databaseName)
    {
        if (partition.getDatabaseName().equals(databaseName)) {
            return partition;
        }
        return Partition.builder(partition).setDatabaseName(databaseName).build();
    }

    private static PartitionWithStatistics withDatabaseName(PartitionWithStatistics partition, String databaseName)
    {
        return new PartitionWithStatistics(
                withDatabaseName(partition.getPartition(), databaseName),
                partition.getPartitionName(),
                partition.getStatistics());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getDatabase(mappedDatabase.targetDatabaseName())
                .map(database -> Database.builder(database).setDatabaseName(mappedDatabase.targetDatabaseName()).build());
    }

    @Override
    public List<String> getAllDatabases()
    {
        return Stream.concat(
                        defaultDelegate.getAllDatabases().stream(),
                        delegatesByPrefix.entrySet().stream()
                                .flatMap(entry -> entry.getValue().getAllDatabases().stream()
                                        .map(name -> entry.getKey() + name)))
                .collect(toImmutableList());
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getTable(mappedDatabase.targetDatabaseName(), tableName);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getTableColumnStatistics(mappedDatabase.targetDatabaseName(), tableName, columnNames);
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getPartitionColumnStatistics(mappedDatabase.targetDatabaseName(), tableName, partitionNames, columnNames);
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return defaultDelegate.useSparkTableStatistics();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().updateTableStatistics(mappedDatabase.targetDatabaseName(), tableName, acidWriteId, mode, statisticsUpdate);
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        MappedDatabase mappedDatabase = resolve(table);
        mappedDatabase.delegate().updatePartitionStatistics(withDatabaseName(table, mappedDatabase.targetDatabaseName()), mode, partitionUpdates);
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getTables(mappedDatabase.targetDatabaseName()).stream()
                .map(tableInfo -> new TableInfo(
                        new SchemaTableName(databaseName, tableInfo.tableName().getTableName()),
                        tableInfo.extendedRelationType()))
                .collect(toImmutableList());
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getTableNamesWithParameters(mappedDatabase.targetDatabaseName(), parameterKey, parameterValues);
    }

    @Override
    public Optional<Iterator<Table>> streamTables(ConnectorSession session, String databaseName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().streamTables(session, mappedDatabase.targetDatabaseName());
    }

    @Override
    public void createDatabase(Database database)
    {
        MappedDatabase mappedDatabase = resolve(database.getDatabaseName());
        mappedDatabase.delegate().createDatabase(Database.builder(database).setDatabaseName(mappedDatabase.targetDatabaseName()).build());
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().dropDatabase(mappedDatabase.targetDatabaseName(), deleteData);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        MappedDatabase from = resolve(databaseName);
        MappedDatabase to = resolve(newDatabaseName);
        if (from.delegate() != to.delegate()) {
            throw new TrinoException(NOT_SUPPORTED, "Rename across schema mapping targets is not supported");
        }
        from.delegate().renameDatabase(from.targetDatabaseName(), to.targetDatabaseName());
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().setDatabaseOwner(mappedDatabase.targetDatabaseName(), principal);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        MappedDatabase mappedDatabase = resolve(table);
        mappedDatabase.delegate().createTable(withDatabaseName(table, mappedDatabase.targetDatabaseName()), principalPrivileges);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().dropTable(mappedDatabase.targetDatabaseName(), tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().replaceTable(mappedDatabase.targetDatabaseName(), tableName, withDatabaseName(newTable, mappedDatabase.targetDatabaseName()), principalPrivileges, environmentContext);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        MappedDatabase from = resolve(databaseName);
        MappedDatabase to = resolve(newDatabaseName);
        if (from.delegate() != to.delegate()) {
            throw new TrinoException(NOT_SUPPORTED, "Rename across schema mapping targets is not supported");
        }
        from.delegate().renameTable(from.targetDatabaseName(), tableName, to.targetDatabaseName(), newTableName);
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().commentTable(mappedDatabase.targetDatabaseName(), tableName, comment);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().setTableOwner(mappedDatabase.targetDatabaseName(), tableName, principal);
    }

    @Override
    public void flushTableCache(String databaseName, String tableName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().flushTableCache(mappedDatabase.targetDatabaseName(), tableName);
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().commentColumn(mappedDatabase.targetDatabaseName(), tableName, columnName, comment);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().addColumn(mappedDatabase.targetDatabaseName(), tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().renameColumn(mappedDatabase.targetDatabaseName(), tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().dropColumn(mappedDatabase.targetDatabaseName(), tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        MappedDatabase mappedDatabase = resolve(table);
        return mappedDatabase.delegate().getPartition(withDatabaseName(table, mappedDatabase.targetDatabaseName()), partitionValues);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getPartitionNamesByFilter(mappedDatabase.targetDatabaseName(), tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        MappedDatabase mappedDatabase = resolve(table);
        return mappedDatabase.delegate().getPartitionsByNames(withDatabaseName(table, mappedDatabase.targetDatabaseName()), partitionNames);
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().addPartitions(
                mappedDatabase.targetDatabaseName(),
                tableName,
                partitions.stream()
                        .map(partition -> withDatabaseName(partition, mappedDatabase.targetDatabaseName()))
                        .collect(toImmutableList()));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().dropPartition(mappedDatabase.targetDatabaseName(), tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().alterPartition(mappedDatabase.targetDatabaseName(), tableName, withDatabaseName(partition, mappedDatabase.targetDatabaseName()));
    }

    @Override
    public void createRole(String role, String grantor)
    {
        defaultDelegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        defaultDelegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        return defaultDelegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        defaultDelegate.grantRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        defaultDelegate.revokeRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return defaultDelegate.listRoleGrants(principal);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().grantTablePrivileges(mappedDatabase.targetDatabaseName(), tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().revokeTablePrivileges(mappedDatabase.targetDatabaseName(), tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().listTablePrivileges(mappedDatabase.targetDatabaseName(), tableName, tableOwner, principal);
    }

    @Override
    public void checkSupportsTransactions()
    {
        defaultDelegate.checkSupportsTransactions();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return defaultDelegate.openTransaction(transactionOwner);
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        defaultDelegate.commitTransaction(transactionId);
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        defaultDelegate.abortTransaction(transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        defaultDelegate.sendTransactionHeartbeat(transactionId);
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        defaultDelegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return defaultDelegate.getValidWriteIds(tables, currentTransactionId);
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return defaultDelegate.getConfigValue(name);
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        MappedDatabase mappedDatabase = resolve(dbName);
        return mappedDatabase.delegate().allocateWriteId(mappedDatabase.targetDatabaseName(), tableName, transactionId);
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, AcidOperation operation, boolean isDynamicPartitionWrite)
    {
        MappedDatabase mappedDatabase = resolve(dbName);
        mappedDatabase.delegate().acquireTableWriteLock(transactionOwner, queryId, transactionId, mappedDatabase.targetDatabaseName(), tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        MappedDatabase mappedDatabase = resolve(dbName);
        mappedDatabase.delegate().updateTableWriteId(mappedDatabase.targetDatabaseName(), tableName, transactionId, writeId, rowCountChange);
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        MappedDatabase mappedDatabase = resolve(dbName);
        mappedDatabase.delegate().addDynamicPartitions(mappedDatabase.targetDatabaseName(), tableName, partitionNames, transactionId, writeId, operation);
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        MappedDatabase mappedDatabase = resolve(table);
        mappedDatabase.delegate().alterTransactionalTable(withDatabaseName(table, mappedDatabase.targetDatabaseName()), transactionId, writeId, principalPrivileges);
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().functionExists(mappedDatabase.targetDatabaseName(), functionName, signatureToken);
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getAllFunctions(mappedDatabase.targetDatabaseName());
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        return mappedDatabase.delegate().getFunctions(mappedDatabase.targetDatabaseName(), functionName);
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().createFunction(mappedDatabase.targetDatabaseName(), functionName, function);
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().replaceFunction(mappedDatabase.targetDatabaseName(), functionName, function);
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        MappedDatabase mappedDatabase = resolve(databaseName);
        mappedDatabase.delegate().dropFunction(mappedDatabase.targetDatabaseName(), functionName, signatureToken);
    }

    @Override
    public Metrics getMetrics()
    {
        Metrics.Accumulator accumulator = Metrics.accumulator().add(defaultDelegate.getMetrics());
        delegatesByPrefix.values().forEach(delegate -> accumulator.add(delegate.getMetrics()));
        return accumulator.get();
    }

    private record MappedDatabase(HiveMetastore delegate, String targetDatabaseName) {}
}
