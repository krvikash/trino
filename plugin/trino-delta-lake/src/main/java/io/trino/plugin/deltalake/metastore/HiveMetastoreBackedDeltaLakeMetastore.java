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
package io.trino.plugin.deltalake.metastore;

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_CREDENTIALS_ENABLED;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_CREDENTIALS_EXPIRE_AT;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_GCS_OAUTH_TOKEN;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_ACCESS_KEY;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_SECRET_KEY;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_SESSION_TOKEN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreBackedDeltaLakeMetastore
        implements DeltaLakeMetastore
{
    public static final String TABLE_PROVIDER_PROPERTY = "spark.sql.sources.provider";
    public static final String TABLE_PROVIDER_VALUE = "DELTA";

    private final HiveMetastore delegate;

    public HiveMetastoreBackedDeltaLakeMetastore(HiveMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<TableInfo> getAllTables(String databaseName)
    {
        return delegate.getTables(databaseName);
    }

    @Override
    public Optional<Table> getRawMetastoreTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName);
    }

    @Override
    public Optional<DeltaMetastoreTable> getTable(String databaseName, String tableName)
    {
        return getRawMetastoreTable(databaseName, tableName)
                .map(HiveMetastoreBackedDeltaLakeMetastore::convertToDeltaMetastoreTable);
    }

    public static void verifyDeltaLakeTable(Table table)
    {
        if (isSomeKindOfAView(table)) {
            // Looks like a view, so not a table
            throw new NotADeltaLakeTableException(table.getDatabaseName(), table.getTableName());
        }
        if (!TABLE_PROVIDER_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_PROVIDER_PROPERTY))) {
            throw new NotADeltaLakeTableException(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        delegate.createDatabase(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        delegate.dropDatabase(databaseName, deleteData);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void replaceTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.replaceTable(table.getDatabaseName(), table.getTableName(), table, principalPrivileges, ImmutableMap.of());
    }

    @Override
    public void dropTable(SchemaTableName schemaTableName, String tableLocation, boolean deleteData)
    {
        delegate.dropTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), deleteData);
    }

    @Override
    public void renameTable(SchemaTableName from, SchemaTableName to)
    {
        delegate.renameTable(from.getSchemaName(), from.getTableName(), to.getSchemaName(), to.getTableName());
    }

    public static DeltaMetastoreTable convertToDeltaMetastoreTable(Table table)
    {
        verifyDeltaLakeTable(table);
        return new DeltaMetastoreTable(
                new SchemaTableName(table.getDatabaseName(), table.getTableName()),
                table.getTableType().equals(MANAGED_TABLE.name()),
                catalogOwned(table),
                getTableLocation(table),
                getTableId(table),
                getVendedCredentials(table));
    }

    private static boolean catalogOwned(Table table)
    {
        if (table.getParameters() == null) {
            return false;
        }

        if (!"supported".equals(table.getParameters().get("delta.feature.catalogOwned-preview"))) {
            return false;
        }

        checkState("true".equals(table.getParameters().get("delta.enableInCommitTimestamps")), "Catalog owned table must enable in-commit timestamps");
        checkState(table.getTableType().equals(MANAGED_TABLE.name()), "Catalog owned table must be managed type table");
        checkState(table.getParameters().containsKey("ucTableId"), "Catalog owned table must have a table id");
        return true;
    }

    private static Optional<VendedCredentials> getVendedCredentials(Table table)
    {
        Map<String, String> parameters = table.getParameters();

        if (!parameters.containsKey(VENDED_CREDENTIALS_ENABLED)) {
            return Optional.empty();
        }

        Optional<String> tableId = getTableId(table);

        Instant expireAt = Instant.MAX;
        if (parameters.containsKey(VENDED_CREDENTIALS_EXPIRE_AT)) {
            expireAt = Instant.ofEpochMilli(Long.parseLong(parameters.get(VENDED_CREDENTIALS_EXPIRE_AT)));
        }

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (parameters.containsKey(VENDED_S3_ACCESS_KEY)) {
            builder.put(VENDED_S3_ACCESS_KEY, parameters.get(VENDED_S3_ACCESS_KEY));
            builder.put(VENDED_S3_SECRET_KEY, parameters.get(VENDED_S3_SECRET_KEY));
            builder.put(VENDED_S3_SESSION_TOKEN, parameters.get(VENDED_S3_SESSION_TOKEN));
        }

        if (parameters.containsKey(VENDED_GCS_OAUTH_TOKEN)) {
            builder.put(VENDED_GCS_OAUTH_TOKEN, parameters.get(VENDED_GCS_OAUTH_TOKEN));
        }

        return Optional.of(new VendedCredentials(tableId, expireAt, builder.buildOrThrow()));
    }

    public static String getTableLocation(Table table)
    {
        Map<String, String> serdeParameters = table.getStorage().getSerdeParameters();
        String location = serdeParameters.get(PATH_PROPERTY);
        if (location == null) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("No %s property defined for table: %s", PATH_PROPERTY, table));
        }
        return location;
    }

    private static Optional<String> getTableId(Table table)
    {
        return Optional.ofNullable(table.getParameters().get("ucTableId"));
    }
}
