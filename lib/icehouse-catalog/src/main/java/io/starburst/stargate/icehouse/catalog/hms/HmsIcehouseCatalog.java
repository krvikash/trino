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
package io.starburst.stargate.icehouse.catalog.hms;

import com.google.common.collect.ImmutableList;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalog;
import io.starburst.stargate.icehouse.exception.IcehouseCatalogException;
import io.starburst.stargate.icehouse.exception.RetryableIcehouseCatalogException;
import io.starburst.stargate.icehouse.exception.TableNotFoundException;
import io.starburst.stargate.icehouse.exception.TerminalIcehouseCatalogException;
import io.starburst.stargate.icehouse.spi.TableIdentifier;
import io.starburst.stargate.icehouse.spi.maintenance.IcebergTableKey;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.TableInfo;
import io.trino.metastore.TableInfo.ExtendedRelationType;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Hive Metastore implementation of {@link IcehouseCatalog}.
 *
 * <p>Owns the {@link HiveMetastore} client for the catalog's lifetime; uses the
 * caller-provided {@link FileIO} (derived from the {@code TrinoFileSystemFactory})
 * for any {@link Table} returned by {@link #loadTable}. Listing uses a two-layer
 * allowlist: {@link ExtendedRelationType#TABLE} plus the
 * {@code table_type=ICEBERG} parameter.
 */
public final class HmsIcehouseCatalog
        implements IcehouseCatalog
{
    private static final String TABLE_TYPE_PROP = "table_type";
    private static final String TABLE_TYPE_ICEBERG = "ICEBERG";

    private final HiveMetastore metastore;
    private final FileIO fileIo;

    public HmsIcehouseCatalog(HiveMetastore metastore, FileIO fileIo)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileIo = requireNonNull(fileIo, "fileIo is null");
    }

    @Override
    public List<String> listSchemas()
    {
        try {
            return metastore.getAllDatabases();
        }
        catch (IcehouseCatalogException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new RetryableIcehouseCatalogException("HMS error during listSchemas: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTables(String schema)
    {
        try {
            ImmutableList.Builder<String> icebergTables = ImmutableList.builder();
            for (TableInfo tableInfo : metastore.getTables(schema)) {
                // Two-layer allowlist: only TABLE (skip views, materialized views, other
                // relation types) and only those marked with table_type=ICEBERG in their parameters.
                if (tableInfo.extendedRelationType() != ExtendedRelationType.TABLE) {
                    continue;
                }
                String tableName = tableInfo.tableName().getTableName();
                Optional<io.trino.metastore.Table> table = metastore.getTable(schema, tableName);
                if (table.isPresent() && isIcebergTable(table.get().getParameters())) {
                    icebergTables.add(tableName);
                }
            }
            return icebergTables.build();
        }
        catch (IcehouseCatalogException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new RetryableIcehouseCatalogException(
                    "HMS error during listTables(schema=%s): %s".formatted(schema, e.getMessage()), e);
        }
    }

    @Override
    public Table loadTable(TableIdentifier tableId)
    {
        IcebergTableKey key = IcebergTableKey.specialize(tableId);
        HmsTableOperations operations = new HmsTableOperations(
                metastore,
                fileIo,
                key.schemaName(),
                key.tableName());
        return new BaseTable(operations, "%s.%s".formatted(key.schemaName(), key.tableName()));
    }

    @Override
    public String metadataLocation(TableIdentifier tableId)
    {
        IcebergTableKey key = IcebergTableKey.specialize(tableId);
        try {
            io.trino.metastore.Table table = metastore.getTable(key.schemaName(), key.tableName())
                    .orElseThrow(() -> new TableNotFoundException(key.schemaName(), key.tableName()));
            Map<String, String> parameters = table.getParameters();
            String missingPropertyError = "Missing required property %s on %s.%s".formatted(
                    BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                    key.schemaName(),
                    key.tableName());
            if (parameters == null) {
                throw new TerminalIcehouseCatalogException(missingPropertyError);
            }
            String metadataLocation = parameters.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
            if (metadataLocation == null) {
                throw new TerminalIcehouseCatalogException(missingPropertyError);
            }
            return metadataLocation;
        }
        catch (IcehouseCatalogException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new RetryableIcehouseCatalogException(
                    "HMS error during metadataLocation(%s.%s): %s".formatted(
                            key.schemaName(), key.tableName(), e.getMessage()),
                    e);
        }
    }

    @Override
    public void close()
    {
        // HiveMetastore implementations used here (TracingHiveMetastore wrapping
        // BridgingHiveMetastore wrapping ThriftHiveMetastore) hold thrift connection
        // resources via factories that close automatically when garbage-collected.
        // Nothing explicit to close on this side; left as a hook for future impls.
    }

    private static boolean isIcebergTable(Map<String, String> parameters)
    {
        if (parameters == null) {
            return false;
        }
        return TABLE_TYPE_ICEBERG.equalsIgnoreCase(parameters.get(TABLE_TYPE_PROP));
    }
}
