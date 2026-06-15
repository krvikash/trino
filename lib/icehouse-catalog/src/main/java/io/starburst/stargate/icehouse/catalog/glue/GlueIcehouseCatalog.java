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
package io.starburst.stargate.icehouse.catalog.glue;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalog;
import io.starburst.stargate.icehouse.exception.RetryableIcehouseCatalogException;
import io.starburst.stargate.icehouse.exception.TableNotFoundException;
import io.starburst.stargate.icehouse.exception.TerminalIcehouseCatalogException;
import io.starburst.stargate.icehouse.spi.TableIdentifier;
import io.starburst.stargate.icehouse.spi.maintenance.IcebergTableKey;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hive.metastore.glue.GlueConverter.getTableTypeNullable;
import static java.util.Objects.requireNonNull;

/**
 * AWS Glue implementation of {@link IcehouseCatalog}.
 *
 * <p>Owns a single {@link GlueClient} for the catalog's lifetime. List operations
 * go directly against the client; {@link #loadTable} constructs a per-table
 * {@link GlueTableOperations} that borrows the same client. The catalog is the
 * sole owner — closing a table-ops only flips its closed flag, and the catalog's
 * {@link #close()} closes the underlying GlueClient.
 *
 * <p>Listing uses a two-layer allowlist: {@code EXTERNAL_TABLE} tableType plus
 * the {@code table_type=ICEBERG} parameter.
 */
public final class GlueIcehouseCatalog
        implements IcehouseCatalog
{
    private static final Logger log = Logger.get(GlueIcehouseCatalog.class);
    private static final String TABLE_TYPE_PROP = "table_type";
    private static final String TABLE_TYPE_ICEBERG = "ICEBERG";

    private final GlueClient glueClient;
    private final FileIO fileIo;
    private final boolean skipArchive;
    private final TypeManager typeManager;

    public GlueIcehouseCatalog(
            GlueClient glueClient,
            FileIO fileIo,
            boolean skipArchive,
            TypeManager typeManager)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.fileIo = requireNonNull(fileIo, "fileIo is null");
        this.skipArchive = skipArchive;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemas()
    {
        try {
            ImmutableList.Builder<String> schemas = ImmutableList.builder();
            String nextToken = null;
            do {
                GetDatabasesResponse response = glueClient.getDatabases(GetDatabasesRequest.builder()
                        .nextToken(nextToken)
                        .build());
                for (Database database : response.databaseList()) {
                    schemas.add(database.name());
                }
                nextToken = response.nextToken();
            }
            while (nextToken != null);
            return schemas.build();
        }
        catch (GlueException e) {
            throw classifyGlueException("listSchemas", e);
        }
    }

    @Override
    public List<String> listTables(String schema)
    {
        try {
            ImmutableList.Builder<String> tables = ImmutableList.builder();
            String nextToken = null;
            do {
                GetTablesResponse response = glueClient.getTables(GetTablesRequest.builder()
                        .databaseName(schema)
                        .nextToken(nextToken)
                        .build());
                for (software.amazon.awssdk.services.glue.model.Table table : response.tableList()) {
                    // Two-layer allowlist: only Glue EXTERNAL_TABLE entries that carry the
                    // table_type=ICEBERG parameter.
                    if (!"EXTERNAL_TABLE".equals(getTableTypeNullable(table))) {
                        continue;
                    }
                    if (isIcebergTable(table.parameters())) {
                        tables.add(table.name());
                    }
                }
                nextToken = response.nextToken();
            }
            while (nextToken != null);
            return tables.build();
        }
        catch (GlueException e) {
            throw classifyGlueException("listTables(schema=%s)".formatted(schema), e);
        }
    }

    @Override
    public Table loadTable(TableIdentifier tableId)
    {
        IcebergTableKey key = IcebergTableKey.specialize(tableId);
        GlueTableOperations operations = new GlueTableOperations(
                glueClient,
                fileIo,
                key.schemaName(),
                key.tableName(),
                skipArchive,
                typeManager);
        return new BaseTable(operations, "%s.%s".formatted(key.schemaName(), key.tableName()));
    }

    @Override
    public String metadataLocation(TableIdentifier tableId)
    {
        IcebergTableKey key = IcebergTableKey.specialize(tableId);
        try {
            software.amazon.awssdk.services.glue.model.Table glueTable = glueClient.getTable(x -> x
                    .databaseName(key.schemaName())
                    .name(key.tableName())).table();
            Map<String, String> parameters = glueTable.parameters();
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
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(key.schemaName(), key.tableName());
        }
        catch (GlueException e) {
            throw classifyGlueException("metadataLocation(%s.%s)".formatted(key.schemaName(), key.tableName()), e);
        }
    }

    @Override
    public void close()
    {
        try {
            glueClient.close();
        }
        catch (Exception e) {
            log.warn(e, "Failed to close Glue client");
        }
    }

    private static boolean isIcebergTable(Map<String, String> parameters)
    {
        if (parameters == null) {
            return false;
        }
        return TABLE_TYPE_ICEBERG.equalsIgnoreCase(parameters.get(TABLE_TYPE_PROP));
    }

    /**
     * Default to retryable for unknown Glue exceptions — conservative, since
     * unknown errors are safer to retry than to permanently fail.
     */
    private static RuntimeException classifyGlueException(String operation, GlueException e)
    {
        if (e instanceof AccessDeniedException || e instanceof EntityNotFoundException || e instanceof InvalidInputException) {
            return new TerminalIcehouseCatalogException(
                    "Non retryable error accessing glue metastore during %s: %s".formatted(operation, e.getMessage()), e);
        }
        return new RetryableIcehouseCatalogException(
                "Transient error accessing glue metastore during %s: %s".formatted(operation, e.getMessage()), e);
    }
}
