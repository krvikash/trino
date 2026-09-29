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

import io.starburst.stargate.icehouse.catalog.AbstractTableOperations;
import io.starburst.stargate.icehouse.exception.InvalidMetadataException;
import io.starburst.stargate.icehouse.exception.TableNotFoundException;
import io.starburst.stargate.icehouse.exception.TerminalIcehouseCatalogException;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.ValidationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_TABLE_COMMENT_CACHE_PREVENTED;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_TABLE_METADATA_INFO_VALID_FOR;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.glueColumns;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class GlueTableOperations
        extends AbstractTableOperations
{
    // Limit per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_TableInput.html#Glue-Type-TableInput-Parameters)
    private static final int GLUE_TABLE_PARAMETER_LENGTH_LIMIT = 512000;

    private final GlueClient glueClient;
    private final boolean skipArchive;
    private final TypeManager typeManager;

    @Nullable
    private String glueVersionId;

    public GlueTableOperations(
            GlueClient glueClient,
            FileIO fileIO,
            String schemaName,
            String tableName,
            boolean skipArchive,
            TypeManager typeManager)
    {
        super(fileIO, schemaName, tableName);
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.skipArchive = skipArchive;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected String getMetadataLocation()
    {
        Table table = getGlueTable();
        glueVersionId = table.versionId();

        Map<String, String> parameters = table.parameters();
        String metadataLocation = parameters.get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new InvalidMetadataException(format("Table is missing [%s] property: %s, %s", METADATA_LOCATION_PROP, schemaName, tableName));
        }

        return metadataLocation;
    }

    // Copied from GlueIcebergTableOperations.commitToExistingTable() with modifications to fit into our implementation of AbstractTableOperations
    @Override
    protected void commitMetadata(TableMetadata newMetadata, String newMetadataLocation, String previousMetadataLocation)
    {
        Table table = getGlueTable();

        // Fail fast if the table was concurrently modified during manifest work.
        if (!Objects.equals(table.versionId(), glueVersionId)) {
            throw new CommitFailedException(
                    "Glue table version changed during commit (expected=%s, found=%s): %s.%s",
                    glueVersionId,
                    table.versionId(),
                    schemaName,
                    tableName);
        }

        // Start from existing parameters to preserve all table metadata
        Map<String, String> parameters = new HashMap<>(table.parameters());
        parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH));
        parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);
        parameters.put(PREVIOUS_METADATA_LOCATION_PROP, previousMetadataLocation);
        parameters.remove(TRINO_TABLE_METADATA_INFO_VALID_FOR); // no longer valid until column caching succeeds

        StorageDescriptor.Builder storageDescriptor = table.storageDescriptor() != null
                ? table.storageDescriptor().toBuilder()
                : StorageDescriptor.builder();

        Optional<List<Column>> glueColumns = glueColumns(typeManager, newMetadata);
        glueColumns.ifPresent(columns -> storageDescriptor.columns(columns));

        String comment = newMetadata.properties().get(TABLE_COMMENT);
        if (comment != null) {
            if (comment.length() <= GLUE_TABLE_PARAMETER_LENGTH_LIMIT) {
                parameters.put(TABLE_COMMENT, comment);
                parameters.remove(TRINO_TABLE_COMMENT_CACHE_PREVENTED);
            }
            else {
                parameters.remove(TABLE_COMMENT);
                parameters.put(TRINO_TABLE_COMMENT_CACHE_PREVENTED, "true");
            }
        }
        else {
            parameters.remove(TABLE_COMMENT);
            parameters.remove(TRINO_TABLE_COMMENT_CACHE_PREVENTED);
        }

        if (glueColumns.isPresent()) {
            parameters.put(TRINO_TABLE_METADATA_INFO_VALID_FOR, newMetadataLocation);
        }

        TableInput.Builder tableInput = TableInput.builder()
                .name(tableName)
                // Iceberg does not distinguish managed and external tables, all tables are treated the same and marked as EXTERNAL
                .tableType("EXTERNAL_TABLE")
                .storageDescriptor(storageDescriptor.build())
                .parameters(parameters);

        try {
            glueClient.updateTable(x -> x
                    .databaseName(schemaName)
                    .tableInput(tableInput.build())
                    .versionId(glueVersionId)
                    .skipArchive(skipArchive));
        }
        catch (ConcurrentModificationException e) {
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            throw new CommitFailedException(e, "Failed to commit to Glue table: %s.%s", schemaName, tableName);
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(format("Cannot commit table update for glue table %s.%s", schemaName, tableName));
        }
        catch (InvalidInputException | ResourceNumberLimitExceededException | ValidationException e) {
            // ResourceNumberLimitExceededException requires manual Glue version cleanup — non-retryable in commit context
            throw new TerminalIcehouseCatalogException(
                    format("Cannot commit table update for Glue table %s.%s (versionId=%s)", schemaName, tableName, glueVersionId),
                    e);
        }
        catch (RuntimeException e) {
            // Cannot determine whether the `updateTable` operation was successful,
            // regardless of the exception thrown (e.g. : timeout exception) or it actually failed
            throw new CommitStateUnknownException(e);
        }
    }

    public Table getGlueTable()
    {
        try {
            return glueClient.getTable(x -> x
                    .databaseName(schemaName)
                    .name(tableName)).table();
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(format("Glue table not found: %s.%s", schemaName, tableName));
        }
        catch (SdkException e) {
            throw new RuntimeException(format("Failed to get Glue table: %s.%s", schemaName, tableName), e);
        }
    }
}
