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
import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.icehouse.catalog.AbstractTableOperations;
import io.starburst.stargate.icehouse.exception.InvalidMetadataException;
import io.starburst.stargate.icehouse.exception.TableNotFoundException;
import io.starburst.stargate.icehouse.exception.TerminalIcehouseCatalogException;
import io.trino.plugin.iceberg.IcebergDefaultValues;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.builderWithExpectedSize;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_DEFAULT_VALUE_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_NOT_NULL_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_TYPE_ID_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_TABLE_COMMENT_CACHE_PREVENTED;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_TABLE_METADATA_INFO_VALID_FOR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public final class GlueTableOperations
        extends AbstractTableOperations
{
    // Limits per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html)
    private static final int GLUE_COLUMN_NAME_LENGTH_LIMIT = 255;
    private static final int GLUE_COLUMN_TYPE_LENGTH_LIMIT = 131072;
    private static final int GLUE_COLUMN_COMMENT_LENGTH_LIMIT = 255;
    private static final int GLUE_COLUMN_PARAMETER_LENGTH_LIMIT = 512000;
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

        Optional<List<Column>> glueColumns = glueColumns(newMetadata);
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
        catch (InvalidInputException | ResourceNumberLimitExceededException e) {
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

    // Copied from cork's GlueIcebergUtil#glueColumns
    private Optional<List<Column>> glueColumns(TableMetadata metadata)
    {
        List<Types.NestedField> icebergColumns = metadata.schema().columns();
        ImmutableList.Builder<Column> glueColumns = builderWithExpectedSize(icebergColumns.size());

        boolean firstColumn = true;
        for (Types.NestedField icebergColumn : icebergColumns) {
            Optional<String> defaultValue = Optional.empty();
            if (icebergColumn.writeDefault() != null) {
                defaultValue = IcebergDefaultValues.formatIcebergDefaultAsSql(icebergColumn.writeDefault(), icebergColumn.type());
            }

            String glueTypeString = toGlueTypeStringLossy(icebergColumn.type());
            if (icebergColumn.name().length() > GLUE_COLUMN_NAME_LENGTH_LIMIT ||
                    requireNonNullElse(icebergColumn.doc(), "").length() > GLUE_COLUMN_COMMENT_LENGTH_LIMIT ||
                    glueTypeString.length() > GLUE_COLUMN_TYPE_LENGTH_LIMIT ||
                    defaultValue.map(String::length).orElse(0) > GLUE_COLUMN_PARAMETER_LENGTH_LIMIT) {
                return Optional.empty();
            }

            String trinoTypeId = TypeConverter.toTrinoType(icebergColumn.type(), typeManager).getTypeId().getId();
            ImmutableMap.Builder<String, String> columnParameters = ImmutableMap.builder();
            if (icebergColumn.isRequired()) {
                columnParameters.put(COLUMN_TRINO_NOT_NULL_PROPERTY, "true");
            }
            if (firstColumn || !glueTypeString.equals(trinoTypeId)) {
                if (trinoTypeId.length() > GLUE_COLUMN_PARAMETER_LENGTH_LIMIT) {
                    return Optional.empty();
                }
                // Store type parameter for some (first) column so that we can later detect whether column parameters weren't erased by something.
                columnParameters.put(COLUMN_TRINO_TYPE_ID_PROPERTY, trinoTypeId);
            }
            defaultValue.ifPresent(value -> columnParameters.put(COLUMN_TRINO_DEFAULT_VALUE_PROPERTY, value));

            glueColumns.add(Column.builder()
                    .name(icebergColumn.name())
                    .type(glueTypeString)
                    .comment(icebergColumn.doc())
                    .parameters(columnParameters.buildOrThrow())
                    .build());

            firstColumn = false;
        }

        return Optional.of(glueColumns.build());
    }

    // Copied from cork's GlueIcebergUtil#toGlueTypeStringLossy
    // (adapted from org.apache.iceberg.aws.glue.IcebergToGlueConverter#toTypeString)
    private static String toGlueTypeStringLossy(Type type)
    {
        return switch (type.typeId()) {
            case BOOLEAN -> "boolean";
            case INTEGER -> "int";
            case LONG -> "bigint";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case DATE -> "date";
            case TIME, STRING, UUID -> "string";
            case TIMESTAMP, TIMESTAMP_NANO -> "timestamp";
            case FIXED, BINARY -> "binary";
            case DECIMAL -> {
                Types.DecimalType decimalType = (Types.DecimalType) type;
                yield format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
            }
            case STRUCT -> {
                Types.StructType structType = type.asStructType();
                String nameToType = structType.fields().stream()
                        .map(f -> format("%s:%s", f.name(), toGlueTypeStringLossy(f.type())))
                        .collect(Collectors.joining(","));
                yield format("struct<%s>", nameToType);
            }
            case LIST -> format("array<%s>", toGlueTypeStringLossy(type.asListType().elementType()));
            case MAP -> {
                Types.MapType mapType = type.asMapType();
                yield format("map<%s,%s>", toGlueTypeStringLossy(mapType.keyType()), toGlueTypeStringLossy(mapType.valueType()));
            }
            default -> "string";
        };
    }
}
