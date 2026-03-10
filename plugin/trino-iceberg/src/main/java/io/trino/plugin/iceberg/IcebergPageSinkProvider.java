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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.starburst.ai.client.ModelClientProvider;
import io.trino.plugin.hive.SortingFileWriterConfig;
import io.trino.plugin.hive.util.SortTempFileFactory;
import io.trino.plugin.iceberg.delete.PositionDeleteFiles;
import io.trino.plugin.iceberg.procedure.IcebergGenerateEmbeddingsHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.TableCredentials;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DeleteFileSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.transformValues;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergSessionProperties.maxPartitionsPerWriter;
import static io.trino.plugin.iceberg.IcebergUtil.contentFileFromJson;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.IcebergUtil.supportsRowLineage;
import static java.util.Objects.requireNonNull;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final IcebergFileSystemFactory fileSystemFactory;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final IcebergPageSourceProviderFactory pageSourceProviderFactory;
    private final DataSize sortingFileWriterBufferSize;
    private final int sortingFileWriterMaxOpenFiles;
    private final Optional<String> sortingFileWriterLocalStagingPath;
    private final SortTempFileFactory sortTempFileFactory;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final ModelClientProvider embeddingClientProvider;

    @Inject
    public IcebergPageSinkProvider(
            IcebergFileSystemFactory fileSystemFactory,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            IcebergPageSourceProviderFactory pageSourceProviderFactory,
            SortingFileWriterConfig sortingFileWriterConfig,
            IcebergConfig icebergConfig,
            SortTempFileFactory sortTempFileFactory,
            TypeManager typeManager,
            PageSorter pageSorter,
            ModelClientProvider embeddingClientProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.sortingFileWriterBufferSize = sortingFileWriterConfig.getWriterSortBufferSize();
        this.sortingFileWriterMaxOpenFiles = sortingFileWriterConfig.getMaxOpenSortFiles();
        this.sortingFileWriterLocalStagingPath = icebergConfig.getSortedWritingLocalStagingPath();
        this.sortTempFileFactory = requireNonNull(sortTempFileFactory, "sortTempFileFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.embeddingClientProvider = requireNonNull(embeddingClientProvider, "embeddingClientProvider is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        IcebergWritableTableHandle tableHandle = (IcebergWritableTableHandle) outputTableHandle;
        return createPageSink(session, tableHandle, extractFileIoProperties(tableCredentials));
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        IcebergWritableTableHandle tableHandle = (IcebergWritableTableHandle) insertTableHandle;
        return createPageSink(session, tableHandle, extractFileIoProperties(tableCredentials));
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, IcebergWritableTableHandle tableHandle, Map<String, String> fileIoProperties)
    {
        Schema schema = SchemaParser.fromJson(tableHandle.schemaAsJson());
        return createPageSink(session, tableHandle, fileIoProperties, schema, tableHandle.partitionColumns());
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, IcebergWritableTableHandle tableHandle, Map<String, String> fileIoProperties, Schema schema, List<IcebergColumnHandle> columns)
    {
        String partitionSpecJson = tableHandle.partitionsSpecsAsJson().get(tableHandle.partitionSpecId());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, partitionSpecJson);
        LocationProvider locationProvider = getLocationProvider(tableHandle.name(), tableHandle.outputPath(), tableHandle.storageProperties());
        return new IcebergPageSink(
                schema,
                partitionSpec,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                fileSystemFactory.create(session.getIdentity(), fileIoProperties),
                columns,
                jsonCodec,
                session,
                tableHandle.fileFormat(),
                tableHandle.storageProperties(),
                maxPartitionsPerWriter(session),
                tableHandle.sortFields(),
                tableHandle.sortOrderId(),
                sortingFileWriterBufferSize,
                sortingFileWriterMaxOpenFiles,
                sortingFileWriterLocalStagingPath,
                sortTempFileFactory,
                typeManager,
                pageSorter);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
                Schema schema = supportsRowLineage(executeHandle.formatVersion()) ?
                        TypeUtil.join(SchemaParser.fromJson(optimizeHandle.schemaAsJson()), new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER)) :
                        SchemaParser.fromJson(optimizeHandle.schemaAsJson());
                PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, optimizeHandle.partitionSpecAsJson());
                LocationProvider locationProvider = getLocationProvider(executeHandle.schemaTableName(),
                        executeHandle.tableLocation(), optimizeHandle.tableStorageProperties());
                return new IcebergPageSink(
                        schema,
                        partitionSpec,
                        locationProvider,
                        fileWriterFactory,
                        pageIndexerFactory,
                        fileSystemFactory.create(session.getIdentity(), extractFileIoProperties(tableCredentials)),
                        optimizeHandle.partitionColumns(),
                        jsonCodec,
                        session,
                        optimizeHandle.fileFormat(),
                        optimizeHandle.tableStorageProperties(),
                        maxPartitionsPerWriter(session),
                        optimizeHandle.sortFields(),
                        optimizeHandle.sortOrderId(),
                        sortingFileWriterBufferSize,
                        sortingFileWriterMaxOpenFiles,
                        sortingFileWriterLocalStagingPath,
                        sortTempFileFactory,
                        typeManager,
                        pageSorter);
            case GENERATE_EMBEDDINGS:
                return createGenerateEmbeddingsPageSink(session, executeHandle, extractFileIoProperties(tableCredentials));
            case OPTIMIZE_MANIFESTS:
            case OPTIMIZE_POSITION_DELETES:
            case DROP_EXTENDED_STATS:
            case ROLLBACK_TO_SNAPSHOT:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via ConnectorMetadata.executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure: " + executeHandle.procedureId());
    }

    private static Map<String, String> extractFileIoProperties(Optional<TableCredentials> tableCredentials)
    {
        return tableCredentials
                .map(credentials -> ((IcebergTableCredentials) credentials).fileIoProperties())
                .orElseGet(ImmutableMap::of);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, Optional<TableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        IcebergMergeTableHandle merge = (IcebergMergeTableHandle) mergeHandle;
        IcebergWritableTableHandle tableHandle = merge.getInsertTableHandle();
        Map<String, String> fileIoProperties = extractFileIoProperties(tableCredentials);
        LocationProvider locationProvider = getLocationProvider(tableHandle.name(), tableHandle.outputPath(), tableHandle.storageProperties());
        Schema schema = SchemaParser.fromJson(tableHandle.schemaAsJson());
        Map<Integer, PartitionSpec> partitionsSpecs = transformValues(tableHandle.partitionsSpecsAsJson(), json -> PartitionSpecParser.fromJson(schema, json));
        ConnectorPageSink pageSink = createPageSink(session, tableHandle, fileIoProperties);
        Map<String, DeleteFileSet> previousDeleteFiles = tableHandle.previousDeleteFiles().stream()
                .collect(toImmutableMap(PositionDeleteFiles::dataFileLocation, file -> DeleteFileSet.of(file.deletes().stream()
                        .map(delete -> (DeleteFile) contentFileFromJson(delete, partitionsSpecs.get(file.partitionSpecId())))
                        .collect(toImmutableList()))));
        // TODO: remove once the DeleteFile supporting the dataSequenceNumber in serialization and deserialization
        //  https://github.com/apache/iceberg/issues/13320
        // Use HashMap since the delete files can be duplicated in the list of previous delete files
        Map<String, Long> dataSequenceNumbers = new HashMap<>();
        ImmutableMap.Builder<String, Long> firstRowIds = ImmutableMap.builder();
        for (PositionDeleteFiles previousDeleteFile : tableHandle.previousDeleteFiles()) {
            dataSequenceNumbers.put(previousDeleteFile.dataFileLocation(), previousDeleteFile.dataSequenceNumber());
            dataSequenceNumbers.putAll(previousDeleteFile.dataSequenceNumbers());
            if (previousDeleteFile.firstRowId() != null) {
                firstRowIds.put(previousDeleteFile.dataFileLocation(), previousDeleteFile.firstRowId());
            }
        }

        Schema newSchema = schema;
        Optional<ConnectorPageSink> updateInsertPageSink = Optional.empty();
        if (supportsRowLineage(tableHandle.formatVersion())) {
            verifyExistingRowIdColumn(schema, tableHandle.partitionColumns());
            newSchema = TypeUtil.join(schema, new Schema(MetadataColumns.ROW_ID));
            ImmutableList.Builder<IcebergColumnHandle> columns = ImmutableList.builder();
            columns.addAll(tableHandle.partitionColumns());
            columns.add(IcebergColumnHandle.rowIdColumnHandle());

            updateInsertPageSink = Optional.of(createPageSink(session, tableHandle, fileIoProperties, newSchema, columns.build()));
        }

        RowLevelOperationMode rowLevelOperationMode = merge.getInsertTableHandle().operationMode();
        return switch (rowLevelOperationMode) {
            case MERGE_ON_READ -> new IcebergMergeSink(
                    locationProvider,
                    fileWriterFactory,
                    fileSystemFactory.create(session.getIdentity(), fileIoProperties),
                    previousDeleteFiles,
                    jsonCodec,
                    session,
                    tableHandle.formatVersion(),
                    tableHandle.fileFormat(),
                    tableHandle.storageProperties(),
                    schema,
                    partitionsSpecs,
                    pageSink,
                    updateInsertPageSink,
                    schema.columns().size(),
                    pageSourceProviderFactory,
                    tableHandle.partitionColumns(),
                    fileIoProperties,
                    tableHandle.previousDeleteFiles().stream()
                            .collect(toImmutableMap(PositionDeleteFiles::dataFileLocation, PositionDeleteFiles::dataFileRecordCount)),
                    dataSequenceNumbers,
                    firstRowIds.buildOrThrow(),
                    merge.getTableHandle().getNameMappingJson());
            case COPY_ON_WRITE -> new CopyOnWriteIcebergMergeSink(
                    locationProvider,
                    fileWriterFactory,
                    fileSystemFactory.create(session.getIdentity(), fileIoProperties),
                    previousDeleteFiles,
                    jsonCodec,
                    session,
                    tableHandle.fileFormat(),
                    tableHandle.storageProperties(),
                    newSchema,
                    partitionsSpecs,
                    pageSink,
                    updateInsertPageSink,
                    schema.columns().size(),
                    pageSourceProviderFactory,
                    getProjectedColumns(schema, typeManager),
                    fileIoProperties,
                    tableHandle.previousDeleteFiles().stream()
                            .collect(toImmutableMap(PositionDeleteFiles::dataFileLocation, PositionDeleteFiles::dataFileRecordCount)),
                    dataSequenceNumbers,
                    firstRowIds.buildOrThrow(),
                    merge.getTableHandle().getNameMappingJson(),
                    tableHandle.formatVersion());
        };
    }

    private static void verifyExistingRowIdColumn(Schema schema, List<IcebergColumnHandle> columns)
    {
        Types.NestedField rowIdField = schema.findField(MetadataColumns.ROW_ID.name());
        if (rowIdField != null && rowIdField.fieldId() != MetadataColumns.ROW_ID.fieldId()) {
            throw new TrinoException(ICEBERG_BAD_DATA, "Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");
        }

        columns.stream()
                .filter(column -> column.getName().equals(MetadataColumns.ROW_ID.name()))
                .filter(column -> !column.isRowIdColumn())
                .findFirst()
                .ifPresent(column -> {
                    throw new TrinoException(ICEBERG_BAD_DATA, "Table column names conflict with names reserved for Iceberg metadata columns: [_row_id]");
                });
    }

    private ConnectorPageSink createGenerateEmbeddingsPageSink(ConnectorSession session, IcebergTableExecuteHandle executeHandle, Map<String, String> fileIoProperties)
    {
        IcebergGenerateEmbeddingsHandle generateEmbeddingsHandle = (IcebergGenerateEmbeddingsHandle) executeHandle.procedureHandle();
        Schema schema = SchemaParser.fromJson(generateEmbeddingsHandle.schemaAsJson());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, generateEmbeddingsHandle.partitionSpecAsJson());
        LocationProvider locationProvider = getLocationProvider(executeHandle.schemaTableName(),
                executeHandle.tableLocation(), generateEmbeddingsHandle.tableStorageProperties());
        ConnectorPageSink delegatePageSink = new IcebergPageSink(
                schema,
                partitionSpec,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                fileSystemFactory.create(session.getIdentity(), fileIoProperties),
                generateEmbeddingsHandle.tableColumns(),
                jsonCodec,
                session,
                generateEmbeddingsHandle.fileFormat(),
                generateEmbeddingsHandle.tableStorageProperties(),
                maxPartitionsPerWriter(session),
                generateEmbeddingsHandle.sortOrder(),
                SortOrder.unsorted().orderId(),
                sortingFileWriterBufferSize,
                sortingFileWriterMaxOpenFiles,
                sortingFileWriterLocalStagingPath,
                sortTempFileFactory,
                typeManager,
                pageSorter);

        Optional<Integer> dataColumnChannel = Optional.empty();
        Optional<Integer> embeddingColumnChannel = Optional.empty();
        for (int columnIndex = 0; columnIndex < schema.columns().size(); columnIndex++) {
            if (schema.columns().get(columnIndex).fieldId() == generateEmbeddingsHandle.dataColumnFieldId()) {
                dataColumnChannel = Optional.of(columnIndex);
            }
            if (schema.columns().get(columnIndex).fieldId() == generateEmbeddingsHandle.embeddingColumnFieldId()) {
                embeddingColumnChannel = Optional.of(columnIndex);
            }
        }

        return new EmbeddingGeneratingPageSink(
                delegatePageSink,
                dataColumnChannel.orElseThrow(),
                embeddingColumnChannel.orElseThrow(),
                generateEmbeddingsHandle.embeddingType(),
                embeddingClientProvider.embeddingModelClient(Slices.utf8Slice(generateEmbeddingsHandle.modelId())));
    }
}
