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
package io.trino.plugin.deltalake;

import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.gpu.HeapMemoryReservationHandler;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.base.type.TimestampTzBlockTransformer;
import io.trino.plugin.deltalake.DeltaLakeGpuParquetPageSource.GpuConstantColumn;
import io.trino.plugin.deltalake.DeltaLakeGpuParquetPageSource.GpuOutputColumn;
import io.trino.plugin.deltalake.DeltaLakeGpuParquetPageSource.GpuParquetFileColumn;
import io.trino.plugin.deltalake.delete.PositionDeleteFilter;
import io.trino.plugin.deltalake.delete.RoaringBitmapArray;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.parquet.GpuParquetConfig;
import io.trino.plugin.hive.parquet.ParquetFileFabricator;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.GpuPageSourceSupport;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.EmptyGpuPageSource;
import io.trino.spi.gpu.IoExecutor;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_MODIFIED_TIME_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_SIZE_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.PATH_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_ID_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_POSITION_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.rowPositionColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetSmallFileThreshold;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetIgnoreStatistics;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetVectorizedDecodingEnabled;
import static io.trino.plugin.deltalake.delete.DeletionVectors.readDeletionVectors;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMappingMode;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.plugin.deltalake.util.DeltaLakeDomains.partitionMatchesPredicate;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetMessageType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class DeltaLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(DeltaLakePageSourceProvider.class);

    private static final JsonCodec<List<String>> PARTITIONS_CODEC = new JsonCodecFactory().listJsonCodec(String.class);

    private static final int MAX_ROW_ID_POSITIONS = 100_000;

    private final DeltaLakeFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final ParquetReaderOptions gpuParquetReaderOptions;
    private final long gpuScanMaxPageSizeBytes;
    private final int domainCompactionThreshold;
    private final DateTimeZone parquetDateTimeZone;
    private final DateTimeZone dateTimeZone;
    private final TypeManager typeManager;

    @Inject
    public DeltaLakePageSourceProvider(
            DeltaLakeFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            GpuParquetConfig gpuParquetConfig,
            DeltaLakeConfig deltaLakeConfig,
            TypeManager typeManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = ParquetReaderOptions.builder(parquetReaderConfig.toParquetReaderOptions()).withBloomFilter(false).build();
        // Each coalesced read is materialized up front into one byte[], so start the buffer at its max size.
        this.gpuParquetReaderOptions = ParquetReaderOptions.builder(parquetReaderOptions)
                .withInitialBufferSize(parquetReaderOptions.getMaxBufferSize())
                .build();
        this.gpuScanMaxPageSizeBytes = gpuParquetConfig.getMaxPageSize().toBytes();
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.parquetDateTimeZone = deltaLakeConfig.getParquetDateTimeZone();
        this.dateTimeZone = deltaLakeConfig.getDateTimeZone();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    /**
     * Plan-time check whether a table scan of the given columns can run on the GPU.
     * Per-split concerns (deletion vectors, file format specifics) are handled in {@link #createGpuPageSource}.
     */
    public static GpuPageSourceSupport getGpuPageSourceSupport(List<ColumnHandle> columns)
    {
        for (ColumnHandle columnHandle : columns) {
            DeltaLakeColumnHandle column = (DeltaLakeColumnHandle) columnHandle;
            if (!column.isBaseColumn()) {
                return GpuPageSourceSupport.unsupported("Non-primitive columns are not supported");
            }
            String name = column.baseColumnName();
            if (name.equals(ROW_ID_COLUMN_NAME) || name.equals(ROW_POSITION_COLUMN_NAME)) {
                return GpuPageSourceSupport.unsupported("Unsupported metadata column");
            }
        }
        return GpuPageSourceSupport.SUPPORTED;
    }

    @Override
    public Optional<ConnectorGpuPageSource> createGpuPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            ConnectorGpuMemoryContext gpuMemoryContext,
            IoExecutor ioExecutor)
    {
        DeltaLakeSplit split = (DeltaLakeSplit) connectorSplit;
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) connectorTable;

        if (split.deletionVector().isPresent()) {
            // Deletion vectors require row-level filtering that the GPU scan does not implement.
            log.debug("GPU page source not supported: split has a deletion vector");
            return Optional.empty();
        }
        if (table.isMerge()) {
            // MERGE relies on the synthetic $row_id row block that the GPU scan does not build.
            log.debug("GPU page source not supported: table is being read for MERGE");
            return Optional.empty();
        }

        ColumnMappingMode columnMappingMode = getColumnMappingMode(table.getMetadataEntry(), table.getProtocolEntry());
        if (columnMappingMode != ColumnMappingMode.NONE && columnMappingMode != ColumnMappingMode.ID && columnMappingMode != ColumnMappingMode.NAME) {
            log.debug("GPU page source not supported: unsupported column mapping mode %s", columnMappingMode);
            return Optional.empty();
        }

        // cuDF reads timestamps as raw UTC without applying delta.parquet.time-zone / delta.time-zone.
        // The CPU path shifts timestamps for a non-UTC zone (TimestampTzBlockTransformer / INT96 rebasing);
        // fall back to CPU rather than produce shifted results.
        if (!dateTimeZone.equals(UTC) || !parquetDateTimeZone.equals(UTC)) {
            log.debug("GPU page source not supported: non-UTC time zone (parquet=%s, output=%s)", parquetDateTimeZone, dateTimeZone);
            return Optional.empty();
        }

        List<DeltaLakeColumnHandle> deltaLakeColumns = columns.stream()
                .map(DeltaLakeColumnHandle.class::cast)
                .collect(toImmutableList());

        TupleDomain<DeltaLakeColumnHandle> effectivePredicate = getUnenforcedPredicate(
                session,
                split,
                table,
                dynamicFilter.getCurrentPredicate())
                .transformKeys(DeltaLakeColumnHandle.class::cast);
        if (effectivePredicate.isNone()) {
            return Optional.of(new EmptyGpuPageSource());
        }

        Location location = Location.of(split.path());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session, tableCredentials.map(DeltaLakeTableCredentials.class::cast));
        TrinoInputFile inputFile = fileSystem.newInputFile(location, split.fileSize());

        return createGpuParquetPageSource(
                gpuMemoryContext,
                inputFile,
                split,
                deltaLakeColumns,
                effectivePredicate,
                columnMappingMode,
                ioExecutor);
    }

    private Optional<ConnectorGpuPageSource> createGpuParquetPageSource(
            ConnectorGpuMemoryContext gpuMemoryContext,
            TrinoInputFile inputFile,
            DeltaLakeSplit split,
            List<DeltaLakeColumnHandle> deltaLakeColumns,
            TupleDomain<DeltaLakeColumnHandle> effectivePredicate,
            ColumnMappingMode columnMappingMode,
            IoExecutor ioExecutor)
    {
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(new HeapMemoryReservationHandler(gpuMemoryContext), 0L);
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        Map<String, Optional<String>> partitionKeys = split.partitionKeys();

        try (ParquetDataSource dataSource = createDataSource(inputFile, OptionalLong.empty(), gpuParquetReaderOptions, memoryContext, stats)) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, gpuParquetReaderOptions, Optional.empty(), Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetadata.getSchema();

            Map<Integer, String> parquetFieldIdToName = columnMappingMode == ColumnMappingMode.ID
                    ? fileSchema.getFields().stream()
                      .filter(field -> field.getId() != null)
                      .collect(toImmutableMap(field -> field.getId().intValue(), Type::getName))
                    : ImmutableMap.of();

            ImmutableList.Builder<GpuOutputColumn> outputColumns = ImmutableList.builder();
            ImmutableList.Builder<HiveColumnHandle> parquetColumns = ImmutableList.builder();
            int parquetIndex = 0;
            for (DeltaLakeColumnHandle column : deltaLakeColumns) {
                if (column.isBaseColumn() && partitionKeys.containsKey(column.basePhysicalColumnName())) {
                    Object value = deserializePartitionValue(column, partitionKeys.get(column.basePhysicalColumnName()));
                    outputColumns.add(new GpuConstantColumn(column, column.baseType(), value));
                }
                else if (column.baseColumnName().equals(PATH_COLUMN_NAME)) {
                    outputColumns.add(new GpuConstantColumn(column, PATH_TYPE, utf8Slice(split.path())));
                }
                else if (column.baseColumnName().equals(FILE_SIZE_COLUMN_NAME)) {
                    outputColumns.add(new GpuConstantColumn(column, FILE_SIZE_TYPE, split.fileSize()));
                }
                else if (column.baseColumnName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                    long packedTimestamp = packDateTimeWithZone(split.fileModifiedTime(), dateTimeZone.getID());
                    outputColumns.add(new GpuConstantColumn(column, FILE_MODIFIED_TIME_TYPE, packedTimestamp));
                }
                else {
                    Optional<HiveColumnHandle> hiveColumn = toHiveColumnHandle(column, columnMappingMode, parquetFieldIdToName);
                    if (hiveColumn.isEmpty()) {
                        // Column not present in the file (schema evolution / column-mapping mismatch): synthesize nulls.
                        outputColumns.add(new GpuConstantColumn(column, column.baseType(), null));
                        continue;
                    }
                    String parquetName = hiveColumn.get().getBaseColumnName();
                    if (!fileSchema.containsField(parquetName)) {
                        outputColumns.add(new GpuConstantColumn(column, column.baseType(), null));
                        continue;
                    }
                    Type parquetType = fileSchema.getType(parquetName);
                    if (parquetType.isPrimitive() && !isSupportedForGpu(parquetType.asPrimitiveType(), column.baseType())) {
                        log.debug("GPU page source not supported: column '%s' has unsupported Parquet type %s for Trino type %s", column.baseColumnName(), parquetType, column.baseType());
                        return Optional.empty();
                    }
                    outputColumns.add(new GpuParquetFileColumn(column, parquetName, parquetIndex));
                    parquetColumns.add(hiveColumn.get());
                    parquetIndex++;
                }
            }

            List<HiveColumnHandle> gpuColumns = parquetColumns.build();
            MessageType requestedSchema = getParquetMessageType(gpuColumns, true, fileSchema)
                    .orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));

            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<HiveColumnHandle> hivePredicate = getParquetTupleDomain(effectivePredicate, columnMappingMode, parquetFieldIdToName);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = ParquetPageSourceFactory.getParquetTupleDomain(descriptorsByPath, hivePredicate, fileSchema, true);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, UTC);

            List<RowGroupInfo> filteredRowGroups = getFilteredRowGroups(
                    split.start(),
                    split.length(),
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    domainCompactionThreshold,
                    gpuParquetReaderOptions);

            ParquetFileFabricator fabricator = new ParquetFileFabricator(
                    ImmutableList.of(new ParquetFileFabricator.FileEntry(inputFile, filteredRowGroups, parquetMetadata)),
                    requestedSchema,
                    gpuMemoryContext,
                    gpuParquetReaderOptions,
                    ioExecutor);

            return Optional.of(new DeltaLakeGpuParquetPageSource(gpuMemoryContext, fabricator, outputColumns.build(), dataSource.getReadBytes(), dataSource.getReadTimeNanos(), gpuScanMaxPageSizeBytes));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(DELTA_LAKE_BAD_DATA, "Failed to create GPU Parquet page source for: " + inputFile.location() + ". " + e.getMessage(), e);
        }
    }

    private static boolean isSupportedForGpu(PrimitiveType parquetType, io.trino.spi.type.Type trinoType)
    {
        // INT96 timestamps appear in files migrated from Hive
        if (parquetType.getPrimitiveTypeName() == PrimitiveTypeName.INT96) {
            return false;
        }
        // Unannotated INT64 has no time unit information; cuDF reads it as plain INT64 rather than a timestamp type
        if ((trinoType instanceof TimestampType || trinoType instanceof TimestampWithTimeZoneType)
                && parquetType.getPrimitiveTypeName() == PrimitiveTypeName.INT64
                && parquetType.getLogicalTypeAnnotation() == null) {
            return false;
        }
        return true;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        DeltaLakeSplit split = (DeltaLakeSplit) connectorSplit;
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) connectorTable;

        List<DeltaLakeColumnHandle> deltaLakeColumns = columns.stream()
                .map(DeltaLakeColumnHandle.class::cast)
                .collect(toImmutableList());

        List<DeltaLakeColumnHandle> regularColumns = deltaLakeColumns.stream()
                .filter(column -> (column.columnType() == REGULAR) || column.baseColumnName().equals(ROW_ID_COLUMN_NAME))
                .collect(toImmutableList());

        Map<String, Optional<String>> partitionKeys = split.partitionKeys();
        ColumnMappingMode columnMappingMode = getColumnMappingMode(table.getMetadataEntry(), table.getProtocolEntry());
        Optional<List<String>> partitionValues = Optional.empty();
        if (deltaLakeColumns.stream().anyMatch(column -> column.baseColumnName().equals(ROW_ID_COLUMN_NAME))) {
            // using ArrayList because partition values can be null
            partitionValues = Optional.of(new ArrayList<>());
            Map<String, DeltaLakeColumnMetadata> columnsMetadataByName = extractSchema(table.getMetadataEntry(), table.getProtocolEntry(), typeManager).stream()
                    .collect(toImmutableMap(DeltaLakeColumnMetadata::name, Function.identity()));
            for (String partitionColumnName : table.getMetadataEntry().getOriginalPartitionColumns()) {
                DeltaLakeColumnMetadata partitionColumn = columnsMetadataByName.get(partitionColumnName);
                checkState(partitionColumn != null, "Partition column %s not found", partitionColumnName);
                Optional<String> value = switch (columnMappingMode) {
                    case NONE -> partitionKeys.get(partitionColumn.name());
                    case ID, NAME -> partitionKeys.get(partitionColumn.physicalName());
                    default -> throw new IllegalStateException("Unknown column mapping mode");
                };
                // Fill partition values in the same order as the partition columns are specified in the table definition
                partitionValues.get().add(value.orElse(null));
            }
        }

        // We reach here when we could not prune the split using file level stats, table predicate
        // and the dynamic filter in the coordinator during split generation. The file level stats
        // in DeltaLakeSplit#statisticsPredicate could help to prune this split when a more selective dynamic filter
        // is available now, without having to access parquet file footer for row-group stats.
        TupleDomain<DeltaLakeColumnHandle> effectivePredicate = getUnenforcedPredicate(
                session,
                split,
                table,
                dynamicFilter.getCurrentPredicate())
                .transformKeys(DeltaLakeColumnHandle.class::cast);
        if (effectivePredicate.isNone()) {
            return new EmptyPageSource();
        }
        // Skip reading the file if none of the actual file columns are being read
        if (effectivePredicate.isAll() &&
                split.start() == 0 && split.length() == split.fileSize() &&
                split.fileRowCount().isPresent() &&
                split.deletionVector().isEmpty() &&
                (regularColumns.isEmpty() || onlyRowIdColumn(regularColumns))) {
            return projectColumns(
                    deltaLakeColumns,
                    ImmutableSet.of(),
                    partitionKeys,
                    partitionValues,
                    generatePages(split.fileRowCount().get(), onlyRowIdColumn(regularColumns)),
                    dateTimeZone,
                    split.path(),
                    split.fileSize(),
                    split.fileModifiedTime());
        }

        Location location = Location.of(split.path());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session, tableCredentials.map(DeltaLakeTableCredentials.class::cast));
        TrinoInputFile inputFile = fileSystem.newInputFile(location, split.fileSize());
        ParquetReaderOptions options = ParquetReaderOptions.builder(parquetReaderOptions)
                .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                .withSmallFileThreshold(getParquetSmallFileThreshold(session))
                .withUseColumnIndex(!table.isMerge() && split.deletionVector().isEmpty() && isParquetUseColumnIndex(session))
                .withIgnoreStatistics(isParquetIgnoreStatistics(session))
                .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session))
                .build();

        Map<Integer, String> parquetFieldIdToName = columnMappingMode == ColumnMappingMode.ID ? loadParquetIdAndNameMapping(inputFile, options) : ImmutableMap.of();

        ImmutableSet.Builder<String> missingColumnNamesBuilder = ImmutableSet.builder();
        ImmutableList.Builder<HiveColumnHandle> hiveColumnHandlesBuilder = ImmutableList.builder();
        for (DeltaLakeColumnHandle column : regularColumns) {
            if (column.baseColumnName().equals(ROW_ID_COLUMN_NAME)) {
                hiveColumnHandlesBuilder.add(PARQUET_ROW_INDEX_COLUMN);
                continue;
            }
            toHiveColumnHandle(column, columnMappingMode, parquetFieldIdToName).ifPresentOrElse(
                    hiveColumnHandlesBuilder::add,
                    () -> missingColumnNamesBuilder.add(column.baseColumnName()));
        }
        if (split.deletionVector().isPresent() && !regularColumns.contains(rowPositionColumnHandle())) {
            hiveColumnHandlesBuilder.add(PARQUET_ROW_INDEX_COLUMN);
        }
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandlesBuilder.build();
        Set<String> missingColumnNames = missingColumnNamesBuilder.build();

        TupleDomain<HiveColumnHandle> parquetPredicate = getParquetTupleDomain(effectivePredicate, columnMappingMode, parquetFieldIdToName);

        ConnectorPageSource delegate = ParquetPageSourceFactory.createPageSource(
                inputFile,
                split.start(),
                split.length(),
                hiveColumnHandles,
                ImmutableList.of(parquetPredicate),
                true,
                parquetDateTimeZone,
                fileFormatDataSourceStats,
                options,
                Optional.empty(),
                Optional.empty(),
                domainCompactionThreshold,
                OptionalLong.of(split.fileSize()),
                memoryContext);

        if (split.deletionVector().isPresent()) {
            var pageFilterSupplier = Suppliers.memoize(() -> {
                List<DeltaLakeColumnHandle> requiredColumns = ImmutableList.<DeltaLakeColumnHandle>builderWithExpectedSize(regularColumns.size() + 1)
                        .addAll(regularColumns)
                        .add(rowPositionColumnHandle())
                        .build();
                PositionDeleteFilter deleteFilter = readDeletes(fileSystem, Location.of(table.location()), split.deletionVector().get());
                return deleteFilter.createPredicate(requiredColumns);
            });

            // trim output columns list so we do not expose PARQUET_ROW_INDEX_COLUMN added for internal purposes
            int[] retainedColumns = IntStream.range(0, regularColumns.size()).toArray();
            delegate = TransformConnectorPageSource.create(delegate, page -> SourcePage.create(pageFilterSupplier.get().apply(page).getColumns(retainedColumns)));
        }

        return projectColumns(
                deltaLakeColumns,
                missingColumnNames,
                partitionKeys,
                partitionValues,
                delegate,
                dateTimeZone,
                split.path(),
                split.fileSize(),
                split.fileModifiedTime());
    }

    public static ConnectorPageSource projectColumns(
            List<DeltaLakeColumnHandle> deltaLakeColumns,
            Set<String> missingColumnNames,
            Map<String, Optional<String>> partitionKeys,
            Optional<List<String>> partitionValues,
            ConnectorPageSource delegate,
            DateTimeZone dateTimeZone,
            String path,
            long fileSize,
            long fileModifiedTime)
    {
        int delegateIndex = 0;
        TransformConnectorPageSource.Builder transform = TransformConnectorPageSource.builder();
        for (DeltaLakeColumnHandle column : deltaLakeColumns) {
            if (column.isBaseColumn() && partitionKeys.containsKey(column.basePhysicalColumnName())) {
                Object prefilledValue = deserializePartitionValue(column, partitionKeys.get(column.basePhysicalColumnName()));
                transform.constantValue(writeNativeValue(column.baseType(), prefilledValue));
            }
            else if (column.baseColumnName().equals(PATH_COLUMN_NAME)) {
                transform.constantValue(writeNativeValue(PATH_TYPE, utf8Slice(path)));
            }
            else if (column.baseColumnName().equals(FILE_SIZE_COLUMN_NAME)) {
                transform.constantValue(writeNativeValue(FILE_SIZE_TYPE, fileSize));
            }
            else if (column.baseColumnName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                long packedTimestamp = packDateTimeWithZone(fileModifiedTime, dateTimeZone.getID());
                transform.constantValue(writeNativeValue(FILE_MODIFIED_TIME_TYPE, packedTimestamp));
            }
            else if (column.baseColumnName().equals(ROW_ID_COLUMN_NAME)) {
                Block pathBlock = writeNativeValue(VARCHAR, utf8Slice(path));
                Block partitionsBlock = writeNativeValue(VARCHAR, wrappedBuffer(PARTITIONS_CODEC.toJsonBytes(partitionValues.orElseThrow(() -> new IllegalStateException("partitionValues not provided")))));
                transform.transform(delegateIndex, new CreateRowIdBlock(pathBlock, partitionsBlock));
                delegateIndex++;
            }
            else if (missingColumnNames.contains(column.baseColumnName())) {
                transform.constantValue(column.type().createNullBlock());
            }
            else if (!dateTimeZone.equals(UTC)) {
                transform.transform(delegateIndex, new TimestampTzBlockTransformer(column.type(), dateTimeZone));
                delegateIndex++;
            }
            else {
                transform.column(delegateIndex);
                delegateIndex++;
            }
        }
        return transform.build(delegate);
    }

    private static Block createRowIdBlock(Block pathValue, Block rowIndexBlock, Block partitionsValue)
    {
        return RowBlock.fromFieldBlocks(rowIndexBlock.getPositionCount(), new Block[] {
                RunLengthEncodedBlock.create(pathValue, rowIndexBlock.getPositionCount()),
                rowIndexBlock,
                RunLengthEncodedBlock.create(partitionsValue, rowIndexBlock.getPositionCount()),
        });
    }

    private static PositionDeleteFilter readDeletes(
            TrinoFileSystem fileSystem,
            Location tableLocation,
            DeletionVectorEntry deletionVector)
    {
        try {
            RoaringBitmapArray deletedRows = readDeletionVectors(fileSystem, tableLocation, deletionVector);
            return new PositionDeleteFilter(deletedRows);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Failed to read deletion vectors", e);
        }
    }

    @Override
    public TupleDomain<ColumnHandle> getUnenforcedPredicate(
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        DeltaLakeSplit split = (DeltaLakeSplit) connectorSplit;
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) connectorTable;

        TupleDomain<ColumnHandle> prunedPredicate = prunePredicate(connectorSession, connectorSplit, connectorTable,
                TupleDomain.intersect(ImmutableList.of(
                        table.getNonPartitionConstraint(),
                        split.statisticsPredicate(),
                        dynamicFilter)));
        return prunedPredicate.simplify(domainCompactionThreshold);
    }

    @Override
    public TupleDomain<ColumnHandle> prunePredicate(
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            TupleDomain<ColumnHandle> predicate)
    {
        DeltaLakeSplit split = (DeltaLakeSplit) connectorSplit;

        TupleDomain<DeltaLakeColumnHandle> predicateOnPartitioningColumn = predicate
                .transformKeys(DeltaLakeColumnHandle.class::cast)
                .filter((columnHandle, _) -> columnHandle.columnType() == PARTITION_KEY);

        if (predicateOnPartitioningColumn.getDomains().isPresent() && !partitionMatchesPredicate(split.partitionKeys(), predicateOnPartitioningColumn.getDomains().get())) {
            return TupleDomain.none();
        }

        return predicate.filter((columnHandle, _) -> ((DeltaLakeColumnHandle) columnHandle).columnType() != PARTITION_KEY)
                // remove domains from predicate that fully contain split data because they are irrelevant for filtering
                .filter((handle, domain) -> !domain.contains(split.statisticsPredicate().getDomain((DeltaLakeColumnHandle) handle, domain.getType())));
    }

    private Map<Integer, String> loadParquetIdAndNameMapping(TrinoInputFile inputFile, ParquetReaderOptions options)
    {
        try (ParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, options, fileFormatDataSourceStats)) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, options, Optional.empty(), Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            return fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null) // field id returns null if undefined
                    .collect(toImmutableMap(field -> field.getId().intValue(), Type::getName));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static TupleDomain<HiveColumnHandle> getParquetTupleDomain(TupleDomain<DeltaLakeColumnHandle> effectivePredicate, ColumnMappingMode columnMapping, Map<Integer, String> fieldIdToName)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<HiveColumnHandle, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            io.trino.spi.type.Type baseType = columnHandle.baseType();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!(baseType instanceof MapType) && !(baseType instanceof ArrayType) && !(baseType instanceof RowType)) {
                Optional<HiveColumnHandle> hiveColumnHandle = toHiveColumnHandle(columnHandle, columnMapping, fieldIdToName);
                hiveColumnHandle.ifPresent(column -> predicate.put(column, domain));
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    public static Optional<HiveColumnHandle> toHiveColumnHandle(DeltaLakeColumnHandle deltaLakeColumnHandle, ColumnMappingMode columnMapping, Map<Integer, String> fieldIdToName)
    {
        return switch (columnMapping) {
            case ID -> {
                Integer fieldId = deltaLakeColumnHandle.baseFieldId().orElseThrow(() -> new IllegalArgumentException("Field ID must exist"));
                if (!fieldIdToName.containsKey(fieldId)) {
                    yield Optional.empty();
                }
                String fieldName = fieldIdToName.get(fieldId);
                Optional<HiveColumnProjectionInfo> hiveColumnProjectionInfo = deltaLakeColumnHandle.projectionInfo()
                        .map(DeltaLakeColumnProjectionInfo::toHiveColumnProjectionInfo);
                yield Optional.of(new HiveColumnHandle(
                        fieldName,
                        0,
                        toHiveType(deltaLakeColumnHandle.basePhysicalType()),
                        deltaLakeColumnHandle.basePhysicalType(),
                        hiveColumnProjectionInfo,
                        deltaLakeColumnHandle.columnType().toHiveColumnType(),
                        Optional.empty()));
            }
            case NAME, NONE -> {
                checkArgument(fieldIdToName.isEmpty(), "Mapping between field id and name must be empty: %s", fieldIdToName);
                yield Optional.of(deltaLakeColumnHandle.toHiveColumnHandle());
            }
            case UNKNOWN -> throw new IllegalArgumentException("Unsupported column mapping: " + columnMapping);
        };
    }

    private static boolean onlyRowIdColumn(List<DeltaLakeColumnHandle> columns)
    {
        return columns.size() == 1 && getOnlyElement(columns).baseColumnName().equals(ROW_ID_COLUMN_NAME);
    }

    private static ConnectorPageSource generatePages(long totalRowCount, boolean projectRowNumber)
    {
        return new FixedPageSource(
                new AbstractIterator<>()
                {
                    private long rowIndex;

                    @Override
                    protected Page computeNext()
                    {
                        if (rowIndex == totalRowCount) {
                            return endOfData();
                        }
                        int pageSize = toIntExact(min(MAX_ROW_ID_POSITIONS, totalRowCount - rowIndex));

                        Page page;
                        if (projectRowNumber) {
                            page = new Page(pageSize, createRowNumberBlock(rowIndex, pageSize));
                        }
                        else {
                            page = new Page(pageSize);
                        }
                        rowIndex += pageSize;
                        return page;
                    }
                },
                0);
    }

    private static Block createRowNumberBlock(long baseIndex, int size)
    {
        long[] rowIndices = new long[size];
        for (int position = 0; position < size; position++) {
            rowIndices[position] = baseIndex + position;
        }
        return new LongArrayBlock(size, Optional.empty(), rowIndices);
    }

    private record CreateRowIdBlock(Block pathBlock, Block partitionsBlock)
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            return createRowIdBlock(pathBlock, block, partitionsBlock);
        }
    }
}
