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
package org.apache.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.ForIcebergPlanning;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergFileWriter;
import io.trino.plugin.iceberg.IcebergFileWriterFactory;
import io.trino.plugin.iceberg.IcebergTypes;
import io.trino.plugin.iceberg.PartitionStatisticsReader;
import io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isPartitionStatisticsCollectOnWrite;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.INCREMENTAL_UPDATE;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.REPLACE;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.apache.iceberg.util.DateTimeUtil.isoTimestampToNanos;
import static org.apache.iceberg.util.DateTimeUtil.isoTimestamptzToNanos;
import static org.apache.iceberg.util.PartitionUtil.coercePartition;
import static org.apache.iceberg.util.SnapshotUtil.ancestorsBetween;

/**
 * Writer for <a href="https://iceberg.apache.org/spec/#partition-statistics">partition statistics</a>.
 * Iceberg official implementation is org.apache.iceberg.PartitionStatsHandler.
 * This class exists in org.apache.iceberg package to access package-private APIs.
 */
public class PartitionStatisticsWriter
{
    private static final Logger log = Logger.get(PartitionStatisticsWriter.class);

    private final TypeManager typeManager;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PartitionStatisticsReader partitionStatisticsReader;
    private final ExecutorService planningExecutor;

    @Inject
    public PartitionStatisticsWriter(
            TypeManager typeManager,
            IcebergFileSystemFactory fileSystemFactory,
            IcebergFileWriterFactory fileWriterFactory,
            PartitionStatisticsReader partitionStatisticsReader,
            @ForIcebergPlanning ExecutorService planningExecutor)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.partitionStatisticsReader = requireNonNull(partitionStatisticsReader, "partitionStatisticsReader is null");
        this.planningExecutor = requireNonNull(planningExecutor, "planningExecutor is null");
    }

    public Optional<PartitionStatisticsFile> writePartitionStats(ConnectorSession session, Table table, long snapshotId)
    {
        if (!isPartitionStatisticsCollectOnWrite(session) || table.spec().isUnpartitioned() || table.currentSnapshot() == null) {
            return Optional.empty();
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        Snapshot snapshot = table.snapshot(snapshotId);
        StructType partitionType = Partitioning.partitionType(table);

        Collection<PartitionStats> stats;
        PartitionStatisticsFile statisticsFile = findLatestStatsFile(table, snapshot.snapshotId());
        if (statisticsFile == null) {
            stats = computeStats(table, snapshot.allManifests(table.io()), REPLACE).values();
        }
        else {
            if (statisticsFile.snapshotId() == snapshotId) {
                log.debug("Returning existing statistics file for snapshot: %s", snapshotId);
                return Optional.of(statisticsFile);
            }
            stats = computeAndMergeStatsIncremental(session, table, snapshot, partitionType, statisticsFile);
        }
        if (stats.isEmpty()) {
            return Optional.empty();
        }

        List<PartitionStats> sortedStats = sortStatsByPartition(stats, partitionType);
        PartitionStatisticsFile partitionStatisticsFile = writePartitionStatsFile(
                session,
                fileSystem,
                table,
                snapshot.snapshotId(),
                PartitionStatsHandler.schema(partitionType, formatVersion(table)),
                table.io().properties(),
                sortedStats);
        return Optional.of(partitionStatisticsFile);
    }

    private PartitionStatisticsFile writePartitionStatsFile(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            Table table,
            long snapshotId,
            Schema dataSchema,
            Map<String, String> storageProperties,
            Iterable<PartitionStats> records)
    {
        FileFormat fileFormat = FileFormat.fromString(table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
        fileFormat = Set.of(PARQUET, ORC).contains(fileFormat) ? fileFormat : PARQUET;
        OutputFile outputFile = newPartitionStatsFile(table, fileFormat, snapshotId);

        Location outputPath = Location.of(outputFile.location());
        IcebergFileWriter fileWriter = fileWriterFactory.createDataFileWriter(
                fileSystem,
                outputPath,
                dataSchema,
                session,
                getFileFormat(table),
                MetricsConfig.forTable(table),
                storageProperties);

        List<org.apache.iceberg.types.Type> icebergTypes = dataSchema.columns().stream()
                .map(Types.NestedField::type)
                .collect(toImmutableList());
        List<Type> trinoTypes = dataSchema.columns().stream()
                .map(field -> toTrinoType(field.type(), typeManager))
                .collect(toImmutableList());

        PageBuilder pageBuilder = new PageBuilder(trinoTypes);
        records.forEach(stats -> {
            pageBuilder.declarePosition();

            // Write 'partition' field
            RowType rowType = (RowType) trinoTypes.get(0);
            BlockBuilder partitionBuilder = pageBuilder.getBlockBuilder(0);
            List<Type> partitionColumnTypes = rowType.getFields().stream().map(RowType.Field::getType).collect(toImmutableList());
            StructLike partition = stats.partition();
            RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
            rowBlockBuilder.buildEntry(fieldBuilders -> {
                for (int j = 0; j < partitionColumnTypes.size(); j++) {
                    org.apache.iceberg.types.Type icebergType = icebergTypes.get(0).asStructType().fields().get(j).type();
                    Object value = convertIcebergValueToTrino(icebergType, partition.get(j, icebergType.typeId().javaClass()));
                    writeNativeValue(partitionColumnTypes.get(j), fieldBuilders.get(j), value);
                }
            });
            partitionBuilder.append(rowBlockBuilder.buildValueBlock(), 0);

            // Write other fields
            for (int i = 1; i < dataSchema.columns().size(); i++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(i);
                Type type = trinoTypes.get(i);
                if (type instanceof BigintType) {
                    Long longValue = stats.get(i, Long.class);
                    if (longValue == null) {
                        output.appendNull();
                    }
                    else {
                        type.writeLong(output, longValue);
                    }
                }
                else if (type instanceof IntegerType) {
                    type.writeLong(output, stats.get(i, Integer.class));
                }
            }

            if (pageBuilder.isFull()) {
                fileWriter.appendRows(pageBuilder.build());
                pageBuilder.reset();
            }
        });
        if (!pageBuilder.isEmpty()) {
            fileWriter.appendRows(pageBuilder.build());
            pageBuilder.reset();
        }
        fileWriter.commit();

        return ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(snapshotId)
                .path(outputFile.location())
                .fileSizeInBytes(outputFile.toInputFile().getLength())
                .build();
    }

    private static OutputFile newPartitionStatsFile(Table table, FileFormat fileFormat, long snapshotId)
    {
        TableOperations operations = ((HasTableOperations) table).operations();
        String partitionStatsLocation = operations.metadataFileLocation(fileFormat.addExtension("partition-stats-%d-%s".formatted(snapshotId, UUID.randomUUID())));
        return table.io().newOutputFile(partitionStatsLocation);
    }

    private Collection<PartitionStats> computeAndMergeStatsIncremental(
            ConnectorSession session,
            Table table,
            Snapshot snapshot,
            StructType partitionType,
            PartitionStatisticsFile previousStatsFile)
    {
        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
        Schema schema = PartitionStatsHandler.schema(partitionType, formatVersion(table));
        InputFile inputFile = table.io().newInputFile(previousStatsFile.path(), previousStatsFile.fileSizeInBytes());
        try (PartitionStatisticsReader.PartitionStatsIterator statsIterator = partitionStatisticsReader.readPartitionStats(session, table, schema, inputFile)) {
            statsIterator.forEachRemaining(partitionStats -> statsMap.put(partitionStats.specId(), partitionStats.partition(), partitionStats));
        }

        // incrementally compute the new stats, partition field will be written as PartitionData
        PartitionMap<PartitionStats> incrementalStatsMap = computeStatsDiff(table, table.snapshot(previousStatsFile.snapshotId()), snapshot);

        // convert PartitionData into GenericRecord and merge stats
        incrementalStatsMap.forEach(
                (key, value) ->
                        statsMap.merge(
                                Pair.of(key.first(), key.second()),
                                value,
                                (existingEntry, newEntry) -> {
                                    //noinspection deprecation
                                    existingEntry.appendStats(newEntry);
                                    return existingEntry;
                                }));

        return statsMap.values();
    }

    @Nullable
    public static Object convertTrinoValueToIceberg(org.apache.iceberg.types.Type icebergType, Type type, @Nullable Object trinoValue, int position)
    {
        if (trinoValue == null) {
            return null;
        }

        if (type instanceof RowType rowType) {
            PartitionData partitionData = new PartitionData(icebergType.asStructType());
            SqlRow sqlRow = (SqlRow) trinoValue;
            List<Type> fieldTypes = rowType.getFields().stream().map(RowType.Field::getType).collect(toImmutableList());
            for (int i = 0; i < fieldTypes.size(); i++) {
                org.apache.iceberg.types.Type icebergFieldType = icebergType.asStructType().fields().get(i).type();
                Type trinoFieldType = fieldTypes.get(i);
                Object value = convertTrinoValueToIceberg(icebergFieldType, trinoFieldType, readNativeValue(trinoFieldType, sqlRow.getRawFieldBlock(i), position), position);
                partitionData.set(i, value);
            }
            return partitionData;
        }

        Object icebergValue = IcebergTypes.convertTrinoValueToIceberg(type, trinoValue);
        if (icebergType.equals(Types.TimestampNanoType.withoutZone())) {
            return isoTimestampToNanos((String) icebergValue);
        }
        if (icebergType.equals(Types.TimestampNanoType.withZone())) {
            return isoTimestamptzToNanos((String) icebergValue);
        }
        return icebergValue;
    }

    private PartitionMap<PartitionStats> computeStatsDiff(Table table, Snapshot fromSnapshot, Snapshot toSnapshot)
    {
        Iterable<Snapshot> snapshots = ancestorsBetween(toSnapshot.snapshotId(), fromSnapshot.snapshotId(), table::snapshot);
        // DELETED manifest entries are not carried over to subsequent snapshots.
        // So, for incremental computation, gather the manifests added by each snapshot
        // instead of relying solely on those from the latest snapshot.
        List<ManifestFile> manifests = StreamSupport.stream(snapshots.spliterator(), false)
                .flatMap(snapshot -> snapshot.allManifests(table.io()).stream()
                        .filter(file -> file.snapshotId().equals(snapshot.snapshotId())))
                .collect(toImmutableList());

        return computeStats(table, manifests, INCREMENTAL_UPDATE);
    }

    @Nullable
    private static PartitionStatisticsFile findLatestStatsFile(Table table, long snapshotId)
    {
        List<PartitionStatisticsFile> partitionStatisticsFiles = table.partitionStatisticsFiles();
        if (partitionStatisticsFiles.isEmpty()) {
            return null;
        }

        Map<Long, PartitionStatisticsFile> stats = partitionStatisticsFiles.stream()
                .collect(Collectors.toMap(PartitionStatisticsFile::snapshotId, Function.identity()));
        for (Snapshot snapshot : SnapshotUtil.ancestorsOf(snapshotId, table::snapshot)) {
            if (stats.containsKey(snapshot.snapshotId())) {
                return stats.get(snapshot.snapshotId());
            }
        }

        return null;
    }

    private PartitionMap<PartitionStats> computeStats(Table table, List<ManifestFile> manifests, StatsUpdateMode updateMode)
    {
        StructType partitionType = Partitioning.partitionType(table);
        Queue<PartitionMap<PartitionStats>> statsByManifest = Queues.newConcurrentLinkedQueue();
        Tasks.foreach(manifests)
                .stopOnFailure()
                .throwFailureWhenFinished()
                .executeWith(planningExecutor)
                .run(manifest -> statsByManifest.add(collectStatsForManifest(table, manifest, partitionType, updateMode)));

        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
        for (PartitionMap<PartitionStats> stats : statsByManifest) {
            mergePartitionMap(stats, statsMap);
        }

        return statsMap;
    }

    private static PartitionMap<PartitionStats> collectStatsForManifest(Table table, ManifestFile manifest, StructType partitionType, StatsUpdateMode updateMode)
    {
        List<String> projection = BaseScan.scanColumns(manifest.content());
        try (ManifestReader<?> reader = ManifestFiles.open(manifest, table.io()).select(projection)) {
            PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
            int specId = manifest.partitionSpecId();
            PartitionSpec spec = table.specs().get(specId);
            PartitionData keyTemplate = new PartitionData(partitionType);

            for (ManifestEntry<?> entry : reader.entries()) {
                ContentFile<?> file = entry.file();
                StructLike coercedPartition = coercePartition(partitionType, spec, file.partition());
                StructLike key = keyTemplate.copyFor(coercedPartition);
                Snapshot snapshot = table.snapshot(entry.snapshotId());
                PartitionStats stats = statsMap.computeIfAbsent(
                        specId,
                        ((PartitionData) file.partition()).copy(),
                        () -> new PartitionStats(key, specId));
                if (entry.isLive()) {
                    // Live can have both added and existing entries. Consider only added entries for
                    // incremental compute as existing entries was already included in previous compute.
                    if (updateMode == REPLACE || entry.status() == ManifestEntry.Status.ADDED) {
                        //noinspection deprecation
                        stats.liveEntry(file, snapshot);
                    }
                }
                else {
                    if (updateMode == INCREMENTAL_UPDATE) {
                        stats.deletedEntryForIncrementalCompute(file, snapshot);
                    }
                    else {
                        //noinspection deprecation
                        stats.deletedEntry(snapshot);
                    }
                }
            }

            return statsMap;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void mergePartitionMap(PartitionMap<PartitionStats> fromMap, PartitionMap<PartitionStats> toMap)
    {
        fromMap.forEach(
                (key, value) ->
                        toMap.merge(
                                key,
                                value,
                                (existingEntry, newEntry) -> {
                                    //noinspection deprecation
                                    existingEntry.appendStats(newEntry);
                                    return existingEntry;
                                }));
    }

    private static List<PartitionStats> sortStatsByPartition(Collection<PartitionStats> stats, StructType partitionType)
    {
        List<PartitionStats> entries = Lists.newArrayList(stats);
        entries.sort(Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType)));
        return entries;
    }
}
