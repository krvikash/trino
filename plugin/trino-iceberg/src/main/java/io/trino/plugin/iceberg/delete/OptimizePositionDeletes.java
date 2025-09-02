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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergFileWriterFactory;
import io.trino.plugin.iceberg.IcebergPageSourceProviderFactory;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.delete.DeletionVectorWriter.DeletionVectorInfo;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewritePositionDeletePlanner;
import org.apache.iceberg.actions.FileRewritePlan;
import org.apache.iceberg.actions.RewritePositionDeleteFiles;
import org.apache.iceberg.actions.RewritePositionDeletesCommitManager;
import org.apache.iceberg.actions.RewritePositionDeletesGroup;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static io.trino.plugin.iceberg.delete.DefaultDeletionVectorWriter.isDeletionVector;
import static io.trino.plugin.iceberg.delete.DefaultDeletionVectorWriter.writeDeletionVectorsPuffin;
import static io.trino.plugin.iceberg.delete.DeleteFile.fromIceberg;
import static io.trino.plugin.iceberg.delete.PositionDeleteReader.readMultiFilePositionDeletes;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.TableUtil.formatVersion;

public class OptimizePositionDeletes
{
    private static final Logger log = Logger.get(OptimizePositionDeletes.class);
    private static final Set<FileFormat> SUPPORTED_FILE_FORMATS = Set.of(PARQUET, ORC, AVRO);
    private static final String REMOVED_POSITION_DELETE_FILES_COUNT_METRIC = "removed_position_delete_files_count";
    private static final String ADDED_DELETE_FILES_COUNT_METRIC = "added_delete_files_count";

    private final IcebergFileSystemFactory fileSystemFactory;
    private final IcebergPageSourceProviderFactory pageSourceProviderFactory;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final String trinoVersion;

    @Inject
    public OptimizePositionDeletes(
            IcebergFileSystemFactory fileSystemFactory,
            IcebergPageSourceProviderFactory pageSourceProviderFactory,
            IcebergFileWriterFactory fileWriterFactory,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
    }

    public Map<String, Long> execute(ConnectorSession session, BaseTable icebergTable, SchemaTableName tableName)
    {
        int version = formatVersion(icebergTable);
        if (version >= 3) {
            return convertToDeletionVectors(session, icebergTable, tableName);
        }
        if (version == 2) {
            return optimizePositionDeletes(session, icebergTable, tableName);
        }
        throw new TrinoException(NOT_SUPPORTED, "Optimize Position Deletes procedure is not supported for Iceberg table %s with version %d".formatted(tableName, version));
    }

    private Map<String, Long> optimizePositionDeletes(ConnectorSession session, BaseTable icebergTable, SchemaTableName tableName)
    {
        BinPackRewritePositionDeletePlanner positionDeletePlanner = new BinPackRewritePositionDeletePlanner(icebergTable);
        positionDeletePlanner.init(ImmutableMap.of(SizeBasedFileRewritePlanner.REWRITE_ALL, "true"));
        FileRewritePlan<RewritePositionDeleteFiles.FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup> plan = positionDeletePlanner.plan();
        if (plan.totalGroupCount() == 0) {
            log.debug("No position deletes to rewrite: %s", tableName);
            return ImmutableMap.of(
                    REMOVED_POSITION_DELETE_FILES_COUNT_METRIC, 0L,
                    ADDED_DELETE_FILES_COUNT_METRIC, 0L);
        }

        long rewrittenCount = 0;
        long addedCount = 0;
        try (FileIO fileIo = icebergTable.io()) {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), fileIo.properties());

            ImmutableSet.Builder<RewritePositionDeletesGroup> fileGroups = ImmutableSet.builder();
            boolean requiresCommit = false;
            try (CloseableIterable<RewritePositionDeletesGroup> groups = plan.groups()) {
                for (RewritePositionDeletesGroup fileGroup : groups) {
                    Set<DeleteFile> outputFiles = optimizePositionDeleteFileGroup(
                            session,
                            fileSystem,
                            getLocationProvider(tableName, icebergTable.location(), fileIo.properties()),
                            fileGroup,
                            icebergTable);
                    fileGroup.setOutputFiles(outputFiles);
                    fileGroups.add(fileGroup);

                    rewrittenCount += fileGroup.rewrittenDeleteFiles().size();
                    addedCount += outputFiles.size();
                    requiresCommit = requiresCommit || !outputFiles.equals(fileGroup.rewrittenDeleteFiles());
                }
            }
            catch (IOException e) {
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed accessing data for table: " + tableName, e);
            }

            if (requiresCommit) {
                RewritePositionDeletesCommitManager commitManager = new RewritePositionDeletesCommitManager(icebergTable);
                commitManager.commit(fileGroups.build());
            }
        }

        return ImmutableMap.of(
                REMOVED_POSITION_DELETE_FILES_COUNT_METRIC, rewrittenCount,
                ADDED_DELETE_FILES_COUNT_METRIC, addedCount);
    }

    private Set<DeleteFile> optimizePositionDeleteFileGroup(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            LocationProvider locationProvider,
            RewritePositionDeletesGroup fileGroup,
            Table table)
    {
        if (fileGroup.rewrittenDeleteFiles().size() <= 1) {
            return ImmutableSet.copyOf(fileGroup.rewrittenDeleteFiles());
        }

        Map<String, DeletionVectorWithPartitionInfo> positionDeletionVectors = buildDeletionVectors(session, fileSystem, table, fileGroup.rewrittenDeleteFiles());
        ImmutableSet.Builder<DeleteFile> rewrittenFiles = ImmutableSet.builder();
        for (Entry<String, DeletionVectorWithPartitionInfo> entry : positionDeletionVectors.entrySet()) {
            Optional<DeletionVector> deletedRows = entry.getValue().deletionVectorBuilder().build();
            if (deletedRows.isEmpty()) {
                continue;
            }

            String dataFilePath = entry.getKey();
            PartitionInfo partitionInfo = entry.getValue().partitionInfo();
            PositionDeleteWriter writer = new PositionDeleteWriter(
                    dataFilePath,
                    partitionInfo.partitionSpec(),
                    partitionInfo.partitionData(),
                    locationProvider,
                    fileWriterFactory,
                    fileSystem,
                    session,
                    formatVersion(table),
                    IcebergFileFormat.fromIceberg(partitionInfo.format()),
                    table.properties(),
                    ImmutableMap.of());

            CommitTaskData commitTaskData;
            try {
                commitTaskData = writer.write(deletedRows.get());
            }
            catch (Throwable t) {
                closeAllSuppress(t, writer::abort);
                throw t;
            }

            FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(partitionInfo.partitionSpec())
                    .withPath(commitTaskData.path())
                    .withFormat(commitTaskData.fileFormat().toIceberg())
                    .ofPositionDeletes()
                    .withReferencedDataFile(dataFilePath)
                    .withFileSizeInBytes(commitTaskData.fileSizeInBytes())
                    .withMetrics(commitTaskData.metrics().metrics());

            commitTaskData.fileSplitOffsets().ifPresent(deleteBuilder::withSplitOffsets);
            partitionInfo.partitionData().ifPresent(deleteBuilder::withPartition);

            rewrittenFiles.add(deleteBuilder.build());
        }

        return rewrittenFiles.build();
    }

    private Map<String, Long> convertToDeletionVectors(ConnectorSession session, BaseTable icebergTable, SchemaTableName tableName)
    {
        if (icebergTable.currentSnapshot() == null) {
            return ImmutableMap.of(
                    REMOVED_POSITION_DELETE_FILES_COUNT_METRIC, 0L,
                    ADDED_DELETE_FILES_COUNT_METRIC, 0L);
        }

        Set<DeleteFile> positionDeleteFiles = new HashSet<>();
        Map<String, DeletionVectorWithPartitionInfo> positionDeletionVectors;
        LocationProvider locationProvider;
        try (FileIO io = icebergTable.io()) {
            for (ManifestFile manifest : icebergTable.currentSnapshot().deleteManifests(io)) {
                try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, io, icebergTable.specs())) {
                    for (DeleteFile deleteFile : reader) {
                        if (deleteFile.content() == FileContent.POSITION_DELETES && !isDeletionVector(deleteFile)) {
                            positionDeleteFiles.add(deleteFile);
                        }
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed reading delete manifests for table: " + tableName, e);
                }
            }

            if (positionDeleteFiles.isEmpty()) {
                return ImmutableMap.of(
                        REMOVED_POSITION_DELETE_FILES_COUNT_METRIC, 0L,
                        ADDED_DELETE_FILES_COUNT_METRIC, 0L);
            }

            positionDeletionVectors = buildDeletionVectors(
                    session,
                    fileSystemFactory.create(session.getIdentity(), io.properties()),
                    icebergTable,
                    positionDeleteFiles);

            locationProvider = getLocationProvider(tableName, icebergTable.location(), io.properties());
        }

        ImmutableMap.Builder<String, DeletionVector> deletionVectors = ImmutableMap.builder();
        ImmutableList.Builder<DeletionVectorInfo> deletionVectorInfos = ImmutableList.builder();
        for (Entry<String, DeletionVectorWithPartitionInfo> entry : positionDeletionVectors.entrySet()) {
            Optional<DeletionVector> deletionVector = entry.getValue().deletionVectorBuilder().build();
            if (deletionVector.isEmpty()) {
                continue;
            }

            String dataFilePath = entry.getKey();
            deletionVectors.put(dataFilePath, deletionVector.get());

            // We use an empty slice because writeDeletionVectorsPuffin only needs basic partition info
            PartitionInfo partitionInfo = entry.getValue().partitionInfo();
            deletionVectorInfos.add(new DeletionVectorInfo(
                    dataFilePath,
                    EMPTY_SLICE,
                    partitionInfo.partitionSpec(),
                    partitionInfo.partitionData()));
        }

        List<DeleteFile> newDeletionVectors = writeDeletionVectorsPuffin(
                session,
                icebergTable,
                locationProvider,
                deletionVectorInfos.build(),
                deletionVectors.buildOrThrow(),
                trinoVersion);

        RewriteFiles rewrite = icebergTable.newRewrite();
        for (DeleteFile positionDeleteFile : positionDeleteFiles) {
            rewrite.deleteFile(positionDeleteFile);
        }
        for (DeleteFile newDeletionVector : newDeletionVectors) {
            rewrite.addFile(newDeletionVector);
        }

        rewrite.commit();

        return ImmutableMap.of(
                REMOVED_POSITION_DELETE_FILES_COUNT_METRIC, (long) positionDeleteFiles.size(),
                ADDED_DELETE_FILES_COUNT_METRIC, (long) newDeletionVectors.size());
    }

    private Map<String, DeletionVectorWithPartitionInfo> buildDeletionVectors(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            Table table,
            Collection<DeleteFile> positionDeleteFiles)
    {
        Map<String, DeletionVectorWithPartitionInfo> deletionVectorBuilders = new HashMap<>();
        for (DeleteFile deleteFile : positionDeleteFiles) {
            try (ConnectorPageSource pageSource = openDeleteFilePageSource(session, fileSystem, table, deleteFile)) {
                PartitionInfo partitionInfo = getPartitionInfoFromDeleteFile(table, deleteFile);
                readMultiFilePositionDeletes(pageSource, (filePath, position) ->
                        deletionVectorBuilders.computeIfAbsent(filePath, _ ->
                                new DeletionVectorWithPartitionInfo(partitionInfo, DeletionVector.builder())).deletionVectorBuilder().add(position));
            }
            catch (IOException e) {
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed reading position delete files", e);
            }
        }

        return deletionVectorBuilders;
    }

    private ConnectorPageSource openDeleteFilePageSource(ConnectorSession session, TrinoFileSystem fileSystem, Table table, DeleteFile deleteFile)
    {
        if (!SUPPORTED_FILE_FORMATS.contains(deleteFile.format())) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported file format: " + deleteFile.format());
        }

        return pageSourceProviderFactory.createPageSourceProvider().openDeleteFile(
                session,
                fileSystem,
                fromIceberg(deleteFile),
                ImmutableList.<IcebergColumnHandle>builder()
                        .add(IcebergColumnHandle.builder(primitiveColumnIdentity(DELETE_FILE_PATH.fieldId(), DELETE_FILE_PATH.name())).columnType(VARCHAR).build())
                        .add(IcebergColumnHandle.builder(primitiveColumnIdentity(DELETE_FILE_POS.fieldId(), DELETE_FILE_POS.name())).columnType(BIGINT).build())
                        .build(),
                TupleDomain.all(),
                formatVersion(table));
    }

    private static PartitionInfo getPartitionInfoFromDeleteFile(Table table, DeleteFile deleteFile)
    {
        PartitionSpec spec = table.specs().get(deleteFile.specId());
        Optional<PartitionData> partitionData = Optional.empty();
        if (spec.isPartitioned()) {
            int size = spec.fields().size();
            Object[] values = new Object[size];
            for (int i = 0; i < size; i++) {
                values[i] = deleteFile.partition().get(i, Object.class);
            }
            partitionData = Optional.of(new PartitionData(values));
        }
        return new PartitionInfo(spec, deleteFile.format(), partitionData);
    }

    private record PartitionInfo(PartitionSpec partitionSpec, FileFormat format, Optional<PartitionData> partitionData) {}

    private record DeletionVectorWithPartitionInfo(PartitionInfo partitionInfo, DeletionVector.Builder deletionVectorBuilder) {}
}
