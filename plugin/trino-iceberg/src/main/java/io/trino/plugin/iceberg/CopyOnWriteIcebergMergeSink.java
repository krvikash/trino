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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DeleteFileSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergUtil.supportsRowLineage;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CopyOnWriteIcebergMergeSink
        extends AbstractIcebergMergeSink
{
    public CopyOnWriteIcebergMergeSink(
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            TrinoFileSystem fileSystem,
            Map<String, DeleteFileSet> previousDeleteFiles,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties,
            Schema schema,
            Map<Integer, PartitionSpec> partitionsSpecs,
            ConnectorPageSink insertPageSink,
            Optional<ConnectorPageSink> updateInsertPageSink,
            int columnCount,
            IcebergPageSourceProviderFactory pageSourceProviderFactory,
            List<IcebergColumnHandle> columns,
            IcebergTableCredentials tableCredentials,
            Map<String, Long> fileCounts,
            Map<String, Long> dataSequenceNumbers,
            Map<String, Long> firstRowIds,
            Optional<String> nameMapping,
            int formatVersion)
    {
        super(locationProvider,
                fileWriterFactory,
                fileSystem,
                previousDeleteFiles,
                jsonCodec,
                session,
                fileFormat,
                storageProperties,
                schema,
                partitionsSpecs,
                insertPageSink,
                updateInsertPageSink,
                columnCount,
                pageSourceProviderFactory,
                columns,
                tableCredentials,
                fileCounts,
                dataSequenceNumbers,
                firstRowIds,
                nameMapping,
                formatVersion);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());

        updateInsertPageSink.ifPresent(pageSink -> fragments.addAll(pageSink.finish().join()));

        fileDeletions.forEach((dataFilePath, deletion) -> {
            PartitionSpec partitionSpec = partitionsSpecs.get(deletion.partitionSpecId());
            PartitionData partitionData = PartitionData.fromJson(deletion.partitionDataJson(), partitionSpec);

            Location dataFile = Location.of(dataFilePath.toStringUtf8());
            String fileName = fileFormat.toIceberg().addExtension(session.getQueryId() + "-" + randomUUID());
            Location outputPath = Location.of(partitionSpec.isPartitioned() ? locationProvider.newDataLocation(partitionSpec, partitionData, fileName) : locationProvider.newDataLocation(fileName));
            IcebergFileWriter fileWriter = fileWriterFactory.createDataFileWriter(fileSystem, outputPath, supportsRowLineage(formatVersion) ? TypeUtil.join(schema, new Schema(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER)) : schema, session, fileFormat, MetricsConfig.getDefault(), storageProperties);
            try {
                rewriteFile(fileWriter, outputPath, dataFile, deletion, partitionSpec, partitionData).ifPresent(fragments::add);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        return completedFuture(fragments);
    }

    private Optional<Slice> rewriteFile(
            IcebergFileWriter writer,
            Location outputPath,
            Location dataFilePath,
            FileDeletion deletion,
            PartitionSpec partitionSpec,
            PartitionData partitionData)
            throws IOException
    {
        boolean writtenRecords = false;
        try (ConnectorPageSource connectorPageSource = createPageSource(dataFilePath, deletion, partitionSpec, partitionData)) {
            while (!connectorPageSource.isFinished()) {
                SourcePage sourcePage = connectorPageSource.getNextSourcePage();
                if (sourcePage == null) {
                    continue;
                }
                // fully load page
                Page page = sourcePage.getPage();
                if (page.getPositionCount() > 0) {
                    writtenRecords = true;
                    writer.appendRows(page);
                }
            }
            if (!writtenRecords) {
                writer.rollback();
                // Let the caller know that no data was left, and to remove the existing data file
                return Optional.of(wrappedBuffer(jsonCodec.toJsonBytes(emptyCommitTaskData(dataFilePath.toString(), PartitionSpecParser.toJson(partitionSpec), Optional.of(deletion.partitionDataJson())))));
            }
            writer.commit();
        }
        catch (Throwable t) {
            try {
                writer.rollback();
            }
            catch (RuntimeException e) {
                if (!t.equals(e)) {
                    t.addSuppressed(e);
                }
            }
            throw t;
        }

        CommitTaskData task = new CommitTaskData(
                outputPath.toString(),
                fileFormat,
                writer.getWrittenBytes(),
                new MetricsWrapper(writer.getFileMetrics().metrics()),
                PartitionSpecParser.toJson(partitionSpec),
                Optional.of(deletion.partitionDataJson()),
                FileContent.DATA,
                Optional.of(dataFilePath.toString()),
                writer.getFileMetrics().splitOffsets(),
                SortOrder.unsorted().orderId(),
                Optional.empty());

        return Optional.of(wrappedBuffer(jsonCodec.toJsonBytes(task)));
    }

    private static CommitTaskData emptyCommitTaskData(String dataFilePath, String partitionSpecJson, Optional<String> partitionDataJson)
    {
        return new CommitTaskData(
                "",
                IcebergFileFormat.PARQUET,
                0,
                new MetricsWrapper(new Metrics()),
                partitionSpecJson,
                partitionDataJson,
                FileContent.DATA,
                Optional.of(dataFilePath),
                Optional.empty(),
                SortOrder.unsorted().orderId(),
                Optional.empty());
    }

    private ConnectorPageSource createPageSource(Location path, FileDeletion deletion, PartitionSpec partitionSpec, PartitionData partitionData)
            throws IOException
    {
        ImmutableList.Builder<DeleteFile> deleteFiles = ImmutableList.builder();
        deletion.rowsToDelete().build().ifPresent(deletionVector ->
                deleteFiles.add(DeleteFile.fromDeletionVector(deletionVector)));
        previousDeleteFiles.getOrDefault(path.toString(), DeleteFileSet.create()).stream()
                .map(DeleteFile::fromIceberg)
                .map(deleteFile -> deleteFile.withDataSequenceNumber(dataSequenceNumbers.get(deleteFile.path())))
                .forEach(deleteFiles::add);
        IcebergPageSourceProvider icebergPageSourceProvider = pageSourceProviderFactory.createPageSourceProvider();
        TrinoInputFile inputFile = fileSystem.newInputFile(path);
        long fileSize = inputFile.length();
        // TODO (https://github.com/trinodb/trino/issues/29958) memory usage reporting
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        return icebergPageSourceProvider.createPageSource(
                session,
                supportsRowLineage(formatVersion) ? withRowLineageColumns(columns) : columns,
                schema,
                partitionSpec,
                partitionData,
                deleteFiles.build(),
                DynamicFilter.EMPTY,
                TupleDomain.all(),
                TupleDomain.all(),
                path.toString(),
                0,
                fileSize,
                fileSize,
                fileCounts.get(path.toString()),
                fileFormat,
                tableCredentials,
                dataSequenceNumbers.containsKey(path.toString()) ? OptionalLong.of(dataSequenceNumbers.get(path.toString())) : OptionalLong.empty(),
                firstRowIds.containsKey(path.toString()) ? OptionalLong.of(firstRowIds.get(path.toString())) : OptionalLong.empty(),
                nameMapping.map(NameMappingParser::fromJson),
                formatVersion,
                false,
                memoryContext,
                Optional.empty());
    }
}
