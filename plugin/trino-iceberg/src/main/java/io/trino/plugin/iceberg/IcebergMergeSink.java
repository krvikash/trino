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

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.delete.DeletionVector;
import io.trino.plugin.iceberg.delete.PositionDeleteWriter;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.DeleteFileSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergMergeSink
        extends AbstractIcebergMergeSink
{
    public IcebergMergeSink(
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            TrinoFileSystem fileSystem,
            Map<String, DeleteFileSet> previousDeleteFiles,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            int formatVersion,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties,
            Schema schema,
            Map<Integer, PartitionSpec> partitionsSpecs,
            ConnectorPageSink insertPageSink,
            Optional<ConnectorPageSink> updateInsertPageSink,
            int columnCount,
            IcebergPageSourceProviderFactory pageSourceProviderFactory,
            List<IcebergColumnHandle> columns,
            Map<String, String> fileIoProperties,
            Map<String, Long> fileCounts,
            Map<String, Long> dataSequenceNumbers,
            Map<String, Long> firstRowIds,
            Optional<String> nameMapping)
    {
        super(
                locationProvider,
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
                fileIoProperties,
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
        writtenBytes = insertPageSink.getCompletedBytes();

        updateInsertPageSink.ifPresent(pageSink -> fragments.addAll(pageSink.finish().join()));

        fileDeletions.forEach((dataFilePath, deletion) -> deletion.rowsToDelete().build().ifPresent(deletionVector -> {
            PositionDeleteWriter writer = createPositionDeleteWriter(
                    dataFilePath.toStringUtf8(),
                    partitionsSpecs.get(deletion.partitionSpecId()),
                    deletion.partitionDataJson());

            fragments.add(writePositionDeletes(writer, deletionVector));
        }));

        return completedFuture(fragments);
    }

    private PositionDeleteWriter createPositionDeleteWriter(String dataFilePath, PartitionSpec partitionSpec, String partitionDataJson)
    {
        Optional<PartitionData> partitionData = Optional.empty();
        if (partitionSpec.isPartitioned()) {
            Type[] columnTypes = partitionSpec.fields().stream()
                    .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                    .toArray(Type[]::new);
            partitionData = Optional.of(PartitionData.fromJson(partitionDataJson, columnTypes));
        }

        IcebergPageSourceProvider icebergPageSourceProvider = (IcebergPageSourceProvider) pageSourceProviderFactory.createPageSourceProvider();
        return new PositionDeleteWriter(
                dataFilePath,
                partitionSpec,
                partitionData,
                locationProvider,
                fileWriterFactory,
                icebergPageSourceProvider.deletePageSourceProvider(session, fileSystem, formatVersion),
                fileSystem,
                session,
                formatVersion,
                fileFormat,
                storageProperties,
                previousDeleteFiles);
    }

    private Slice writePositionDeletes(PositionDeleteWriter writer, DeletionVector rowsToDelete)
    {
        try {
            CommitTaskData task = writer.write(rowsToDelete);
            writtenBytes += task.fileSizeInBytes();
            return wrappedBuffer(jsonCodec.toJsonBytes(task));
        }
        catch (Throwable t) {
            closeAllSuppress(t, writer::abort);
            throw t;
        }
    }
}
