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
import io.trino.plugin.iceberg.delete.PositionDeleteWriter;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.DeleteFileSet;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergMergeSink
        extends AbstractIcebergMergeSink
{
    private final int formatVersion;

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
            int columnCount)
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
                columnCount);
        this.formatVersion = formatVersion;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());

        fileDeletions.forEach((dataFilePath, deletion) -> {
            PositionDeleteWriter writer = createPositionDeleteWriter(
                    dataFilePath.toStringUtf8(),
                    partitionsSpecs.get(deletion.partitionSpecId()),
                    deletion.partitionDataJson());

            fragments.addAll(writePositionDeletes(writer, deletion.rowsToDelete()));
        });

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

        return new PositionDeleteWriter(
                dataFilePath,
                partitionSpec,
                partitionData,
                locationProvider,
                fileWriterFactory,
                fileSystem,
                jsonCodec,
                session,
                formatVersion,
                fileFormat,
                storageProperties,
                previousDeleteFiles);
    }

    private static Collection<Slice> writePositionDeletes(PositionDeleteWriter writer, ImmutableLongBitmapDataProvider rowsToDelete)
    {
        try {
            return writer.write(rowsToDelete);
        }
        catch (Throwable t) {
            closeAllSuppress(t, writer::abort);
            throw t;
        }
    }
}
