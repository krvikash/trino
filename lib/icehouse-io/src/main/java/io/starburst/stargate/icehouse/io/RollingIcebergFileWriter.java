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
package io.starburst.stargate.icehouse.io;

import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.starburst.stargate.icehouse.spi.file.FileFormat;
import io.starburst.stargate.icehouse.spi.file.OutputFile;
import io.starburst.stargate.icehouse.task.primitives.ThrowingFunction;
import io.trino.FeaturesConfig;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.metadata.TypeRegistry;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.iceberg.IcebergConfig.VariantMapping;
import io.trino.plugin.iceberg.IcebergFileWriter;
import io.trino.plugin.iceberg.IcebergParquetFileWriter;
import io.trino.plugin.iceberg.IcebergTypeManager;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.util.PrimitiveTypeMapBuilder;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.InternalTypeManager;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.stargate.icehouse.spi.file.FileMetrics.fromIcebergMetrics;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * An Iceberg file writer that automatically rolls over to new files when they reach the {@link #targetFileSize} bytes.
 * It accepts a {@link #completedFileConsumer} that is invoked when a file is completed.
 */
public class RollingIcebergFileWriter
        implements IcebergFileWriter
{
    private static final IcebergTypeManager TYPE_MANAGER = new IcebergTypeManager(
            new InternalTypeManager(new TypeRegistry(new TypeOperators(), new FeaturesConfig())),
            VariantMapping.VARIANT);
    private static final String TRINO_VERSION = requireNonNullElse(IcebergUtil.class.getPackage().getImplementationVersion(), "unknown");

    private final TrinoFileSystem fileSystem;
    private final Supplier<Location> locationSupplier;
    private final ThrowingFunction<Location, IcebergFileWriter, IOException> icebergFileWriterCreator;
    private final Consumer<OutputFile> completedFileConsumer;
    private final long targetFileSize;
    private final Closer rollbackCloser = Closer.create();

    private Location currentWriterLocation;
    private IcebergFileWriter currentWriter;

    public RollingIcebergFileWriter(
            TrinoFileSystem fileSystem,
            Supplier<Location> locationSupplier,
            ThrowingFunction<Location, IcebergFileWriter, IOException> icebergFileWriterCreator,
            Consumer<OutputFile> completedFileConsumer,
            long targetFileSize)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.locationSupplier = requireNonNull(locationSupplier, "locationSupplier is null");
        this.icebergFileWriterCreator = requireNonNull(icebergFileWriterCreator, "icebergFileWriterCreator is null");
        this.completedFileConsumer = requireNonNull(completedFileConsumer, "completedFileConsumer is null");
        this.targetFileSize = targetFileSize;
    }

    @Override
    public FileMetrics getFileMetrics()
    {
        throw new IllegalStateException("getFileMetrics should not be called on RollingIcebergFileWriter");
    }

    @Override
    public long getWrittenBytes()
    {
        return 0;
    }

    @Override
    public long getMemoryUsage()
    {
        if (currentWriter != null) {
            return currentWriter.getMemoryUsage();
        }

        return 0;
    }

    @Override
    public void appendRows(Page dataPage)
    {
        try {
            if (currentWriter == null || currentWriter.getWrittenBytes() > targetFileSize) {
                finalizeCurrentWriter();
                currentWriterLocation = locationSupplier.get();
                currentWriter = icebergFileWriterCreator.apply(currentWriterLocation);
            }
            currentWriter.appendRows(dataPage);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public RollbackAction commit()
    {
        try {
            finalizeCurrentWriter();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return rollbackCloser::close;
    }

    private void finalizeCurrentWriter()
            throws IOException
    {
        if (currentWriter == null) {
            return;
        }
        RollbackAction rollbackAction = currentWriter.commit();
        Metrics metrics = currentWriter.getFileMetrics().metrics();
        Long recordCount = metrics.recordCount();
        if (recordCount != null && recordCount == 0) {
            // Skip emitting empty data files (e.g. when all input rows were filtered out by delete files during compaction).
            // Delete the just-written file immediately rather than relying on orphan file removal.
            rollbackAction.run();
            return;
        }
        rollbackCloser.register(rollbackAction::run);
        long fileLength = fileSystem.newInputFile(currentWriterLocation).length();
        completedFileConsumer.accept(new OutputFile(
                currentWriterLocation.toString(),
                FileFormat.PARQUET,
                fileLength,
                fromIcebergMetrics(metrics),
                currentWriter.getFileMetrics().splitOffsets().orElseThrow()));
    }

    @Override
    public void rollback()
    {
        try {
            rollbackCloser.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    public static IcebergFileWriter createParquetWriter(
            TrinoFileSystem fileSystem,
            Schema schema,
            Location outputPath,
            Optional<DataSize> maxRowGroupSize)
            throws IOException
    {
        return createParquetWriter(fileSystem, schema, outputPath, maxRowGroupSize, CompressionCodec.ZSTD);
    }

    public static IcebergFileWriter createParquetWriter(
            TrinoFileSystem fileSystem,
            Schema schema,
            Location outputPath,
            Optional<DataSize> maxRowGroupSize,
            CompressionCodec compressionCodec)
            throws IOException
    {
        List<String> fileColumnNames = schema.columns().stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> fileColumnTypes = schema.columns().stream()
                .map(column -> toTrinoType(column.type(), TYPE_MANAGER))
                .collect(toImmutableList());

        TrinoOutputFile outputFile = fileSystem.newOutputFile(outputPath);

        // iceberg-arrow's vectorized parquet reader has a known issue reading DELTA_LENGTH_BYTE_ARRAY (apache/iceberg#17017)
        ParquetWriterOptions.Builder optionsBuilder = ParquetWriterOptions.builder()
                .setUseDeltaLengthByteArrayEncoding(false);
        maxRowGroupSize.ifPresent(optionsBuilder::setMaxBlockSize);
        return new IcebergParquetFileWriter(
                MetricsConfig.getDefault(),
                outputFile,
                () -> {},
                fileColumnTypes,
                fileColumnNames,
                ParquetSchemaUtil.convert(schema, "table"),
                PrimitiveTypeMapBuilder.makeTypeMap(fileColumnTypes, fileColumnNames),
                optionsBuilder.build(),
                IntStream.range(0, fileColumnNames.size()).toArray(),
                compressionCodec,
                TRINO_VERSION);
    }
}
