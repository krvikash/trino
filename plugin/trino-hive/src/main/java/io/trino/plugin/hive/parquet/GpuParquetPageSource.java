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
package io.trino.plugin.hive.parquet;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.Table;
import com.google.common.base.Throwables;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * GPU-accelerated Parquet page source that reads entire fabricated Parquet files
 * using cuDF and returns a single GpuPage per split.
 */
public class GpuParquetPageSource
        implements ConnectorGpuPageSource
{
    private final ParquetFileFabricator fabricator;
    private final List<HiveColumnHandle> gpuColumns;
    private final List<ColumnMapping> columnMappings;

    private boolean finished;
    private ParquetFileFabricator.FabricatedParquet fabricatedParquet;

    public GpuParquetPageSource(
            ParquetFileFabricator fabricator,
            List<HiveColumnHandle> gpuColumns,
            List<ColumnMapping> columnMappings)
    {
        this.fabricator = requireNonNull(fabricator, "fabricator is null");
        this.gpuColumns = requireNonNull(gpuColumns, "gpuColumns is null");
        this.columnMappings = requireNonNull(columnMappings, "columnMappings is null");
    }

    @Override
    public @Move Result readNext()
    {
        if (finished) {
            return new Finished();
        }

        if (fabricatedParquet == null) {
            try {
                fabricatedParquet = fabricator.fabricate();
                fabricator.close(); // release
                return new Yielded();
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Failed to fabricate Parquet file", e);
            }
        }

        try {
            if (fabricatedParquet.rowCount() == 0) {
                finished = true;
                return new Finished();
            }

            @Own GpuPage page = readAndConvert();
            finished = true;
            return new Data(page);
        }
        finally {
            fabricatedParquet = null;
        }
    }

    private @Move GpuPage readAndConvert()
    {
        // cuDF doesn't support reading zero columns
        if (gpuColumns.isEmpty()) {
            return createGpuPageFromPrefilledColumns(toIntExact(fabricatedParquet.rowCount()));
        }

        // Build cuDF ParquetOptions with only the GPU columns
        ParquetOptions.Builder optionsBuilder = ParquetOptions.builder();
        for (HiveColumnHandle col : gpuColumns) {
            optionsBuilder.includeColumn(col.getBaseColumnName());
        }
        ParquetOptions options = optionsBuilder.build();

        byte[] data = fabricatedParquet.data().orElseThrow(() -> new IllegalStateException("No fabricated Parquet data available"));

        // Allocate host buffer and copy byte array data
        try (HostMemoryBuffer hostBuffer = HostMemoryBuffer.allocate(data.length)) {
            hostBuffer.setBytes(0, data, 0, data.length);

            // Read from fabricated buffer using cuDF
            try (Table table = Table.readParquet(options, hostBuffer)) {
                // Validate column types match expectations
                validateColumnTypes(table);

                // Convert to GpuPage
                return convertToGpuPage(table);
            }
        }
    }

    private GpuPage createGpuPageFromPrefilledColumns(int rowCount)
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            // When there are no GPU columns, we only have PREFILLED columns (partition keys, etc.)
            Column[] columns = new Column[columnMappings.size()];
            for (int i = 0; i < columnMappings.size(); i++) {
                ColumnMapping mapping = columnMappings.get(i);

                columns[i] = closer.register(switch (mapping.getKind()) {
                    case REGULAR ->
                        // This should never happen since gpuColumns is empty
                            throw new IllegalStateException("Found REGULAR column when gpuColumns is empty");

                    case PREFILLED -> {
                        // Partition key or other prefilled value: create RLE block
                        Block rleBlock = RunLengthEncodedBlock.create(
                                mapping.getHiveColumnHandle().getType(),
                                mapping.getPrefilledValue().getValue(),
                                rowCount);
                        yield new Column.Blocks(List.of(rleBlock));
                    }

                    case INTERIM, SYNTHESIZED, EMPTY -> throw new TrinoException(
                            HIVE_UNSUPPORTED_FORMAT,
                            format("GPU Parquet reader does not support column mapping kind: %s",
                                    mapping.getKind()));
                });
            }

            return new GpuPage(rowCount, columns);
        }
        catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private void validateColumnTypes(Table table)
    {
        if (table.getNumberOfColumns() != gpuColumns.size()) {
            throw new TrinoException(
                    HIVE_UNSUPPORTED_FORMAT,
                    format("Expected %d columns from cuDF but got %d", gpuColumns.size(), table.getNumberOfColumns()));
        }

        for (int i = 0; i < gpuColumns.size(); i++) {
            HiveColumnHandle column = gpuColumns.get(i);
            ColumnVector cudfColumn = table.getColumn(i);
            Type trinoType = column.getType();

            DType expectedDType = toDType(trinoType)
                    .orElseThrow(() -> new TrinoException(
                            HIVE_UNSUPPORTED_FORMAT,
                            format("Unsupported type for GPU: %s", trinoType)));

            if (!cudfColumn.getType().equals(expectedDType)) {
                throw new TrinoException(
                        HIVE_UNSUPPORTED_FORMAT,
                        format("Column %s type mismatch: expected %s but got %s",
                                column.getBaseColumnName(),
                                expectedDType,
                                cudfColumn.getType()));
            }
        }
    }

    private GpuPage convertToGpuPage(Table table)
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            Column[] columns = new Column[columnMappings.size()];
            int gpuColumnIndex = 0;
            int rowCount = toIntExact(table.getRowCount());

            for (int i = 0; i < columnMappings.size(); i++) {
                ColumnMapping mapping = columnMappings.get(i);

                columns[i] = closer.register(switch (mapping.getKind()) {
                    case REGULAR -> {
                        // GPU column: increment refcount before storing in DeviceMemory
                        ColumnVector cudfCol = table.getColumn(gpuColumnIndex++);
                        yield new Column.DeviceMemory(cudfCol.incRefCount());
                    }

                    case PREFILLED -> {
                        // Partition key or other prefilled value: create RLE block
                        Block rleBlock = RunLengthEncodedBlock.create(
                                mapping.getHiveColumnHandle().getType(),
                                mapping.getPrefilledValue().getValue(),
                                rowCount);
                        yield new Column.Blocks(List.of(rleBlock));
                    }

                    case INTERIM, SYNTHESIZED, EMPTY -> throw new TrinoException(
                            HIVE_UNSUPPORTED_FORMAT,
                            format("GPU Parquet reader does not support column mapping kind: %s",
                                    mapping.getKind()));
                });
            }

            return new GpuPage(rowCount, columns);
        }
        catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        fabricatedParquet = null;
        fabricator.close();
    }
}
