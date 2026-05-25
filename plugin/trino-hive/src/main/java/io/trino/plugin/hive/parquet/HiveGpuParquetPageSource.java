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
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.base.Throwables;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.gpu.GpuTypeConversion.toGpuMapping;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * GPU-accelerated Parquet page source that reads entire fabricated Parquet files
 * using cuDF and returns a single GpuPage per split.
 */
public class HiveGpuParquetPageSource
        implements ConnectorGpuPageSource
{
    private final ConnectorGpuMemoryContext memoryContext;
    private final ParquetFileFabricator fabricator;
    private final List<HiveColumnHandle> gpuColumns;
    private final List<ColumnMapping> columnMappings;

    private boolean finished;
    private @Own ParquetFileFabricator.FabricatedParquet fabricatedParquet;

    public HiveGpuParquetPageSource(
            ConnectorGpuMemoryContext memoryContext,
            ParquetFileFabricator fabricator,
            List<HiveColumnHandle> gpuColumns,
            List<ColumnMapping> columnMappings)
    {
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
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
            fabricatedParquet = fabricator.fabricate();
            return new Yielded();
        }

        try {
            if (fabricatedParquet.rowCount() == 0) {
                finished = true;
                return new Finished();
            }

            try (var page = ClosingRef.own(readAndConvert());
                    // TODO pre-allocate before the page gets into GPU memory
                    var allocation = ClosingRef.own(memoryContext.allocate(page.borrow().retainedMemory()))) {
                finished = true;
                return new Data(allocation.take(), page.take());
            }
        }
        finally {
            fabricatedParquet.close();
            fabricatedParquet = null;
        }
    }

    private @Move GpuPage readAndConvert()
    {
        // cuDF doesn't support reading zero columns
        if (gpuColumns.isEmpty()) {
            return createGpuPageFromPrefilledColumns(toIntExact(fabricatedParquet.rowCount()));
        }

        // Force timestamp columns (including INT96) to be read at microsecond precision.
        // cuDF's INT96 decode silently overflows int64 (https://github.com/rapidsai/cudf/issues/22930):
        // with TIMESTAMP_NANOSECONDS the valid range is only ~1677..2262;
        // with TIMESTAMP_MICROSECONDS it extends to ~year ±292,271.
        ParquetOptions.Builder optionsBuilder = ParquetOptions.builder()
                .withTimeUnit(DType.TIMESTAMP_MICROSECONDS);
        for (HiveColumnHandle col : gpuColumns) {
            optionsBuilder.includeColumn(col.getBaseColumnName());
        }
        ParquetOptions options = optionsBuilder.build();

        @Borrow Buffers data = fabricatedParquet.data().orElseThrow(() -> new IllegalStateException("No fabricated Parquet data available"));

        // Read from the fabricated buffers using cuDF
        try (Table table = Table.readParquet(options, data.buffers().toArray(HostMemoryBuffer[]::new))) {
            return convertToGpuPage(table);
        }
    }

    private GpuPage createGpuPageFromPrefilledColumns(int rowCount)
    {
        @Own Column[] columns = new Column[columnMappings.size()];
        try {
            // When there are no GPU columns, we only have PREFILLED columns (partition keys, etc.)
            for (int i = 0; i < columnMappings.size(); i++) {
                ColumnMapping mapping = columnMappings.get(i);

                columns[i] = switch (mapping.getKind()) {
                    // This should never happen since gpuColumns is empty
                    case REGULAR -> throw new IllegalStateException("Found REGULAR column when gpuColumns is empty");

                    // Partition key or other prefilled value
                    case PREFILLED -> prefilledColumn(mapping, rowCount);

                    case INTERIM, SYNTHESIZED, EMPTY -> throw new TrinoException(
                            HIVE_UNSUPPORTED_FORMAT,
                            "GPU Parquet reader does not support column mapping kind: " + mapping.getKind());
                };
            }

            return new GpuPage(rowCount, columns);
        }
        catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            closeColumns(columns);
        }
    }

    private @Move GpuPage convertToGpuPage(@Borrow Table table)
    {
        if (table.getNumberOfColumns() != gpuColumns.size()) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, format("Expected %d columns from cuDF but got %d", gpuColumns.size(), table.getNumberOfColumns()));
        }

        @Own Column[] columns = new Column[columnMappings.size()];
        try {
            int gpuColumnIndex = 0;
            int rowCount = toIntExact(table.getRowCount());

            for (int i = 0; i < columnMappings.size(); i++) {
                ColumnMapping mapping = columnMappings.get(i);

                columns[i] = switch (mapping.getKind()) {
                    case REGULAR -> {
                        int index = gpuColumnIndex++;
                        HiveColumnHandle gpuColumn = gpuColumns.get(index);
                        @Borrow ColumnVector cudfColumn = table.getColumn(index);
                        Type trinoType = gpuColumn.getType();
                        DType expectedDType = toDType(trinoType)
                                .orElseThrow(() -> new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Unsupported type for GPU: " + trinoType));
                        @Own ColumnVector evolved = GpuColumnEvolution.evolveColumn(gpuColumn.getBaseColumnName(), cudfColumn, expectedDType, trinoType);
                        yield new Column.DeviceMemory(evolved);
                    }

                    // Partition key or other prefilled value
                    case PREFILLED -> prefilledColumn(mapping, rowCount);

                    case INTERIM, SYNTHESIZED, EMPTY -> throw new TrinoException(
                            HIVE_UNSUPPORTED_FORMAT,
                            "GPU Parquet reader does not support column mapping kind: " + mapping.getKind());
                };
            }

            return new GpuPage(rowCount, columns);
        }
        catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            closeColumns(columns);
        }
    }

    private static @Move Column prefilledColumn(ColumnMapping mapping, int rowCount)
    {
        checkArgument(mapping.getKind() == HivePageSourceProvider.ColumnMappingKind.PREFILLED, "Invalid mapping kind: %s", mapping.getKind());
        // Currently (https://starburstdata.atlassian.net/browse/ENG-9808) GPU execution does not support mixed GPU/CPU data so, create a ColumnVector
        Type type = mapping.getHiveColumnHandle().getType();
        try (Scalar scalar = toGpuMapping(type)
                .orElseThrow(() -> new UnsupportedOperationException("Unsupported type: " + type))
                .toScalar()
                .copyToScalar(Optional.ofNullable(mapping.getPrefilledValue().getValue()))) {
            return new Column.DeviceMemory(ColumnVector.fromScalar(scalar, rowCount));
        }
    }

    @Override
    public void close()
    {
        if (fabricatedParquet != null) {
            fabricatedParquet.close();
            fabricatedParquet = null;
        }
    }
}
