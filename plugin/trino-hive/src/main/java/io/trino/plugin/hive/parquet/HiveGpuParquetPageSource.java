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
import ai.rapids.cudf.ParquetChunkedReader;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.base.Throwables;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.gpu.GpuTypeConversion.toGpuMapping;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.min;
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
    private final long footerCompletedBytes;
    private final long footerReadTimeNanos;
    private final long maxPageSizeBytes;

    private boolean finished;
    @Own
    @Nullable
    private ParquetFileFabricator.FabricatedParquet fabricatedParquet;
    @Own
    @Nullable
    private ParquetChunkedReader chunkedReader;
    // Zero-column reads (e.g. COUNT(*)) synthesize prefilled pages; emit them in row-bounded chunks.
    private int emittedPrefilledRows;
    private int prefilledRowsPerPage;

    public HiveGpuParquetPageSource(
            ConnectorGpuMemoryContext memoryContext,
            ParquetFileFabricator fabricator,
            List<HiveColumnHandle> gpuColumns,
            List<ColumnMapping> columnMappings,
            long footerCompletedBytes,
            long footerReadTimeNanos,
            long maxPageSizeBytes)
    {
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.fabricator = requireNonNull(fabricator, "fabricator is null");
        this.gpuColumns = requireNonNull(gpuColumns, "gpuColumns is null");
        this.columnMappings = requireNonNull(columnMappings, "columnMappings is null");
        this.footerCompletedBytes = footerCompletedBytes;
        this.footerReadTimeNanos = footerReadTimeNanos;
        this.maxPageSizeBytes = maxPageSizeBytes;
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

        if (fabricatedParquet.rowCount() == 0) {
            finished = true;
            close();
            return new Finished();
        }

        // cuDF doesn't support reading zero columns, so a zero-column read (e.g. COUNT(*)) synthesizes
        // prefilled columns. Emit them in row-bounded chunks so a large split doesn't materialize one
        // oversized page.
        if (gpuColumns.isEmpty()) {
            int totalRows = toIntExact(fabricatedParquet.rowCount());
            if (prefilledRowsPerPage == 0) {
                prefilledRowsPerPage = prefilledRowsPerPage(totalRows);
            }
            int rows = min(prefilledRowsPerPage, totalRows - emittedPrefilledRows);
            if (rows <= 0) {
                finished = true;
                close();
                return new Finished();
            }
            try (var page = ClosingRef.own(createGpuPageFromPrefilledColumns(rows));
                    var allocation = ClosingRef.own(memoryContext.allocate(page.borrow().retainedMemory()))) {
                emittedPrefilledRows += rows;
                return new Data(allocation.take(), page.take());
            }
        }

        // Decode the split in bounded chunks so no single GpuPage exceeds maxPageSizeBytes.
        if (chunkedReader == null) {
            chunkedReader = createChunkedReader();
        }
        // cuDF's ParquetChunkedReader is driven by hasNext(); readChunk() alone does not advance to
        // end-of-data. readChunk() may return null or an empty table when a step yields no rows.
        while (chunkedReader.hasNext()) {
            try (Table table = chunkedReader.readChunk()) {
                if (table == null || table.getRowCount() == 0) {
                    continue;
                }
                try (var page = ClosingRef.own(convertToGpuPage(table));
                        var allocation = ClosingRef.own(memoryContext.allocate(page.borrow().retainedMemory()))) {
                    return new Data(allocation.take(), page.take());
                }
            }
        }
        finished = true;
        close();
        return new Finished();
    }

    // Rows per synthetic prefilled page
    private int prefilledRowsPerPage(int totalRows)
    {
        verify(totalRows > 0, "totalRows must be positive");
        long bytesPerRow;
        try (GpuPage singleRow = createGpuPageFromPrefilledColumns(1)) {
            MemoryAmount retainedMemory = singleRow.retainedMemory();
            bytesPerRow = retainedMemory.heapBytes() + retainedMemory.offHeapBytes() + retainedMemory.gpuDeviceBytes();
        }
        return clamp(maxPageSizeBytes / max(bytesPerRow, 1), 1, totalRows);
    }

    private @Own ParquetChunkedReader createChunkedReader()
    {
        // Force timestamp columns (including INT96) to be read at microsecond precision.
        // cuDF's INT96 decode silently overflows int64 (https://github.com/rapidsai/cudf/issues/22930):
        // with TIMESTAMP_NANOSECONDS the valid range is only ~1677..2262;
        // with TIMESTAMP_MICROSECONDS it extends to ~year ±292,271.
        ParquetOptions.Builder optionsBuilder = ParquetOptions.builder()
                .withTimeUnit(DType.TIMESTAMP_MICROSECONDS);
        for (HiveColumnHandle col : gpuColumns) {
            optionsBuilder.includeColumn(col.getBaseColumnName());
        }

        @Borrow Buffers data = fabricatedParquet.data().orElseThrow(() -> new IllegalStateException("No fabricated Parquet data available"));
        // passReadLimit 0 = unlimited; chunkSizeByteLimit bounds each emitted chunk's device size.
        return new ParquetChunkedReader(maxPageSizeBytes, /*passReadLimit=*/ 0, optionsBuilder.build(), data.buffers().toArray(HostMemoryBuffer[]::new));
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
    public long getCompletedBytes()
    {
        return footerCompletedBytes + fabricator.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return footerReadTimeNanos + fabricator.getReadTimeNanos();
    }

    @Override
    public void close()
    {
        finished = true;
        // Close the reader before the host buffers it reads from.
        if (chunkedReader != null) {
            chunkedReader.close();
            chunkedReader = null;
        }
        if (fabricatedParquet != null) {
            fabricatedParquet.close();
            fabricatedParquet = null;
        }
    }
}
