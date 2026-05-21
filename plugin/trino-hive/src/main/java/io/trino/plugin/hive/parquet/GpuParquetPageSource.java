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

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.Table;
import com.google.common.base.Throwables;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

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
    private @Own ParquetFileFabricator.FabricatedParquet fabricatedParquet;

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

        // Force timestamp columns (including INT96) to be read at microsecond precision. cuDF's default
        // decodes INT96 as int64 nanoseconds and silently wraps values outside ~1677..2262; with
        // TIMESTAMP_MICROSECONDS the reader produces valid micros across the full INT96 range. All
        // Trino short-timestamp precisions fit in microseconds, so no precision is lost.
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
            throw new TrinoException(
                    HIVE_UNSUPPORTED_FORMAT,
                    format("Expected %d columns from cuDF but got %d", gpuColumns.size(), table.getNumberOfColumns()));
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
                        @Borrow ColumnVector cudfCol = table.getColumn(index);
                        Type trinoType = gpuColumn.getType();
                        DType expectedDType = toDType(trinoType)
                                .orElseThrow(() -> new TrinoException(
                                        HIVE_UNSUPPORTED_FORMAT,
                                        format("Unsupported type for GPU: %s", trinoType)));
                        @Own ColumnVector evolved = evolveColumn(gpuColumn.getBaseColumnName(), cudfCol, expectedDType, trinoType);
                        yield new Column.DeviceMemory(evolved);
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

    /**
     * Evolve a cuDF column to the expected DType, applying a cast when the Parquet physical type
     * differs from the Trino logical type.
     * <ul>
     *   <li>Timestamp family (any unit → any unit): handles INT96/INT64 MILLIS read as MICROS
     *       (due to withTimeUnit) being downcast to the precision Trino expects.</li>
     *   <li>Decimal family (DECIMAL32/64/128 → DECIMAL64/128): Hive writes FIXED_LEN_BYTE_ARRAY
     *       (read by cuDF as DECIMAL128); external writers may use INT32/INT64-backed decimals.
     *       The target is DECIMAL64 for short Trino decimals (precision ≤ 18) and DECIMAL128 for
     *       long Trino decimals.</li>
     *   <li>Integer widening (INT8/INT16/INT32 → INT16/INT32/INT64): covers schema evolution
     *       where a column was widened after the table was written.</li>
     *   <li>Integer → decimal: INT32/INT64-backed Parquet decimals where cuDF returns a plain
     *       integer type rather than a decimal type.</li>
     * </ul>
     */
    private static @Move ColumnVector evolveColumn(String columnName, @Borrow ColumnVector cudfCol, DType expectedDType, Type trinoType)
    {
        DType actualDType = cudfCol.getType();
        if (actualDType.equals(expectedDType)) {
            return cudfCol.incRefCount();
        }
        if (actualDType.isTimestampType() && expectedDType.isTimestampType()) {
            return cudfCol.castTo(expectedDType);
        }
        if (actualDType.isDecimalType() && expectedDType.isDecimalType()) {
            return cudfCol.castTo(expectedDType);
        }
        if (isIntegerType(actualDType) && (isIntegerType(expectedDType) || expectedDType.isDecimalType())
                && expectedDType.getSizeInBytes() >= actualDType.getSizeInBytes()) {
            return cudfCol.castTo(expectedDType);
        }
        if (trinoType instanceof VarbinaryType && actualDType.equals(DType.STRING) && expectedDType.equals(DType.LIST)) {
            // cuDF reads Parquet BINARY as STRING; reinterpret the byte payload as LIST<UINT8>.
            // The STRING data buffer becomes the child UINT8 column; offsets and validity carry
            // over unchanged.
            BaseDeviceMemoryBuffer dataBuffer = cudfCol.getData();
            long childRowCount = dataBuffer == null ? 0 : dataBuffer.getLength();
            try (ColumnView childView = new ColumnView(DType.UINT8, childRowCount, Optional.of(0L), dataBuffer, null);
                    ColumnView listView = new ColumnView(DType.LIST, cudfCol.getRowCount(),
                            Optional.of(cudfCol.getNullCount()),
                            cudfCol.getValid(), cudfCol.getOffsets(), new ColumnView[] {childView})) {
                return listView.copyToColumnVector();
            }
        }
        throw new TrinoException(
                HIVE_UNSUPPORTED_FORMAT,
                format("Column %s: cannot evolve cuDF type %s to expected type %s",
                        columnName, actualDType, expectedDType));
    }

    private static boolean isIntegerType(DType dtype)
    {
        return dtype == DType.INT8 || dtype == DType.INT16 || dtype == DType.INT32 || dtype == DType.INT64;
    }

    @Override
    public void close()
    {
        if (fabricatedParquet != null) {
            fabricatedParquet.close();
            fabricatedParquet = null;
        }
        fabricator.close();
    }

    // TODO deduplicate with io.trino.operator.gpu.GpuUtils.closeColumns
    private static void closeColumns(@Move Column[] columns)
    {
        for (Column column : columns) {
            if (column != null) {
                column.close();
            }
        }
    }
}
