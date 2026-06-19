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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.hive.parquet.Buffers;
import io.trino.plugin.hive.parquet.ParquetFileFabricator;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.hive.parquet.GpuColumnEvolution.evolveColumn;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.gpu.GpuTypeConversion.toGpuMapping;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IcebergGpuParquetPageSource
        implements ConnectorGpuPageSource
{
    public sealed interface GpuOutputColumn
    {
        IcebergColumnHandle column();
    }

    record GpuParquetFileColumn(IcebergColumnHandle column, String parquetName, int parquetIndex)
            implements GpuOutputColumn
    {
        GpuParquetFileColumn
        {
            requireNonNull(column, "column is null");
            requireNonNull(parquetName, "parquetName is null");
        }
    }

    record GpuConstantColumn(IcebergColumnHandle column, Type type, @Nullable Object value)
            implements GpuOutputColumn
    {
        GpuConstantColumn
        {
            requireNonNull(column, "column is null");
            requireNonNull(type, "type is null");
        }
    }

    private final ConnectorGpuMemoryContext memoryContext;
    private final ParquetFileFabricator fabricator;
    private final List<GpuOutputColumn> outputColumns;
    private final String[] parquetColumnNames;

    private boolean finished;
    private @Nullable @Own ParquetFileFabricator.FabricatedParquet fabricatedParquet;

    public IcebergGpuParquetPageSource(ConnectorGpuMemoryContext memoryContext, ParquetFileFabricator fabricator, List<GpuOutputColumn> outputColumns)
    {
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.fabricator = requireNonNull(fabricator, "fabricator is null");
        this.parquetColumnNames = outputColumns.stream()
                .filter(GpuParquetFileColumn.class::isInstance)
                .map(GpuParquetFileColumn.class::cast)
                .map(GpuParquetFileColumn::parquetName)
                .toArray(String[]::new);
        this.outputColumns = ImmutableList.copyOf(outputColumns);
    }

    @Override
    public @Move Result readNext()
    {
        if (finished) {
            return new Finished();
        }

        if (fabricatedParquet == null) {
            fabricatedParquet = fabricator.fabricate();
            fabricator.close();
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
        if (parquetColumnNames.length == 0) {
            return createPageFromConstants(toIntExact(fabricatedParquet.rowCount()));
        }

        ParquetOptions options = ParquetOptions.builder()
                .includeColumn(parquetColumnNames)
                .build();

        @Borrow Buffers data = fabricatedParquet.data().orElseThrow(() -> new IllegalStateException("No fabricated Parquet data available"));

        try (Table table = Table.readParquet(options, data.buffers().toArray(HostMemoryBuffer[]::new))) {
            return convertToGpuPage(table);
        }
    }

    private @Move GpuPage createPageFromConstants(int rowCount)
    {
        @Own Column[] columns = new Column[outputColumns.size()];
        try {
            for (int i = 0; i < outputColumns.size(); i++) {
                columns[i] = createConstantColumn((GpuConstantColumn) outputColumns.get(i), rowCount);
            }
            return new GpuPage(rowCount, columns);
        }
        finally {
            closeColumns(columns);
        }
    }

    private @Move GpuPage convertToGpuPage(@Borrow Table table)
    {
        if (table.getNumberOfColumns() != parquetColumnNames.length) {
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, format("Expected %d columns from cuDF but got %d", parquetColumnNames.length, table.getNumberOfColumns()));
        }

        @Own Column[] columns = new Column[outputColumns.size()];
        int rowCount = toIntExact(table.getRowCount());
        try {
            for (int i = 0; i < outputColumns.size(); i++) {
                columns[i] = switch (outputColumns.get(i)) {
                    case GpuParquetFileColumn parquetColumn -> {
                        @Borrow ColumnVector cudfColumn = table.getColumn(parquetColumn.parquetIndex());
                        Type trinoType = parquetColumn.column().getType();
                        DType expectedDType = toDType(trinoType)
                                .orElseThrow(() -> new UnsupportedOperationException("Unsupported type: " + trinoType));
                        @Own ColumnVector evolved = evolveColumn(parquetColumn.column().getName(), cudfColumn, expectedDType, trinoType);
                        yield new Column.DeviceMemory(evolved);
                    }
                    case GpuConstantColumn constantColumn -> createConstantColumn(constantColumn, rowCount);
                };
            }
            return new GpuPage(rowCount, columns);
        }
        finally {
            closeColumns(columns);
        }
    }

    private static @Move Column createConstantColumn(GpuConstantColumn constantColumn, int rowCount)
    {
        Type type = constantColumn.type();
        try (Scalar scalar = toGpuMapping(type)
                .orElseThrow(() -> new UnsupportedOperationException("Unsupported type: " + type))
                .toScalar()
                .copyToScalar(Optional.ofNullable(constantColumn.value()))) {
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
        fabricator.close();
    }
}
