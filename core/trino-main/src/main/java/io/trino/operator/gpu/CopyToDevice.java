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
package io.trino.operator.gpu;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.operator.gpu.Column.Blocks;
import io.trino.operator.gpu.Column.DeviceMemory;
import io.trino.operator.gpu.borrow.Borrow;
import io.trino.operator.gpu.borrow.Move;
import io.trino.operator.gpu.borrow.Own;
import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class CopyToDevice
        implements GpuOperation
{
    private final GpuOperation source;
    private final List<Type> types;
    private final int columnCount;
    private final Set<Integer> copyColumns;

    public CopyToDevice(GpuOperation source, List<Type> types, Set<Integer> copyColumns)
    {
        this.source = requireNonNull(source, "source is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.columnCount = types.size();
        this.copyColumns = ImmutableSet.copyOf(requireNonNull(copyColumns, "copyColumns is null"));
        checkArgument(!copyColumns.isEmpty(), "No columns to copy");
        copyColumns.forEach(column -> checkArgument(
                0 <= column && column < columnCount,
                "Invalid column to copy: %s, there are %s columns",
                column,
                columnCount));
    }

    @Override
    public @Move Result execute()
    {
        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Finished finished -> finished;
            case Yielded yielded -> yielded;
            case Data(GpuPage page) -> {
                try (page) {
                    yield new Data(processPage(page));
                }
            }
        };
    }

    private @Move GpuPage processPage(@Borrow GpuPage page)
    {
        checkArgument(page.columnCount() == columnCount, "Page has wrong column count");
        @Own Column[] newColumns = new Column[page.columnCount()];
        try {
            for (int columnIndex = 0; columnIndex < page.columnCount(); columnIndex++) {
                if (copyColumns.contains(columnIndex)) {
                    newColumns[columnIndex] = switch (page.column(columnIndex)) {
                        case Blocks blocks -> new DeviceMemory(copyToDevice(blocks, types.get(columnIndex)));
                        case DeviceMemory deviceMemory ->
                            // already on the device
                                new DeviceMemory(deviceMemory.columnVector().incRefCount());
                    };
                }
                else {
                    newColumns[columnIndex] = switch (page.column(columnIndex)) {
                        case Blocks blocks -> blocks;
                        case DeviceMemory deviceMemory -> new DeviceMemory(deviceMemory.columnVector().incRefCount());
                    };
                }
            }

            return new GpuPage(page.positionCount(), newColumns);
        }
        finally {
            for (Column column : newColumns) {
                if (column != null) {
                    column.close();
                }
            }
        }
    }

    @Override
    public void close()
    {
        source.close();
    }

    private @Move ColumnVector copyToDevice(Blocks blocks, Type type)
    {
        if (type == BOOLEAN) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == TINYINT) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == SMALLINT) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == INTEGER) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == BIGINT) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == REAL) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == DOUBLE) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof DecimalType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == NUMBER) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof CharType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof VarcharType) {
            return copyVarcharToDevice(blocks);
        }
        if (type instanceof VarbinaryType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type == DATE) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimeType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimeWithTimeZoneType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimestampType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        if (type instanceof TimestampWithTimeZoneType) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private @Move ColumnVector copyVarcharToDevice(Blocks blocks)
    {
        // TODO (https://starburstdata.atlassian.net/browse/ENG-9841): Optimize Block → Device memory transfer, avoid HostColumnVector
        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.STRING, blocks.positionCount())) {
            for (Block block : blocks.blocks()) {
                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                    if (block.isNull(blockPosition)) {
                        builder.appendNull();
                    }
                    else {
                        Slice slice = VarcharType.VARCHAR.getSlice(block, blockPosition);
                        builder.appendUTF8String(slice.byteArray(), slice.byteArrayOffset(), slice.length());
                    }
                }
            }
            return builder.buildAndPutOnDevice();
        }
    }
}
