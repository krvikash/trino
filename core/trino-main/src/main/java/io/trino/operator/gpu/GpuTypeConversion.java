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
import ai.rapids.cudf.Scalar;
import io.airlift.slice.Slice;
import io.trino.operator.gpu.Column.Blocks;
import io.trino.operator.gpu.borrow.Move;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public final class GpuTypeConversion
{
    private GpuTypeConversion() {}

    public static boolean isConvertible(Type type)
    {
        return toDType(type).isPresent();
    }

    public static Optional<DType> toDType(Type type)
    {
        return toGpuMapping(type)
                .map(GpuTypeMapping::dType);
    }

    public static Optional<GpuTypeMapping> toGpuMapping(Type type)
    {
        requireNonNull(type, "type is null");

        if (type == BOOLEAN) {
            return Optional.of(new GpuTypeMapping(
                    DType.BOOL8,
                    value -> Scalar.fromBool((Boolean) value.orElse(null)),
                    blocks -> {
                        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.BOOL8, blocks.positionCount())) {
                            for (Block block : blocks.blocks()) {
                                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                                    if (block.isNull(blockPosition)) {
                                        builder.appendNull();
                                    }
                                    else {
                                        builder.append(BOOLEAN.getBoolean(block, blockPosition));
                                    }
                                }
                            }
                            return builder.buildAndPutOnDevice();
                        }
                    }));
        }

        if (type == TINYINT) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT8,
                    value -> Scalar.fromByte(value.map(v -> ((Long) v).byteValue()).orElse(null)),
                    blocks -> {
                        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.INT8, blocks.positionCount())) {
                            for (Block block : blocks.blocks()) {
                                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                                    if (block.isNull(blockPosition)) {
                                        builder.appendNull();
                                    }
                                    else {
                                        builder.append(TINYINT.getByte(block, blockPosition));
                                    }
                                }
                            }
                            return builder.buildAndPutOnDevice();
                        }
                    }));
        }

        if (type == SMALLINT) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT16,
                    value -> Scalar.fromShort(value.map(v -> ((Long) v).shortValue()).orElse(null)),
                    blocks -> {
                        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.INT16, blocks.positionCount())) {
                            for (Block block : blocks.blocks()) {
                                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                                    if (block.isNull(blockPosition)) {
                                        builder.appendNull();
                                    }
                                    else {
                                        builder.append(SMALLINT.getShort(block, blockPosition));
                                    }
                                }
                            }
                            return builder.buildAndPutOnDevice();
                        }
                    }));
        }

        if (type == INTEGER) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT32,
                    value -> Scalar.fromInt(value.map(v -> ((Long) v).intValue()).orElse(null)),
                    blocks -> {
                        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.INT32, blocks.positionCount())) {
                            for (Block block : blocks.blocks()) {
                                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                                    if (block.isNull(blockPosition)) {
                                        builder.appendNull();
                                    }
                                    else {
                                        builder.append(INTEGER.getInt(block, blockPosition));
                                    }
                                }
                            }
                            return builder.buildAndPutOnDevice();
                        }
                    }));
        }

        if (type == BIGINT) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT64,
                    value -> Scalar.fromLong((Long) value.orElse(null)),
                    blocks -> {
                        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.INT64, blocks.positionCount())) {
                            for (Block block : blocks.blocks()) {
                                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                                    if (block.isNull(blockPosition)) {
                                        builder.appendNull();
                                    }
                                    else {
                                        builder.append(BIGINT.getLong(block, blockPosition));
                                    }
                                }
                            }
                            return builder.buildAndPutOnDevice();
                        }
                    }));
        }

        if (type == REAL) {
            return Optional.of(new GpuTypeMapping(
                    DType.FLOAT32,
                    value -> Scalar.fromFloat(value.map(v -> Float.intBitsToFloat(((Long) v).intValue())).orElse(null)),
                    blocks -> {
                        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.FLOAT32, blocks.positionCount())) {
                            for (Block block : blocks.blocks()) {
                                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                                    if (block.isNull(blockPosition)) {
                                        builder.appendNull();
                                    }
                                    else {
                                        builder.append(REAL.getFloat(block, blockPosition));
                                    }
                                }
                            }
                            return builder.buildAndPutOnDevice();
                        }
                    }));
        }

        if (type == DOUBLE) {
            return Optional.of(new GpuTypeMapping(
                    DType.FLOAT64,
                    value -> Scalar.fromDouble((Double) value.orElse(null)),
                    blocks -> {
                        try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.FLOAT64, blocks.positionCount())) {
                            for (Block block : blocks.blocks()) {
                                for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
                                    if (block.isNull(blockPosition)) {
                                        builder.appendNull();
                                    }
                                    else {
                                        builder.append(DOUBLE.getDouble(block, blockPosition));
                                    }
                                }
                            }
                            return builder.buildAndPutOnDevice();
                        }
                    }));
        }

        if (type instanceof VarcharType) {
            return Optional.of(new GpuTypeMapping(
                    DType.STRING,
                    value -> Scalar.fromUTF8String(value.map(v -> ((Slice) v).getBytes()).orElse(null)),
                    blocks -> {
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
                    }));
        }
        return Optional.empty();
    }

    public record GpuTypeMapping(DType dType, ToScalar toScalar, ToColumn toColumn)
    {
        public GpuTypeMapping
        {
            requireNonNull(dType, "dType is null");
            requireNonNull(toColumn, "toColumn is null");
        }
    }

    public interface ToScalar
    {
        @Move
        Scalar copyToScalar(Optional<Object> trinoNativeValue);
    }

    public interface ToColumn
    {
        @Move
        ColumnVector copyToDevice(Blocks blocks);
    }
}
