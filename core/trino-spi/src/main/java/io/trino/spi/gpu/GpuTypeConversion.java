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
package io.trino.spi.gpu;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Scalar;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BooleanArrayBlock;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.toIntExact;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class GpuTypeConversion
{
    private GpuTypeConversion() {}

    private static final HostColumnVector.DataType BYTE_LIST_ELEMENT_TYPE = new HostColumnVector.BasicType(false, DType.UINT8);

    private static final Logger log = Logger.getLogger(GpuTypeConversion.class.getName());

    public static boolean isConvertible(Type type)
    {
        return toDType(type).isPresent();
    }

    public static Optional<List<DType>> toDTypes(List<Type> type)
    {
        List<DType> converted = new ArrayList<>(type.size());
        for (Type t : type) {
            Optional<DType> dType = toDType(t);
            if (dType.isEmpty()) {
                return Optional.empty();
            }
            converted.add(dType.get());
        }
        return Optional.of(unmodifiableList(converted));
    }

    public static Optional<DType> toDType(Type type)
    {
        return toGpuMapping(type)
                .map(GpuTypeMapping::dType);
    }

    public static Optional<GpuTypeMapping> toGpuMapping(Type type)
    {
        requireNonNull(type, "type is null");

        return switch (type) {
            case BooleanType _ -> Optional.of(new GpuTypeMapping(
                    DType.BOOL8,
                    value -> Scalar.fromBool((Boolean) value.orElse(null)),
                    blocks -> copyByteBlocksToDevice(blocks, DType.BOOL8),
                    nullChecked(Scalar::getBoolean)));

            case TinyintType _ -> Optional.of(new GpuTypeMapping(
                    DType.INT8,
                    value -> Scalar.fromByte(value.map(v -> ((Long) v).byteValue()).orElse(null)),
                    blocks -> copyByteBlocksToDevice(blocks, DType.INT8),
                    nullChecked(scalar -> (long) scalar.getByte())));

            case SmallintType _ -> Optional.of(new GpuTypeMapping(
                    DType.INT16,
                    value -> Scalar.fromShort(value.map(v -> ((Long) v).shortValue()).orElse(null)),
                    blocks -> copyShortBlocksToDevice(blocks, DType.INT16),
                    nullChecked(scalar -> (long) scalar.getShort())));

            case IntegerType _ -> Optional.of(new GpuTypeMapping(
                    DType.INT32,
                    value -> Scalar.fromInt(value.map(v -> ((Long) v).intValue()).orElse(null)),
                    blocks -> copyIntBlocksToDevice(blocks, DType.INT32),
                    nullChecked(scalar -> (long) scalar.getInt())));

            case BigintType _ -> Optional.of(new GpuTypeMapping(
                    DType.INT64,
                    value -> Scalar.fromLong((Long) value.orElse(null)),
                    blocks -> copyLongBlocksToDevice(blocks, DType.INT64),
                    nullChecked(Scalar::getLong)));

            case RealType _ -> Optional.of(new GpuTypeMapping(
                    DType.FLOAT32,
                    value -> Scalar.fromFloat(value.map(v -> Float.intBitsToFloat(((Long) v).intValue())).orElse(null)),
                    // IntArrayBlock stores floatToIntBits — same bit pattern as IEEE 754 float, so direct copy works
                    blocks -> copyIntBlocksToDevice(blocks, DType.FLOAT32),
                    nullChecked(scalar -> (long) Float.floatToIntBits(scalar.getFloat()))));

            case DoubleType _ -> Optional.of(new GpuTypeMapping(
                    DType.FLOAT64,
                    value -> Scalar.fromDouble((Double) value.orElse(null)),
                    // LongArrayBlock stores doubleToLongBits — same bit pattern as IEEE 754 double, so direct copy works
                    blocks -> copyLongBlocksToDevice(blocks, DType.FLOAT64),
                    nullChecked(Scalar::getDouble)));

            case DecimalType decimalType when decimalType.isShort() -> {
                // Trino scale s means unscaled / 10^s; cuDF scale convention is unscaled * 10^scale, so negate
                int cudfScale = -decimalType.getScale();
                DType dType = DType.create(DType.DTypeEnum.DECIMAL64, cudfScale);
                yield Optional.of(new GpuTypeMapping(
                        dType,
                        value -> value.map(o -> Scalar.fromDecimal(cudfScale, (Long) o))
                                .orElseGet(() -> Scalar.fromNull(dType)),
                        blocks -> copyLongBlocksToDevice(blocks, dType),
                        nullChecked(Scalar::getLong)));
            }

            case DecimalType decimalType -> {
                int cudfScale = -decimalType.getScale();
                DType dType = DType.create(DType.DTypeEnum.DECIMAL128, cudfScale);
                yield Optional.of(new GpuTypeMapping(
                        dType,
                        value -> value.map(o -> Scalar.fromDecimal(cudfScale, ((Int128) o).toBigInteger()))
                                .orElseGet(() -> Scalar.fromNull(dType)),
                        blocks -> copyInt128BlocksToDevice(blocks, dType),
                        nullChecked(scalar -> Int128.valueOf(scalar.getBigDecimal().unscaledValue()))));
            }

            case DateType _ -> Optional.of(new GpuTypeMapping(
                    DType.TIMESTAMP_DAYS,
                    value -> Scalar.timestampDaysFromInt(value.map(v -> ((Long) v).intValue()).orElse(null)),
                    blocks -> copyIntBlocksToDevice(blocks, DType.TIMESTAMP_DAYS),
                    nullChecked(scalar -> (long) scalar.getInt())));

            case TimestampType timestampType when timestampType.getPrecision() == 0 -> Optional.of(new GpuTypeMapping(
                    DType.TIMESTAMP_SECONDS,
                    // Trino short TimestampType stores epochMicros; rescale to match the cuDF DType
                    value -> Scalar.timestampFromLong(DType.TIMESTAMP_SECONDS, value.map(v -> (Long) v / 1_000_000L).orElse(null)),
                    blocks -> copyRescaledLongBlocksToDevice(blocks, DType.TIMESTAMP_SECONDS, 1_000_000L),
                    nullChecked(scalar -> scalar.getLong() * 1_000_000L)));

            case TimestampType timestampType when timestampType.getPrecision() == 3 -> Optional.of(new GpuTypeMapping(
                    DType.TIMESTAMP_MILLISECONDS,
                    // Trino short TimestampType stores epochMicros; rescale to match the cuDF DType
                    value -> Scalar.timestampFromLong(DType.TIMESTAMP_MILLISECONDS, value.map(v -> (Long) v / 1_000L).orElse(null)),
                    blocks -> copyRescaledLongBlocksToDevice(blocks, DType.TIMESTAMP_MILLISECONDS, 1_000L),
                    nullChecked(scalar -> scalar.getLong() * 1_000L)));

            case TimestampType timestampType when timestampType.getPrecision() == 6 -> Optional.of(new GpuTypeMapping(
                    DType.TIMESTAMP_MICROSECONDS,
                    value -> Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, (Long) value.orElse(null)),
                    blocks -> copyLongBlocksToDevice(blocks, DType.TIMESTAMP_MICROSECONDS),
                    nullChecked(Scalar::getLong)));

            // For CHAR(n) the GPU representation is DType.STRING with trailing spaces trimmed (same as Trino stack representation)
            case CharType _, VarcharType _ -> Optional.of(new GpuTypeMapping(
                    DType.STRING,
                    value -> Scalar.fromUTF8String(value.map(v -> ((Slice) v).getBytes()).orElse(null)),
                    GpuTypeConversion::copyVariableWithBlocksToDeviceString,
                    nullChecked(scalar -> Slices.wrappedBuffer(scalar.getUTF8()))));

            case VarbinaryType _ -> Optional.of(new GpuTypeMapping(
                    DType.LIST,
                    value -> value.map(v -> {
                        byte[] bytes = ((Slice) v).getBytes();
                        try (ColumnVector child = ColumnVector.fromUnsignedBytes(bytes)) {
                            return Scalar.listFromColumnView(child);
                        }
                    }).orElseGet(() -> Scalar.listFromNull(BYTE_LIST_ELEMENT_TYPE)),
                    GpuTypeConversion::copyVarbinaryBlocksToDevice,
                    Optional.empty()));

            default -> {
                log.log(Level.FINE, () -> "Type is not supported for GPU execution: %s".formatted(type.getDisplayName()));
                yield Optional.empty();
            }
        };
    }

    private static @Move ColumnVector copyByteBlocksToDevice(Blocks blocks, DType dType)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            try (HostColumnVector.Builder builder = HostColumnVector.builder(dType, 0)) {
                return builder.buildAndPutOnDevice();
            }
        }

        HostMemoryBuffer data = null;
        HostMemoryBuffer validity = null;
        try {
            data = HostMemoryBuffer.allocate(totalPositions);
            int destOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                switch (block) {
                    case RunLengthEncodedBlock rle -> {
                        ByteArrayBlock value = (ByteArrayBlock) rle.getValue();
                        data.setMemory(destOffset, count, value.getRawValues()[value.getRawValuesOffset()]);
                    }
                    case DictionaryBlock dictionary -> {
                        ByteArrayBlock valueBlock = (ByteArrayBlock) dictionary.getUnderlyingValueBlock();
                        byte[] rawValues = valueBlock.getRawValues();
                        int rawOffset = valueBlock.getRawValuesOffset();
                        byte[] temp = new byte[count];
                        for (int i = 0; i < count; i++) {
                            temp[i] = rawValues[rawOffset + dictionary.getUnderlyingValuePosition(i)];
                        }
                        data.setBytes(destOffset, temp, 0, count);
                    }
                    case ByteArrayBlock byteBlock -> data.setBytes(destOffset, byteBlock.getRawValues(), byteBlock.getRawValuesOffset(), count);
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
                destOffset += count;
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            try (HostColumnVector hcv = new HostColumnVector(dType, totalPositions, Optional.of(nullCount), data, validity, null, List.of())) {
                data = null;
                validity = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (data != null) {
                data.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    private static @Move ColumnVector copyShortBlocksToDevice(Blocks blocks, DType dType)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            try (HostColumnVector.Builder builder = HostColumnVector.builder(dType, 0)) {
                return builder.buildAndPutOnDevice();
            }
        }

        HostMemoryBuffer data = null;
        HostMemoryBuffer validity = null;
        try {
            data = HostMemoryBuffer.allocate((long) totalPositions * Short.BYTES);
            long destByteOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                switch (block) {
                    case RunLengthEncodedBlock rle -> {
                        ShortArrayBlock value = (ShortArrayBlock) rle.getValue();
                        short[] temp = new short[count];
                        Arrays.fill(temp, value.getRawValues()[value.getRawValuesOffset()]);
                        data.setShorts(destByteOffset, temp, 0, count);
                    }
                    case DictionaryBlock dictionary -> {
                        ShortArrayBlock valueBlock = (ShortArrayBlock) dictionary.getUnderlyingValueBlock();
                        short[] rawValues = valueBlock.getRawValues();
                        int rawOffset = valueBlock.getRawValuesOffset();
                        short[] temp = new short[count];
                        for (int i = 0; i < count; i++) {
                            temp[i] = rawValues[rawOffset + dictionary.getUnderlyingValuePosition(i)];
                        }
                        data.setShorts(destByteOffset, temp, 0, count);
                    }
                    case ShortArrayBlock shortBlock -> data.setShorts(destByteOffset, shortBlock.getRawValues(), shortBlock.getRawValuesOffset(), count);
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
                destByteOffset += (long) count * Short.BYTES;
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            try (HostColumnVector hcv = new HostColumnVector(dType, totalPositions, Optional.of(nullCount), data, validity, null, List.of())) {
                data = null;
                validity = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (data != null) {
                data.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    private static @Move ColumnVector copyIntBlocksToDevice(Blocks blocks, DType dType)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            try (HostColumnVector.Builder builder = HostColumnVector.builder(dType, 0)) {
                return builder.buildAndPutOnDevice();
            }
        }

        HostMemoryBuffer data = null;
        HostMemoryBuffer validity = null;
        try {
            data = HostMemoryBuffer.allocate((long) totalPositions * Integer.BYTES);
            long destByteOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                switch (block) {
                    case RunLengthEncodedBlock rle -> {
                        IntArrayBlock value = (IntArrayBlock) rle.getValue();
                        int[] temp = new int[count];
                        Arrays.fill(temp, value.getRawValues()[value.getRawValuesOffset()]);
                        data.setInts(destByteOffset, temp, 0, count);
                    }
                    case DictionaryBlock dictionary -> {
                        IntArrayBlock valueBlock = (IntArrayBlock) dictionary.getUnderlyingValueBlock();
                        int[] rawValues = valueBlock.getRawValues();
                        int rawOffset = valueBlock.getRawValuesOffset();
                        int[] temp = new int[count];
                        for (int i = 0; i < count; i++) {
                            temp[i] = rawValues[rawOffset + dictionary.getUnderlyingValuePosition(i)];
                        }
                        data.setInts(destByteOffset, temp, 0, count);
                    }
                    case IntArrayBlock intBlock -> data.setInts(destByteOffset, intBlock.getRawValues(), intBlock.getRawValuesOffset(), count);
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
                destByteOffset += (long) count * Integer.BYTES;
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            try (HostColumnVector hcv = new HostColumnVector(dType, totalPositions, Optional.of(nullCount), data, validity, null, List.of())) {
                data = null;
                validity = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (data != null) {
                data.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    private static @Move ColumnVector copyLongBlocksToDevice(Blocks blocks, DType dType)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            try (HostColumnVector.Builder builder = HostColumnVector.builder(dType, 0)) {
                return builder.buildAndPutOnDevice();
            }
        }

        HostMemoryBuffer data = null;
        HostMemoryBuffer validity = null;
        try {
            data = HostMemoryBuffer.allocate((long) totalPositions * Long.BYTES);
            long destByteOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                switch (block) {
                    case RunLengthEncodedBlock rle -> {
                        LongArrayBlock value = (LongArrayBlock) rle.getValue();
                        long[] temp = new long[count];
                        Arrays.fill(temp, value.getRawValues()[value.getRawValuesOffset()]);
                        data.setLongs(destByteOffset, temp, 0, count);
                    }
                    case DictionaryBlock dictionary -> {
                        LongArrayBlock valueBlock = (LongArrayBlock) dictionary.getUnderlyingValueBlock();
                        long[] rawValues = valueBlock.getRawValues();
                        int rawOffset = valueBlock.getRawValuesOffset();
                        long[] temp = new long[count];
                        for (int i = 0; i < count; i++) {
                            temp[i] = rawValues[rawOffset + dictionary.getUnderlyingValuePosition(i)];
                        }
                        data.setLongs(destByteOffset, temp, 0, count);
                    }
                    case LongArrayBlock longBlock -> data.setLongs(destByteOffset, longBlock.getRawValues(), longBlock.getRawValuesOffset(), count);
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
                destByteOffset += (long) count * Long.BYTES;
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            try (HostColumnVector hcv = new HostColumnVector(dType, totalPositions, Optional.of(nullCount), data, validity, null, List.of())) {
                data = null;
                validity = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (data != null) {
                data.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    /**
     * cuDF DECIMAL128 expects 16 bytes per value laid out little-endian: bytes 0-7 are the low
     * 64 bits, bytes 8-15 are the high 64 bits. Trino's {@link Int128ArrayBlock} stores values
     * as {@code (high, low)} long pairs, so we swap each pair when writing to the buffer.
     * {@link HostMemoryBuffer#setLongs} writes each long in native (little-endian) byte order.
     */
    private static @Move ColumnVector copyInt128BlocksToDevice(Blocks blocks, DType dType)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            try (HostColumnVector.Builder builder = HostColumnVector.builder(dType, 0)) {
                return builder.buildAndPutOnDevice();
            }
        }

        HostMemoryBuffer data = null;
        HostMemoryBuffer validity = null;
        try {
            data = HostMemoryBuffer.allocate((long) totalPositions * Int128ArrayBlock.INT128_BYTES);
            long destByteOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                long[] cudfLowHighPairs = new long[count * 2];
                switch (block) {
                    case RunLengthEncodedBlock runLengthEncodedBlock -> {
                        Int128ArrayBlock valueBlock = (Int128ArrayBlock) runLengthEncodedBlock.getValue();
                        long high = valueBlock.getInt128High(0);
                        long low = valueBlock.getInt128Low(0);
                        for (int i = 0; i < count; i++) {
                            cudfLowHighPairs[2 * i] = low;
                            cudfLowHighPairs[2 * i + 1] = high;
                        }
                    }
                    case DictionaryBlock dictionaryBlock -> {
                        Int128ArrayBlock valueBlock = (Int128ArrayBlock) dictionaryBlock.getUnderlyingValueBlock();
                        for (int i = 0; i < count; i++) {
                            int underlyingPosition = dictionaryBlock.getUnderlyingValuePosition(i);
                            cudfLowHighPairs[2 * i] = valueBlock.getInt128Low(underlyingPosition);
                            cudfLowHighPairs[2 * i + 1] = valueBlock.getInt128High(underlyingPosition);
                        }
                    }
                    case Int128ArrayBlock int128ArrayBlock -> {
                        for (int i = 0; i < count; i++) {
                            cudfLowHighPairs[2 * i] = int128ArrayBlock.getInt128Low(i);
                            cudfLowHighPairs[2 * i + 1] = int128ArrayBlock.getInt128High(i);
                        }
                    }
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
                data.setLongs(destByteOffset, cudfLowHighPairs, 0, cudfLowHighPairs.length);
                destByteOffset += (long) count * Int128ArrayBlock.INT128_BYTES;
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            try (HostColumnVector hcv = new HostColumnVector(dType, totalPositions, Optional.of(nullCount), data, validity, null, List.of())) {
                data = null;
                validity = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (data != null) {
                data.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    private static @Move ColumnVector copyRescaledLongBlocksToDevice(Blocks blocks, DType dType, long divisor)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            try (HostColumnVector.Builder builder = HostColumnVector.builder(dType, 0)) {
                return builder.buildAndPutOnDevice();
            }
        }

        HostMemoryBuffer data = null;
        HostMemoryBuffer validity = null;
        try {
            data = HostMemoryBuffer.allocate((long) totalPositions * Long.BYTES);
            long destByteOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                long[] temp = new long[count];
                switch (block) {
                    case RunLengthEncodedBlock rle -> {
                        LongArrayBlock value = (LongArrayBlock) rle.getValue();
                        Arrays.fill(temp, value.getRawValues()[value.getRawValuesOffset()] / divisor);
                    }
                    case DictionaryBlock dictionary -> {
                        LongArrayBlock valueBlock = (LongArrayBlock) dictionary.getUnderlyingValueBlock();
                        long[] rawValues = valueBlock.getRawValues();
                        int rawOffset = valueBlock.getRawValuesOffset();
                        for (int i = 0; i < count; i++) {
                            temp[i] = rawValues[rawOffset + dictionary.getUnderlyingValuePosition(i)] / divisor;
                        }
                    }
                    case LongArrayBlock longBlock -> {
                        long[] rawValues = longBlock.getRawValues();
                        int rawOffset = longBlock.getRawValuesOffset();
                        for (int i = 0; i < count; i++) {
                            temp[i] = rawValues[rawOffset + i] / divisor;
                        }
                    }
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
                data.setLongs(destByteOffset, temp, 0, count);
                destByteOffset += (long) count * Long.BYTES;
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            try (HostColumnVector hcv = new HostColumnVector(dType, totalPositions, Optional.of(nullCount), data, validity, null, List.of())) {
                data = null;
                validity = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (data != null) {
                data.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    private static @Move ColumnVector copyVariableWithBlocksToDeviceString(Blocks blocks)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            try (HostColumnVector.Builder builder = HostColumnVector.builder(DType.STRING, 0)) {
                return builder.buildAndPutOnDevice();
            }
        }

        HostMemoryBuffer data = null;
        HostMemoryBuffer offsets = null;
        HostMemoryBuffer validity = null;
        try {
            // First pass: compute total data bytes and fill offsets
            OffsetsResult offsetsResult = buildVarcharOffsets(blocks, totalPositions);
            offsets = offsetsResult.buffer();
            long totalDataBytes = offsetsResult.totalDataBytes();

            // Second pass: copy data bytes
            data = HostMemoryBuffer.allocate(Math.max(totalDataBytes, 1));
            long dataByteOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                switch (block) {
                    case RunLengthEncodedBlock rle -> {
                        VariableWidthBlock value = (VariableWidthBlock) rle.getValue();
                        Slice slice = value.getSlice(0);
                        int sliceLen = slice.length();
                        if (sliceLen > 0) {
                            byte[] sliceBytes = slice.byteArray();
                            int sliceOffset = slice.byteArrayOffset();
                            for (int i = 0; i < count; i++) {
                                data.setBytes(dataByteOffset, sliceBytes, sliceOffset, sliceLen);
                                dataByteOffset += sliceLen;
                            }
                        }
                    }
                    case DictionaryBlock dictionaryBlock -> {
                        VariableWidthBlock dictionary = (VariableWidthBlock) dictionaryBlock.getUnderlyingValueBlock();
                        int arrayBase = dictionary.getRawArrayBase();
                        int[] rawOffsets = dictionary.getRawOffsets();
                        Slice rawSlice = dictionary.getRawSlice();
                        for (int i = 0; i < count; i++) {
                            int underlyingPos = dictionaryBlock.getUnderlyingValuePosition(i);
                            int start = rawOffsets[arrayBase + underlyingPos];
                            int len = rawOffsets[arrayBase + underlyingPos + 1] - start;
                            if (len > 0) {
                                data.setBytes(dataByteOffset, rawSlice.byteArray(), rawSlice.byteArrayOffset() + start, len);
                            }
                            dataByteOffset += len;
                        }
                    }
                    case VariableWidthBlock valueBlock -> {
                        int arrayBase = valueBlock.getRawArrayBase();
                        int[] rawOffsets = valueBlock.getRawOffsets();
                        Slice rawSlice = valueBlock.getRawSlice();
                        int blockDataStart = rawOffsets[arrayBase];
                        int blockDataLength = rawOffsets[arrayBase + count] - blockDataStart;
                        if (blockDataLength > 0) {
                            data.setBytes(dataByteOffset, rawSlice.byteArray(), rawSlice.byteArrayOffset() + blockDataStart, blockDataLength);
                        }
                        dataByteOffset += blockDataLength;
                    }
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            try (HostColumnVector hcv = new HostColumnVector(DType.STRING, totalPositions, Optional.of(nullCount), data, validity, offsets, List.of())) {
                data = null;
                validity = null;
                offsets = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (data != null) {
                data.close();
            }
            if (offsets != null) {
                offsets.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    /**
     * VARBINARY is represented as DType.LIST with DType.UINT8 element.
     * Layout: top-level list has (rows+1) int offsets and a top-level validity buffer.
     */
    private static @Move ColumnVector copyVarbinaryBlocksToDevice(Blocks blocks)
    {
        int totalPositions = blocks.positionCount();
        if (totalPositions == 0) {
            HostMemoryBuffer offsets = null;
            HostMemoryBuffer childData = null;
            HostColumnVectorCore child = null;
            try {
                offsets = HostMemoryBuffer.allocate(Integer.BYTES);
                childData = HostMemoryBuffer.allocate(0);
                offsets.setInt(0, 0);
                child = new HostColumnVectorCore(DType.UINT8, 0, Optional.of(0L), childData, null, null, List.of());
                childData = null;
                try (HostColumnVector hcv = new HostColumnVector(DType.LIST, 0, Optional.of(0L), null, null, offsets, List.of(child))) {
                    child = null;
                    offsets = null;
                    return hcv.copyToDevice();
                }
            }
            finally {
                if (child != null) {
                    child.close();
                }
                if (offsets != null) {
                    offsets.close();
                }
                if (childData != null) {
                    childData.close();
                }
            }
        }

        HostMemoryBuffer offsets = null;
        HostMemoryBuffer childData = null;
        HostMemoryBuffer validity = null;
        HostColumnVectorCore child = null;
        try {
            OffsetsResult offsetsResult = buildVarcharOffsets(blocks, totalPositions);
            offsets = offsetsResult.buffer();
            long totalDataBytes = offsetsResult.totalDataBytes();

            childData = HostMemoryBuffer.allocate(Math.max(totalDataBytes, 1));
            long dataByteOffset = 0;
            for (Block block : blocks.blocks()) {
                int count = block.getPositionCount();
                switch (block) {
                    case RunLengthEncodedBlock rle -> {
                        VariableWidthBlock value = (VariableWidthBlock) rle.getValue();
                        Slice slice = value.getSlice(0);
                        int sliceLen = slice.length();
                        if (sliceLen > 0) {
                            byte[] sliceBytes = slice.byteArray();
                            int sliceOffset = slice.byteArrayOffset();
                            for (int i = 0; i < count; i++) {
                                childData.setBytes(dataByteOffset, sliceBytes, sliceOffset, sliceLen);
                                dataByteOffset += sliceLen;
                            }
                        }
                    }
                    case DictionaryBlock dictionaryBlock -> {
                        VariableWidthBlock dictionary = (VariableWidthBlock) dictionaryBlock.getUnderlyingValueBlock();
                        int arrayBase = dictionary.getRawArrayBase();
                        int[] rawOffsets = dictionary.getRawOffsets();
                        Slice rawSlice = dictionary.getRawSlice();
                        for (int i = 0; i < count; i++) {
                            int underlyingPos = dictionaryBlock.getUnderlyingValuePosition(i);
                            int start = rawOffsets[arrayBase + underlyingPos];
                            int len = rawOffsets[arrayBase + underlyingPos + 1] - start;
                            if (len > 0) {
                                childData.setBytes(dataByteOffset, rawSlice.byteArray(), rawSlice.byteArrayOffset() + start, len);
                            }
                            dataByteOffset += len;
                        }
                    }
                    case VariableWidthBlock valueBlock -> {
                        int arrayBase = valueBlock.getRawArrayBase();
                        int[] rawOffsets = valueBlock.getRawOffsets();
                        Slice rawSlice = valueBlock.getRawSlice();
                        int blockDataStart = rawOffsets[arrayBase];
                        int blockDataLength = rawOffsets[arrayBase + count] - blockDataStart;
                        if (blockDataLength > 0) {
                            childData.setBytes(dataByteOffset, rawSlice.byteArray(), rawSlice.byteArrayOffset() + blockDataStart, blockDataLength);
                        }
                        dataByteOffset += blockDataLength;
                    }
                    default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
                }
            }

            ValidityResult validityResult = buildValidity(blocks, totalPositions);
            validity = validityResult.buffer();
            long nullCount = validityResult.nullCount();

            child = new HostColumnVectorCore(DType.UINT8, totalDataBytes, Optional.of(0L), childData, null, null, List.of());
            childData = null;
            try (HostColumnVector hcv = new HostColumnVector(DType.LIST, totalPositions, Optional.of(nullCount), null, validity, offsets, List.of(child))) {
                child = null;
                offsets = null;
                validity = null;
                return hcv.copyToDevice();
            }
        }
        finally {
            if (child != null) {
                child.close();
            }
            if (childData != null) {
                childData.close();
            }
            if (offsets != null) {
                offsets.close();
            }
            if (validity != null) {
                validity.close();
            }
        }
    }

    private record OffsetsResult(@Own HostMemoryBuffer buffer, long totalDataBytes) {}

    /**
     * Compute per-position offsets for a sequence of VARCHAR blocks and load them
     * into a freshly allocated {@link HostMemoryBuffer} via a single bulk write.
     */
    private static @Move OffsetsResult buildVarcharOffsets(Blocks blocks, int totalPositions)
    {
        int[] offsetsArray = new int[totalPositions + 1];
        long totalDataBytes = 0;
        int position = 0;
        for (Block block : blocks.blocks()) {
            int count = block.getPositionCount();
            switch (block) {
                case RunLengthEncodedBlock rle -> {
                    VariableWidthBlock value = (VariableWidthBlock) rle.getValue();
                    int sliceLen = value.getSliceLength(0);
                    long blockStart = totalDataBytes;
                    for (int i = 0; i < count; i++) {
                        offsetsArray[position++] = toIntExact(blockStart + (long) sliceLen * i);
                    }
                    totalDataBytes += (long) sliceLen * count;
                }
                case DictionaryBlock dictionaryBlock -> {
                    VariableWidthBlock dictionary = (VariableWidthBlock) dictionaryBlock.getUnderlyingValueBlock();
                    int arrayBase = dictionary.getRawArrayBase();
                    int[] rawOffsets = dictionary.getRawOffsets();
                    for (int i = 0; i < count; i++) {
                        offsetsArray[position++] = toIntExact(totalDataBytes);
                        int underlyingPos = dictionaryBlock.getUnderlyingValuePosition(i);
                        totalDataBytes += rawOffsets[arrayBase + underlyingPos + 1] - rawOffsets[arrayBase + underlyingPos];
                    }
                }
                case VariableWidthBlock valueBlock -> {
                    int arrayBase = valueBlock.getRawArrayBase();
                    int[] rawOffsets = valueBlock.getRawOffsets();
                    int blockDataStart = rawOffsets[arrayBase];
                    long blockStart = totalDataBytes;
                    for (int i = 0; i < count; i++) {
                        offsetsArray[position++] = toIntExact(blockStart + (rawOffsets[arrayBase + i] - blockDataStart));
                    }
                    totalDataBytes += rawOffsets[arrayBase + count] - blockDataStart;
                }
                default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
            }
        }
        offsetsArray[position] = toIntExact(totalDataBytes);

        HostMemoryBuffer buffer = HostMemoryBuffer.allocate((long) (totalPositions + 1) * Integer.BYTES);
        try {
            buffer.setInts(0, offsetsArray, 0, totalPositions + 1);
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
        return new OffsetsResult(buffer, totalDataBytes);
    }

    private record ValidityResult(@Own @Nullable HostMemoryBuffer buffer, long nullCount) {}

    /**
     * Build the Arrow-style validity bitmask from Trino blocks using ValueBlock.getNulls().
     * Returns a null buffer with zero null count if there are no nulls.
     */
    private static @Move ValidityResult buildValidity(Blocks blocks, int totalPositions)
    {
        HostMemoryBuffer validity = null;
        long nullCount = 0;
        int position = 0;
        for (Block block : blocks.blocks()) {
            if (validity == null && block.mayHaveNull()) {
                validity = allocateValidityBuffer(totalPositions);
            }
            nullCount += packBlockValidity(block, position, validity);
            position += block.getPositionCount();
        }

        if (nullCount == 0) {
            if (validity != null) {
                validity.close();
            }
            return new ValidityResult(null, 0);
        }
        return new ValidityResult(validity, nullCount);
    }

    /**
     * Pack null bits for a single block into the Arrow validity bitmask buffer.
     * Arrow validity uses LSB-first bit ordering: bit 0 of byte 0 = position 0,
     * bit 7 of byte 0 = position 7, bit 0 of byte 1 = position 8, etc.
     * A set bit (1) means valid, a cleared bit (0) means null.
     * Returns the number of nulls found.
     */
    private static long packBlockValidity(Block block, int destPosition, HostMemoryBuffer validityBuffer)
    {
        if (validityBuffer == null || !block.mayHaveNull()) {
            return 0;
        }
        if (block instanceof RunLengthEncodedBlock rle) {
            if (rle.getValue().isNull(0)) {
                int positionCount = block.getPositionCount();
                int firstByteIndex = destPosition >>> 3;
                int lastByteIndex = (destPosition + positionCount - 1) >>> 3;
                int byteCount = lastByteIndex - firstByteIndex + 1;
                int firstBitInByte = destPosition & 7;
                int lastBitInByte = (destPosition + positionCount) & 7; // 0 means the end is byte-aligned

                // Start with all zeros (all null), then restore bits outside our range at the edges
                byte[] validityBytes = new byte[byteCount];
                if (firstBitInByte != 0) {
                    // Preserve the lower bits (positions before ours) in the first byte
                    // e.g. firstBitInByte=3: mask=0b00000111 keeps bits 0-2
                    validityBytes[0] = (byte) (validityBuffer.getByte(firstByteIndex) & ((1 << firstBitInByte) - 1));
                }
                if (lastBitInByte != 0) {
                    // Preserve the upper bits (positions after ours) in the last byte
                    // e.g. lastBitInByte=5: mask=0b11100000 keeps bits 5-7
                    validityBytes[byteCount - 1] |= (byte) (validityBuffer.getByte(lastByteIndex) & -(1 << lastBitInByte));
                }
                validityBuffer.setBytes(firstByteIndex, validityBytes, 0, byteCount);
                return positionCount;
            }
            return 0;
        }
        Optional<BooleanArrayBlock> nullsOptional = block.getUnderlyingValueBlock().getNulls();
        if (nullsOptional.isEmpty()) {
            return 0;
        }
        BooleanArrayBlock nullsBlock = nullsOptional.get();
        boolean[] isNull = nullsBlock.getRawValues();
        int isNullOffset = nullsBlock.getRawValuesOffset();
        int positionCount = block.getPositionCount();

        int firstByteIndex = destPosition >>> 3;
        int lastByteIndex = (destPosition + positionCount - 1) >>> 3;
        int byteCount = lastByteIndex - firstByteIndex + 1;
        byte[] validityBytes = new byte[byteCount];
        validityBuffer.getBytes(validityBytes, 0, firstByteIndex, byteCount);

        long nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (isNull[isNullOffset + block.getUnderlyingValuePosition(i)]) {
                int bitPosition = destPosition + i;
                int byteIndex = (bitPosition >>> 3) - firstByteIndex;
                int bitIndex = bitPosition & 7;
                validityBytes[byteIndex] &= (byte) ~(1 << bitIndex);
                nullCount++;
            }
        }
        validityBuffer.setBytes(firstByteIndex, validityBytes, 0, byteCount);
        return nullCount;
    }

    private static HostMemoryBuffer allocateValidityBuffer(int totalPositions)
    {
        // ceil(totalPositions / 8), rounded up to 64-byte boundary (matching cuDF allocation)
        int bytes = (totalPositions + 7) / 8;
        long bufferSize = (long) ((bytes + 63) / 64) * 64;
        HostMemoryBuffer validity = HostMemoryBuffer.allocate(bufferSize);
        validity.setMemory(0, bufferSize, (byte) 0xFF);
        return validity;
    }

    public record GpuTypeMapping(DType dType, ToScalar toScalar, ToColumn toColumn, Optional<FromScalar> fromScalar)
    {
        public GpuTypeMapping
        {
            requireNonNull(dType, "dType is null");
            requireNonNull(toScalar, "toScalar is null");
            requireNonNull(toColumn, "toColumn is null");
            requireNonNull(fromScalar, "fromScalar is null");
        }

        public GpuTypeMapping(DType dType, ToScalar toScalar, ToColumn toColumn, FromScalar fromScalar)
        {
            this(dType, toScalar, toColumn, Optional.of(fromScalar));
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

    public interface FromScalar
    {
        @Nullable
        Object trinoValue(@Borrow Scalar scalar);
    }

    private static FromScalar nullChecked(FromScalar delegate)
    {
        return scalar -> !scalar.isValid() ? null : delegate.trinoValue(scalar);
    }
}
