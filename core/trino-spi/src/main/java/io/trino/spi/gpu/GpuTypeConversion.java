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
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Scalar;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BooleanArrayBlock;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GpuTypeConversion
{
    private GpuTypeConversion() {}

    private static final Logger log = Logger.getLogger(GpuTypeConversion.class.getName());

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
                    blocks -> copyByteBlocksToDevice(blocks, DType.BOOL8)));
        }

        if (type == TINYINT) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT8,
                    value -> Scalar.fromByte(value.map(v -> ((Long) v).byteValue()).orElse(null)),
                    blocks -> copyByteBlocksToDevice(blocks, DType.INT8)));
        }

        if (type == SMALLINT) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT16,
                    value -> Scalar.fromShort(value.map(v -> ((Long) v).shortValue()).orElse(null)),
                    blocks -> copyShortBlocksToDevice(blocks, DType.INT16)));
        }

        if (type == INTEGER) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT32,
                    value -> Scalar.fromInt(value.map(v -> ((Long) v).intValue()).orElse(null)),
                    blocks -> copyIntBlocksToDevice(blocks, DType.INT32)));
        }

        if (type == BIGINT) {
            return Optional.of(new GpuTypeMapping(
                    DType.INT64,
                    value -> Scalar.fromLong((Long) value.orElse(null)),
                    blocks -> copyLongBlocksToDevice(blocks, DType.INT64)));
        }

        if (type == REAL) {
            return Optional.of(new GpuTypeMapping(
                    DType.FLOAT32,
                    value -> Scalar.fromFloat(value.map(v -> Float.intBitsToFloat(((Long) v).intValue())).orElse(null)),
                    // IntArrayBlock stores floatToIntBits — same bit pattern as IEEE 754 float, so direct copy works
                    blocks -> copyIntBlocksToDevice(blocks, DType.FLOAT32)));
        }

        if (type == DOUBLE) {
            return Optional.of(new GpuTypeMapping(
                    DType.FLOAT64,
                    value -> Scalar.fromDouble((Double) value.orElse(null)),
                    // LongArrayBlock stores doubleToLongBits — same bit pattern as IEEE 754 double, so direct copy works
                    blocks -> copyLongBlocksToDevice(blocks, DType.FLOAT64)));
        }

        if (type instanceof DecimalType decimalType && decimalType.isShort()) {
            // Trino scale s means unscaled / 10^s; cuDF scale convention is unscaled * 10^scale, so negate
            int cudfScale = -decimalType.getScale();
            DType dType = DType.create(DType.DTypeEnum.DECIMAL64, cudfScale);
            return Optional.of(new GpuTypeMapping(
                    dType,
                    value -> value.isPresent() ? Scalar.fromDecimal(cudfScale, (Long) value.get()) : Scalar.fromNull(dType),
                    blocks -> copyLongBlocksToDevice(blocks, dType)));
        }

        if (type == DATE) {
            return Optional.of(new GpuTypeMapping(
                    DType.TIMESTAMP_DAYS,
                    value -> Scalar.timestampDaysFromInt(value.map(v -> ((Long) v).intValue()).orElse(null)),
                    blocks -> copyIntBlocksToDevice(blocks, DType.TIMESTAMP_DAYS)));
        }

        if (type instanceof TimestampType timestampType) {
            // ShortTimestampType always stores epochMicros; rescale to match the cuDF DType
            return switch (timestampType.getPrecision()) {
                case 0 -> Optional.of(new GpuTypeMapping(
                        DType.TIMESTAMP_SECONDS,
                        value -> Scalar.timestampFromLong(DType.TIMESTAMP_SECONDS, value.map(v -> (Long) v / 1_000_000L).orElse(null)),
                        blocks -> copyRescaledLongBlocksToDevice(blocks, DType.TIMESTAMP_SECONDS, 1_000_000L)));
                case 3 -> Optional.of(new GpuTypeMapping(
                        DType.TIMESTAMP_MILLISECONDS,
                        value -> Scalar.timestampFromLong(DType.TIMESTAMP_MILLISECONDS, value.map(v -> (Long) v / 1_000L).orElse(null)),
                        blocks -> copyRescaledLongBlocksToDevice(blocks, DType.TIMESTAMP_MILLISECONDS, 1_000L)));
                case 6 -> Optional.of(new GpuTypeMapping(
                        DType.TIMESTAMP_MICROSECONDS,
                        value -> Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, (Long) value.orElse(null)),
                        blocks -> copyLongBlocksToDevice(blocks, DType.TIMESTAMP_MICROSECONDS)));
                default -> {
                    log.log(Level.FINE, () -> "Type is not supported for GPU execution: %s".formatted(type.getDisplayName()));
                    yield Optional.empty();
                }
            };
        }

        if (type instanceof VarcharType) {
            return Optional.of(new GpuTypeMapping(
                    DType.STRING,
                    value -> Scalar.fromUTF8String(value.map(v -> ((Slice) v).getBytes()).orElse(null)),
                    GpuTypeConversion::copyVarcharBlocksToDevice));
        }

        log.log(Level.FINE, () -> "Type is not supported for GPU execution: %s".formatted(type.getDisplayName()));
        return Optional.empty();
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

    private static @Move ColumnVector copyVarcharBlocksToDevice(Blocks blocks)
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

    public record GpuTypeMapping(DType dType, ToScalar toScalar, ToColumn toColumn)
    {
        public GpuTypeMapping
        {
            requireNonNull(dType, "dType is null");
            requireNonNull(toScalar, "toScalar is null");
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
