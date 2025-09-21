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
package io.trino.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.IntArrayBlock;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.block.IntArrayAdaptiveBlockEncoding.EncodingMethod.BITPACKING;
import static io.trino.block.IntArrayAdaptiveBlockEncoding.EncodingMethod.BITPACKING_DELTA;
import static io.trino.block.IntArrayAdaptiveBlockEncoding.EncodingMethod.RAW;
import static io.trino.block.IntArrayAdaptiveBlockEncoding.EncodingMethod.RLE;
import static io.trino.block.IntArrayAdaptiveBlockEncoding.EncodingMethod.V_BYTE;
import static io.trino.block.VByteUtils.vByteDecodeInts;
import static io.trino.block.VByteUtils.vByteEncodeInts;
import static io.trino.spi.block.BlockShim.getRawValueIsNull;
import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.lang.System.arraycopy;

/**
 * Adaptive encoding for int array blocks, selecting the most efficient encoding
 * method based on the characteristics of the data.
 *
 * <p>Layout of an encoded block:
 * <pre>
 * +-------------------+----------------------+--------------------+-----------+
 * | 4 bytes           | n bytes (optional)   | 4 bytes (optional) | 1 byte    |
 * | Position count    | Null bits (if any)   | Non-null count     | Method ID |
 * +-------------------+----------------------+--------------------+-----------+
 * | x bytes: values (method-dependent size)                                   |
 * +---------------------------------------------------------------------------+
 * </pre>
 */
public class IntArrayAdaptiveBlockEncoding
        implements BlockEncoding
{
    private final boolean vByteEncodingEnabled;

    public IntArrayAdaptiveBlockEncoding(boolean vByteEncodingEnabled)
    {
        this.vByteEncodingEnabled = vByteEncodingEnabled;
    }

    @Override
    public String getName()
    {
        return "INT_AD";
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return IntArrayBlock.class;
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        int positionCount = input.readInt();

        byte[] valueIsNullPacked = retrieveNullBits(input, positionCount);
        int[] values = new int[positionCount];

        if (valueIsNullPacked == null) {
            decodeInts(input, values, positionCount);
            return new IntArrayBlock(positionCount, Optional.empty(), values);
        }
        boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

        int nonNullPositionCount = input.readInt();

        decodeInts(input, values, nonNullPositionCount);
        restoreNullPositions(positionCount, nonNullPositionCount, values, valueIsNull, valueIsNullPacked);

        return new IntArrayBlock(positionCount, Optional.of(valueIsNull), values);
    }

    private static void restoreNullPositions(int positionCount, int nonNullPositionCount, int[] values, boolean[] valueIsNull, byte[] valueIsNullPacked)
    {
        int position = nonNullPositionCount - 1;

        // Handle Last (positionCount % 8) values
        for (int i = positionCount - 1; i >= (positionCount & ~0b111) && position >= 0; i--) {
            values[i] = values[position];
            if (!valueIsNull[i]) {
                position--;
            }
        }

        // Handle the remaining positions.
        for (int i = (positionCount & ~0b111) - 8; i >= 0 && position >= 0; i -= 8) {
            byte packed = valueIsNullPacked[i >>> 3];
            if (packed == 0) { // Only values
                arraycopy(values, position - 7, values, i, 8);
                position -= 8;
            }
            else if (packed != -1) { // At least one non-null
                for (int j = i + 7; j >= i && position >= 0; j--) {
                    values[j] = values[position];
                    if (!valueIsNull[j]) {
                        position--;
                    }
                }
            }
            // Do nothing if there are only nulls
        }
    }

    private void decodeInts(SliceInput input, int[] values, int length)
    {
        EncodingMethod method = EncodingMethod.fromByte(input.readByte());
        switch (method) {
            case RAW:
                input.readInts(values, 0, length);
                break;
            case RLE:
                readRleEncodedInts(input, values);
                break;
            case BITPACKING:
                BitPackingUtils.decode(input, values, length);
                break;
            case BITPACKING_DELTA:
                BitPackingUtils.decodeDelta(input, values, length);
                break;
            case V_BYTE:
                vByteDecodeInts(input, length, values);
                break;
            default:
                throw new IllegalArgumentException("Unsupported encoding method: " + method);
        }
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput output, Block block)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        int positionCount = intArrayBlock.getPositionCount();
        output.appendInt(positionCount);

        encodeNullsAsBits(output, getRawValueIsNull(intArrayBlock), intArrayBlock.getRawValuesOffset(), positionCount);

        if (!intArrayBlock.mayHaveNull()) {
            encodeInts(output, intArrayBlock.getRawValues(), intArrayBlock.getRawValuesOffset(), positionCount);
        }
        else {
            int[] valuesWithoutNull = new int[positionCount];
            int nonNullPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                valuesWithoutNull[nonNullPositionCount] = intArrayBlock.getInt(i);
                if (!intArrayBlock.isNull(i)) {
                    nonNullPositionCount++;
                }
            }

            output.writeInt(nonNullPositionCount);

            encodeInts(output, valuesWithoutNull, 0, nonNullPositionCount);
        }
    }

    private void encodeInts(SliceOutput output, int[] values, int offset, int length)
    {
        BlockAnalysis analysis = selectedEncodingMethod(values, offset, length);
        EncodingMethod method = analysis.method();

        output.writeByte(method.getId());
        switch (method) {
            case RAW:
                output.writeInts(values, offset, length);
                break;
            case RLE:
                writeRleEncodedInts(output, values, offset, length, analysis);
                break;
            case BITPACKING:
                BitPackingUtils.encode(output, values, offset, length);
                break;
            case BITPACKING_DELTA:
                BitPackingUtils.encodeDelta(output, values, offset, length);
                break;
            case V_BYTE:
                vByteEncodeInts(output, values, offset, length);
                break;
            default:
                throw new IllegalStateException("Unsupported encoding method: " + method);
        }
    }

    private static void writeRleEncodedInts(SliceOutput output, int[] input, int offset, int length, BlockAnalysis analysis)
    {
        checkArgument(length > 0, "RLE encoding requires at least one value");

        int[] runLengths = new int[analysis.runCount()];
        int[] runValues = new int[analysis.runCount()];

        int previous = input[offset];
        int runLength = 1;
        int runIndex = 0;

        for (int i = offset + 1; i < offset + length; i++) {
            int current = input[i];

            if (current == previous) {
                runLength++;
            }
            else {
                runLengths[runIndex] = runLength;
                runValues[runIndex] = previous;
                runIndex++;
                previous = current;
                runLength = 1;
            }
        }
        runLengths[runIndex] = runLength;
        runValues[runIndex] = previous;

        output.writeInt(analysis.runCount());
        BitPackingUtils.encode(output, runLengths, 0, runLengths.length);
        BitPackingUtils.encode(output, runValues, 0, runValues.length);
    }

    private static void readRleEncodedInts(SliceInput input, int[] output)
    {
        int runCount = input.readInt();

        int[] runLengths = new int[runCount];
        int[] runValues = new int[runCount];

        BitPackingUtils.decode(input, runLengths, runLengths.length);
        BitPackingUtils.decode(input, runValues, runValues.length);

        int offset = 0;
        for (int i = 0; i < runCount; i++) {
            int runLength = runLengths[i];
            int value = runValues[i];

            Arrays.fill(output, offset, offset + runLength, value);
            offset += runLength;
        }
    }

    private BlockAnalysis selectedEncodingMethod(int[] values, int offset, int length)
    {
        if (length == 0) {
            return new BlockAnalysis(RAW, 0);
        }

        int runCount = 1;
        int maxWidthMask = values[offset];
        int deltaMaxWidthMask = 0;

        for (int i = offset + 1; i < offset + length; i++) {
            int current = values[i];
            int previous = values[i - 1];

            int newRun = (current != previous) ? 1 : 0;
            runCount += newRun;

            maxWidthMask |= current;
            deltaMaxWidthMask |= (current - previous);
        }

        int rawSizeInBytes = length * Integer.BYTES;

        int maxRunLengthWidth = bitWidth(length);
        int maxValueWidth = bitWidth(maxWidthMask);
        int firstValueWidth = bitWidth(values[offset]);
        int deltaMaxWidth = bitWidth(deltaMaxWidthMask);

        // Assume all run lengths and run values use the maximum bit width.
        int rleSizeInBytes = BitPackingUtils.estimateEncodedIntsSizeInBytes(runCount, maxRunLengthWidth)
                             + BitPackingUtils.estimateEncodedIntsSizeInBytes(runCount, maxValueWidth)
                             + Integer.BYTES; // To store the run count
        // Assume all values use the maximum bit width.
        int bitPackingSizeInBytes = BitPackingUtils.estimateEncodedIntsSizeInBytes(length, maxValueWidth);
        // Assume all deltas use the maximum delta bit width.
        int deltaBitPackingSizeInBytes = BitPackingUtils.estimateDeltaEncodedIntsSizeInBytes(length, firstValueWidth, deltaMaxWidth);

        EncodingMethod method = RAW;
        int minSizeInBytes = rawSizeInBytes;

        if (rleSizeInBytes < minSizeInBytes) {
            minSizeInBytes = rleSizeInBytes;
            method = RLE;
        }
        if (bitPackingSizeInBytes < minSizeInBytes) {
            minSizeInBytes = bitPackingSizeInBytes;
            method = BITPACKING;
        }
        if (deltaBitPackingSizeInBytes < minSizeInBytes) {
            minSizeInBytes = deltaBitPackingSizeInBytes;
            method = BITPACKING_DELTA;
        }
        if (vByteEncodingEnabled) {
            int vByteSizeInBytes = VByteUtils.estimateEncodedIntsSizeInBytes(length, maxValueWidth);
            if (vByteSizeInBytes < minSizeInBytes) {
                method = V_BYTE;
            }
        }

        return new BlockAnalysis(method, runCount);
    }

    private static int bitWidth(int value)
    {
        return Integer.SIZE - Integer.numberOfLeadingZeros(value);
    }

    private record BlockAnalysis(EncodingMethod method, int runCount) {}

    enum EncodingMethod
    {
        RAW,
        RLE,
        BITPACKING,
        BITPACKING_DELTA,
        V_BYTE;

        public static EncodingMethod fromByte(byte id)
        {
            EncodingMethod[] values = values();
            checkArgument(id >= 0 && id < values.length, "Invalid EncodingMethod id: %s", id);
            return values[id];
        }

        public byte getId()
        {
            return (byte) ordinal();
        }
    }
}
