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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.block.AdaptiveIntEncoding.EncodingMethod.BITPACKING;
import static io.trino.block.AdaptiveIntEncoding.EncodingMethod.BITPACKING_DELTA;
import static io.trino.block.AdaptiveIntEncoding.EncodingMethod.RAW;
import static io.trino.block.AdaptiveIntEncoding.EncodingMethod.RLE;
import static io.trino.block.AdaptiveIntEncoding.EncodingMethod.V_BYTE;
import static io.trino.block.VByteUtils.vByteDecodeInts;
import static io.trino.block.VByteUtils.vByteEncodeInts;

class AdaptiveIntEncoding
{
    private final boolean vByteEncodingEnabled;

    public AdaptiveIntEncoding(boolean vByteEncodingEnabled)
    {
        this.vByteEncodingEnabled = vByteEncodingEnabled;
    }

    public void decode(SliceInput input, int[] values, int length)
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
                throw new IllegalArgumentException("Unsupported block mode: " + method);
        }
    }

    public void encode(SliceOutput output, int[] values, int offset, int length)
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
