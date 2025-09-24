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
import io.starburst.vbyte.VByteDecoder;
import io.starburst.vbyte.VByteEncoder;

public final class VByteUtils
{
    private VByteUtils() {}

    public static void vByteEncodeLongs(SliceOutput sliceOutput, long[] values, int valuesOffset, int valuesCount)
    {
        byte[] vbyteEncodedValues = new byte[VByteEncoder.maxLongsEncodedLength(valuesCount)];
        int vbyteEncodedValuesSize = VByteEncoder.encodeLongs(values, valuesOffset, valuesCount, vbyteEncodedValues, 0, vbyteEncodedValues.length);
        sliceOutput.writeInt(vbyteEncodedValuesSize);
        sliceOutput.write(vbyteEncodedValues, 0, vbyteEncodedValuesSize);
    }

    public static void vByteDecodeLongs(SliceInput sliceInput, int valuesCount, long[] values)
    {
        int vbyteEncodedValuesSize = sliceInput.readInt();
        byte[] vbyteEncodedValues = new byte[vbyteEncodedValuesSize];
        sliceInput.read(vbyteEncodedValues);
        VByteDecoder.decodeLongs(vbyteEncodedValues, 0, vbyteEncodedValues.length, valuesCount, values, 0);
    }

    public static void vByteEncodeInts(SliceOutput sliceOutput, int[] values, int valuesOffset, int valuesCount)
    {
        byte[] vbyteEncodedValues = new byte[VByteEncoder.maxLongsEncodedLength(valuesCount)];
        int vbyteEncodedValuesSize = VByteEncoder.encodeInts(values, valuesOffset, valuesCount, vbyteEncodedValues, 0, vbyteEncodedValues.length);
        sliceOutput.writeInt(vbyteEncodedValuesSize);
        sliceOutput.write(vbyteEncodedValues, 0, vbyteEncodedValuesSize);
    }

    public static void vByteDecodeInts(SliceInput sliceInput, int valuesCount, int[] values)
    {
        vByteDecodeInts(sliceInput, valuesCount, values, 0);
    }

    public static void vByteDecodeInts(SliceInput sliceInput, int valuesCount, int[] values, int valuesOffset)
    {
        int vByteEncodedValuesSize = sliceInput.readInt();
        byte[] vByteEncodedValues = new byte[vByteEncodedValuesSize];
        sliceInput.read(vByteEncodedValues);
        VByteDecoder.decodeInts(vByteEncodedValues, 0, vByteEncodedValues.length, valuesCount, values, valuesOffset);
    }

    public static int estimateEncodedIntsSizeInBytes(int length, int maxValueWidth)
    {
        // Follows the StreamVByte format and alternative encoding specifications:
        // https://github.com/fast-pack/streamvbyte?tab=readme-ov-file#format-specification
        // https://github.com/fast-pack/streamvbyte?tab=readme-ov-file#alternative-encoding
        int controlBytes = (length + 3) / 4;
        int byteCountPerValue = estimateByteCountPerValue(maxValueWidth);
        long dataBytes = (long) length * byteCountPerValue;
        long estimatedSize = (long) controlBytes + dataBytes + Integer.BYTES; // +4 for the encoded size
        return (int) Math.min(estimatedSize, Integer.MAX_VALUE);
    }

    public static int estimateEncodedLongsSizeInBytes(int length, int maxValueWidth)
    {
        // Follows the StreamVByte format and alternative encoding specifications:
        // https://github.com/fast-pack/streamvbyte?tab=readme-ov-file#format-specification
        // https://github.com/fast-pack/streamvbyte?tab=readme-ov-file#alternative-encoding
        int controlBytes = (length * 2 + 3) / 4;
        int lowMaxValueWidth = Math.min(32, maxValueWidth);
        int highMaxValueWidth = Math.max(0, maxValueWidth - 32);
        int byteCountPerValue = estimateByteCountPerValue(lowMaxValueWidth) + estimateByteCountPerValue(highMaxValueWidth);
        long dataBytes = (long) length * byteCountPerValue;
        long estimatedSize = (long) controlBytes + dataBytes + Integer.BYTES; // +4 for the encoded size
        return (int) Math.min(estimatedSize, Integer.MAX_VALUE);
    }

    private static int estimateByteCountPerValue(int maxValueWidth)
    {
        if (maxValueWidth == 0) {
            return 0;
        }
        else if (maxValueWidth > 16) {
            return 4;
        }
        return (maxValueWidth + 7) / 8;
    }
}
