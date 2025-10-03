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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import me.lemire.integercompression.BinaryPacking;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.SkippableIntegerCODEC;
import me.lemire.integercompression.VariableByte;
import me.lemire.integercompression.differential.IntegratedBinaryPacking;
import me.lemire.integercompression.differential.IntegratedVariableByte;
import me.lemire.integercompression.differential.SkippableIntegratedComposition;
import me.lemire.integercompression.differential.SkippableIntegratedIntegerCODEC;
import me.lemire.longcompression.LongBinaryPacking;
import me.lemire.longcompression.LongVariableByte;
import me.lemire.longcompression.SkippableLongCODEC;
import me.lemire.longcompression.SkippableLongComposition;
import me.lemire.longcompression.differential.LongDelta;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

final class BitPackingUtils
{
    @VisibleForTesting
    public static final int INT_BLOCK_SIZE = BinaryPacking.BLOCK_SIZE;
    private static final int INT_GROUP_SIZE_IN_BLOCKS = 4;
    @VisibleForTesting
    public static final int LONG_BLOCK_SIZE = LongBinaryPacking.BLOCK_SIZE;
    private static final int LONG_GROUP_SIZE_IN_BLOCKS = 8;
    private static final SkippableIntegerCODEC INT_CODEC = new SkippableComposition(new BinaryPacking(), new HeapVariableByte());
    private static final SkippableIntegratedIntegerCODEC INT_DELTA_CODEC = new SkippableIntegratedComposition(new IntegratedBinaryPacking(), new HeapIntegratedVariableByte());
    private static final SkippableLongCODEC LONG_CODEC = new SkippableLongComposition(new LongBinaryPacking(), new HeapLongVariableByte());

    static {
        // Verify that the assumption that both codecs use the same block size holds
        checkState(BinaryPacking.BLOCK_SIZE == IntegratedBinaryPacking.BLOCK_SIZE, "Block size differs between codecs");
    }

    private BitPackingUtils() {}

    public static int estimateEncodedIntsSizeInBytes(int length, int valueBitWidth)
    {
        return estimateEncodedIntsSizeInBytes(length, valueBitWidth, valueBitWidth);
    }

    public static int estimateDeltaEncodedIntsSizeInBytes(int length, int firstValueBitWidth, int deltaBitWidth)
    {
        return estimateEncodedIntsSizeInBytes(length, firstValueBitWidth, deltaBitWidth);
    }

    private static int estimateEncodedIntsSizeInBytes(int length, int firstValueBitWidth, int remainingValuesBitWidth)
    {
        checkArgument(length > 0, "length must be greater than 0");

        // Given an array [v0, v1, ..., vN], bit-packing encoding divides the input into fixed-size blocks of 32 values (INT_BLOCK_SIZE).
        // Each block is encoded in (maxBitWidth(v_i..v_{i+31}) * 31) bits, rounded up to a multiple of 32 bits for alignment.
        //
        // This method does not compute max bit width per block. Instead, it assumes:
        //   - the first value uses a known bit width: firstValueBitWidth
        //   - all remaining values use another known bit width: remainingValuesBitWidth
        //
        // The encoded layout consists of two parts:
        //
        // 1. Groups of 4 blocks, each preceded by a single 32-bit header word encoding 4 block headers (H0..H3).
        //    Layout of one group:
        //    +------------------+-------------------------+-----+------------------------------+
        //    |  Headers H0..H3  |    Block 0 (v0..v31)    | ... |      Block 3 (v96..v127)     |
        //    +------------------+-------------------------+-----+------------------------------+
        //    |       1 int      | firstValueBitWidth ints | ... | remainingValuesBitWidth ints |
        //    +------------------+-------------------------+-----+------------------------------+
        //
        // 2. Remainder blocks (if the block count is not a multiple of 4). Each block has its own 32-bit header followed by encoded data.
        //    Layout of one remainder block:
        //    +------------------+----------------------------------------------------+
        //    |     Header Hn    |                      Block n                       |
        //    +------------------+----------------------------------------------------+
        //    |       1 int      | firstValueBitWidth or remainingValuesBitWidth ints |
        //    +------------------+----------------------------------------------------+
        int blockCount = length / INT_BLOCK_SIZE;
        int headersSizeInInts = blockCount / INT_GROUP_SIZE_IN_BLOCKS + (blockCount % INT_GROUP_SIZE_IN_BLOCKS);
        int blocksSizeInInts = 0;
        if (blockCount > 0) {
            blocksSizeInInts = Math.max(firstValueBitWidth, remainingValuesBitWidth) + Math.max(0, (blockCount - 1) * remainingValuesBitWidth);
        }
        int bitPackedLengthInInts = headersSizeInInts + blocksSizeInInts;

        // The remaining integers (length % INT_BLOCK_SIZE) are encoded using variable-byte encoding.
        //
        // Variable-byte encoding represents each integer with 1–5 bytes, depending on its bit width:
        //
        //   Bit width range   Encoded size
        //   ---------------  ----------------
        //      0 – 7 bits    1 byte
        //     8 – 14 bits    2 bytes
        //    15 – 21 bits    3 bytes
        //    22 – 28 bits    4 bytes
        //    29 – 32 bits    5 bytes
        //
        // The entire encoded stream is padded to an 4-byte boundary.
        //
        // If no values were bit-packed, the first value is encoded separately using firstValueBitWidth,
        // and the remaining values are encoded with remainingValuesBitWidth. Otherwise, all remainder
        // values use remainingValuesBitWidth.
        int remainder = length % INT_BLOCK_SIZE;
        int remainingValuesInBytes = variableByteSizeForInt(remainingValuesBitWidth);
        int reminderSizeInBytes;
        if (bitPackedLengthInInts == 0) {
            int firstValueSizeInBytes = variableByteSizeForInt(firstValueBitWidth);
            reminderSizeInBytes = firstValueSizeInBytes + (remainder - 1) * remainingValuesInBytes;
            reminderSizeInBytes += Integer.BYTES; // JavaFastPFOR adds an extra 4 bytes when the first codec encodes no values.
        }
        else {
            reminderSizeInBytes = remainder * remainingValuesInBytes;
        }
        int variableByteSizeInInts = (reminderSizeInBytes + Integer.BYTES - 1) / Integer.BYTES;

        // The calculated sizes above use ints for consistency with the library, which operates on int arrays.
        // Now we need to convert them to bytes so the estimation is comparable with other encoding methods.
        return (bitPackedLengthInInts + variableByteSizeInInts) * Integer.BYTES + Integer.BYTES; // +4 bytes for encoded length
    }

    private static int variableByteSizeForInt(int bitWidth)
    {
        return Math.max(1, (bitWidth + 6) / 7);
    }

    public static void encode(SliceOutput output, int[] values, int offset, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        int maxEncodedLength = INT_CODEC.maxHeadlessCompressedLength(new IntWrapper(), length);
        int[] encoded = new int[maxEncodedLength];
        IntWrapper encodedOffset = new IntWrapper(0);
        IntWrapper valuesOffset = new IntWrapper(offset);

        INT_CODEC.headlessCompress(values, valuesOffset, length, encoded, encodedOffset);

        int encodedLength = encodedOffset.intValue();
        output.writeInt(encodedLength);
        output.writeInts(encoded, 0, encodedLength);
    }

    public static void decode(SliceInput input, int[] values, int length)
    {
        decode(input, values, 0, length);
    }

    public static void decode(SliceInput input, int[] values, int offset, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        int encodedLength = input.readInt();
        int[] encoded = new int[encodedLength];
        IntWrapper encodedOffset = new IntWrapper(0);
        IntWrapper valuesOffset = new IntWrapper(offset);

        input.readInts(encoded, 0, encodedLength);
        INT_CODEC.headlessUncompress(encoded, encodedOffset, encodedLength, values, valuesOffset, length);
    }

    public static void encodeDelta(SliceOutput output, int[] values, int offset, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        int maxEncodedLength = INT_CODEC.maxHeadlessCompressedLength(new IntWrapper(), length);
        int[] encoded = new int[maxEncodedLength];
        IntWrapper encodedOffset = new IntWrapper(0);
        IntWrapper valuesOffset = new IntWrapper(offset);
        IntWrapper initialValue = new IntWrapper(0);

        INT_DELTA_CODEC.headlessCompress(values, valuesOffset, length, encoded, encodedOffset, initialValue);

        int encodedLength = encodedOffset.intValue();
        output.writeInt(encodedLength);
        output.writeInts(encoded, 0, encodedLength);
    }

    public static void decodeDelta(SliceInput input, int[] values, int offset, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        int encodedLength = input.readInt();
        int[] encoded = new int[encodedLength];
        IntWrapper encodedOffset = new IntWrapper(0);
        IntWrapper valuesOffset = new IntWrapper(offset);
        IntWrapper initialValue = new IntWrapper(0);

        input.readInts(encoded, 0, encodedLength);
        INT_DELTA_CODEC.headlessUncompress(encoded, encodedOffset, encoded.length, values, valuesOffset, length, initialValue);
    }

    public static int estimateEncodedLongsSizeInBytes(int length, int valueBitWidth)
    {
        return estimateEncodedLongsSizeInBytes(length, valueBitWidth, valueBitWidth);
    }

    public static int estimateDeltaEncodedLongsSizeInBytes(int length, int firstValueBitWidth, int deltaBitWidth)
    {
        return estimateEncodedLongsSizeInBytes(length, firstValueBitWidth, deltaBitWidth);
    }

    private static int estimateEncodedLongsSizeInBytes(int length, int firstValueBitWidth, int remainingValuesBitWidth)
    {
        checkArgument(length > 0, "length must be greater than 0");

        // Given an array [v0, v1, ..., vN], bit-packing encoding divides the input into fixed-size blocks of 64 values (LONG_BLOCK_SIZE).
        // Each block is encoded in (maxBitWidth(v_i..v_{i+63}) * 64) bits, rounded up to a multiple of 64 bits for alignment.
        //
        // This method does not compute max bit width per block. Instead, it assumes:
        //   - the first value uses a known bit width: firstValueBitWidth
        //   - all remaining values use another known bit width: remainingValuesBitWidth
        //
        // The encoded stream has two parts:
        //
        // 1. Groups of 8 blocks, each preceded by a single 64-bit header word encoding 8 block headers (H0..H7).
        //    Layout of one group:
        //      +-------------------+--------------------------+-----+-------------------------------+
        //      |   Headers H0..H7  |    Block 0 (v0..v63)     | ... |     Block 7 (v448..v511)      |
        //      +-------------------+--------------------------+-----+-------------------------------+
        //      |       1 long      | firstValueBitWidth longs | ... | remainingValuesBitWidth longs |
        //      +-------------------+--------------------------+-----+-------------------------------+
        //
        // 2. Remainder blocks (if the block count is not a multiple of 8). Each block has its own 64-bit header followed by encoded data.
        //    Layout of one remainder block:
        //      +-------------------+-----------------------------------------------------+
        //      |     Header Hn     |                      Block n                        |
        //      +-------------------+-----------------------------------------------------+
        //      |       1 long      | firstValueBitWidth or remainingValuesBitWidth longs |
        //      +-------------------+-----------------------------------------------------+
        int blockCount = length / LONG_BLOCK_SIZE;
        int headersSizeInLongs = blockCount / LONG_GROUP_SIZE_IN_BLOCKS + (blockCount % LONG_GROUP_SIZE_IN_BLOCKS);
        int blocksSizeInLongs = 0;
        if (blockCount > 0) {
            blocksSizeInLongs = Math.max(firstValueBitWidth, remainingValuesBitWidth) + Math.max(0, (blockCount - 1) * remainingValuesBitWidth);
        }
        int bitPackedSizeInLongs = headersSizeInLongs + blocksSizeInLongs;

        // The remaining longs (length % LONG_BLOCK_SIZE) are encoded using variable-byte encoding.
        //
        // Variable-byte encoding represents each long with 1–10 bytes, depending on its bit width:
        //
        //   Bit width range   Encoded size
        //   ---------------  ----------------
        //      0 – 7 bits    1 byte
        //     8 – 14 bits    2 bytes
        //    15 – 21 bits    3 bytes
        //    22 – 28 bits    4 bytes
        //    29 – 35 bits    5 bytes
        //    36 – 42 bits    6 bytes
        //    43 – 49 bits    7 bytes
        //    50 – 56 bits    8 bytes
        //    57 – 63 bits    9 bytes
        //         64 bits    10 bytes
        //
        // The entire encoded stream is padded to an 8-byte boundary.
        //
        // If no values were bit-packed, the first value is encoded separately using firstValueBitWidth,
        // and the remaining values are encoded with remainingValuesBitWidth. Otherwise, all remainder
        // values use remainingValuesBitWidth.
        int remainder = length % LONG_BLOCK_SIZE;
        int remainingValuesInBytes = variableByteSizeForLong(remainingValuesBitWidth);
        int reminderSizeInBytes;
        if (bitPackedSizeInLongs == 0) {
            int firstValueSizeInBytes = variableByteSizeForLong(firstValueBitWidth);
            reminderSizeInBytes = firstValueSizeInBytes + (remainder - 1) * remainingValuesInBytes;
            reminderSizeInBytes += Long.BYTES; // JavaFastPFOR adds an extra 8 bytes when the first codec encodes no values.
        }
        else {
            reminderSizeInBytes = remainder * remainingValuesInBytes;
        }
        int variableByteSizeInLongs = (reminderSizeInBytes + Long.BYTES - 1) / Long.BYTES;

        // The calculated sizes above use longs for consistency with the library, which operates on longs arrays.
        // Now we need to convert them to bytes so the estimation is comparable with other encoding methods.
        return (bitPackedSizeInLongs + variableByteSizeInLongs) * Long.BYTES + Integer.BYTES; // +4 bytes for encoded length
    }

    private static int variableByteSizeForLong(int bitWidth)
    {
        return Math.max(1, (bitWidth + 6) / 7);
    }

    public static void encode(SliceOutput output, long[] values, int offset, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        int maxEncodedLength = LONG_CODEC.maxHeadlessCompressedLength(new IntWrapper(), length);
        long[] encoded = new long[maxEncodedLength];
        IntWrapper encodedOffset = new IntWrapper(0);
        IntWrapper valuesOffset = new IntWrapper(offset);

        LONG_CODEC.headlessCompress(values, valuesOffset, length, encoded, encodedOffset);

        int encodedLength = encodedOffset.intValue();
        output.writeInt(encodedLength);
        output.writeLongs(encoded, 0, encodedLength);
    }

    public static void decode(SliceInput input, long[] values, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        int encodedLength = input.readInt();
        long[] encoded = new long[encodedLength];
        IntWrapper encodedOffset = new IntWrapper(0);
        IntWrapper valuesOffset = new IntWrapper(0);

        input.readLongs(encoded, 0, encodedLength);
        LONG_CODEC.headlessUncompress(encoded, encodedOffset, encodedLength, values, valuesOffset, length);
    }

    public static void encodeDelta(SliceOutput output, long[] values, int offset, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        long[] deltas = new long[length];
        System.arraycopy(values, offset, deltas, 0, length);
        LongDelta.delta(deltas);
        encode(output, deltas, 0, length);
    }

    public static void decodeDelta(SliceInput input, long[] values, int length)
    {
        checkArgument(length > 0, "length must be greater than 0");

        decode(input, values, length);
        LongDelta.fastinverseDelta(values, 0, length, 0);
    }

    // Heap-based variants of VariableByte encoders.
    // The original implementations allocate direct byte buffers without reusing them.
    // These versions use heap buffers instead to reduce GC pressure.

    private static class HeapVariableByte
            extends VariableByte
    {
        @Override
        protected ByteBuffer makeBuffer(int sizeInBytes)
        {
            return ByteBuffer.allocate(sizeInBytes);
        }
    }

    private static class HeapIntegratedVariableByte
            extends IntegratedVariableByte
    {
        @Override
        protected ByteBuffer makeBuffer(int sizeInBytes)
        {
            return ByteBuffer.allocate(sizeInBytes);
        }
    }

    private static class HeapLongVariableByte
            extends LongVariableByte
    {
        @Override
        protected ByteBuffer makeBuffer(int sizeInBytes)
        {
            return ByteBuffer.allocate(sizeInBytes);
        }
    }
}
