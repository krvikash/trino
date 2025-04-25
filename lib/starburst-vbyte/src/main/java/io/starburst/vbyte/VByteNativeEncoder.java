/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.vbyte;

import java.lang.foreign.MemorySegment;

import static io.starburst.vbyte.VByteNative.SIZE_OF_INT;
import static io.starburst.vbyte.VByteNative.SIZE_OF_LONG;

public final class VByteNativeEncoder
        implements VByteEncoder
{
    @Override
    public int encodeInts(int[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice((long) inputOffset * SIZE_OF_INT, (long) inputLength * SIZE_OF_INT);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return VByteNative.encode(inputSegment, inputLength, outputSegment);
    }

    @Override
    public int encodeLongs(long[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice((long) inputOffset * SIZE_OF_LONG, (long) inputLength * SIZE_OF_LONG);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return VByteNative.encode(inputSegment, inputLength * 2, outputSegment);
    }
}
