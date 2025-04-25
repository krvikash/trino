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

public final class VByteNativeDecoder
        implements VByteDecoder
{
    @Override
    public int decodeInts(byte[] input, int inputOffset, int inputLength, int encodedIntsCount, int[] output, int outputOffset)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice((long) outputOffset * SIZE_OF_INT);
        return VByteNative.decode(inputSegment, encodedIntsCount, outputSegment);
    }

    @Override
    public int decodeLongs(byte[] input, int inputOffset, int inputLength, int encodedLongsCount, long[] output, int outputOffset)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice((long) outputOffset * SIZE_OF_LONG);
        return VByteNative.decode(inputSegment, encodedLongsCount * 2, outputSegment);
    }
}
