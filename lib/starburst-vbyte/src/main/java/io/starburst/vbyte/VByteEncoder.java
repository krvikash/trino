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
import static java.lang.ref.Reference.reachabilityFence;

public final class VByteEncoder
{
    private VByteEncoder() {}

    public static int maxIntsEncodedLength(int inputIntsCount)
    {
        return VByteUtils.maxIntsEncodedLength(inputIntsCount);
    }

    public static int maxLongsEncodedLength(int inputLongsCount)
    {
        return VByteUtils.maxLongsEncodedLength(inputLongsCount);
    }

    /**
     * Encode array of longs using vbyte encoding.
     *
     * @return number of bytes written to the output
     */
    public static int encodeInts(int[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice((long) inputOffset * SIZE_OF_INT, (long) inputLength * SIZE_OF_INT);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        try {
            return VByteNative.encode(inputSegment, inputLength, outputSegment);
        }
        finally {
            reachabilityFence(input);
            reachabilityFence(output);
        }
    }

    /**
     * Encode array o integers using vbyte encoding.
     *
     * @return number of bytes written to the output
     */
    public static int encodeLongs(long[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice((long) inputOffset * SIZE_OF_LONG, (long) inputLength * SIZE_OF_LONG);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        try {
            return VByteNative.encode(inputSegment, inputLength * 2, outputSegment);
        }
        finally {
            reachabilityFence(input);
            reachabilityFence(output);
        }
    }
}
