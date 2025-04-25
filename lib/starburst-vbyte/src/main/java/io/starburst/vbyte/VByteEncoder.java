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

public sealed interface VByteEncoder
        permits VByteNativeEncoder
{
    static VByteEncoder create()
    {
        VByteNative.verifyEnabled();
        return new VByteNativeEncoder();
    }

    default int maxIntsEncodedLength(int inputIntsCount)
    {
        return VByteUtils.maxIntsEncodedLength(inputIntsCount);
    }

    default int maxLongsEncodedLength(int inputLongsCount)
    {
        return VByteUtils.maxLongsEncodedLength(inputLongsCount);
    }

    /**
     * Encode array of longs using vbyte encoding.
     *
     * @return number of bytes written to the output
     */
    int encodeLongs(long[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength);

    /**
     * Encode array o integers using vbyte encoding.
     *
     * @return number of bytes written to the output
     */
    int encodeInts(int[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength);
}
