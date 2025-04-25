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

public sealed interface VByteDecoder
        permits VByteNativeDecoder
{
    static VByteDecoder create()
    {
        VByteNative.verifyEnabled();
        return new VByteNativeDecoder();
    }

    /**
     * Decode encodedIntsCount integers encoded using vbyte encoding from input byte array.
     *
     * @return number or read bytes
     */
    int decodeInts(byte[] input, int inputOffset, int inputLength, int encodedIntsCount, int[] output, int outputOffset);

    /**
     * Decode encodedLongsCount longs encoded using vbyte encoding from input byte array.
     *
     * @return number or read bytes
     */
    int decodeLongs(byte[] input, int inputOffset, int inputLength, int encodedLongsCount, long[] output, int outputOffset);
}
