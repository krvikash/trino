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

import org.junit.jupiter.api.Test;

import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestVByteEncoding
{
    @Test
    void testIntRoundTrip()
    {
        int count = 1000;

        int[] input = new int[count];
        int skip = Integer.MAX_VALUE / count * 2;
        int value = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            input[i] = value;
            value += skip;
        }

        testIntRoundTrip(input);
        testIntRoundTrip(input, 100, 500); // subArray
    }

    @Test
    public void testIntEffectiveness()
    {
        // test small values
        int[] input = new int[100];
        for (int i = 0; i < input.length; i++) {
            input[i] = i;
        }

        long encodedLength = testIntRoundTrip(input);
        assertThat(encodedLength).isEqualTo(124);
    }

    private static int testIntRoundTrip(int[] input)
    {
        return testIntRoundTrip(input, 0, input.length);
    }

    private static int testIntRoundTrip(int[] input, int offset, int count)
    {
        byte[] encoded = new byte[VByteUtils.maxIntsEncodedLength(count)];
        int encodedLength = VByteEncoder.encodeInts(input, offset, count, encoded, 0, encoded.length);

        int[] decoded = new int[count];
        int decodedLength = VByteDecoder.decodeInts(encoded, 0, encodedLength, count, decoded, 0);

        assertThat(encodedLength).isEqualTo(decodedLength);
        assertThat(decoded).containsExactly(copyOfRange(input, offset, offset + count));
        return encodedLength;
    }

    @Test
    void testLongRoundTrip()
    {
        int count = 1000;

        long[] input = new long[count];
        long skip = Long.MAX_VALUE / count * 2;
        long value = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            input[i] = value;
            value += skip;
        }

        testLongRoundTrip(input);
        testLongRoundTrip(input, 100, 500); // subArray
    }

    @Test
    public void testLongEffectiveness()
    {
        // test small values
        long[] input = new long[100];
        for (int i = 0; i < input.length; i++) {
            input[i] = i;
        }

        long encodedLength = testLongRoundTrip(input);
        assertThat(encodedLength).isEqualTo(149);
    }

    private static int testLongRoundTrip(long[] input)
    {
        return testLongRoundTrip(input, 0, input.length);
    }

    private static int testLongRoundTrip(long[] input, int offset, int count)
    {
        byte[] encoded = new byte[VByteUtils.maxLongsEncodedLength(count)];
        int encodedLength = VByteEncoder.encodeLongs(input, offset, count, encoded, 0, encoded.length);

        long[] decoded = new long[count];
        int decodedLength = VByteDecoder.decodeLongs(encoded, 0, encodedLength, count, decoded, 0);

        assertThat(encodedLength).isEqualTo(decodedLength);
        assertThat(decoded).containsExactly(copyOfRange(input, offset, offset + count));
        return encodedLength;
    }
}
