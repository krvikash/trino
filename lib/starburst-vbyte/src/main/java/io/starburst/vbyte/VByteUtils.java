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

final class VByteUtils
{
    private VByteUtils() {}

    public static int maxIntsEncodedLength(int inputIntsCount)
    {
        int controlBytes = (inputIntsCount + 3) / 4;
        int dataBytes = inputIntsCount * 4;
        int padding = 16;
        return controlBytes + dataBytes + padding;
    }

    public static int maxLongsEncodedLength(int inputLongsCount)
    {
        return maxIntsEncodedLength(inputLongsCount * 2);
    }
}
