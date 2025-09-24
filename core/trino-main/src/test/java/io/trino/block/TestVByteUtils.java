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

import io.airlift.slice.DynamicSliceOutput;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class TestVByteUtils
{
    @Test
    void testEstimateEncodedIntsSizeInBytes()
    {
        for (int length = 1; length < 1024; length++) {
            int[] values = new int[length];

            for (int bitWidth = 0; bitWidth <= 32; bitWidth++) {
                int value = bitWidth == 32 ? -1 : (1 << bitWidth) - 1;
                Arrays.fill(values, value);

                int estimatedSizeInBytes = VByteUtils.estimateEncodedIntsSizeInBytes(length, bitWidth);

                DynamicSliceOutput encoded = new DynamicSliceOutput(0);
                VByteUtils.vByteEncodeInts(encoded, values, 0, length);
                assertThat(estimatedSizeInBytes).isEqualTo(encoded.size());
            }
        }
    }

    @Test
    void testEstimateEncodedIntsSizeInBytesOverflow()
    {
        int estimatedSizeInBytes = VByteUtils.estimateEncodedIntsSizeInBytes(Integer.MAX_VALUE, 32);
        assertThat(estimatedSizeInBytes).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testEstimateEncodedLongsSizeInBytes()
    {
        for (int length = 1; length < 1024; length++) {
            long[] values = new long[length];

            for (int bitWidth = 0; bitWidth <= 64; bitWidth++) {
                long value = bitWidth == 64 ? -1 : (1L << bitWidth) - 1;
                Arrays.fill(values, value);

                int estimatedSizeInBytes = VByteUtils.estimateEncodedLongsSizeInBytes(length, bitWidth);

                DynamicSliceOutput encoded = new DynamicSliceOutput(0);
                VByteUtils.vByteEncodeLongs(encoded, values, 0, length);
                assertThat(estimatedSizeInBytes).isEqualTo(encoded.size());
            }
        }
    }

    @Test
    void testEstimateEncodedLongsSizeInBytesOverflow()
    {
        int estimatedSizeInBytes = VByteUtils.estimateEncodedLongsSizeInBytes(Integer.MAX_VALUE, 32);
        assertThat(estimatedSizeInBytes).isEqualTo(Integer.MAX_VALUE);
    }
}
