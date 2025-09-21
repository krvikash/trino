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

class TestBitPackingUtils
{
    // We want to test consecutive lengths up to this limit to ensure input arrays that
    // are not aligned to the block size are handled correctly. The factor of 5 is arbitrary,
    // chosen simply to cover cases with multiple blocks.
    private static final int MAX_INT_ARRAY_LENGTH = BitPackingUtils.INT_BLOCK_SIZE * 5;

    @Test
    void testEstimateEncodedIntsSizeInBytes()
    {
        for (int length = 1; length < MAX_INT_ARRAY_LENGTH; length++) {
            int[] values = new int[length];

            for (int bitWidth = 0; bitWidth <= 32; bitWidth++) {
                int value = bitWidth == 32 ? -1 : (1 << bitWidth) - 1;
                Arrays.fill(values, value);

                int estimatedSizeInBytes = BitPackingUtils.estimateEncodedIntsSizeInBytes(length, bitWidth);

                DynamicSliceOutput encoded = new DynamicSliceOutput(0);
                BitPackingUtils.encode(encoded, values, 0, length);
                assertThat(estimatedSizeInBytes).isEqualTo(encoded.size());
            }
        }
    }

    @Test
    void testEstimateDeltaEncodedIntsSizeInBytes()
    {
        for (int length = 1; length < MAX_INT_ARRAY_LENGTH; length++) {
            int[] values = new int[length];

            for (int firstValueBitWidth = 0; firstValueBitWidth <= 32; firstValueBitWidth++) {
                int firstValue = firstValueBitWidth == 32 ? -1 : (1 << firstValueBitWidth) - 1;
                values[0] = firstValue;

                for (int deltaBitWidth = 0; deltaBitWidth <= 32; deltaBitWidth++) {
                    int delta = deltaBitWidth == 32 ? -1 : (1 << deltaBitWidth) - 1;
                    for (int i = 1; i < length; i++) {
                        values[i] = values[i - 1] + delta;
                    }

                    int estimatedSizeInBytes = BitPackingUtils.estimateDeltaEncodedIntsSizeInBytes(length, firstValueBitWidth, deltaBitWidth);

                    DynamicSliceOutput encoded = new DynamicSliceOutput(0);
                    BitPackingUtils.encodeDelta(encoded, values, 0, length);
                    assertThat(estimatedSizeInBytes).isEqualTo(encoded.size());
                }
            }
        }
    }
}
