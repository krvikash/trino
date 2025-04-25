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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.starburst.vbyte.VByteDecoder;
import io.starburst.vbyte.VByteEncoder;

public final class VByteUtils
{
    private VByteUtils() {}

    public static void vByteEncodeLongs(VByteEncoder vByteEncoder, SliceOutput sliceOutput, long[] values, int valuesOffset, int valuesCount)
    {
        byte[] vbyteEncodedValues = new byte[vByteEncoder.maxLongsEncodedLength(valuesCount)];
        int vbyteEncodedValuesSize = vByteEncoder.encodeLongs(values, valuesOffset, valuesCount, vbyteEncodedValues, 0, vbyteEncodedValues.length);
        sliceOutput.writeInt(vbyteEncodedValuesSize);
        sliceOutput.write(vbyteEncodedValues, 0, vbyteEncodedValuesSize);
    }

    public static void vByteDecodeLongs(VByteDecoder vByteDecoder, SliceInput sliceInput, int valuesCount, long[] values)
    {
        int vbyteEncodedValuesSize = sliceInput.readInt();
        byte[] vbyteEncodedValues = new byte[vbyteEncodedValuesSize];
        sliceInput.read(vbyteEncodedValues);
        vByteDecoder.decodeLongs(vbyteEncodedValues, 0, vbyteEncodedValues.length, valuesCount, values, 0);
    }
}
