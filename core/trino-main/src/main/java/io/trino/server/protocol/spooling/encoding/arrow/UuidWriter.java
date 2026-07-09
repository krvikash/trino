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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.type.Int128;
import org.apache.arrow.vector.FixedSizeBinaryVector;

public final class UuidWriter
        extends FixedWidthWriter<FixedSizeBinaryVector>
{
    public UuidWriter(FixedSizeBinaryVector vector)
    {
        super(vector);
    }

    @Override
    public void write(Block block)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
            }
            else {
                Int128ArrayBlock arrayBlock = (Int128ArrayBlock) block;
                Int128 uuid = arrayBlock.getInt128(position);
                vector.set(position, uuid.toBigEndianBytes());
            }
        }
        vector.setValueCount(positionCount);
    }
}
