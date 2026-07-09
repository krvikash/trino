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
import io.trino.spi.block.IntArrayBlock;
import org.apache.arrow.vector.IntVector;

import java.lang.foreign.MemorySegment;

import static io.trino.server.protocol.spooling.encoding.arrow.ArrowBulkCopy.copyFixedWidth;
import static io.trino.server.protocol.spooling.encoding.arrow.ArrowBulkCopy.writeValidity;
import static io.trino.spi.type.IntegerType.INTEGER;

public final class IntegerWriter
        extends FixedWidthWriter<IntVector>
{
    public IntegerWriter(IntVector vector)
    {
        super(vector);
    }

    @Override
    public void write(Block block)
    {
        int positionCount = block.getPositionCount();
        if (block instanceof IntArrayBlock valueBlock) {
            copyFixedWidth(vector, MemorySegment.ofArray(valueBlock.getRawValues()), valueBlock.getRawValuesOffset(), positionCount);
            writeValidity(vector, valueBlock, positionCount);
            vector.setValueCount(positionCount);
            return;
        }
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
            }
            else {
                vector.set(position, INTEGER.getInt(block, position));
            }
        }
        vector.setValueCount(positionCount);
    }
}
