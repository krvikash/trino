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

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import org.apache.arrow.vector.VarBinaryVector;

import static io.trino.server.protocol.spooling.encoding.arrow.ArrowBulkCopy.copyVariableWidth;
import static io.trino.server.protocol.spooling.encoding.arrow.ArrowBulkCopy.writeValidity;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public final class VarbinaryWriter
        extends VariableWidthWriter<VarBinaryVector>
{
    public VarbinaryWriter(VarBinaryVector vector)
    {
        super(vector);
    }

    @Override
    public void write(Block block)
    {
        int positionCount = block.getPositionCount();
        if (block instanceof VariableWidthBlock valueBlock) {
            copyVariableWidth(vector, valueBlock, positionCount);
            writeValidity(vector, valueBlock, positionCount);
            vector.setLastSet(positionCount - 1);
            vector.setValueCount(positionCount);
            return;
        }
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
            }
            else {
                Slice slice = VARBINARY.getSlice(block, position);
                vector.setSafe(position, slice.byteArray(), slice.byteArrayOffset(), slice.length());
            }
        }
        vector.setValueCount(positionCount);
    }
}
