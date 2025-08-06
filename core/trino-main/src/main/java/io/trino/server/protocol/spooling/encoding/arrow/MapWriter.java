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
import io.trino.spi.block.ColumnarMap;
import org.apache.arrow.vector.complex.MapVector;

import static java.util.Objects.requireNonNull;

public final class MapWriter
        implements ArrowWriter
{
    private final MapVector vector;
    private final ArrowWriter keyWriter;
    private final ArrowWriter valueWriter;

    public MapWriter(MapVector vector, ArrowWriter keyWriter, ArrowWriter valueWriter)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.keyWriter = requireNonNull(keyWriter, "keyWriter is null");
        this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
    }

    @Override
    public void initialize(Block block)
    {
        ColumnarMap mapBlock = ColumnarMap.toColumnarMap(block);
        Block valueBlock = mapBlock.getValuesBlock();
        vector.setInitialCapacity(valueBlock.getPositionCount());
        vector.allocateNew();
    }

    @Override
    public void write(Block block)
    {
        ColumnarMap mapBlock = ColumnarMap.toColumnarMap(block);
        Block keyBlock = mapBlock.getKeysBlock();
        Block valueBlock = mapBlock.getValuesBlock();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
                continue;
            }

            vector.startNewValue(position);
            int entries = mapBlock.getEntryCount(position);
            vector.endValue(position, entries);
        }
        keyWriter.write(keyBlock);
        valueWriter.write(valueBlock);
        vector.setValueCount(block.getPositionCount());
    }

    @Override
    public String toString()
    {
        return ArrowWriter.describeWriter(this, vector, keyWriter, valueWriter);
    }
}
