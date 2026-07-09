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
import io.trino.spi.block.RowBlock;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

import static io.trino.server.protocol.spooling.encoding.arrow.ArrowWriter.roundToSegment;
import static java.util.Objects.requireNonNull;

public final class RowWriter
        implements ArrowWriter
{
    private final StructVector vector;
    private final List<ArrowWriter> childWriters;

    public RowWriter(StructVector vector, List<ArrowWriter> childWriters)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.childWriters = requireNonNull(childWriters, "childWriters is null");
    }

    @Override
    public void initialize(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew();
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
                vector.setIndexDefined(position);
            }
        }

        List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
        for (int i = 0; i < fields.size(); i++) {
            Block childBlock = fields.get(i);
            childWriters.get(i).initialize(childBlock);
            childWriters.get(i).write(childBlock);
        }
        vector.setValueCount(positionCount);
    }

    @Override
    public long estimatedVectorSizeInBytes(Block block)
    {
        // Struct vector: a validity buffer plus each field vector.
        long size = roundToSegment((block.getPositionCount() + 7) / 8);
        List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
        for (int i = 0; i < fields.size(); i++) {
            size += childWriters.get(i).estimatedVectorSizeInBytes(fields.get(i));
        }
        return size;
    }

    @Override
    public String toString()
    {
        return ArrowWriter.describeWriter(this, vector, childWriters.toArray(new ArrowWriter[0]));
    }
}
