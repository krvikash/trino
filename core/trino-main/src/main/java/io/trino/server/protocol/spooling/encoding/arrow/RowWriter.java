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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

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
        List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
        List<FieldVector> children = vector.getChildrenFromFields();
        for (int i = 0; i < children.size(); i++) {
            Block childBlock = fields.get(i);

            for (int position = 0; position < childBlock.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    vector.setNull(position);
                }
                else {
                    vector.setIndexDefined(position);
                }
            }

            childWriters.get(i).initialize(childBlock);
            childWriters.get(i).write(childBlock);
        }
        vector.setValueCount(block.getPositionCount());
    }

    @Override
    public String toString()
    {
        return ArrowWriter.describeWriter(this, vector, childWriters.toArray(new ArrowWriter[0]));
    }
}
