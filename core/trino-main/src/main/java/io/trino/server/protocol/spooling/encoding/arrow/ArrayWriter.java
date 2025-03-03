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

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import org.apache.arrow.vector.complex.ListVector;

import static java.util.Objects.requireNonNull;

public final class ArrayWriter
        implements ArrowWriter
{
    private final ListVector vector;
    private final ArrowWriter elementWriter;

    public ArrayWriter(ListVector vector, ArrowWriter elementWriter)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.elementWriter = requireNonNull(elementWriter, "elementWriter is null");
    }

    @Override
    public void initialize(Block block)
    {
        if (!(block instanceof ArrayBlock arrayBlock)) {
            throw new IllegalArgumentException("ArrayBlock is expected but got " + block.getClass().getSimpleName());
        }

        Block dataBlock = arrayBlock.getElementsBlock();
        vector.setInitialCapacity(dataBlock.getPositionCount());
        vector.allocateNew();
    }

    @Override
    public void write(Block block)
    {
        if (!(block instanceof ArrayBlock arrayBlock)) {
            throw new IllegalArgumentException("ArrayBlock is expected but got " + block.getClass().getSimpleName());
        }

        Block dataBlock = arrayBlock.getElementsBlock();
        for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
            if (block.isNull(blockPosition)) {
                vector.setNull(blockPosition);
                continue;
            }
            Block elementBlock = arrayBlock.getArray(blockPosition);
            vector.startNewValue(blockPosition);
            elementWriter.write(dataBlock);
            vector.endValue(blockPosition, elementBlock.getPositionCount());
        }
        vector.setValueCount(block.getPositionCount());
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "{vector=" + vector.getName() + ", elementWriter=" + elementWriter + "}";
    }
}
