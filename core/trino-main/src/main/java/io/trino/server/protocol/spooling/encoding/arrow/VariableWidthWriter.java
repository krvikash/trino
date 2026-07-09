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
import org.apache.arrow.vector.VariableWidthVector;

import static io.trino.server.protocol.spooling.encoding.arrow.ArrowWriter.roundToSegment;
import static java.util.Objects.requireNonNull;

public abstract sealed class VariableWidthWriter<V extends VariableWidthVector>
        implements ArrowWriter
        permits CharWriter,
                VarbinaryWriter,
                VarcharWriter
{
    protected final V vector;

    protected VariableWidthWriter(V vector)
    {
        this.vector = requireNonNull(vector, "vector is null");
    }

    @Override
    public void initialize(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew(block.getSizeInBytes(), block.getPositionCount());
    }

    @Override
    public long estimatedVectorSizeInBytes(Block block)
    {
        // Mirrors initialize(): a data buffer sized from the block, plus the offset and validity buffers.
        int positionCount = block.getPositionCount();
        return roundToSegment(block.getSizeInBytes())
                + roundToSegment((long) (positionCount + 1) * Integer.BYTES)
                + roundToSegment((positionCount + 7) / 8);
    }

    @Override
    public String toString()
    {
        return ArrowWriter.describeWriter(this, vector);
    }
}
