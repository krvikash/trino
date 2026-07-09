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
import org.apache.arrow.vector.NullVector;

import static java.util.Objects.requireNonNull;

public final class NullWriter
        implements ArrowWriter
{
    private final NullVector vector;

    public NullWriter(NullVector vector)
    {
        this.vector = requireNonNull(vector, "vector is null");
    }

    @Override
    public void initialize(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew();
    }

    @Override
    public long estimatedVectorSizeInBytes(Block block)
    {
        return 0; // NullVector has no backing buffers
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
                throw new UnsupportedOperationException();
            }
        }
        vector.setValueCount(positionCount);
    }

    @Override
    public String toString()
    {
        return ArrowWriter.describeWriter(this, vector);
    }
}
