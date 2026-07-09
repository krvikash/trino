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

public final class NullWriter
        extends PrimitiveWriter<NullVector>
{
    public NullWriter(NullVector vector)
    {
        super(vector);
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
    protected void setNull(int offset)
    {
        vector.setNull(offset);
    }

    @Override
    protected void writeValue(int offset, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }
}
