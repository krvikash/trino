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
import org.apache.arrow.vector.TimeMicroVector;

import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public final class TimeMicroWriter
        extends FixedWidthWriter<TimeMicroVector>
{
    public TimeMicroWriter(TimeMicroVector vector)
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
                vector.set(position, TIME_MICROS.getLong(block, position) / PICOSECONDS_PER_MICROSECOND);
            }
        }
        vector.setValueCount(positionCount);
    }
}
