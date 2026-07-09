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
import io.trino.spi.block.Fixed12Block;
import org.apache.arrow.vector.TimeStampNanoVector;

import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.addExact;
import static java.lang.Math.floorDiv;
import static java.lang.Math.multiplyExact;

public final class TimestampNanoWriter
        extends FixedWidthWriter<TimeStampNanoVector>
{
    public TimestampNanoWriter(TimeStampNanoVector vector)
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
                if (!(block instanceof Fixed12Block fixed12Block)) {
                    throw new IllegalArgumentException("Expected block to be Fixed12Block but got " + block.getClass().getSimpleName());
                }

                vector.set(position, addExact(multiplyExact(fixed12Block.getFixed12First(position), NANOSECONDS_PER_MICROSECOND), floorDiv(fixed12Block.getFixed12Second(position), PICOSECONDS_PER_NANOSECOND)));
            }
        }
        vector.setValueCount(positionCount);
    }
}
