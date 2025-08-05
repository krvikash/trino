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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.complex.StructVector;

import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MICROS;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;

public final class TimeMicroWithTimeZoneWriter
        implements ArrowWriter
{
    private final StructVector vector;
    private final TimeMicroVector timeVector;
    private final IntVector offsetVector;

    public TimeMicroWithTimeZoneWriter(StructVector vector)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.timeVector = ArrowWriter.checkedCast(vector.getChild("time"), TimeMicroVector.class);
        this.offsetVector = ArrowWriter.checkedCast(vector.getChild("offset"), IntVector.class);
    }

    @Override
    public void initialize(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
    }

    @Override
    public void write(Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
            }
            else {
                long value = TIME_TZ_MICROS.getLong(block, position);
                long timeNanos = unpackTimeNanos(value);
                timeVector.set(position, floorDiv(timeNanos, NANOSECONDS_PER_MICROSECOND));
                offsetVector.set(position, unpackOffsetMinutes(value));
                vector.setIndexDefined(position);
            }
            vector.setValueCount(block.getPositionCount());
        }
    }
}
