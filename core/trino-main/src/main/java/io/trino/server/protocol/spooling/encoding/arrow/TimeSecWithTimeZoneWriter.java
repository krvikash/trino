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
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.complex.StructVector;

import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIME_OFFSET_VECTOR_NAME;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIME_VECTOR_NAME;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_SECONDS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class TimeSecWithTimeZoneWriter
        implements ArrowWriter
{
    private final StructVector vector;
    private final TimeSecVector timeVector;
    private final SmallIntVector offsetVector;

    public TimeSecWithTimeZoneWriter(StructVector vector)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.timeVector = ArrowWriter.checkedCast(vector.getChild(TIME_VECTOR_NAME), TimeSecVector.class);
        this.offsetVector = ArrowWriter.checkedCast(vector.getChild(TIME_OFFSET_VECTOR_NAME), SmallIntVector.class);
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
                long value = TIME_TZ_SECONDS.getLong(block, position);
                long timeNanos = unpackTimeNanos(value);
                timeVector.set(position, toIntExact(floorDiv(timeNanos, NANOSECONDS_PER_SECOND)));
                offsetVector.set(position, unpackOffsetMinutes(value));
                vector.setIndexDefined(position);
            }
        }
        vector.setValueCount(block.getPositionCount());
    }

    @Override
    public String toString()
    {
        return ArrowWriter.describeWriter(this, vector);
    }
}
