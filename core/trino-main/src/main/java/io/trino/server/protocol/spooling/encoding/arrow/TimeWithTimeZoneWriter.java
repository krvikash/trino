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
import io.trino.spi.block.LongArrayBlock;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.complex.StructVector;

import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.NANOSECONDS_PER_SECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// TODO: specialize this class for different time precisions
public final class TimeWithTimeZoneWriter
        implements ArrowWriter
{
    private final StructVector vector;
    private final int precision;
    private final FieldVector timeVector;
    private final IntVector offsetVector;

    public TimeWithTimeZoneWriter(StructVector vector, int precision)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.timeVector = ArrowWriter.checkedCast(vector.getChild("time"), FieldVector.class);
        this.offsetVector = ArrowWriter.checkedCast(vector.getChild("offset"), IntVector.class);
        this.precision = precision;
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
                if (!(block instanceof LongArrayBlock arrayBlock)) {
                    throw new IllegalArgumentException("Expected a LongArrayBlock for TimeWithTimeZone, but got " + block.getClass().getSimpleName());
                }
                long value = arrayBlock.getLong(position);
                long timeNanos = unpackTimeNanos(value);
                switch (precision) {
                    case 0 -> ArrowWriter.checkedCast(timeVector, TimeSecVector.class)
                            .set(position, toIntExact(floorDiv(timeNanos, NANOSECONDS_PER_SECOND)));
                    case 3 -> ArrowWriter.checkedCast(timeVector, TimeMilliVector.class)
                            .set(position, toIntExact(floorDiv(timeNanos, NANOSECONDS_PER_MILLISECOND)));
                    case 6 -> ArrowWriter.checkedCast(timeVector, TimeNanoVector.class)
                            .set(position, timeNanos);
                    case 12 -> throw new UnsupportedOperationException("Precision " + precision + " is not supported for TimeWithTimeZone");
                }
                offsetVector.set(position, unpackOffsetMinutes(value));
                vector.setIndexDefined(position);
            }
            vector.setValueCount(block.getPositionCount());
        }
    }
}
