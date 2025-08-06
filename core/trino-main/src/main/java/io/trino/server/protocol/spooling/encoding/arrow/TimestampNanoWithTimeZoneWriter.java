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
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;

import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIMESTAMP_VECTOR_NAME;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIMEZONE_VECTOR_NAME;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class TimestampNanoWithTimeZoneWriter
        implements ArrowWriter
{
    private final StructVector vector;
    private final TimeStampNanoVector timestampVector;
    private final VarCharVector timezoneVector;

    public TimestampNanoWithTimeZoneWriter(StructVector vector)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.timestampVector = ArrowWriter.checkedCast(vector.getChild(TIMESTAMP_VECTOR_NAME), TimeStampNanoVector.class);
        this.timezoneVector = ArrowWriter.checkedCast(vector.getChild(TIMEZONE_VECTOR_NAME), VarCharVector.class);
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
                LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) TIMESTAMP_TZ_NANOS.getObject(block, position);
                timestampVector.set(position, value.getEpochMillis() * NANOSECONDS_PER_MILLISECOND + floorDiv(value.getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND));
                timezoneVector.setSafe(position, TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey()).getId().getBytes(UTF_8));
                vector.setIndexDefined(position);
            }
        }
        vector.setValueCount(block.getPositionCount());
    }
}
