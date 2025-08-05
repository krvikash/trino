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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public sealed interface ArrowWriter
        permits ArrayWriter,
        MapWriter,
        PrimitiveWriter,
        RowWriter,

        TimeSecWithTimeZoneWriter, // time(0) with time zone
        TimeMilliWithTimeZoneWriter, // time(3) with time zone
        TimeMicroWithTimeZoneWriter, // time(6) with time zone
        TimeNanoWithTimeZoneWriter, // time(9) with time zone
        // time(12) is not supported by Arrow

        TimestampSecWithTimeZoneWriter, // timestamp(0) with time zone
        TimestampMilliWithTimeZoneWriter, // timestamp(3) with time zone
        TimestampMicroWithTimeZoneWriter, // timestamp(6) with time zone
        TimestampNanoWithTimeZoneWriter // timestamp(9) with time zone
        // timestamp(12) is not supported by Arrow

        // No extension types yet
{
    /**
     * Usually we want to call ValueVector.setInitialCapacity() to hint Arrow how many positions
     * will be there then we MUST call ValueVector.allocateNew() to get a memory allocation before writing.
     */
    void initialize(Block block);

    /**
     * This method implementation MUST end with ValueVector.setValueCount() call
     * which informs Arrow how many valid positions there are.
     */
    void write(Block block);

    @Override
    String toString();

    static <T extends FieldVector> T checkedCast(ValueVector vector, Class<T> clazz)
    {
        requireNonNull(vector, "vector is null");
        checkArgument(clazz.isInstance(vector), "Expected %s, but got %s", clazz, vector.getClass());
        return clazz.cast(vector);
    }
}
