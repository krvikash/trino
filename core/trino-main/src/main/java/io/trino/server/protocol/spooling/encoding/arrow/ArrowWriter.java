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

import com.google.common.base.MoreObjects.ToStringHelper;
import io.trino.spi.block.Block;
import org.apache.arrow.memory.rounding.SegmentRoundingPolicy;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Sealed interface for writing Trino values into Arrow vectors.
 *
 * <p>Time with time zone writers by precision:
 * <ul>
 *   <li>{@link TimeSecWithTimeZoneWriter} — time(0) with time zone
 *   <li>{@link TimeMilliWithTimeZoneWriter} — time(3) with time zone
 *   <li>{@link TimeMicroWithTimeZoneWriter} — time(6) with time zone
 *   <li>{@link TimeNanoWithTimeZoneWriter} — time(9) with time zone
 * </ul>
 * time(12) is not supported by Arrow.
 *
 * <p>Timestamp with time zone writers by precision:
 * <ul>
 *   <li>{@link TimestampSecWithTimeZoneWriter} — timestamp(0) with time zone
 *   <li>{@link TimestampMilliWithTimeZoneWriter} — timestamp(3) with time zone
 *   <li>{@link TimestampMicroWithTimeZoneWriter} — timestamp(6) with time zone
 *   <li>{@link TimestampNanoWithTimeZoneWriter} — timestamp(9) with time zone
 * </ul>
 * timestamp(12) is not supported by Arrow.
 */
public sealed interface ArrowWriter
        permits ArrayWriter,
                FixedWidthWriter,
                MapWriter,
                NullWriter,
                RowWriter,
                TimeMicroWithTimeZoneWriter,
                TimeMilliWithTimeZoneWriter,
                TimeNanoWithTimeZoneWriter,
                TimeSecWithTimeZoneWriter,
                TimestampMicroWithTimeZoneWriter,
                TimestampMilliWithTimeZoneWriter,
                TimestampNanoWithTimeZoneWriter,
                TimestampSecWithTimeZoneWriter,
                VariableWidthWriter
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

    /**
     * Estimates how many bytes the Arrow vector would allocate for the given block, matching what
     * {@link #initialize(Block)} reserves. Used to split wide pages into batches that fit a memory budget;
     * the estimate need not be exact but should not undershoot what allocation actually reserves.
     */
    long estimatedVectorSizeInBytes(Block block);

    @Override
    String toString();

    /**
     * Rounds a buffer size up to a whole allocator segment, matching the SegmentRoundingPolicy configured in
     * QueryDataEncodingModule, so vector size estimates do not undershoot what allocation actually reserves.
     */
    static long roundToSegment(long sizeInBytes)
    {
        if (sizeInBytes <= 0) {
            return 0;
        }
        long segment = SegmentRoundingPolicy.MIN_SEGMENT_SIZE;
        return ((sizeInBytes + segment - 1) / segment) * segment;
    }

    static <T extends FieldVector> T checkedCast(ValueVector vector, Class<T> clazz)
    {
        requireNonNull(vector, "vector is null");
        checkArgument(clazz.isInstance(vector), "Expected %s, but got %s", clazz, vector.getClass());
        return clazz.cast(vector);
    }

    static String describeWriter(ArrowWriter writer, ValueVector vector, ArrowWriter... children)
    {
        ToStringHelper toStringHelper = toStringHelper(writer)
                .add("type", vector.getMinorType())
                .add("name", vector.getName())
                .add("count", vector.getValueCount());

        if (children.length > 0) {
            toStringHelper.add("children", children);
        }
        return toStringHelper.toString();
    }
}
