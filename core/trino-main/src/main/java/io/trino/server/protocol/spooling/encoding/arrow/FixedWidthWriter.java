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
import org.apache.arrow.vector.FixedWidthVector;

import static io.trino.server.protocol.spooling.encoding.arrow.ArrowWriter.roundToSegment;

public abstract sealed class FixedWidthWriter<V extends FixedWidthVector>
        extends PrimitiveWriter<V>
        permits BigintWriter,
                BooleanWriter,
                DateWriter,
                DecimalWriter,
                DoubleWriter,
                IntegerWriter,
                IntervalDayWriter,
                IntervalYearMonthWriter,
                IpAddressWriter,
                RealWriter,
                SmallIntWriter,
                TimeMicroWriter,
                TimeMilliWriter,
                TimeNanoWriter,
                TimeSecWriter,
                TimestampMicroWriter,
                TimestampMilliWriter,
                TimestampNanoWriter,
                TimestampSecWriter,
                TinyIntWriter,
                UuidWriter
{
    protected FixedWidthWriter(V vector)
    {
        super(vector);
    }

    @Override
    public void initialize(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew(block.getPositionCount());
    }

    @Override
    public long estimatedVectorSizeInBytes(Block block)
    {
        int positionCount = block.getPositionCount();
        // getBufferSizeFor is exact for fixed-width vectors; round the validity and data buffers separately,
        // matching the allocator, which reserves each buffer in whole segments.
        long validity = (positionCount + 7) / 8;
        return roundToSegment(validity) + roundToSegment(vector.getBufferSizeFor(positionCount) - validity);
    }
}
