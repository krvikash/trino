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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import io.trino.spi.type.Type.Range;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLongTimestampType
        extends AbstractTestType
{
    public TestLongTimestampType()
    {
        super(TIMESTAMP_NANOS, SqlTimestamp.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_NANOS.createFixedSizeBlockBuilder(15);
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(1111_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(1111_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(1111_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(3333_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(3333_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(4444_123, 123_000));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        LongTimestamp timestamp = (LongTimestamp) value;
        return new LongTimestamp(timestamp.getEpochMicros() + 1, 0);
    }

    @Test
    public void testRange()
    {
        Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo(new LongTimestamp(Long.MIN_VALUE, 0));
        assertThat(range.getMax()).isEqualTo(new LongTimestamp(Long.MAX_VALUE, 999_000));
    }

    @Test
    public void testRangeEveryPrecision()
    {
        for (MaxPrecision entry : maxPrecisions()) {
            Range range = createTimestampType(entry.precision()).getRange().orElseThrow();
            assertThat(range.getMin()).isEqualTo(new LongTimestamp(Long.MIN_VALUE, 0));
            assertThat(range.getMax()).isEqualTo(entry.expectedMax());
        }
    }

    public static List<MaxPrecision> maxPrecisions()
    {
        return ImmutableList.of(
                new MaxPrecision(7, new LongTimestamp(Long.MAX_VALUE, 900_000)),
                new MaxPrecision(8, new LongTimestamp(Long.MAX_VALUE, 990_000)),
                new MaxPrecision(9, new LongTimestamp(Long.MAX_VALUE, 999_000)),
                new MaxPrecision(10, new LongTimestamp(Long.MAX_VALUE, 999_900)),
                new MaxPrecision(11, new LongTimestamp(Long.MAX_VALUE, 999_990)),
                new MaxPrecision(12, new LongTimestamp(Long.MAX_VALUE, 999_999)));
    }

    @Test
    public void testPreviousValue()
    {
        LongTimestamp minValue = new LongTimestamp(Long.MIN_VALUE, 0);
        LongTimestamp nextToMinValue = new LongTimestamp(Long.MIN_VALUE, 1_000);
        LongTimestamp previousToMaxValue = new LongTimestamp(Long.MAX_VALUE, 998_000);
        LongTimestamp maxValue = new LongTimestamp(Long.MAX_VALUE, 999_000);

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(nextToMinValue))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(new LongTimestamp(1111_123, 122_000)));
        assertThat(type.getPreviousValue(new LongTimestamp(1483228800000L, 0)))
                .isEqualTo(Optional.of(new LongTimestamp(1483228799999L, 999_000)));

        assertThat(type.getPreviousValue(previousToMaxValue))
                .isEqualTo(Optional.of(new LongTimestamp(Long.MAX_VALUE, 997_000)));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(previousToMaxValue));
    }

    @Test
    public void testNextValue()
    {
        LongTimestamp minValue = new LongTimestamp(Long.MIN_VALUE, 0);
        LongTimestamp nextToMinValue = new LongTimestamp(Long.MIN_VALUE, 1_000);
        LongTimestamp previousToMaxValue = new LongTimestamp(Long.MAX_VALUE, 998_000);
        LongTimestamp maxValue = new LongTimestamp(Long.MAX_VALUE, 999_000);

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(nextToMinValue));
        assertThat(type.getNextValue(nextToMinValue))
                .isEqualTo(Optional.of(new LongTimestamp(Long.MIN_VALUE, 2_000)));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(new LongTimestamp(1111_123, 124_000)));
        assertThat(type.getNextValue(new LongTimestamp(1483228799999L, 999_000)))
                .isEqualTo(Optional.of(new LongTimestamp(1483228800000L, 0)));

        assertThat(type.getNextValue(previousToMaxValue))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testPreviousValueEveryPrecision(int precision, LongTimestamp minValue, LongTimestamp maxValue, int step)
    {
        Type type = createTimestampType(precision);

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(new LongTimestamp(minValue.getEpochMicros(), minValue.getPicosOfMicro() + step)))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(new LongTimestamp(0, 999_999)))
                .isEqualTo(Optional.of(new LongTimestamp(0, 999_999 - step)));

        assertThat(type.getPreviousValue(new LongTimestamp(maxValue.getEpochMicros(), maxValue.getPicosOfMicro() - step)))
                .isEqualTo(Optional.of(new LongTimestamp(maxValue.getEpochMicros(), maxValue.getPicosOfMicro() - 2 * step)));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(new LongTimestamp(maxValue.getEpochMicros(), maxValue.getPicosOfMicro() - step)));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testNextValueEveryPrecision(int precision, LongTimestamp minValue, LongTimestamp maxValue, int step)
    {
        Type type = createTimestampType(precision);

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(new LongTimestamp(minValue.getEpochMicros(), minValue.getPicosOfMicro() + step)));
        assertThat(type.getNextValue(new LongTimestamp(minValue.getEpochMicros(), minValue.getPicosOfMicro() + step)))
                .isEqualTo(Optional.of(new LongTimestamp(minValue.getEpochMicros(), minValue.getPicosOfMicro() + 2 * step)));

        assertThat(type.getNextValue(new LongTimestamp(0, 0)))
                .isEqualTo(Optional.of(new LongTimestamp(0, step)));

        assertThat(type.getNextValue(new LongTimestamp(maxValue.getEpochMicros(), maxValue.getPicosOfMicro() - step)))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }

    private static Stream<Arguments> testPreviousNextValueEveryPrecisionDataProvider()
    {
        return Stream.of(
                Arguments.of(7, new LongTimestamp(Long.MIN_VALUE, 0), new LongTimestamp(Long.MAX_VALUE, 900_000), 100000),
                Arguments.of(8, new LongTimestamp(Long.MIN_VALUE, 0), new LongTimestamp(Long.MAX_VALUE, 990_000), 10000),
                Arguments.of(9, new LongTimestamp(Long.MIN_VALUE, 0), new LongTimestamp(Long.MAX_VALUE, 999_000), 1000),
                Arguments.of(10, new LongTimestamp(Long.MIN_VALUE, 0), new LongTimestamp(Long.MAX_VALUE, 999_900), 100),
                Arguments.of(11, new LongTimestamp(Long.MIN_VALUE, 0), new LongTimestamp(Long.MAX_VALUE, 999_990), 10),
                Arguments.of(12, new LongTimestamp(Long.MIN_VALUE, 0), new LongTimestamp(Long.MAX_VALUE, 999_999), 1));
    }

    record MaxPrecision(int precision, LongTimestamp expectedMax) {}
}
