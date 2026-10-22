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
package io.trino.operator.torin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.lang.Math.floorDiv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuDateDiff
{
    private static final List<String> DATE_UNITS = List.of("day", "week");
    private static final List<String> TIMESTAMP_UNITS = List.of("millisecond", "second", "minute", "hour", "day", "week");

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDateDiff(NullsProvider nullsProvider)
    {
        assertDateDiffMatchesCpu(DATE, DATE_UNITS, randomDateTimeBlock(DATE, 64, nullsProvider, 1), randomDateTimeBlock(DATE, 64, nullsProvider, 2));
        for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            Type type = createTimestampType(precision);
            assertDateDiffMatchesCpu(type, TIMESTAMP_UNITS, randomDateTimeBlock(type, 64, nullsProvider, 1), randomDateTimeBlock(type, 64, nullsProvider, 2));
        }
    }

    /// A partial unit is truncated toward zero, so a difference just short of a whole unit is zero
    /// in either direction, and a negative difference is not floored.
    @Test
    void testDateDiffTruncatesTowardZero()
    {
        List<Long> epochDays = List.of(0L, 1L, 6L, 7L, 8L, 13L, 14L, -1L, -6L, -7L, -8L, -13L, -14L);
        BlockBuilder start = DATE.createBlockBuilder(null, epochDays.size() * 2);
        BlockBuilder end = DATE.createBlockBuilder(null, epochDays.size() * 2);
        for (long epochDay : epochDays) {
            DATE.writeLong(start, 0);
            DATE.writeLong(end, epochDay);
            DATE.writeLong(start, epochDay);
            DATE.writeLong(end, 0);
        }
        assertDateDiffMatchesCpu(DATE, DATE_UNITS, start.build(), end.build());
    }

    /// The CPU implementation floors a timestamp to the millisecond before taking the difference,
    /// so the sub-millisecond digits of either side, before and after the epoch, never count.
    @Test
    void testDateDiffIgnoresSubMillisecondDigits()
    {
        List<Long> epochMicros = List.of(0L, 1L, 999L, 1_000L, 1_001L, -1L, -999L, -1_000L, -1_001L, 59_999_999L, -59_999_999L, MICROSECONDS_PER_DAY - 1, -MICROSECONDS_PER_DAY + 1);
        for (int precision = 4; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            TimestampType type = createTimestampType(precision);
            BlockBuilder start = type.createBlockBuilder(null, epochMicros.size() * epochMicros.size());
            BlockBuilder end = type.createBlockBuilder(null, epochMicros.size() * epochMicros.size());
            for (long startMicros : epochMicros) {
                for (long endMicros : epochMicros) {
                    writeTimestamp(start, type, roundDownToPrecision(type, startMicros), picosOfMicroScale(type) * 9);
                    writeTimestamp(end, type, roundDownToPrecision(type, endMicros), 0);
                }
            }
            assertDateDiffMatchesCpu(type, TIMESTAMP_UNITS, start.build(), end.build());
        }
    }

    /// The whole date range and the whole range of the microsecond carrier, where an unchecked
    /// difference would be closest to overflowing.
    @Test
    void testDateDiffAtRangeBounds()
    {
        assertDateDiffMatchesCpu(DATE, DATE_UNITS, dateBlock(Integer.MIN_VALUE, Integer.MAX_VALUE, 0), dateBlock(Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE));
        for (int precision = 0; precision <= 6; precision++) {
            TimestampType type = createTimestampType(precision);
            long max = roundDownToPrecision(type, Long.MAX_VALUE);
            long min = -max;
            BlockBuilder start = type.createBlockBuilder(null, 3);
            BlockBuilder end = type.createBlockBuilder(null, 3);
            type.writeLong(start, min);
            type.writeLong(end, max);
            type.writeLong(start, max);
            type.writeLong(end, min);
            type.writeLong(start, 0);
            type.writeLong(end, min);
            assertDateDiffMatchesCpu(type, TIMESTAMP_UNITS, start.build(), end.build());
        }
    }

    /// Trino matches the unit case-insensitively, so every spelling has to run on the GPU.
    @Test
    void testDateDiffUnitInDifferentCases()
    {
        Block start = dateBlock(0, 100, -100);
        Block end = dateBlock(15, -3, 1_000);
        assertDateDiffMatchesCpu(DATE, List.of("DAY", "Day", "wEeK"), start, end);
    }

    /// Calendar units, a unit that is not a constant, and the types without a GPU implementation all
    /// stay on the CPU, as do units the CPU implementation rejects for a date.
    @Test
    void testDateDiffFallsBack()
    {
        for (String unit : List.of("month", "quarter", "year", "millisecond", "second", "minute", "hour", "invalid")) {
            assertThat(compileExpression(dateDiff(unit, DATE), layoutFor(List.of(DATE, DATE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .as("unit: %s", unit)
                    .isEmpty();
        }
        Type timestampType = createTimestampType(3);
        for (String unit : List.of("month", "quarter", "year", "invalid", "wee\u212A")) {
            assertThat(compileExpression(dateDiff(unit, timestampType), layoutFor(List.of(timestampType, timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .as("unit: %s", unit)
                    .isEmpty();
        }
        for (Type type : List.of(createTimestampType(10), createTimestampWithTimeZoneType(3))) {
            assertThat(compileExpression(dateDiff("day", type), layoutFor(List.of(type, type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .as("type: %s", type)
                    .isEmpty();
        }
        Expression variableUnit = new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_diff", fromTypes(VARCHAR, DATE, DATE)),
                ImmutableList.of(new Reference(VARCHAR, "unit"), field(0, DATE), field(1, DATE)));
        assertThat(compileExpression(
                variableUnit,
                ImmutableMap.of(new Symbol(DATE, "ref0"), 0, new Symbol(DATE, "ref1"), 1, new Symbol(VARCHAR, "unit"), 2),
                SQL_STANDARD,
                TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
    }

    /// The customer's shape: a column against a date constant.
    @Test
    void testDateDiffAgainstConstant()
    {
        Type unitType = createVarcharType(3);
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_diff", fromTypes(unitType, DATE, DATE)),
                ImmutableList.of(
                        new Constant(unitType, Slices.utf8Slice("day")),
                        field(0, DATE),
                        new Constant(DATE, LocalDate.of(2025, 10, 1).toEpochDay())));
        Block dates = randomDateTimeBlock(DATE, 64, NullsProvider.RANDOM_NULLS, 1);
        assertGpuMatchesCpu(List.of(new Page(dates)), List.of(DATE), expression);
    }

    private void assertDateDiffMatchesCpu(Type type, List<String> units, Block start, Block end)
    {
        for (String unit : units) {
            try {
                assertGpuMatchesCpu(List.of(new Page(start, end)), List.of(type, type), dateDiff(unit, type));
            }
            catch (Throwable t) {
                t.addSuppressed(new Exception("unit: " + unit));
                t.addSuppressed(new Exception("type: " + type));
                throw t;
            }
        }
    }

    private static Expression dateDiff(String unit, Type type)
    {
        Type unitType = createVarcharType(unit.length());
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_diff", fromTypes(unitType, type, type)),
                ImmutableList.of(
                        new Constant(unitType, Slices.utf8Slice(unit)),
                        field(0, type),
                        field(1, type)));
    }

    private static Block dateBlock(long... epochDays)
    {
        BlockBuilder builder = DATE.createBlockBuilder(null, epochDays.length);
        for (long epochDay : epochDays) {
            DATE.writeLong(builder, epochDay);
        }
        return builder.build();
    }

    /// Random dates or timestamps over most of the range the GPU carrier can hold.
    private static Block randomDateTimeBlock(Type type, int positionsCount, NullsProvider nullsProvider, long seed)
    {
        Random random = new Random(seed);
        Optional<long[]> validity = nullsProvider.getValidityWords(positionsCount);
        BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
        for (int position = 0; position < positionsCount; position++) {
            if (validity.isPresent() && !Bitmap.isSet(validity.get(), 0, position)) {
                builder.appendNull();
                continue;
            }
            if (type == DATE) {
                // Also near dates, so that short differences are covered
                DATE.writeLong(builder, random.nextBoolean() ? random.nextInt() : random.nextInt(-30, 30));
                continue;
            }
            TimestampType timestampType = (TimestampType) type;
            if (timestampType.isShort()) {
                long epochMicros = random.nextBoolean() ? random.nextLong() : random.nextLong(-30 * MICROSECONDS_PER_DAY, 30 * MICROSECONDS_PER_DAY);
                timestampType.writeLong(builder, roundDownToPrecision(timestampType, epochMicros));
                continue;
            }
            // A nanosecond carrier reaches only about 1677 to 2262
            long bound = Long.MAX_VALUE / NANOSECONDS_PER_MICROSECOND - 1;
            long epochMicros = random.nextBoolean() ? random.nextLong(-bound, bound) : random.nextLong(-30 * MICROSECONDS_PER_DAY, 30 * MICROSECONDS_PER_DAY);
            int scale = picosOfMicroScale(timestampType);
            writeTimestamp(builder, timestampType, epochMicros, random.nextInt(1_000_000 / scale) * scale);
        }
        return builder.build();
    }

    private static void writeTimestamp(BlockBuilder builder, TimestampType type, long epochMicros, int picosOfMicro)
    {
        if (type.isShort()) {
            type.writeLong(builder, epochMicros);
            return;
        }
        type.writeObject(builder, new LongTimestamp(epochMicros, picosOfMicro));
    }

    private static long roundDownToPrecision(TimestampType type, long epochMicros)
    {
        long scale = 1;
        for (int digits = type.getPrecision(); digits < TimestampType.MAX_SHORT_PRECISION; digits++) {
            scale *= 10;
        }
        return floorDiv(epochMicros, scale) * scale;
    }

    /// Smallest non-zero picosOfMicro the precision can hold.
    private static int picosOfMicroScale(TimestampType type)
    {
        int scale = 1;
        for (int digits = type.getPrecision(); digits < TimestampType.MAX_PRECISION; digits++) {
            scale *= 10;
        }
        return scale;
    }
}
