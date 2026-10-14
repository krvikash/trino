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
package io.trino.operator.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuDateAdd;
import io.trino.spi.Page;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
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
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuDateAdd
{
    private static final List<Type> DATE_TIME_TYPES = ImmutableList.<Type>builder()
            .add(DATE)
            .addAll(IntStream.rangeClosed(0, TimestampType.MAX_PRECISION).mapToObj(TimestampType::createTimestampType).toList())
            .addAll(IntStream.rangeClosed(0, TimestampWithTimeZoneType.MAX_PRECISION).mapToObj(TimestampWithTimeZoneType::createTimestampWithTimeZoneType).toList())
            .addAll(IntStream.rangeClosed(0, TimeType.MAX_PRECISION).mapToObj(TimeType::createTimeType).toList())
            .build();

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDateAdd(NullsProvider nullsProvider)
    {
        for (GpuDateAdd.Unit unit : GpuDateAdd.Unit.values()) {
            for (Type dateTimeType : DATE_TIME_TYPES) {
                for (long value : List.of(-13L, 5L)) {
                    try {
                        if (!isDateAddSupported(unit, dateTimeType)) {
                            assertThat(compileExpression(dateAdd(unit.trinoDateAddUnit(), new Constant(BIGINT, value), dateTimeType), layoutFor(List.of(dateTimeType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                                    .isEmpty();
                            continue;
                        }
                        assertDateAddMatchesCpu(unit.trinoDateAddUnit(), value, dateTimeType, dateTimeBlock(dateTimeType, 64, nullsProvider));
                    }
                    catch (Throwable t) {
                        t.addSuppressed(new Exception("unit: " + unit));
                        t.addSuppressed(new Exception("value: " + value));
                        t.addSuppressed(new Exception("type: " + dateTimeType));
                        throw t;
                    }
                }
            }
        }
    }

    /// Month ends, leap days and the epoch boundary, where the day of month has to be clamped to
    /// the last day of the resulting month.
    @Test
    void testDateAddCalendarEdgeCases()
    {
        List<LocalDate> dates = List.of(
                LocalDate.of(2024, 1, 31),
                LocalDate.of(2024, 2, 29),
                LocalDate.of(2024, 3, 31),
                LocalDate.of(2024, 5, 31),
                LocalDate.of(2023, 1, 31),
                LocalDate.of(2023, 2, 28),
                LocalDate.of(2023, 11, 30),
                LocalDate.of(2023, 12, 31),
                LocalDate.of(2000, 2, 29),
                LocalDate.of(1900, 2, 28),
                LocalDate.of(1969, 12, 31),
                LocalDate.of(1970, 1, 1),
                LocalDate.of(1968, 2, 29));

        for (GpuDateAdd.Unit unit : GpuDateAdd.Unit.values()) {
            for (Type dateTimeType : DATE_TIME_TYPES) {
                for (long value : List.of(-13L, -4L, -1L, 0L, 1L, 4L, 13L)) {
                    if (!isDateAddSupported(unit, dateTimeType)) {
                        continue;
                    }
                    try {
                        assertDateAddMatchesCpu(unit.trinoDateAddUnit(), value, dateTimeType, dateTimeBlock(dateTimeType, dates));
                    }
                    catch (Throwable t) {
                        t.addSuppressed(new Exception("unit: " + unit));
                        t.addSuppressed(new Exception("value: " + value));
                        t.addSuppressed(new Exception("type: " + dateTimeType));
                        throw t;
                    }
                }
            }
        }
    }

    /// cuDF clamps the day of month to the last day of the resulting month, the way Trino does.
    @Test
    void testDateAddClampsDayOfMonth()
    {
        assertDateAddOnGpu("month", 1, LocalDate.of(2024, 1, 31), LocalDate.of(2024, 2, 29));
        assertDateAddOnGpu("month", 1, LocalDate.of(2023, 1, 31), LocalDate.of(2023, 2, 28));
        assertDateAddOnGpu("month", -1, LocalDate.of(2024, 3, 31), LocalDate.of(2024, 2, 29));
        assertDateAddOnGpu("month", 1, LocalDate.of(2024, 5, 31), LocalDate.of(2024, 6, 30));
        assertDateAddOnGpu("quarter", 1, LocalDate.of(2023, 11, 30), LocalDate.of(2024, 2, 29));
        assertDateAddOnGpu("year", 1, LocalDate.of(2024, 2, 29), LocalDate.of(2025, 2, 28));
        assertDateAddOnGpu("year", -1, LocalDate.of(2024, 2, 29), LocalDate.of(2023, 2, 28));
        assertDateAddOnGpu("year", 4, LocalDate.of(2024, 2, 29), LocalDate.of(2028, 2, 29));
        // 1900 is not a leap year
        assertDateAddOnGpu("year", -100, LocalDate.of(2000, 2, 29), LocalDate.of(1900, 2, 28));
        assertDateAddOnGpu("day", 1, LocalDate.of(2024, 2, 28), LocalDate.of(2024, 2, 29));
        assertDateAddOnGpu("day", 1, LocalDate.of(2023, 2, 28), LocalDate.of(2023, 3, 1));
        assertDateAddOnGpu("week", -1, LocalDate.of(1970, 1, 1), LocalDate.of(1969, 12, 25));
        // Trino matches the unit case-insensitively
        assertDateAddOnGpu("MONTH", 1, LocalDate.of(2024, 1, 31), LocalDate.of(2024, 2, 29));
    }

    private void assertDateAddOnGpu(String unit, long value, LocalDate input, LocalDate expected)
    {
        Expression expression = dateAdd(unit, new Constant(BIGINT, value), DATE);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(DATE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        Page result = getOnlyElement(executeWithGpu(
                List.of(new Page(writeNativeValue(DATE, input.toEpochDay()))),
                List.of(DATE),
                expression,
                gpuExpression));
        assertThat(readNativeValue(DATE, result.getBlock(0), 0))
                .as("date_add('%s', %s, %s)", unit, value, input)
                .isEqualTo(expected.toEpochDay());
    }

    /// A shift moves a timestamp in the direction of its sign, so an input sitting at the bound it
    /// moves away from is still valid and must not be rejected.
    @Test
    void testDateAddAtCarrierBound()
    {
        // cuDF's calendar, and so a calendar unit, reaches only to year -32767 and year 32767
        long minCalendarDay = LocalDate.of(-32767, 1, 1).toEpochDay();
        long maxCalendarDay = LocalDate.of(32767, 12, 31).toEpochDay();
        for (String unit : List.of("month", "quarter", "year")) {
            // A positive shift moves away from the smallest date the calendar covers
            for (long epochDay : List.of(minCalendarDay, minCalendarDay + 1, minCalendarDay + 30, minCalendarDay + 400)) {
                assertDateAddAtBound(unit, 1, epochDay);
            }
            // A negative shift moves away from the largest date the calendar covers
            for (long epochDay : List.of(maxCalendarDay, maxCalendarDay - 1, maxCalendarDay - 30, maxCalendarDay - 400)) {
                assertDateAddAtBound(unit, -1, epochDay);
            }
        }
        // A fixed-length shift is plain arithmetic, so it reaches the whole date range
        for (String unit : List.of("day", "week")) {
            for (long epochDay : List.of((long) Integer.MIN_VALUE, Integer.MIN_VALUE + 1L, Integer.MIN_VALUE + 30L)) {
                assertDateAddAtBound(unit, 1, epochDay);
            }
            for (long epochDay : List.of((long) Integer.MAX_VALUE, Integer.MAX_VALUE - 1L, Integer.MAX_VALUE - 30L)) {
                assertDateAddAtBound(unit, -1, epochDay);
            }
        }
    }

    /// Outside cuDF's calendar, addCalendricalMonths silently returns a wrong timestamp, so a
    /// calendar unit has to fail there rather than produce it.
    ///
    /// The microsecond carrier reaches about year 294247, so from year 32768 up it is cuDF's
    /// calendar alone that rejects these inputs, and the CPU implementation answers them. That band
    /// is the one place the GPU is deliberately stricter.
    @Test
    void testDateAddOutsideCudfCalendar()
    {
        Type timestampType = createTimestampType(6);
        for (String unit : List.of("month", "quarter", "year")) {
            for (long epochDay : List.of(
                    (long) Integer.MIN_VALUE,
                    LocalDate.of(-32768, 12, 31).toEpochDay(),
                    LocalDate.of(32768, 1, 1).toEpochDay(),
                    (long) Integer.MAX_VALUE)) {
                assertGpuDateAddThrows(unit, 1, DATE, epochDay);
            }
            for (long epochDay : List.of(
                    LocalDate.of(-100_000, 1, 1).toEpochDay(),
                    LocalDate.of(-32768, 12, 31).toEpochDay(),
                    LocalDate.of(32768, 1, 1).toEpochDay(),
                    LocalDate.of(100_000, 1, 1).toEpochDay())) {
                long epochMicros = epochDay * MICROSECONDS_PER_DAY;
                assertGpuDateAddThrows(unit, 1, timestampType, epochMicros);
                assertDateAddSucceedsOnCpu(unit, 1, timestampType, epochMicros);
            }
        }
    }

    private void assertDateAddAtBound(String unit, long value, long epochDay)
    {
        try {
            assertDateAddMatchesCpu(unit, value, DATE, writeNativeValue(DATE, epochDay));
        }
        catch (Throwable t) {
            t.addSuppressed(new Exception("unit: " + unit));
            t.addSuppressed(new Exception("value: " + value));
            t.addSuppressed(new Exception("epochDay: " + epochDay));
            throw t;
        }
    }

    /// date_add runs on the GPU only for a constant shift that can be applied without leaving the
    /// representable range; everything else is left to the CPU.
    @Test
    void testDateAddFallsBackOnUnusableShift()
    {
        Type timestampType = createTimestampType(6);
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(timestampType, "ref0"), 0,
                new Symbol(BIGINT, "shift"), 1);
        assertThat(compileExpression(dateAdd("day", new Reference(BIGINT, "shift"), timestampType), layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
        assertThat(compileExpression(dateAdd("day", new Constant(BIGINT, null), timestampType), layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
        // No representable input is left, and the CPU implementation fails for every input
        assertThat(compileExpression(dateAdd("day", new Constant(BIGINT, Long.MAX_VALUE), timestampType), layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
        // A month count cuDF cannot take as a 32-bit scalar
        assertThat(compileExpression(dateAdd("year", new Constant(BIGINT, 1_000_000_000L), timestampType), layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
        // A month count cuDF can take, but which reserves more than cuDF's calendar spans, so that
        // no representable input is left
        assertThat(compileExpression(dateAdd("year", new Constant(BIGINT, 3_000_000L), DATE), layoutFor(List.of(DATE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
    }

    /// The CPU implementation holds the result in milliseconds and rescales it with an exact
    /// multiplication, so it runs out of range before the carrier column does. At every carrier's
    /// bound, the last input it accepts has to produce the same value on both engines and the first
    /// input it rejects has to fail on both.
    ///
    /// The bounds below are the carrier's parity bound less one day, since the shift is one day.
    ///
    /// Precisions sharing a carrier differ in whether the CPU implementation rounds: it rounds to
    /// the declared precision for `timestamp(0)` to `timestamp(3)` and not beyond. Every value here
    /// is therefore a valid value of its own type, a whole number of the precision's own steps, and
    /// not the raw carrier bound. Rounding then has nothing to do and the two engines agree. Rolling
    /// these back to the carrier bound makes the engines disagree, because rounding a bound that is
    /// not a multiple of the step moves the result past what the CPU can rescale.
    @Test
    void testDateAddAtOverflowBound()
    {
        // timestamp(0): the largest result is 9223372036854 seconds, MAX_EPOCH_MILLIS / 1000
        assertDateAddBound(createTimestampType(0), 1, 9_223_371_950_454_000_000L, 9_223_371_950_455_000_000L);
        assertDateAddBound(createTimestampType(0), -1, -9_223_371_950_454_000_000L, -9_223_371_950_455_000_000L);
        // timestamp(1) and timestamp(2) ride the millisecond carrier, in steps of 100 and 10
        // milliseconds, and are the precisions the CPU implementation still rounds
        assertDateAddBound(createTimestampType(1), 1, 9_223_371_950_454_700_000L, 9_223_371_950_454_800_000L);
        assertDateAddBound(createTimestampType(1), -1, -9_223_371_950_454_700_000L, -9_223_371_950_454_800_000L);
        assertDateAddBound(createTimestampType(2), 1, 9_223_371_950_454_770_000L, 9_223_371_950_454_780_000L);
        assertDateAddBound(createTimestampType(2), -1, -9_223_371_950_454_770_000L, -9_223_371_950_454_780_000L);
        // timestamp(3): the largest result is 9223372036854775 milliseconds, MAX_EPOCH_MILLIS
        assertDateAddBound(createTimestampType(3), 1, 9_223_371_950_454_775_000L, 9_223_371_950_454_776_000L);
        assertDateAddBound(createTimestampType(3), -1, -9_223_371_950_454_775_000L, -9_223_371_950_454_776_000L);
        // timestamp(4) and timestamp(5) ride the microsecond carrier, in steps of 100 and 10
        // microseconds, and are past the precisions the CPU implementation rounds
        assertDateAddBound(createTimestampType(4), 1, 9_223_371_950_454_775_800L, 9_223_371_950_454_775_900L);
        assertDateAddBound(createTimestampType(4), -1, -9_223_371_950_454_775_000L, -9_223_371_950_454_775_100L);
        assertDateAddBound(createTimestampType(5), 1, 9_223_371_950_454_775_800L, 9_223_371_950_454_775_810L);
        assertDateAddBound(createTimestampType(5), -1, -9_223_371_950_454_775_000L, -9_223_371_950_454_775_010L);
        // timestamp(6): the largest result is Long.MAX_VALUE, because the CPU adds the microseconds
        // of the millisecond after rescaling, and the smallest is MAX_EPOCH_MILLIS * -1000, because
        // that remainder is never negative
        assertDateAddBound(createTimestampType(6), 1, Long.MAX_VALUE - MICROSECONDS_PER_DAY, 9_223_371_950_454_776_000L);
        assertDateAddBound(createTimestampType(6), -1, -9_223_371_950_454_775_000L, -9_223_371_950_454_775_001L);
    }

    /// Results in the last 192 microseconds of the range leave the microsecond carrier once the
    /// sub-millisecond part is added back. Both engines reject them.
    @Test
    void testDateAddPastLargestRepresentableResult()
    {
        Type timestampType = createTimestampType(6);
        for (long epochMicros : List.of(Long.MAX_VALUE - MICROSECONDS_PER_DAY + 1, 9_223_371_950_454_775_999L)) {
            assertDateAddMatchesCpu("day", 1, timestampType, writeNativeValue(timestampType, epochMicros));
        }
    }

    /// An everyday input whose result is not representable: the shift, not the input, is what leaves
    /// the carrier's range.
    @Test
    void testDateAddResultOutOfRange()
    {
        long epochDay = LocalDate.of(2024, 1, 1).toEpochDay();
        // Roughly the whole INT32 day count, in either direction
        assertGpuDateAddThrows("day", 2_147_600_000L, DATE, epochDay);
        assertGpuDateAddThrows("day", -2_147_600_000L, DATE, epochDay);

        // Roughly the whole range of a microsecond carrier, which is what the CPU rescales to
        long epochMicros = epochDay * MICROSECONDS_PER_DAY;
        for (Type timestampType : List.of(createTimestampType(0), createTimestampType(3), createTimestampType(6))) {
            try {
                assertDateAddMatchesCpu("day", 106_751_991L, timestampType, writeNativeValue(timestampType, epochMicros));
            }
            catch (Throwable t) {
                t.addSuppressed(new Exception("type: " + timestampType));
                throw t;
            }
        }
    }

    /// A calendar unit reaches only as far as cuDF's calendar, so a valid date near its end has a
    /// valid result the GPU cannot produce. It fails rather than returning cuDF's wrong answer,
    /// where the CPU implementation succeeds.
    @Test
    void testDateAddCalendarResultOutOfRange()
    {
        assertGpuDateAddThrows("year", 1, DATE, LocalDate.of(32767, 12, 1).toEpochDay());
        assertGpuDateAddThrows("month", 1, DATE, LocalDate.of(32767, 12, 31).toEpochDay());
        assertGpuDateAddThrows("year", -1, DATE, LocalDate.of(-32767, 1, 1).toEpochDay());
    }

    /// The bound reported is the one that was violated, not the result: here the result is
    /// representable and it is cuDF's calendar, less the reserve for a twelve month shift, that
    /// binds. The bounds are epoch days, the carrier's own ticks for a date.
    @Test
    void testDateAddOutOfRangeMessageNamesTheBound()
    {
        Expression expression = dateAdd("year", new Constant(BIGINT, 1L), DATE);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(DATE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(
                List.of(new Page(writeNativeValue(DATE, LocalDate.of(32767, 12, 1).toEpochDay()))),
                List.of(DATE),
                expression,
                gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("date_add('year', 1, ...) argument out of range supported by GPU: must be in [-12687428, 11248365]");
    }

    /// The range check reduces with `any()`, which skips nulls, and combines the two comparisons
    /// with `NULL_LOGICAL_OR`, which keeps a null null. So a null row must neither be read as out of
    /// range itself nor hide a row that is. Nothing else pins this, because the helpers that compare
    /// against the CPU take single-row inputs on the exception path.
    @Test
    void testDateAddWithNullRows()
    {
        Long outOfRange = LocalDate.of(32768, 1, 1).toEpochDay();
        Long inRange = LocalDate.of(2024, 1, 31).toEpochDay();
        // A null does not hide the row that is out of range, whichever side of it the null sits on
        assertGpuDateAddThrows("month", 1, DATE, dateBlock(null, outOfRange));
        assertGpuDateAddThrows("month", 1, DATE, dateBlock(outOfRange, null));
        // A null is not itself read as out of range, including when every row is null
        assertDateAddMatchesCpu("month", 1, DATE, dateBlock(null, inRange));
        assertDateAddMatchesCpu("month", 1, DATE, dateBlock(null, null));
    }

    /// A date result outside the INT32 day count fails here with NUMERIC_VALUE_OUT_OF_RANGE. The CPU
    /// implementation gets as far as writing the block and fails with GENERIC_INTERNAL_ERROR, so the
    /// query fails either way, with a different error code.
    @Test
    void testDateAddPastLargestDate()
    {
        assertGpuDateAddThrows("day", 1, DATE, Integer.MAX_VALUE);
        assertGpuDateAddThrows("day", -1, DATE, Integer.MIN_VALUE);
    }

    /// Trino matches the unit case-insensitively, so every spelling has to run on the GPU and agree
    /// with the CPU rather than quietly falling back.
    @Test
    void testDateAddUnitInDifferentCases()
    {
        List<LocalDate> dates = List.of(LocalDate.of(2024, 1, 31), LocalDate.of(2024, 2, 29), LocalDate.of(2023, 12, 31));
        for (String unit : List.of("month", "MONTH", "Month", "mOnTh")) {
            try {
                assertDateAddMatchesCpu(unit, 1, DATE, dateTimeBlock(DATE, dates));
                assertDateAddMatchesCpu(unit, -1, createTimestampType(6), dateTimeBlock(createTimestampType(6), dates));
            }
            catch (Throwable t) {
                t.addSuppressed(new Exception("unit: " + unit));
                throw t;
            }
        }
    }

    /// timestamp(7) to timestamp(9) ride the nanosecond carrier, which spans roughly 1677 to 2262.
    /// Inside it the CPU implementation never runs out of range, so every unit has to agree.
    @Test
    void testDateAddNanosecondCarrier()
    {
        long epochMicros = LocalDate.of(2024, 1, 31).toEpochDay() * MICROSECONDS_PER_DAY + 123_456;
        for (int precision = 7; precision <= 9; precision++) {
            TimestampType timestampType = createTimestampType(precision);
            for (GpuDateAdd.Unit unit : GpuDateAdd.Unit.values()) {
                for (long value : List.of(-13L, -1L, 1L, 5L)) {
                    try {
                        assertDateAddMatchesCpu(unit.trinoDateAddUnit(), value, timestampType, writeNativeValue(
                                timestampType,
                                new LongTimestamp(epochMicros, picosOfMicroScale(timestampType) * 7)));
                    }
                    catch (Throwable t) {
                        t.addSuppressed(new Exception("unit: " + unit));
                        t.addSuppressed(new Exception("value: " + value));
                        t.addSuppressed(new Exception("type: " + timestampType));
                        throw t;
                    }
                }
            }
        }
    }

    /// The CPU implementation keeps picosOfMicro across a shift, and so must the GPU, which carries
    /// it in the low three digits of the nanosecond count.
    @Test
    void testDateAddKeepsSubMicrosecondDigits()
    {
        long epochMicros = LocalDate.of(2024, 1, 31).toEpochDay() * MICROSECONDS_PER_DAY;
        for (int precision = 7; precision <= 9; precision++) {
            TimestampType timestampType = createTimestampType(precision);
            int scale = picosOfMicroScale(timestampType);
            for (int picosOfMicro : List.of(0, scale, scale * 5, 999_999 / scale * scale)) {
                try {
                    assertDateAddMatchesCpu("day", 1, timestampType, writeNativeValue(timestampType, new LongTimestamp(epochMicros, picosOfMicro)));
                    assertDateAddMatchesCpu("month", -1, timestampType, writeNativeValue(timestampType, new LongTimestamp(epochMicros, picosOfMicro)));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("type: " + timestampType));
                    t.addSuppressed(new Exception("picosOfMicro: " + picosOfMicro));
                    throw t;
                }
            }
        }
    }

    /// Long.MAX_VALUE nanoseconds is 2262-04-11. A result past it has no nanosecond representation,
    /// so it fails here even though the CPU implementation, which works in microseconds, returns it.
    @Test
    void testDateAddPastNanosecondCarrier()
    {
        TimestampType timestampType = createTimestampType(9);
        long shiftNanos = MICROSECONDS_PER_DAY * NANOSECONDS_PER_MICROSECOND;
        long lastAcceptedNanos = Long.MAX_VALUE - shiftNanos;
        // The last input whose result still fits agrees with the CPU
        assertDateAddMatchesCpu("day", 1, timestampType, writeNativeValue(timestampType, new LongTimestamp(
                floorDiv(lastAcceptedNanos, NANOSECONDS_PER_MICROSECOND),
                floorMod(lastAcceptedNanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND)));
        // One nanosecond further the result leaves the carrier
        assertGpuDateAddThrows("day", 1, timestampType, new LongTimestamp(
                floorDiv(lastAcceptedNanos + 1, NANOSECONDS_PER_MICROSECOND),
                floorMod(lastAcceptedNanos + 1, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND));
    }

    private void assertGpuDateAddThrows(String unit, long value, Type dateTimeType, long nativeValue)
    {
        assertGpuDateAddThrows(unit, value, dateTimeType, (Object) nativeValue);
    }

    /// The other half of a deliberate divergence: an input the GPU rejects is one the CPU
    /// implementation answers, so a change that made the CPU reject it too would go unnoticed.
    private void assertDateAddSucceedsOnCpu(String unit, long value, Type dateTimeType, long nativeValue)
    {
        Expression expression = dateAdd(unit, new Constant(BIGINT, value), dateTimeType);
        List<Page> cpuResults = executeWithCpu(
                compileCpuExpression(expression, layoutFor(List.of(dateTimeType))),
                List.of(new Page(writeNativeValue(dateTimeType, nativeValue))));
        assertThat(cpuResults)
                .as("CPU date_add('%s', %s, %s) on %s", unit, value, nativeValue, dateTimeType)
                .isNotEmpty();
    }

    private void assertGpuDateAddThrows(String unit, long value, Type dateTimeType, Object nativeValue)
    {
        assertGpuDateAddThrows(unit, value, dateTimeType, writeNativeValue(dateTimeType, nativeValue));
    }

    private void assertGpuDateAddThrows(String unit, long value, Type dateTimeType, Block dateTimes)
    {
        Expression expression = dateAdd(unit, new Constant(BIGINT, value), dateTimeType);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(dateTimeType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        try {
            assertTrinoExceptionThrownBy(() -> executeWithGpu(
                    List.of(new Page(dateTimes.getPositionCount(), dateTimes)),
                    List.of(dateTimeType),
                    expression,
                    gpuExpression))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
        catch (Throwable t) {
            t.addSuppressed(new Exception("unit: " + unit));
            t.addSuppressed(new Exception("value: " + value));
            t.addSuppressed(new Exception("type: " + dateTimeType));
            throw t;
        }
    }

    /// A `date` block, where a null element becomes a null row.
    private static Block dateBlock(Long... epochDays)
    {
        BlockBuilder builder = DATE.createBlockBuilder(null, epochDays.length);
        for (Long epochDay : epochDays) {
            if (epochDay == null) {
                builder.appendNull();
                continue;
            }
            DATE.writeLong(builder, epochDay);
        }
        return builder.build();
    }

    private void assertDateAddBound(Type dateTimeType, long value, long lastAccepted, long firstRejected)
    {
        try {
            assertDateAddMatchesCpu("day", value, dateTimeType, writeNativeValue(dateTimeType, lastAccepted));
            assertDateAddMatchesCpu("day", value, dateTimeType, writeNativeValue(dateTimeType, firstRejected));
        }
        catch (Throwable t) {
            t.addSuppressed(new Exception("type: " + dateTimeType));
            t.addSuppressed(new Exception("value: " + value));
            throw t;
        }
    }

    /// A shift past the largest representable timestamp fails on the CPU, which holds the result in
    /// milliseconds and rescales it with an exact multiplication. The GPU must fail the same way.
    @Test
    void testDateAddOverflow()
    {
        Map<Type, Long> largestTimestamp = Map.of(
                createTimestampType(0), Long.MAX_VALUE / 1_000_000 * 1_000_000,
                createTimestampType(3), Long.MAX_VALUE / 1_000 * 1_000,
                createTimestampType(6), Long.MAX_VALUE);

        for (Map.Entry<Type, Long> entry : largestTimestamp.entrySet()) {
            Type timestampType = entry.getKey();
            for (String unit : List.of("second", "day", "week", "month", "year")) {
                try {
                    assertDateAddMatchesCpu(unit, 1, timestampType, writeNativeValue(timestampType, entry.getValue()));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("unit: " + unit));
                    t.addSuppressed(new Exception("type: " + timestampType));
                    throw t;
                }
            }
        }
    }

    private static boolean isDateAddSupported(GpuDateAdd.Unit unit, Type dateTimeType)
    {
        if (dateTimeType.equals(DATE)) {
            return switch (unit) {
                // the CPU implementation rejects these as invalid DATE fields
                case MILLISECOND, SECOND, MINUTE, HOUR -> false;
                case DAY, WEEK, MONTH, QUARTER, YEAR -> true;
            };
        }
        if (!(dateTimeType instanceof TimestampType timestampType) || timestampType.getPrecision() > 9) {
            // time and the time zone aware types have no GPU representation at all, and neither has
            // timestamp(10) and finer
            return false;
        }
        // the CPU implementation rounds a millisecond shift to the declared precision
        return unit != GpuDateAdd.Unit.MILLISECOND || timestampType.getPrecision() >= 3;
    }

    private void assertDateAddMatchesCpu(String unit, long value, Type dateTimeType, Block dateTimes)
    {
        int channelA = 0;
        assertGpuMatchesCpu(
                List.of(new Page(dateTimes.getPositionCount(), dateTimes)),
                List.of(dateTimeType),
                dateAdd(unit, new Constant(BIGINT, value), dateTimeType),
                Set.of(channelA));
    }

    private static Expression dateAdd(String unit, Expression value, Type dateTimeType)
    {
        Type unitType = createVarcharType(unit.length());
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_add", fromTypes(unitType, BIGINT, dateTimeType)),
                ImmutableList.of(
                        new Constant(unitType, Slices.utf8Slice(unit)),
                        value,
                        field(0, dateTimeType)));
    }

    /// Random dates or timestamps a few hundred years around the epoch, so that neither engine runs
    /// out of range. Overflow is covered separately.
    private static Block dateTimeBlock(Type dateTimeType, int positionsCount, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        Optional<long[]> validity = nullsProvider.getValidityWords(positionsCount);
        BlockBuilder builder = dateTimeType.createBlockBuilder(null, positionsCount);
        for (int position = 0; position < positionsCount; position++) {
            if (validity.isPresent() && !Bitmap.isSet(validity.get(), 0, position)) {
                builder.appendNull();
                continue;
            }
            if (dateTimeType.equals(DATE)) {
                DATE.writeLong(builder, random.nextLong(-150_000, 150_000));
            }
            else {
                TimestampType timestampType = (TimestampType) dateTimeType;
                if (timestampType.isShort()) {
                    long scale = 1;
                    for (int digits = timestampType.getPrecision(); digits < TimestampType.MAX_SHORT_PRECISION; digits++) {
                        scale *= 10;
                    }
                    timestampType.writeLong(builder, random.nextLong(-13_000_000_000_000_000L, 13_000_000_000_000_000L) / scale * scale);
                }
                else {
                    // A nanosecond carrier reaches only about 1677 to 2262, and a shift has to stay inside it
                    timestampType.writeObject(builder, new LongTimestamp(
                            random.nextLong(-8_000_000_000_000_000L, 8_000_000_000_000_000L),
                            randomPicosOfMicro(random, timestampType)));
                }
            }
        }
        return builder.build();
    }

    /// Smallest non-zero picosOfMicro the precision can hold, so that generated values exercise the
    /// sub-microsecond digits without overstating the precision.
    private static int picosOfMicroScale(TimestampType timestampType)
    {
        int scale = 1;
        for (int digits = timestampType.getPrecision(); digits < TimestampType.MAX_PRECISION; digits++) {
            scale *= 10;
        }
        return scale;
    }

    private static int randomPicosOfMicro(Random random, TimestampType timestampType)
    {
        int scale = picosOfMicroScale(timestampType);
        return random.nextInt(1_000_000 / scale) * scale;
    }

    private static Block dateTimeBlock(Type dateTimeType, List<LocalDate> dates)
    {
        BlockBuilder builder = dateTimeType.createBlockBuilder(null, dates.size() * 2);
        for (LocalDate date : dates) {
            if (dateTimeType.equals(DATE)) {
                DATE.writeLong(builder, date.toEpochDay());
                DATE.writeLong(builder, date.toEpochDay());
                continue;
            }
            TimestampType timestampType = (TimestampType) dateTimeType;
            long midnightMicros = date.toEpochDay() * MICROSECONDS_PER_DAY;
            long endOfDayMicros = midnightMicros + MICROSECONDS_PER_DAY - 1;
            if (!timestampType.isShort()) {
                timestampType.writeObject(builder, new LongTimestamp(midnightMicros, 0));
                // Sub-microsecond digits too, which the shift has to carry over untouched
                timestampType.writeObject(builder, new LongTimestamp(endOfDayMicros, picosOfMicroScale(timestampType) * 9));
                continue;
            }
            long scale = 1;
            for (int digits = timestampType.getPrecision(); digits < TimestampType.MAX_SHORT_PRECISION; digits++) {
                scale *= 10;
            }
            timestampType.writeLong(builder, midnightMicros);
            // floor, so that a pre-epoch end of day does not round up to the next midnight
            timestampType.writeLong(builder, floorDiv(endOfDayMicros, scale) * scale);
        }
        return builder.build();
    }
}
