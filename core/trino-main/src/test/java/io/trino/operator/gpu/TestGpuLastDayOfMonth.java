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
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createInRangeDateTimeBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.lang.Math.multiplyExact;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuLastDayOfMonth
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLastDayOfMonth(NullsProvider nullsProvider)
    {
        testLastDayOfMonth(DATE, nullsProvider);
        for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            testLastDayOfMonth(createTimestampType(precision), nullsProvider);
        }
    }

    private void testLastDayOfMonth(Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(type);
        int positionsCount = 64;
        // cuDF's lastDayOfMonth() internally extracts the year as INT16, so
        // dates with years outside cuDF's calendar produce incorrect results.
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createInRangeDateTimeBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("last_day_of_month", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    void testLastDayOfMonthCornerCases()
    {
        List<LocalDate> dates = List.of(
                LocalDate.of(2000, 2, 15),  // leap year Feb
                LocalDate.of(2000, 2, 29),  // leap year Feb last day
                LocalDate.of(2001, 2, 15),  // non-leap year Feb
                LocalDate.of(1900, 2, 15),  // century non-leap year Feb
                LocalDate.of(2024, 1, 1),   // first day of 31-day month
                LocalDate.of(2024, 1, 31),  // already last day of 31-day month
                LocalDate.of(2024, 4, 15),  // 30-day month
                LocalDate.of(2024, 12, 31), // year end
                LocalDate.of(1969, 6, 15),  // before epoch
                LocalDate.of(1, 1, 1));     // year 1

        testLastDayOfMonthCornerCases(DATE, dates);
        for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            testLastDayOfMonthCornerCases(createTimestampType(precision), dates);
        }
    }

    private void testLastDayOfMonthCornerCases(Type type, List<LocalDate> dates)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(type);
        BlockBuilder builder = type.createBlockBuilder(null, dates.size());
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("last_day_of_month", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        for (LocalDate date : dates) {
            long epochDay = date.toEpochDay();
            if (type == DATE) {
                type.writeLong(builder, epochDay);
            }
            else if (type instanceof TimestampType timestampType && timestampType.isShort()) {
                type.writeLong(builder, multiplyExact(epochDay, MICROSECONDS_PER_DAY));
            }
            else {
                long epochMicros = multiplyExact(epochDay, MICROSECONDS_PER_DAY);
                if (epochMicros < Long.MIN_VALUE / NANOSECONDS_PER_MICROSECOND || epochMicros > Long.MAX_VALUE / NANOSECONDS_PER_MICROSECOND) {
                    // GPU converts long timestamps to TIMESTAMP_NANOSECONDS, which has a narrower representable range
                    List<Page> outOfRangePages = List.of(new Page(writeNativeValue(type, new LongTimestamp(epochMicros, 0))));
                    assertTrinoExceptionThrownBy(() -> assertGpuMatchesCpu(outOfRangePages, inputTypes, expression, Set.of(channelA)))
                            .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                            .hasMessage("Timestamp value out of range support for GPU for type %s: 0001-01-01T00:00".formatted(type));
                    continue;
                }
                type.writeObject(builder, new LongTimestamp(epochMicros, 0));
            }
        }
        List<Page> inputPages = List.of(new Page(builder.build()));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    void testLastDayOfMonthOutOfRangeThrows()
    {
        int channelA = 0;
        Type type = DATE;
        long outOfRangeDays = LocalDate.of(100_000, 6, 15).toEpochDay();
        List<Page> inputPages = List.of(new Page(writeNativeValue(type, outOfRangeDays)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("last_day_of_month", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Year out of range supported by GPU: must be in [-32767, 32767]");
    }
}
