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
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createInRangeDateTimeBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuExtract
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDateTimeExtract(NullsProvider nullsProvider)
    {
        for (String functionName : List.of("day", "hour", "minute", "second")) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                testDateTimeExtract(functionName, createTimestampType(precision), nullsProvider);
            }
        }
        // Trino only defines day() (aka day_of_month) for DATE — hour/minute/second are timestamp-only.
        testDateTimeExtract("day", DATE, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testYearExtract(NullsProvider nullsProvider)
    {
        // cuDF's year() returns INT16, so it can only represent years within cuDF's calendar.
        // Random test data spans the full long/int range and would overflow, so generate inputs in a
        // realistic range. The GPU compiler does not statically constrain the input range — callers
        // are responsible for ensuring values fit, which is the case for all production data sets.
        testYearExtract(DATE, nullsProvider);
        for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            testYearExtract(createTimestampType(precision), nullsProvider);
        }
    }

    private void testYearExtract(Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int positionsCount = 64;
        List<Type> inputTypes = List.of(type);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createInRangeDateTimeBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    void testYearExtractOutOfRangeThrows()
    {
        // Year 100000 is outside cuDF's INT16 year range, so GPU must throw. CPU would happily return 100000.
        int channelA = 0;
        Type type = DATE;
        long outOfRangeDays = LocalDate.of(100_000, 6, 15).toEpochDay();
        List<Page> inputPages = List.of(new Page(writeNativeValue(type, outOfRangeDays)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Year out of range supported by GPU: must be in [-32767, 32767]");
    }

    /// cuDF's calendar reaches year -32767 through year 32767, so the last year at either end has
    /// to be answered and the first year past it rejected rather than silently answered wrong.
    @Test
    void testYearExtractAtCudfCalendarBound()
    {
        assertYearExtractMatchesCpu(LocalDate.of(-32767, 1, 1));
        assertYearExtractMatchesCpu(LocalDate.of(32767, 12, 31));
        assertYearExtractThrows(LocalDate.of(-32768, 12, 31));
        assertYearExtractThrows(LocalDate.of(32768, 1, 1));
    }

    private void assertYearExtractMatchesCpu(LocalDate date)
    {
        assertGpuMatchesCpu(
                List.of(new Page(writeNativeValue(DATE, date.toEpochDay()))),
                List.of(DATE),
                yearExtract(DATE),
                Set.of(0));
    }

    private void assertYearExtractThrows(LocalDate date)
    {
        Expression expression = yearExtract(DATE);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(DATE)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(
                List.of(new Page(writeNativeValue(DATE, date.toEpochDay()))),
                List.of(DATE),
                expression,
                gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Year out of range supported by GPU: must be in [-32767, 32767]");
    }

    private static Expression yearExtract(Type type)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(type)),
                ImmutableList.of(field(0, type)));
    }

    private void testDateTimeExtract(String functionName, Type timestampType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(timestampType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(timestampType, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(timestampType)),
                ImmutableList.of(field(channelA, timestampType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }
}
