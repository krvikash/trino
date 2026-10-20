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

import ai.rapids.cudf.DType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuDateTrunc.Field;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuDateTrunc
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDateTrunc(NullsProvider nullsProvider)
    {
        for (String unit : Arrays.stream(Field.values()).map(Field::trinoDateTruncUnit).toList()) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                try {
                    testDateTrunc(unit, createTimestampType(precision), nullsProvider);
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("unit: " + unit));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    private void testDateTrunc(String unit, Type timestampType, NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(timestampType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(timestampType, positionsCount, nullsProvider)));

        assertGpuMatchesCpu(inputPages, inputTypes, dateTruncExpression(unit, timestampType));
    }

    private static Expression dateTruncExpression(String unit, Type timestampType)
    {
        Type unitType = createVarcharType(unit.length());
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_trunc", fromTypes(unitType, timestampType)),
                ImmutableList.of(new Constant(unitType, Slices.utf8Slice(unit)), field(0, timestampType)));
    }

    @Test
    void testDateTruncWithUnitInDifferentCases()
    {
        for (String unit : List.of("minute", "MINUTE", "MiNuTe")) {
            testDateTrunc(unit, createTimestampType(6), NullsProvider.RANDOM_NULLS);
        }
    }

    @Test
    void testDateTruncMicrosecondUnderflow()
    {
        TimestampType type = createTimestampType(6);
        for (Field field : Field.values()) {
            OptionalLong period = field.periodInNativeUnits(DType.TIMESTAMP_MICROSECONDS);
            if (period.isEmpty()) {
                continue;
            }
            long firstSafe = firstRepresentableDateTruncValue(period.orElseThrow());
            String unit = field.trinoDateTruncUnit();

            assertDateTruncGpuThrows(unit, type, Long.MIN_VALUE);
            assertDateTruncGpuThrows(unit, type, firstSafe - 1);
            assertDateTruncMatchesCpu(unit, type, firstSafe);
        }
    }

    @Test
    void testDateTruncUnderflowWithNulls()
    {
        TimestampType type = createTimestampType(6);
        String unit = "day";
        long firstSafe = firstRepresentableDateTruncValue(MICROSECONDS_PER_DAY);

        assertDateTruncMatchesCpu(unit, type, shortTimestampBlock(type, (Long) null));
        assertDateTruncMatchesCpu(unit, type, shortTimestampBlock(type, null, firstSafe));
        assertDateTruncGpuThrows(unit, type, shortTimestampBlock(type, null, Long.MIN_VALUE));
        assertDateTruncGpuThrows(unit, type, shortTimestampBlock(type, Long.MIN_VALUE, null));
    }

    @Test
    void testDateTruncNanosecondUnderflow()
    {
        TimestampType type = createTimestampType(9);
        long firstConvertibleNanos = BigInteger.valueOf(Long.MIN_VALUE)
                .divide(BigInteger.valueOf(NANOSECONDS_PER_MICROSECOND))
                .multiply(BigInteger.valueOf(NANOSECONDS_PER_MICROSECOND))
                .longValueExact();
        LongTimestamp firstConvertible = nanosToLongTimestamp(firstConvertibleNanos);
        for (Field field : Field.values()) {
            OptionalLong period = field.periodInNativeUnits(DType.TIMESTAMP_NANOSECONDS);
            if (period.isEmpty()) {
                continue;
            }
            long periodValue = period.orElseThrow();
            long firstSafe = firstRepresentableDateTruncValue(periodValue);
            String unit = field.trinoDateTruncUnit();

            assertDateTruncGpuThrows(unit, type, firstConvertible);
            assertDateTruncGpuThrows(unit, type, nanosToLongTimestamp(firstSafe - 1));
            assertDateTruncMatchesCpu(unit, type, nanosToLongTimestamp(firstSafe));
        }
    }

    private static Block shortTimestampBlock(TimestampType type, Long... epochMicros)
    {
        BlockBuilder builder = type.createBlockBuilder(null, epochMicros.length);
        for (Long value : epochMicros) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, value);
            }
        }
        return builder.build();
    }

    private void assertDateTruncGpuThrows(String unit, Type timestampType, Object trinoValue)
    {
        assertDateTruncGpuThrows(unit, timestampType, writeNativeValue(timestampType, trinoValue));
    }

    private void assertDateTruncGpuThrows(String unit, Type timestampType, Block timestamps)
    {
        Expression expression = dateTruncExpression(unit, timestampType);
        List<Page> inputPages = List.of(new Page(timestamps));
        List<Type> inputTypes = List.of(timestampType);

        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("date_trunc('%s', ...) result out of range supported by GPU".formatted(unit));
    }

    private void assertDateTruncMatchesCpu(String unit, Type timestampType, Object trinoValue)
    {
        assertDateTruncMatchesCpu(unit, timestampType, writeNativeValue(timestampType, trinoValue));
    }

    private void assertDateTruncMatchesCpu(String unit, Type timestampType, Block timestamps)
    {
        assertGpuMatchesCpu(
                List.of(new Page(timestamps)),
                List.of(timestampType),
                dateTruncExpression(unit, timestampType));
    }

    private static LongTimestamp nanosToLongTimestamp(long nanos)
    {
        long epochMicros = floorDiv(nanos, NANOSECONDS_PER_MICROSECOND);
        int picosOfMicro = floorMod(nanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    private static long firstRepresentableDateTruncValue(long period)
    {
        // BigInteger.divide rounds toward zero, yielding the first multiple within the long range.
        BigInteger periodBigInteger = BigInteger.valueOf(period);
        return BigInteger.valueOf(Long.MIN_VALUE)
                .divide(periodBigInteger)
                .multiply(periodBigInteger)
                .longValueExact();
    }
}
