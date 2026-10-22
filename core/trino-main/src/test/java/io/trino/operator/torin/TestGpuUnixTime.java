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
import io.trino.Session;
import io.trino.operator.torin.expression.GpuFromUnixTime;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.torin.GpuTestUtils.TESTED_GPU_TIMESTAMP_WITH_TIME_ZONE_TYPES;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBlock;
import static io.trino.operator.torin.GpuTestUtils.createFloatingPointBlock;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuUnixTime
{
    /// `from_unixtime` returns the session time zone, and the GPU representation holds only UTC, so
    /// the CPU side of its comparison has to run in a UTC session too.
    private static final Session UTC_SESSION = testSessionBuilder(TEST_SESSION)
            .setTimeZoneKey(UTC_KEY)
            .build();

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testToUnixTime(NullsProvider nullsProvider)
    {
        for (Type type : TESTED_GPU_TIMESTAMP_WITH_TIME_ZONE_TYPES) {
            int channel = 0;
            List<Type> inputTypes = List.of(type);
            List<Page> inputPages = List.of(new Page(createBlock(type, 64, nullsProvider)));
            assertGpuMatchesCpu(inputPages, inputTypes, toUnixTime(field(channel, type)));
        }
    }

    /// 2020-05-10 12:34:56.12345 UTC, where converting the milliseconds and the picoseconds of the
    /// millisecond separately and adding the two rounds to a different double.
    @Test
    void testToUnixTimeRoundsOnce()
    {
        // 2020-05-10 12:34:56.12345 UTC, as epoch milliseconds plus 0.45 of a millisecond
        long epochMillis = 1589114096123L;
        int picosOfMilli = 450_000_000;

        for (int precision = 5; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            BlockBuilder builder = type.createBlockBuilder(null, 2);
            type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY));
            builder.appendNull();

            assertGpuMatchesCpu(List.of(new Page(builder.build())), List.of(type), toUnixTime(field(0, type)));
        }
    }

    /// `to_unixtime` is declared only for `timestamp(p) with time zone`, so a plain `timestamp`
    /// reaches it through a cast that resolves the session time zone. cuDF has no zone rules, so the
    /// cast does not compile and the whole call stays on the CPU.
    @Test
    void testToUnixTimeOnTimestampFallsBackToCpu()
    {
        for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            Type argumentType = createTimestampType(precision);
            TimestampWithTimeZoneType castType = createTimestampWithTimeZoneType(precision);
            Expression expression = toUnixTime(new Cast(field(0, argumentType), castType));
            assertThat(compileExpression(expression, layoutFor(List.of(argumentType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .as("to_unixtime over %s", argumentType)
                    .isEmpty();
        }
    }

    @Test
    void testToUnixTimeUnsupportedPrecisionFallsBackToCpu()
    {
        for (int precision = MAX_GPU_TIMESTAMP_PRECISION + 1; precision <= TimestampWithTimeZoneType.MAX_PRECISION; precision++) {
            Type type = createTimestampWithTimeZoneType(precision);
            assertThat(compileExpression(toUnixTime(field(0, type)), layoutFor(List.of(type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .as("to_unixtime over %s", type)
                    .isEmpty();
        }
    }

    private static Expression toUnixTime(Expression argument)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("to_unixtime", fromTypes(argument.type())),
                ImmutableList.of(argument));
    }

    /// The input covers the halfway fractions of both signs, the doubles nearest a half millisecond,
    /// whose product with 1000 may land on either side of the half, and their neighbours.
    @Test
    void testFromUnixTime()
    {
        List<Double> values = new ArrayList<>(asList(
                0.0,
                -0.0,
                null,
                0.0005,
                -0.0005,
                0.0015,
                -0.0015,
                0.0025,
                -0.0025,
                4.9999999999999994e-4,
                -4.9999999999999994e-4,
                GpuFromUnixTime.MIN_EPOCH_MILLIS / MILLISECONDS_PER_SECOND,
                GpuFromUnixTime.MAX_EPOCH_MILLIS / MILLISECONDS_PER_SECOND,
                Math.nextDown(0.0005),
                Math.nextUp(0.0005),
                null,
                1.0,
                -1.0,
                1589093396.1234498,
                -1589093396.1234498,
                // The product with 1000 rounds to the halfway 1000.5
                1.0005,
                -1589067296.1235,
                1589067296.1235,
                0.5005,
                0.5004999,
                1589067296.1235003,
                -4.999999999999999e-4));
        Random random = new Random(42);
        for (int i = 0; i < 512; i++) {
            values.add(random.nextDouble(-2e12, 2e12));
            values.add(random.nextDouble(-1000, 1000));
            values.add(random.nextDouble(-1, 1));
        }

        assertFromUnixTimeMatchesCpu(List.of(new Page(createFloatingPointBlock(DOUBLE, values))));
    }

    /// The CPU implementation returns the epoch for NaN rather than failing.
    @Test
    void testFromUnixTimeNotANumber()
    {
        assertFromUnixTimeMatchesCpu(List.of(new Page(createFloatingPointBlock(DOUBLE, List.of(Double.NaN)))));
    }

    @Test
    void testFromUnixTimeOutOfRange()
    {
        List<Double> values = List.of(
                (GpuFromUnixTime.MIN_EPOCH_MILLIS - 1.0) / MILLISECONDS_PER_SECOND,
                (GpuFromUnixTime.MAX_EPOCH_MILLIS + 1.0) / MILLISECONDS_PER_SECOND,
                // Rounds away from zero to one millisecond below the minimum
                -2251799813685.2485,
                Double.MAX_VALUE,
                -Double.MAX_VALUE,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY);
        for (Double value : values) {
            assertFromUnixTimeMatchesCpu(List.of(new Page(createFloatingPointBlock(DOUBLE, List.of(value)))));
        }
    }

    /// The result carries the session time zone, and the GPU representation holds only UTC, so
    /// anything else leaves the call on the CPU.
    @Test
    void testFromUnixTimeOutsideUtcSessionFallsBackToCpu()
    {
        Expression expression = fromUnixTime(field(0, DOUBLE));
        for (TimeZoneKey zoneKey : List.of(getTimeZoneKey("Europe/Warsaw"), getTimeZoneKeyForOffset(5 * 60 + 30))) {
            assertThat(compileExpression(expression, layoutFor(List.of(DOUBLE)), SQL_STANDARD, zoneKey))
                    .as("from_unixtime in %s", zoneKey.getId())
                    .isEmpty();
        }
    }

    @Test
    void testFromUnixTimeOtherOverloadsFallBackToCpu()
    {
        Map<Symbol, Integer> layout = layoutFor(List.of(DOUBLE));
        Expression withZoneId = new Call(
                FUNCTION_RESOLUTION.resolveFunction("from_unixtime", fromTypes(DOUBLE, VARCHAR)),
                ImmutableList.of(field(0, DOUBLE), new Constant(VARCHAR, utf8Slice("America/New_York"))));
        assertThat(compileExpression(withZoneId, layout, SQL_STANDARD, UTC_KEY)).isEmpty();

        Expression withOffset = new Call(
                FUNCTION_RESOLUTION.resolveFunction("from_unixtime", fromTypes(DOUBLE, BIGINT, BIGINT)),
                ImmutableList.of(field(0, DOUBLE), new Constant(BIGINT, 5L), new Constant(BIGINT, 30L)));
        assertThat(compileExpression(withOffset, layout, SQL_STANDARD, UTC_KEY)).isEmpty();

        Type nanosType = createDecimalType(20, 3);
        Expression fromUnixTimeNanos = new Call(
                FUNCTION_RESOLUTION.resolveFunction("from_unixtime_nanos", fromTypes(nanosType)),
                ImmutableList.of(field(0, nanosType)));
        assertThat(compileExpression(fromUnixTimeNanos, layoutFor(List.of(nanosType)), SQL_STANDARD, UTC_KEY)).isEmpty();
    }

    /// `from_unixtime` compiles only in a UTC session, so both engines are run in one.
    private static void assertFromUnixTimeMatchesCpu(List<Page> inputPages)
    {
        Expression expression = fromUnixTime(field(0, DOUBLE));
        assertGpuMatchesCpu(UTC_SESSION, inputPages, List.of(DOUBLE), expression);
    }

    private static Expression fromUnixTime(Expression argument)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("from_unixtime", fromTypes(argument.type())),
                ImmutableList.of(argument));
    }
}
