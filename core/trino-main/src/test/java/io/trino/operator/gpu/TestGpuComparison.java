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
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuComparison
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @MethodSource("comparisonTestCases")
    void testComparison(Type type, ComparisonOperator operator, NullsProvider nullsProvider)
    {
        if (type == NUMBER) {
            // type currently not supported
            assertThat(isConvertible(type)).as("Expected %s to be not supported on GPU", type)
                    .isFalse();
            return;
        }

        List<Type> inputTypes = List.of(type, type);
        int channelA = 0;
        int channelB = 1;
        Expression expression = comparison(operator, field(channelA, type), field(channelB, type));

        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        if (operator == ComparisonOperator.IDENTICAL && (type == REAL || type == DOUBLE)) {
            // operator currently not supported
            assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .isEmpty();
            return;
        }
        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    private static Stream<Object[]> comparisonTestCases()
    {
        return Lists.cartesianProduct(
                        List.of(
                                BOOLEAN,
                                TINYINT,
                                SMALLINT,
                                INTEGER,
                                BIGINT,
                                REAL,
                                DOUBLE,
                                createDecimalType(3, 0),
                                createDecimalType(13, 0),
                                createDecimalType(27, 0),
                                createDecimalType(27, 5),
                                createDecimalType(38),
                                NUMBER,
                                VARCHAR),
                        ImmutableList.copyOf(ComparisonOperator.values()),
                        ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    @ParameterizedTest
    @EnumSource(ComparisonOperator.class)
    void testComparisonCharPaddedSemantics(ComparisonOperator operator)
    {
        if (operator == ComparisonOperator.IDENTICAL) {
            return;
        }
        // \u0001 is less than space, so comparisons would return wrong results without proper CHAR padding
        Type charType = createCharType(2);
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(charType, charType);
        List<Page> inputPages = List.of(new Page(
                charBlock(charType, "a", "a\u0001", "b", "a"),
                charBlock(charType, "a\u0001", "a", "a", "ab")));

        Expression expression = comparison(operator, field(channelA, charType), field(channelB, charType));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testComparisonAfterWideningCharCast()
    {
        CharType sourceType = createCharType(1);
        CharType targetType = createCharType(2);
        List<Type> inputTypes = List.of(sourceType, targetType);
        List<Page> inputPages = List.of(new Page(
                charBlock(sourceType, "a", "a", ""),
                charBlock(targetType, "a", "a\u0001", "")));

        // The widened value must acquire the target width before a GPU comparison uses it.
        Expression widened = new Cast(field(0, sourceType), targetType);
        Expression equal = comparison(ComparisonOperator.EQUAL, widened, field(1, targetType));
        assertGpuMatchesCpu(inputPages, inputTypes, equal);

        Expression lessThan = comparison(ComparisonOperator.LESS_THAN, widened, field(1, targetType));
        assertGpuMatchesCpu(inputPages, inputTypes, lessThan);
    }

    private static Block charBlock(Type charType, String... values)
    {
        BlockBuilder builder = charType.createBlockBuilder(null, values.length);
        for (String value : values) {
            charType.writeSlice(builder, Slices.utf8Slice(value));
        }
        return builder.build();
    }

    /// Trino compares a `timestamp with time zone` by instant, so a constant in another zone is
    /// equal to a column value of the same instant in UTC.
    @Test
    void testTimestampWithTimeZoneComparedWithConstantInAnotherZone()
    {
        TimeZoneKey warsaw = getTimeZoneKey("Europe/Warsaw");
        long epochMillis = 1718454896123L;
        for (int precision : List.of(3, 6, 9)) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            BlockBuilder builder = type.createBlockBuilder(null, 3);
            writeNativeValue(type, builder, timestampWithTimeZone(type, epochMillis, UTC_KEY));
            writeNativeValue(type, builder, timestampWithTimeZone(type, epochMillis + 1, UTC_KEY));
            builder.appendNull();
            List<Page> inputPages = List.of(new Page(builder.build()));
            List<Type> inputTypes = List.of(type);

            Expression in = new In(field(0, type), ImmutableList.of(
                    new Constant(type, timestampWithTimeZone(type, epochMillis, warsaw)),
                    new Constant(type, timestampWithTimeZone(type, epochMillis + 2, UTC_KEY))));
            assertGpuMatchesCpu(inputPages, inputTypes, in);

            Expression lessThan = comparison(ComparisonOperator.LESS_THAN, field(0, type), new Constant(type, timestampWithTimeZone(type, epochMillis + 1, warsaw)));
            assertGpuMatchesCpu(inputPages, inputTypes, lessThan);
        }
    }

    private static Object timestampWithTimeZone(TimestampWithTimeZoneType type, long epochMillis, TimeZoneKey zoneKey)
    {
        if (type.isShort()) {
            return packDateTimeWithZone(epochMillis, zoneKey);
        }
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, zoneKey);
    }
}
