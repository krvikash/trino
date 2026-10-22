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
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.operator.torin.expression.CompiledExpression;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.WhenClause;
import io.trino.testing.TestingSession;
import io.trino.type.CharVarcharCoercion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.LEGACY_VARCHAR_TO_CHAR_COERCION;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.TEST_STRINGS;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.torin.GpuTestUtils.createRepeatingValuesBlock;
import static io.trino.operator.torin.GpuTestUtils.createVarcharPages;
import static io.trino.operator.torin.GpuTestUtils.executeWithGpu;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuCasts
{
    private static final CharType CHAR_CAST_SOURCE_TYPE = createCharType(8);

    /// `char(8)` values in Trino's representation, which strips trailing spaces: empty and all-space
    /// values, leading and interior spaces, lengths on either side of the tested target lengths, and
    /// `TEST_STRINGS` cut to the char length, contributing multi-byte characters.
    private final List<Object> charCastValues = Stream.concat(
                    Stream.of("", " ", "        ", "a", "ab ", "abc", "abcdefgh", " abc", "a b c"),
                    TEST_STRINGS.stream())
            .map(value -> (Object) truncateToLengthAndTrimSpaces(Slices.utf8Slice(value), CHAR_CAST_SOURCE_TYPE))
            .distinct()
            .collect(toImmutableList());

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @MethodSource("charToVarcharCastTestCases")
    void testCastCharToVarchar(Type targetType, CharVarcharCoercion charVarcharCoercion, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(CHAR_CAST_SOURCE_TYPE);
        int positionsCount = 256;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(CHAR_CAST_SOURCE_TYPE, positionsCount, nullsProvider, charCastValues, 1)));

        Expression expression = new Cast(field(channelA, CHAR_CAST_SOURCE_TYPE), targetType);
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty(LEGACY_VARCHAR_TO_CHAR_COERCION, Boolean.toString(charVarcharCoercion == CharVarcharCoercion.LEGACY))
                .build();
        assertGpuMatchesCpu(session, inputPages, inputTypes, expression, Set.of(channelA));
    }

    private static Stream<Object[]> charToVarcharCastTestCases()
    {
        return Lists.cartesianProduct(
                        // Target lengths above, at and below the source char length, plus unbounded varchar
                        List.of(VARCHAR, createVarcharType(20), createVarcharType(9), createVarcharType(8), createVarcharType(7), createVarcharType(3), createVarcharType(1), createVarcharType(0)),
                        ImmutableList.copyOf(CharVarcharCoercion.values()),
                        ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testTypeOnlyCast(NullsProvider nullsProvider)
    {
        // VARCHAR(10) -> VARCHAR(20) shares the same cudf STRING dtype, so visitCast should
        // skip emitting GpuCast and forward the inner expression directly.
        int channelA = 0;
        VarcharType sourceType = createVarcharType(10);
        Type targetType = createVarcharType(20);
        List<Type> inputTypes = List.of(sourceType);
        List<Page> inputPages = createVarcharPages(sourceType, List.of(64), nullsProvider);

        Expression expression = new Cast(field(channelA, sourceType), targetType);

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCastDateToVarchar(NullsProvider nullsProvider)
    {
        int positionsCount = 256;
        List<Object> dates = Stream.of(
                        LocalDate.of(0, 1, 1),
                        LocalDate.of(1, 1, 1),
                        LocalDate.of(999, 12, 31),
                        LocalDate.of(1969, 12, 31),
                        LocalDate.of(1970, 1, 1),
                        LocalDate.of(2000, 2, 29),
                        LocalDate.of(2024, 10, 14),
                        LocalDate.of(9999, 12, 31))
                .map(date -> (Object) date.toEpochDay())
                .collect(toImmutableList());
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(DATE, positionsCount, nullsProvider, dates, 1)));
        for (Type targetType : List.of(VARCHAR, createVarcharType(10))) {
            assertGpuMatchesCpu(inputPages, List.of(DATE), new Cast(field(0, DATE), targetType));
        }
    }

    @Test
    void testCastDateToVarcharYearOutOfRange()
    {
        List<Type> inputTypes = List.of(DATE);
        Expression expression = new Cast(field(0, DATE), VARCHAR);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        for (LocalDate date : List.of(LocalDate.of(-1, 12, 31), LocalDate.of(10000, 1, 1), LocalDate.of(-12345, 6, 7))) {
            List<Page> inputPages = List.of(new Page(dateBlock(Arrays.asList(null, 0L, date.toEpochDay()))));
            assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                    .hasMessage("Year outside of range supported by GPU for formatting");
        }
    }

    @Test
    void testCastDateToVarcharAllNullAndEmptyInputs()
    {
        Expression expression = new Cast(field(0, DATE), VARCHAR);
        for (List<Long> values : List.of(Arrays.<Long>asList(null, null), List.<Long>of())) {
            assertGpuMatchesCpu(List.of(new Page(dateBlock(values))), List.of(DATE), expression);
        }
    }

    @Test
    void testCastDateToVarcharLazyEvaluation()
    {
        long outOfRange = LocalDate.of(10000, 1, 1).toEpochDay();
        Expression condition = comparison(ComparisonOperator.LESS_THAN, field(0, DATE), new Constant(DATE, outOfRange));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(condition, new Cast(field(0, DATE), VARCHAR))),
                new Constant(VARCHAR, Slices.utf8Slice("out of range")));
        assertGpuMatchesCpu(List.of(new Page(dateBlock(Arrays.asList(outOfRange, 0L, null)))), List.of(DATE), expression);
    }

    @Test
    void testTimestampToMillisCastOverflow()
    {
        TimestampType timestampType = createTimestampType(6);
        List<Type> inputTypes = List.of(timestampType);
        Expression expression = new Cast(field(0, timestampType), createTimestampType(3));
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        for (long value : List.of(Long.MIN_VALUE, Long.MIN_VALUE + 307, Long.MAX_VALUE - 499, Long.MAX_VALUE)) {
            List<Page> inputPages = List.of(new Page(timestampCastInputBlock(Arrays.asList(null, 0L, value, null))));
            assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                    .hasMessage("Timestamp rounding argument out of range supported by GPU");
        }
    }

    @Test
    void testTimestampToMillisCastRounding()
    {
        TimestampType timestampType = createTimestampType(6);
        List<Page> inputPages = List.of(new Page(timestampCastInputBlock(Arrays.asList(
                null, -1500L, -501L, -500L, 499L, 500L, Long.MAX_VALUE - 500))));
        Expression expression = new Cast(field(0, timestampType), createTimestampType(3));
        assertGpuMatchesCpu(inputPages, List.of(timestampType), expression);
    }

    @Test
    void testTimestampToMillisCastAtLowerBound()
    {
        // Inputs below this bound round to a millisecond that cannot be represented in Trino's
        // microseconds. The CPU's unchecked negative rounding wraps here, so check the GPU directly.
        TimestampType timestampType = createTimestampType(6);
        TimestampType resultType = createTimestampType(3);
        List<Type> inputTypes = List.of(timestampType);
        Expression expression = new Cast(field(0, timestampType), resultType);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        List<Page> result = executeWithGpu(
                List.of(new Page(writeNativeValue(timestampType, Long.MIN_VALUE + 308))),
                inputTypes,
                expression,
                gpuExpression);
        assertSameDataInOrder(result, List.of(new Page(writeNativeValue(resultType, -9_223_372_036_854_775_000L))), List.of(resultType));
    }

    @Test
    void testTimestampToMillisCastAllNullAndEmptyInputs()
    {
        TimestampType timestampType = createTimestampType(6);
        Expression expression = new Cast(field(0, timestampType), createTimestampType(3));
        for (List<Long> values : List.of(Arrays.<Long>asList(null, null), List.<Long>of())) {
            assertGpuMatchesCpu(List.of(new Page(timestampCastInputBlock(values))), List.of(timestampType), expression);
        }
    }

    @Test
    void testTimestampToMillisCastLazyEvaluation()
    {
        TimestampType timestampType = createTimestampType(6);
        TimestampType resultType = createTimestampType(3);
        List<Type> inputTypes = List.of(timestampType);
        List<Page> inputPages = List.of(new Page(timestampCastInputBlock(Arrays.asList(Long.MAX_VALUE, 500L, null))));
        Expression condition = comparison(ComparisonOperator.LESS_THAN, field(0, timestampType), new Constant(timestampType, 1000L));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(condition, new Cast(field(0, timestampType), resultType))),
                new Constant(resultType, 0L));
        assertGpuMatchesCpu(inputPages, inputTypes, expression);

        // A constant remains non-null when inputs are masked, so this branch must be filtered.
        Expression constantExpression = new Case(
                ImmutableList.of(new WhenClause(new IsNull(field(0, timestampType)), new Cast(new Constant(timestampType, Long.MAX_VALUE), resultType))),
                new Constant(resultType, 0L));
        assertGpuMatchesCpu(
                List.of(new Page(timestampCastInputBlock(List.of(0L, 500L)))),
                inputTypes,
                constantExpression);
    }

    private static Block dateBlock(List<Long> epochDays)
    {
        BlockBuilder builder = DATE.createBlockBuilder(null, epochDays.size());
        for (Long days : epochDays) {
            if (days == null) {
                builder.appendNull();
            }
            else {
                DATE.writeLong(builder, days);
            }
        }
        return builder.build();
    }

    private static Block timestampCastInputBlock(List<Long> epochMicros)
    {
        TimestampType timestampType = createTimestampType(6);
        BlockBuilder builder = timestampType.createBlockBuilder(null, epochMicros.size());
        for (Long micros : epochMicros) {
            if (micros == null) {
                builder.appendNull();
            }
            else {
                timestampType.writeLong(builder, micros);
            }
        }
        return builder.build();
    }
}
