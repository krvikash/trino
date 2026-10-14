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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuDateAdd;
import io.trino.operator.gpu.expression.GpuDateTrunc.Field;
import io.trino.operator.gpu.expression.GpuFromUnixTime;
import io.trino.operator.gpu.expression.GpuUpper;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.scalar.StringFunctions;
import io.trino.operator.scalar.timestamp.FormatDateTime;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.testing.TestingSession;
import io.trino.type.CharVarcharCoercion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TIMESTAMP_WITH_TIME_ZONE_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.TEST_STRINGS;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createFloatingPointBlock;
import static io.trino.operator.gpu.GpuTestUtils.createInRangeDateTimeBlock;
import static io.trino.operator.gpu.GpuTestUtils.createRepeatingValuesBlock;
import static io.trino.operator.gpu.GpuTestUtils.createVarcharPages;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.getOnlyColumn;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.GpuTestUtils.varcharsBlock;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.operator.gpu.expression.GpuLower.UNSUPPORTED_CODE_POINTS;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.ir.TestingIr.nullIf;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static io.trino.type.CodePointsType.CODE_POINTS;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuExpressions
{
    /// `from_unixtime` returns the session time zone, and the GPU representation holds only UTC, so
    /// the CPU side of its comparison has to run in a UTC session too.
    private static final FullConnectorSession UTC_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().setTimeZoneKey(UTC_KEY).build(),
            ConnectorIdentity.ofUser("test"));

    /// Search strings for `replace`, chosen to occur in `TEST_STRINGS` and in `REPLACE_EDGE_CASE_STRINGS`:
    /// single and multi character, ASCII and multi-byte, plus an empty and a never-matching one.
    private static final List<String> REPLACE_SEARCH_STRINGS = ImmutableList.of(
            "", "a", "b", "z", "ab", "aa", "papa", "abcdefghij", "%", "test", "ą", "łąką", "ワ", "а", "no such substring");

    /// Values covering `replace` edge cases the generated input may not hit: no match, a whole-string
    /// match, adjacent and overlapping candidates, matches at the very start and end, and an empty value.
    private static final List<String> REPLACE_EDGE_CASE_STRINGS = ImmutableList.of(
            "",
            "a",
            "b",
            "aa",
            "aaa",
            "aaaa",
            "aaaaa",
            "ab",
            "abab",
            "ababab",
            "abcdefghij",
            "xa",
            "ax",
            "xax",
            "banana",
            "papapapaya",
            "%test%",
            "test at start",
            "ends with test",
            "no match at all",
            "Łania szła piękną łąką pod Warszawą",
            "ワルシャワ近郊の美しい草原を雌鹿が歩いていた。",
            "Слава Україні");

    private static final List<String> STARTS_WITH_EDGE_CASE_STRINGS = List.of(
            "",
            "a",
            "abc",
            "abd",
            "xabc",
            "한국어",
            "あいう",
            "中文",
            "שלום",
            "مرحبا",
            "e\u0301clair",
            "नमस्ते",
            "𐐀abc");

    private static final List<Type> DATE_TIME_TYPES = ImmutableList.<Type>builder()
            .add(DATE)
            .addAll(IntStream.rangeClosed(0, TimestampType.MAX_PRECISION).mapToObj(TimestampType::createTimestampType).toList())
            .addAll(IntStream.rangeClosed(0, TimestampWithTimeZoneType.MAX_PRECISION).mapToObj(TimestampWithTimeZoneType::createTimestampWithTimeZoneType).toList())
            .addAll(IntStream.rangeClosed(0, TimeType.MAX_PRECISION).mapToObj(TimeType::createTimeType).toList())
            .build();

    private static final int CODE_POINTS_PER_PAGE = 65536;

    /// Inputs for the trim tests. Every code point Trino treats as whitespace appears at a boundary,
    /// alongside code points a coarser rule would wrongly strip: those just below the space character,
    /// and the Unicode spaces that are not whitespace (NO-BREAK SPACE, FIGURE SPACE).
    private static final List<String> TRIM_TEST_STRINGS = ImmutableList.of(
            "",
            " ",
            "     ",
            "\t\n\u000B\f\r",
            "\u001C\u001D\u001E\u001F",
            "abc",
            "  abc",
            "abc  ",
            "  abc  ",
            "\tabc\n",
            "\ra b c\u000B",
            "  a b c  ",
            "\u3000abc\u3000",
            " \u3000 abc \u3000 ",
            "\u2000\u2001\u2002\u2003abc\u2004\u2005\u2006",
            "\u2008\u2009\u200Aabc\u2028\u2029",
            "\u1680abc\u205F",
            "\u001Cabc\u001F",
            "\u00A0abc\u00A0",
            "\u2007abc\u2007",
            " \u00A0abc\u00A0 ",
            " \u2007 ",
            "\u0001abc\u0002",
            "\u0001\u0002\u0003",
            " \u0001abc\u0002 ",
            " \u0141ania sz\u0142a pi\u0119kn\u0105 \u0142\u0105k\u0105 ",
            "\u3000\u30EF\u30EB\u30B7\u30E3\u30EF\u8FD1\u90CA\u306E\u7F8E\u3057\u3044\u8349\u539F\u3092\u96CC\u9E7F\u304C\u6B69\u3044\u3066\u3044\u305F\u3002\u3000",
            "\t\u0421\u043B\u0430\u0432\u0430 \u0423\u043A\u0440\u0430\u0457\u043D\u0456\u3000",
            "\u0105\u0119\u0107abc\u0105\u0119\u0107",
            "\u3002\u3002abc\u3002\u3002",
            "ab  c",
            "xyzabcxyz",
            "xxx",
            "xyx yxy xyx",
            "abcxyz",
            "xyzabc",
            "  xyz abc xyz  ");

    /// Character sets for the two-argument trim tests, written as they would be in SQL. The empty set is
    /// the `codePoints.length == 0` compile path, and "c" reproduces https://github.com/trinodb/trino/issues/31004.
    private static final List<String> TRIM_CODE_POINT_SETS = ImmutableList.of(
            "",
            " ",
            "c",
            "xyz",
            "x",
            " x",
            "\u0105\u0119\u3002",
            " \u3000\u00A0",
            "abcxyz ");

    private static final int TRIM_CHAR_LENGTH = 5;

    /// Inputs for the `char(TRIM_CHAR_LENGTH)` trim tests. Each is exactly `TRIM_CHAR_LENGTH` code points,
    /// so its last character really is the value's last character rather than being followed by padding.
    /// That is what makes a trim set containing that character interesting: removing it can expose spaces
    /// that the CPU then removes as well, which https://github.com/trinodb/trino/issues/31004 reports as
    /// wrong. Until upstream changes, these pin what the CPU does today so the GPU is not the one to
    /// diverge.
    private static final List<String> TRIM_EXACT_LENGTH_CHAR_STRINGS = exactLengthStrings(
            "ab  c",
            "abc  ",
            "a b c",
            "  abc",
            "ccabc",
            "abccc",
            "cc  c",
            "c    ",
            "xyzab",
            "     ",
            "\tab c",
            "\u0105\u0119\u0107ab",
            "\u3002\u3002abc");

    private static List<String> exactLengthStrings(String... strings)
    {
        for (String string : strings) {
            int codePointCount = string.codePointCount(0, string.length());
            verify(codePointCount == TRIM_CHAR_LENGTH, "Expected %s code points, [%s] has %s", TRIM_CHAR_LENGTH, string, codePointCount);
        }
        return ImmutableList.copyOf(strings);
    }

    /// A column type paired with the strings to fill it with.
    private record TrimInput(Type type, List<String> strings) {}

    private static final List<TrimInput> TRIM_INPUTS = ImmutableList.of(
            new TrimInput(VARCHAR, TRIM_TEST_STRINGS),
            // Wide enough that every value keeps implicit trailing padding, so trailing space handling runs
            new TrimInput(createCharType(64), TRIM_TEST_STRINGS),
            // Tight, so each value's last character is genuinely last
            new TrimInput(createCharType(TRIM_CHAR_LENGTH), TRIM_EXACT_LENGTH_CHAR_STRINGS));

    /// Every code point that can appear in a varchar, computed once per test class instance. The
    /// surrogate block is excluded because unpaired surrogates are not valid UTF-8.
    private final int[] allValidCodePoints = allValidCodePoints();

    private static int[] allValidCodePoints()
    {
        int[] codePoints = new int[Character.MAX_CODE_POINT + 1 - (Character.MAX_SURROGATE - Character.MIN_SURROGATE + 1)];
        int index = 0;
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (codePoint < Character.MIN_SURROGATE || codePoint > Character.MAX_SURROGATE) {
                codePoints[index++] = codePoint;
            }
        }
        verify(index == codePoints.length, "Expected %s code points, got %s", codePoints.length, index);
        return codePoints;
    }

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
    @EnumSource(NullsProvider.class)
    void testIsNull(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Expression expression = new IsNull(field(channelA, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNestedLet(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // Let s1 = ref0 + ref1 in (Let s2 = s1 + ref2 in (s1 + s2))
        // The same s1 binding is addressable in both the inner value and body.
        Symbol s1 = new Symbol(BIGINT, "s1");
        Symbol s2 = new Symbol(BIGINT, "s2");
        Expression inner = new Let(
                s2,
                add(s1.toSymbolReference(), field(2, BIGINT)),
                add(s1.toSymbolReference(), s2.toSymbolReference()));
        Expression expression = new Let(s1, add(field(0, BIGINT), field(1, BIGINT)), inner);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1, 2));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLetShadowsExternalSymbol(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Reference reference = field(0, BIGINT);
        // The value reads the external symbol; the body reads the binding; the sibling reads the external symbol again.
        Expression expression = add(
                new Let(Symbol.from(reference),
                        add(reference, field(1, BIGINT)),
                        add(reference, reference)),
                reference);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLetShadowsOuterBinding(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Symbol symbol = new Symbol(BIGINT, "s");
        Reference reference = symbol.toSymbolReference();
        // The inner value reads the outer binding, which is restored after the inner body.
        Expression inner = new Let(symbol, add(reference, field(1, BIGINT)), add(reference, reference));
        Expression expression = new Let(symbol, field(0, BIGINT), add(inner, reference));

        // The CPU compiler rejects duplicate nested Let names, so use an equivalent expression without bindings.
        Expression sum = add(field(0, BIGINT), field(1, BIGINT));
        Map<Symbol, Integer> layout = layoutFor(inputTypes);
        PageProcessor cpuProcessor = compileCpuExpression(add(add(sum, sum), field(0, BIGINT)), layout);
        CompiledExpression gpuExpression = compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertThat(gpuExpression.inputChannels().getInputChannels()).containsExactly(0, 1);
        assertGpuMatchesCpu(inputPages, inputTypes, expression, cpuProcessor, gpuExpression, false);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSiblingLets(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // (Let s1 = ref0 + ref1 in s1 + s1) + (Let s2 = ref0 - ref1 in s2 + s2)
        // Sibling bindings have separate scopes.
        Symbol s1 = new Symbol(BIGINT, "s1");
        Symbol s2 = new Symbol(BIGINT, "s2");
        Expression left = new Let(
                s1,
                add(field(0, BIGINT), field(1, BIGINT)),
                add(s1.toSymbolReference(), s1.toSymbolReference()));
        Expression right = new Let(
                s2,
                subtract(field(0, BIGINT), field(1, BIGINT)),
                add(s2.toSymbolReference(), s2.toSymbolReference()));
        Expression expression = add(left, right);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    private static Expression add(Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(left, right));
    }

    private static Expression subtract(Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(BIGINT, BIGINT)),
                ImmutableList.of(left, right));
    }

    @ParameterizedTest
    @MethodSource("nullIfTestCases")
    void testNullIf(Type type, List<Object> values, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 64;
        // Both operands cycle through the same few values, the second one advancing a value only
        // once the first has cycled through all of them. That walks every pair, so the operands are
        // equal on a quarter of the rows. The shared block generators would not do: each reseeds
        // Random with the same constant, so two columns come out identical and every row takes the
        // NULL branch, leaving the pass-through branch unexercised.
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, values, 1),
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, values, values.size())));

        // NULLIF(a, b)
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, type), field(channelB, type));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    private static Stream<Arguments> nullIfTestCases()
    {
        Map<Type, List<Object>> valuesByType = ImmutableMap.of(
                BIGINT, ImmutableList.of(-1L, 0L, 7L, 42L),
                INTEGER, ImmutableList.of(-1L, 0L, 7L, 42L),
                DATE, ImmutableList.of(-1L, 0L, 7L, 42L),
                createDecimalType(10, 2), ImmutableList.of(-1L, 0L, 7L, 42L),
                DOUBLE, ImmutableList.of(-1.5, 0.0, 7.25, 42.5),
                VARCHAR, ImmutableList.of(Slices.utf8Slice(""), Slices.utf8Slice("alpha"), Slices.utf8Slice("beta"), Slices.utf8Slice("\u0141\u0105ka")));
        return valuesByType.entrySet().stream()
                .flatMap(entry -> Stream.of(NullsProvider.values())
                        .map(nullsProvider -> arguments(entry.getKey(), entry.getValue(), nullsProvider)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNullIfWithNullSecondOperand(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // NULLIF(a, NULL): the equality is NULL on every row, so the CASE must fall through to its
        // default and yield `a` — a NULL condition is not-matched, neither a match nor a NULL result.
        // Unlike the plain IF tests, the branches differ on every non-null row here, so treating the
        // NULL condition as a match would show up as a wrongly NULLed-out result.
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, BIGINT), new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNullIfWithNonTrivialValue(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -3, 3),
                createBigintBlock(positionsCount, nullsProvider, -3, 3)));

        // NULLIF(a + b, 0) — the non-trivial first operand is bound with a Let so it is evaluated once
        Expression expression = nullIf(
                emptySymbolAllocator(),
                add(field(channelA, BIGINT), field(channelB, BIGINT)),
                new Constant(BIGINT, 0L));
        assertThat(expression).isInstanceOf(Let.class);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @MethodSource("nullIfWithIntegerZeroTestCases")
    void testNullIfWithIntegerZero(Type type, List<Object> values, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(type);
        int positionsCount = 64;
        // A quarter of the values are zero, so the NULL-producing branch is actually taken.
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(type, positionsCount, nullsProvider, values, 1)));

        // NULLIF(a, integer 0), the divide-by-zero guard in `x / NULLIF(y, 0)`: the integer zero is
        // cast to the column type inside the comparison, while the result keeps the column type.
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, type), new Constant(INTEGER, 0L), type);
        assertThat(expression.type()).isEqualTo(type);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    private static Stream<Arguments> nullIfWithIntegerZeroTestCases()
    {
        // decimal(38,4) is a long decimal, carried on cuDF's DECIMAL128 rather than the INT64 that
        // short decimals use, so it exercises a different code path than the decimal in testNullIf.
        Map<Type, List<Object>> valuesByType = ImmutableMap.of(
                BIGINT, ImmutableList.of(0L, 1L, -7L, 42L),
                createDecimalType(38, 4), ImmutableList.of(
                        Int128.ZERO,
                        Int128.valueOf(1),
                        Int128.valueOf(-70000),
                        Int128.valueOf("12345678901234567890123456789012345678")));
        return valuesByType.entrySet().stream()
                .flatMap(entry -> Stream.of(NullsProvider.values())
                        .map(nullsProvider -> arguments(entry.getKey(), entry.getValue(), nullsProvider)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNullIfWithMixedOperandTypes(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(INTEGER, BIGINT);
        int positionsCount = 64;
        // Few enough values that the operands are equal on a quarter of the rows — see testNullIf.
        List<Object> values = ImmutableList.of(-1L, 0L, 7L, 42L);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createRepeatingValuesBlock(INTEGER, positionsCount, nullsProvider, values, 1),
                createRepeatingValuesBlock(BIGINT, positionsCount, nullsProvider, values, values.size())));

        // NULLIF(a, b) over an integer and a bigint operand: the equality is performed at bigint, so a
        // Cast appears inside the condition, while the result keeps the integer type of the first operand.
        Expression expression = nullIf(emptySymbolAllocator(), field(channelA, INTEGER), field(channelB, BIGINT), BIGINT);
        assertThat(expression.type()).isEqualTo(INTEGER);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
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

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA), charVarcharCoercion);
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

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
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
        assertGpuMatchesCpu(inputPages, List.of(timestampType), expression, Set.of(0));
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
            assertGpuMatchesCpu(List.of(new Page(timestampCastInputBlock(values))), List.of(timestampType), expression, Set.of(0));
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
        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0));

        // A constant remains non-null when inputs are masked, so this branch must be filtered.
        Expression constantExpression = new Case(
                ImmutableList.of(new WhenClause(new IsNull(field(0, timestampType)), new Cast(new Constant(timestampType, Long.MAX_VALUE), resultType))),
                new Constant(resultType, 0L));
        assertGpuMatchesCpu(
                List.of(new Page(timestampCastInputBlock(List.of(0L, 500L)))),
                inputTypes,
                constantExpression,
                Set.of(0));
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
        int channelA = 0;
        List<Type> inputTypes = List.of(timestampType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(timestampType, positionsCount, nullsProvider)));

        assertGpuMatchesCpu(inputPages, inputTypes, dateTruncExpression(unit, timestampType), Set.of(channelA));
    }

    private static Expression dateTruncExpression(String unit, Type timestampType)
    {
        Type unitType = createVarcharType(unit.length());
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_trunc", fromTypes(unitType, timestampType)),
                ImmutableList.of(new Constant(unitType, Slices.utf8Slice(unit)), field(0, timestampType)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testFormatDateTime(NullsProvider nullsProvider)
    {
        // Without a year field every value is formattable, so the whole range is covered
        for (String pattern : List.of("MM-dd HH:mm:ss.SSS", "'at' hh 'o''clock', 100% [ss]")) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                Type timestampType = createTimestampType(precision);
                try {
                    List<Page> inputPages = List.of(new Page(64, createBlock(timestampType, 64, nullsProvider)));
                    assertGpuMatchesCpu(inputPages, List.of(timestampType), formatDateTimeExpression(timestampType, pattern), Set.of(0));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("pattern: " + pattern));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    @Test
    void testFormatDateTimeYear()
    {
        Random random = new Random(7);
        for (String pattern : List.of("yyyy-MM-dd HH:mm:ss.SSS", "yy/MM/dd", "DDD", "''yyyy''")) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                TimestampType timestampType = createTimestampType(precision);
                // Precisions above 6 are nanoseconds on the GPU, which span only years 1677 to 2262
                int firstYear = timestampType.isShort() ? 0 : 1678;
                int lastYear = timestampType.isShort() ? 9999 : 2261;
                long minEpochMicros = LocalDateTime.of(firstYear, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
                long maxEpochMicros = LocalDateTime.of(lastYear + 1, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
                List<Long> epochMicros = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    long micros = random.nextLong(minEpochMicros, maxEpochMicros);
                    epochMicros.add(i % 5 == 0 ? null : Timestamps.round(micros, max(0, 6 - precision)));
                }
                try {
                    assertFormatDateTimeMatchesCpu(timestampType, pattern, epochMicros);
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("pattern: " + pattern));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    @Test
    void testFormatDateTimeYearOutOfRange()
    {
        String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
        long firstEpochMicros = LocalDateTime.of(0, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastEpochMicros = LocalDateTime.of(10000, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
        for (TimestampType timestampType : List.of(createTimestampType(3), createTimestampType(6))) {
            assertFormatDateTimeMatchesCpu(timestampType, pattern, List.of(firstEpochMicros, lastEpochMicros));
            assertFormatDateTimeGpuThrows(timestampType, pattern, firstEpochMicros - 1_000);
            assertFormatDateTimeGpuThrows(timestampType, pattern, lastEpochMicros + 1_000);
            assertFormatDateTimeGpuThrows(timestampType, "yy", firstEpochMicros - 1_000);
            assertFormatDateTimeMatchesCpu(timestampType, "DDD", List.of(firstEpochMicros - 1_000, lastEpochMicros + 1_000));
        }
        // Rounding to milliseconds carries into year 10000
        assertFormatDateTimeGpuThrows(createTimestampType(6), pattern, lastEpochMicros + 500);
        // Fields that do not depend on the year are formattable in any year
        assertFormatDateTimeMatchesCpu(createTimestampType(6), "MM-dd", List.of(firstEpochMicros - 1_000, lastEpochMicros + 1_000));
        // Day of year is computed from cuDF's calendar year
        long firstCalendarEpochMicros = LocalDateTime.of(-32767, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastCalendarEpochMicros = LocalDateTime.of(32768, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "DDD", List.of(firstCalendarEpochMicros, lastCalendarEpochMicros));
        assertFormatDateTimeGpuThrows(createTimestampType(3), "DDD", firstCalendarEpochMicros - 1_000, "Year out of range supported by GPU: must be in [-32767, 32767]");
        assertFormatDateTimeGpuThrows(createTimestampType(3), "DDD", lastCalendarEpochMicros + 1_000, "Year out of range supported by GPU: must be in [-32767, 32767]");
        // ISO week is computed from cuDF's calendar year and its neighbors, so the last calendar year overflows
        long firstWeekEpochMicros = LocalDateTime.of(-32767, 1, 7, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastWeekEpochMicros = LocalDateTime.of(32767, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "ww", List.of(firstCalendarEpochMicros, firstWeekEpochMicros, lastWeekEpochMicros));
        assertFormatDateTimeGpuThrows(createTimestampType(3), "ww", firstCalendarEpochMicros - 1_000);
        assertFormatDateTimeGpuThrows(createTimestampType(3), "ww", lastWeekEpochMicros + 1_000);
        // The year of the ISO week is -1 in the first days of year 0
        long firstWeekYearEpochMicros = LocalDateTime.of(1, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "xxxx-ww", List.of(firstWeekYearEpochMicros, lastEpochMicros));
        assertFormatDateTimeGpuThrows(createTimestampType(3), "xxxx", firstWeekYearEpochMicros - 1_000);
        assertFormatDateTimeGpuThrows(createTimestampType(3), "xxxx", lastEpochMicros + 1_000);
    }

    private void assertFormatDateTimeMatchesCpu(TimestampType timestampType, String pattern, List<Long> epochMicros)
    {
        Page input = new Page(timestampBlock(timestampType, epochMicros));
        assertGpuMatchesCpu(List.of(input), List.of(timestampType), formatDateTimeExpression(timestampType, pattern), Set.of(0));
    }

    private void assertFormatDateTimeGpuThrows(TimestampType timestampType, String pattern, long epochMicros)
    {
        assertFormatDateTimeGpuThrows(timestampType, pattern, epochMicros, "format_datetime() year outside of range supported by GPU");
    }

    private void assertFormatDateTimeGpuThrows(TimestampType timestampType, String pattern, long epochMicros, String expectedMessage)
    {
        Expression expression = formatDateTimeExpression(timestampType, pattern);
        List<Page> inputPages = List.of(new Page(timestampBlock(timestampType, List.of(epochMicros))));
        List<Type> inputTypes = List.of(timestampType);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage(expectedMessage);
    }

    private static Block timestampBlock(TimestampType timestampType, List<Long> epochMicros)
    {
        BlockBuilder builder = timestampType.createBlockBuilder(null, epochMicros.size());
        for (Long micros : epochMicros) {
            if (micros == null) {
                builder.appendNull();
            }
            else if (timestampType.isShort()) {
                timestampType.writeLong(builder, micros);
            }
            else {
                timestampType.writeObject(builder, new LongTimestamp(micros, 0));
            }
        }
        return builder.build();
    }

    @Test
    void testFormatDateTimeRoundsToMillis()
    {
        // Values near millisecond, second and year boundaries, including negative ones, where rounding carries over
        long[] epochNanos = {
                0,
                499_999,
                500_000,
                -500_000,
                -500_001,
                999_999_999,
                -1,
                1_577_836_799_999_500_000L,
                -1_000_000_000_000_000_000L,
        };
        for (int precision = 4; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            TimestampType timestampType = createTimestampType(precision);
            BlockBuilder builder = timestampType.createBlockBuilder(null, epochNanos.length);
            for (long nanos : epochNanos) {
                long aligned = Timestamps.round(nanos, 9 - precision);
                if (timestampType.isShort()) {
                    timestampType.writeLong(builder, floorDiv(aligned, NANOSECONDS_PER_MICROSECOND));
                }
                else {
                    timestampType.writeObject(builder, new LongTimestamp(
                            floorDiv(aligned, NANOSECONDS_PER_MICROSECOND),
                            floorMod(aligned, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND));
                }
            }
            Page input = new Page(builder.build());
            assertGpuMatchesCpu(List.of(input), List.of(timestampType), formatDateTimeExpression(timestampType, "yyyy-MM-dd HH:mm:ss.SSS"), Set.of(0));
        }
    }

    @Test
    void testFormatDateTimeFixedValues()
    {
        TimestampType timestampType = createTimestampType(3);

        // hh (12-hour clock) around midnight, noon, just after noon, and before 1970
        long midnight = LocalDateTime.of(1969, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long noon = LocalDateTime.of(1969, 12, 31, 12, 0).toEpochSecond(UTC) * 1_000_000;
        long justAfterNoon = LocalDateTime.of(1969, 12, 31, 13, 0).toEpochSecond(UTC) * 1_000_000;
        long lastMinuteOfDay = LocalDateTime.of(1970, 1, 1, 23, 59).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(timestampType, "hh", List.of(midnight, noon, justAfterNoon, lastMinuteOfDay));

        // DDD (day of year) across leap-year boundaries in different centuries
        long dayBeforeLeapDay1900 = LocalDateTime.of(1900, 2, 28, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long dayAfterLeapDay1900 = LocalDateTime.of(1900, 3, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long leapDay2000 = LocalDateTime.of(2000, 2, 29, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long dayAfterLeapDay2000 = LocalDateTime.of(2000, 3, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long leapDayYear0 = LocalDateTime.of(0, 2, 29, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long dayAfterLeapDayYear0 = LocalDateTime.of(0, 3, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastDay1900 = LocalDateTime.of(1900, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastDay2000 = LocalDateTime.of(2000, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastDayYear0 = LocalDateTime.of(0, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(
                timestampType,
                "DDD",
                List.of(dayBeforeLeapDay1900, dayAfterLeapDay1900, leapDay2000, dayAfterLeapDay2000, leapDayYear0, dayAfterLeapDayYear0, lastDay1900, lastDay2000, lastDayYear0));

        // ww (ISO week) and xxxx (its year) where the year of the ISO week differs from the year
        List<Long> weekBoundaries = List.of(
                LocalDateTime.of(2020, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2021, 1, 3, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2021, 1, 4, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2018, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2024, 12, 29, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2024, 12, 30, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(1969, 12, 29, 0, 0).toEpochSecond(UTC) * 1_000_000);
        assertFormatDateTimeMatchesCpu(timestampType, "xxxx-ww", weekBoundaries);
        // In year 0, which starts in the last ISO week of year -1
        long firstDayYear0 = LocalDateTime.of(0, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(timestampType, "ww", List.of(firstDayYear0, lastDayYear0));

        // Zero-padding at width boundaries, with both yyyy and yy
        List<Long> paddingYears = List.of(
                LocalDateTime.of(5, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(99, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(100, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(999, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(1000, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(9999, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000);
        assertFormatDateTimeMatchesCpu(timestampType, "yyyy", paddingYears);
        assertFormatDateTimeMatchesCpu(timestampType, "yy", paddingYears);
        assertFormatDateTimeMatchesCpu(timestampType, "xxxx", paddingYears);

        long someInstant = LocalDateTime.of(2026, 9, 30, 1, 2, 3).toEpochSecond(UTC) * 1_000_000 + 456_000;
        // A common pattern with no separators between fields
        assertFormatDateTimeMatchesCpu(timestampType, "yyyyMMddHHmmssSSS", List.of(someInstant));
        // Non-ASCII literal text, including a character outside the basic multilingual plane
        assertFormatDateTimeMatchesCpu(timestampType, "yyyy年MM月dd日 😀", List.of(someInstant));
        // Quoted text, unterminated quotes, and characters that are not special in Joda-Time but may be in cuDF
        for (String pattern : List.of(
                "'%Y' yyyy",
                "ss%",
                "'100%'",
                "'%y'",
                "'%f'",
                "yyyy 'abc",
                "yyyy'",
                "'o''clock' HH",
                "\"yyyy\"",
                "'\"'",
                "yyyy\\MM",
                "'\\\\'",
                "yyyy\u0000",
                "'\t'")) {
            assertFormatDateTimeMatchesCpu(timestampType, pattern, List.of(someInstant));
        }

        // Fraction of second of every width, truncated rather than rounded, also before 1970
        long fiveMillis = 5_000;
        long millisecondBefore1970 = -1_000;
        long lastMillisecondOfSecond = 999_000;
        for (int digits = 1; digits <= 9; digits++) {
            assertFormatDateTimeMatchesCpu(timestampType, "S".repeat(digits), List.of(fiveMillis, millisecondBefore1970, lastMillisecondOfSecond, someInstant));
        }
    }

    @Test
    void testFormatDateTimeAllCharacters()
    {
        // Every character other than a quote and the ASCII letters, which Joda-Time reads as fields, is literal text
        String pattern = IntStream.range(0, 1024)
                .filter(codePoint -> codePoint != '\'' && !(codePoint >= 'A' && codePoint <= 'Z') && !(codePoint >= 'a' && codePoint <= 'z'))
                .mapToObj(Character::toString)
                .collect(joining());
        long someInstant = LocalDateTime.of(2026, 9, 30, 13, 2, 3).toEpochSecond(UTC) * 1_000_000 + 456_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), pattern, List.of(someInstant));
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "'" + pattern + "'", List.of(someInstant));
    }

    @Test
    void testFormatDateTimeRandomPatterns()
    {
        // Patterns the GPU compiles must print like the CPU, and patterns the CPU rejects must not compile for the GPU
        List<String> tokens = List.of(
                "yyyy",
                "yy",
                "MM",
                "dd",
                "DDD",
                "HH",
                "hh",
                "mm",
                "ss",
                "S",
                "SSS",
                "SSSSSS",
                "SSSSSSSSS",
                "ee",
                "ww",
                "xxxx",
                "Y",
                "x",
                "w",
                "e",
                "K",
                "k",
                "C",
                "G",
                "E",
                "a",
                "z",
                "Z",
                "T",
                "M",
                "y",
                "'",
                "''",
                "'abc'",
                "%",
                "-",
                " ",
                "年",
                "😀");
        TimestampType timestampType = createTimestampType(3);
        long someInstant = LocalDateTime.of(2026, 9, 30, 13, 2, 3).toEpochSecond(UTC) * 1_000_000 + 456_000;
        long before1970 = LocalDateTime.of(1901, 2, 3, 4, 5, 6).toEpochSecond(UTC) * 1_000_000 + 7_000;
        Random random = new Random(11);
        for (int i = 0; i < 300; i++) {
            StringBuilder pattern = new StringBuilder();
            int tokenCount = random.nextInt(1, 6);
            for (int j = 0; j < tokenCount; j++) {
                pattern.append(tokens.get(random.nextInt(tokens.size())));
            }
            Expression expression = formatDateTimeExpression(timestampType, pattern.toString());
            boolean gpuCompiles = compileExpression(expression, layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).isPresent();
            try {
                FormatDateTime.format(SESSION, someInstant, Slices.utf8Slice(pattern.toString()));
            }
            catch (TrinoException _) {
                assertThat(gpuCompiles)
                        .describedAs(pattern.toString())
                        .isFalse();
                continue;
            }
            if (gpuCompiles) {
                try {
                    assertFormatDateTimeMatchesCpu(timestampType, pattern.toString(), List.of(someInstant, before1970));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("pattern: " + pattern));
                    throw t;
                }
            }
        }
    }

    @Test
    void testFormatDateTimeDayOfWeek()
    {
        // Midnight, the last second before noon, and noon on each day of a week, before and after 1970
        List<Long> epochMicros = new ArrayList<>();
        for (LocalDateTime day : List.of(LocalDateTime.of(2026, 9, 28, 0, 0), LocalDateTime.of(1901, 2, 4, 0, 0))) {
            for (int dayOffset = 0; dayOffset < 7; dayOffset++) {
                long dayEpochMicros = day.plusDays(dayOffset).toEpochSecond(UTC) * 1_000_000;
                epochMicros.add(dayEpochMicros);
                epochMicros.add(dayEpochMicros + 12 * 3_600_000_000L - 1_000_000);
                epochMicros.add(dayEpochMicros + 12 * 3_600_000_000L);
            }
        }
        for (int precision : List.of(0, 3, 6, 9)) {
            assertFormatDateTimeMatchesCpu(createTimestampType(precision), "HH:mm:ss e ee", epochMicros);
        }
    }

    @Test
    void testFormatDateTimeUnsupportedPattern()
    {
        Type timestampType = createTimestampType(3);
        // Fields with an unsupported width, text/locale/time-zone fields, lookalikes of supported fields, patterns
        // Joda-Time rejects, a pattern that translates to an empty cuDF format, and a non-constant pattern
        for (String pattern : List.of(
                "M",
                "d",
                "yyy",
                "yyyyy",
                "DD",
                "ddd",
                "SSSSSSSSSS",
                "MMM",
                "EEE",
                "a",
                "Z",
                "z",
                "q",
                "T",
                "YYYY",
                "xx",
                "K",
                "k",
                "eee",
                "w",
                "",
                "'")) {
            assertThat(compileExpression(formatDateTimeExpression(timestampType, pattern), layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .describedAs(pattern)
                    .isEmpty();
        }
        Expression nonConstantPattern = new Call(
                FUNCTION_RESOLUTION.resolveFunction("format_datetime", fromTypes(timestampType, VARCHAR)),
                ImmutableList.of(field(0, timestampType), field(1, VARCHAR)));
        assertThat(compileExpression(nonConstantPattern, layoutFor(List.of(timestampType, VARCHAR)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private static Expression formatDateTimeExpression(Type timestampType, String pattern)
    {
        Type patternType = createVarcharType(pattern.length());
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("format_datetime", fromTypes(timestampType, patternType)),
                ImmutableList.of(field(0, timestampType), new Constant(patternType, Slices.utf8Slice(pattern))));
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
                dateTruncExpression(unit, timestampType),
                Set.of(0));
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

    @Test
    void testLower()
    {
        // Exercise every code point (in MAX_BATCH_SIZE pages) except those cuDF and Trino disagree on.
        List<String> supportedCodePoints = IntStream.rangeClosed(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT)
                .filter(codePoint -> !UNSUPPORTED_CODE_POINTS.contains(codePoint))
                .mapToObj(codePoint -> new String(Character.toChars(codePoint)))
                .collect(toImmutableList());
        List<Page> inputPages = Lists.partition(supportedCodePoints, MAX_BATCH_SIZE).stream()
                .map(codePoints -> new Page(varcharsBlock(codePoints)))
                .collect(toImmutableList());

        assertGpuMatchesCpu(inputPages, List.of(VARCHAR), createLowerExpression(VARCHAR), Set.of(0));
    }

    @Test
    void testLowerRejectedCodePoints()
    {
        Type type = VARCHAR;
        for (int codePoint : UNSUPPORTED_CODE_POINTS) {
            List<Page> inputPages = List.of(new Page(writeNativeValue(type, Slices.utf8Slice(new String(Character.toChars(codePoint))))));
            Expression expression = createLowerExpression(type);
            CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                    .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
            assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                    .describedAs("U+%04X", codePoint)
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("lower() on GPU does not support code points whose lowercase mapping differs between cuDF and Trino");
        }
    }

    @Test
    void testUnsupportedCodePointsDisagreeBetweenCudfAndTrino()
    {
        // Confirms UNSUPPORTED_CODE_POINTS is still accurate: if cuDF ever fixes one of these mappings to
        // match Trino, this test starts failing for that code point and the entry can be pruned from the list.
        for (int codePoint : UNSUPPORTED_CODE_POINTS) {
            String value = new String(Character.toChars(codePoint));
            String trinoLower = StringFunctions.lower(Slices.utf8Slice(value)).toStringUtf8();
            try (ColumnVector column = ColumnVector.fromStrings(value);
                    ColumnVector lowered = column.lower();
                    HostColumnVector host = lowered.copyToHost()) {
                assertThat(host.getJavaString(0))
                        .describedAs("U+%04X", codePoint)
                        .isNotEqualTo(trinoLower);
            }
        }
    }

    @Test
    void testLowerReconcilesAndHandlesNulls()
    {
        assertGpuMatchesCpu(
                List.of(new Page(varcharsBlock(Arrays.asList("HELLO", "Æther", "İstanbul", null, "ŁĄKA", "", "MiXeD123")))),
                List.of(VARCHAR),
                createLowerExpression(VARCHAR),
                Set.of(0));
        assertGpuMatchesCpu(
                List.of(new Page(varcharsBlock(Arrays.asList("HELLO", "Æther", "İ", null, "ŁĄKA", "", "ABC")))),
                List.of(createCharType(8)),
                createLowerExpression(createCharType(8)),
                Set.of(0));
    }

    private static Expression createLowerExpression(Type type)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("lower", fromTypes(type)),
                ImmutableList.of(field(0, type)));
    }

    @Test
    void testUpper()
    {
        // Exercise every code point (in MAX_BATCH_SIZE pages) except those cuDF and Trino disagree on. (these are covered by testUpperRejectedCodePoints
        List<String> supportedCodePoints = IntStream.rangeClosed(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT)
                .filter(codePoint -> !GpuUpper.UNSUPPORTED_CODE_POINTS.contains(codePoint))
                .mapToObj(codePoint -> new String(Character.toChars(codePoint)))
                .collect(toImmutableList());
        List<Page> inputPages = Lists.partition(supportedCodePoints, MAX_BATCH_SIZE).stream()
                .map(codePoints -> new Page(varcharsBlock(codePoints)))
                .collect(toImmutableList());

        assertGpuMatchesCpu(inputPages, List.of(VARCHAR), createUpperExpression(VARCHAR), Set.of(0));
    }

    @Test
    void testUpperMatchesCpu()
    {
        // cuDF maps ß to "SS"; the GPU must keep Trino's ß, wherever it sits and whatever surrounds it
        assertUpperMatchesCpu(
                "ß",
                "ßß",
                "straße",
                "ßabc",
                "abcß",
                "sß",
                "SSß",
                "ss",
                "SS",
                // capital sharp S is supported as is
                "ẞ",
                "ßẞ",
                // above the first unsupported code point, so the rejection check runs and passes before the swap
                "ßαβγ",
                "ß😀",
                "😀ß😀",
                "hello",
                "æther",
                "łąka",
                "MiXeD123",
                null,
                "");
        // Þ (U+00DE) is the last code point before ß, so the batch takes the plain cuDF upper()
        assertUpperMatchesCpu("abc", "Þþ", "xyzÞ", null, "");
        // é (U+00E9) is above ß, so the batch takes the ß swap path even though it has no ß
        assertUpperMatchesCpu("é", "abc", null);
        assertUpperMatchesCpu((String) null);
        assertUpperMatchesCpu("");
        assertGpuMatchesCpu(
                List.of(new Page(varcharsBlock(Arrays.asList("ß", "sß", "ßß", null, "", "æther", "łąka", "abc")))),
                List.of(createCharType(8)),
                createUpperExpression(createCharType(8)),
                Set.of(0));
    }

    @Test
    void testUpperRejectedCodePoints()
    {
        for (int codePoint : GpuUpper.UNSUPPORTED_CODE_POINTS) {
            String value = Character.toString(codePoint);
            assertUpperRejected(value);
            assertUpperRejected("ab" + value + "cd");
            assertUpperRejected("straße", null, "", value + "ß");
        }

        // A real ß placeholder (ƛ, U+019B) must be rejected, not swapped to ß, whether or not a ß is in the same row or batch
        assertUpperRejected("ßƛ");
        assertUpperRejected("ƛß");
        assertUpperRejected("straße", "ƛ");
        assertUpperRejected("ƛ", null, "ß");
    }

    @Test
    void testUnsupportedUpperCodePointsDisagreeBetweenCudfAndTrino()
    {
        // Confirms GpuUpper.UNSUPPORTED_CODE_POINTS is still accurate: if cuDF ever fixes one of these mappings to
        // match Trino, this test starts failing for that code point and the entry can be pruned from the list.
        for (int codePoint : GpuUpper.UNSUPPORTED_CODE_POINTS) {
            String value = Character.toString(codePoint);
            String trinoUpper = StringFunctions.upper(Slices.utf8Slice(value)).toStringUtf8();
            assertThat(cudfUpper(value))
                    .describedAs("U+%04X", codePoint)
                    .isNotEqualTo(trinoUpper);
        }

        // cuDF uses Unicode's full case mapping and expands these to several code points, while Trino maps each
        // code point on its own
        Map<Integer, String> cudfExpansions = ImmutableMap.<Integer, String>builder()
                .put(0x0149, "ʼN") // ŉ, Trino leaves it unchanged
                .put(0x01F0, "J̌") // ǰ
                .put(0x0390, "Ϊ́") // ΐ, expands to three code points
                .put(0x0587, "ԵՒ") // Armenian ligature և
                .put(0x1E9A, "Aʾ") // ẚ
                .put(0x1F80, "ἈΙ") // ᾀ, Trino maps it to the single code point ᾈ (U+1F88)
                .put(0x1FB3, "ΑΙ") // ᾳ, Trino maps it to the single code point ᾼ (U+1FBC)
                .put(0x1FB7, "Α͂Ι") // ᾷ, expands to three code points
                .put(0xFB00, "FF") // ﬀ
                .put(0xFB03, "FFI") // ﬃ
                .put(0xFB17, "ՄԽ") // Armenian ligature ﬗ
                .buildOrThrow();
        cudfExpansions.forEach((codePoint, expansion) -> {
            assertThat(GpuUpper.UNSUPPORTED_CODE_POINTS).contains(codePoint);
            assertThat(cudfUpper(Character.toString(codePoint)))
                    .describedAs("U+%04X", codePoint)
                    .isEqualTo(expansion);
        });

        // The ß placeholder (ƛ, U+019B) is only safe because cuDF leaves it unchanged and it is unsupported
        assertThat(cudfUpper("ƛ")).isEqualTo("ƛ");
        assertThat(GpuUpper.UNSUPPORTED_CODE_POINTS).contains(0x019B);
    }

    private static Expression createUpperExpression(Type type)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("upper", fromTypes(type)),
                ImmutableList.of(field(0, type)));
    }

    private void assertUpperMatchesCpu(String... values)
    {
        assertGpuMatchesCpu(List.of(new Page(varcharsBlock(Arrays.asList(values)))), List.of(VARCHAR), createUpperExpression(VARCHAR), Set.of(0));
    }

    private void assertUpperRejected(String... values)
    {
        List<Page> inputPages = List.of(new Page(varcharsBlock(Arrays.asList(values))));
        Expression expression = createUpperExpression(VARCHAR);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(VARCHAR)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(VARCHAR), expression, gpuExpression))
                .describedAs("%s", Arrays.asList(values))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("upper() on GPU does not support code points whose uppercase mapping differs between cuDF and Trino");
    }

    private static String cudfUpper(String value)
    {
        try (ColumnVector column = ColumnVector.fromStrings(value);
                ColumnVector upper = column.upper();
                HostColumnVector host = upper.copyToHost()) {
            return host.getJavaString(0);
        }
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

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testToUnixTime(NullsProvider nullsProvider)
    {
        for (Type type : TESTED_GPU_TIMESTAMP_WITH_TIME_ZONE_TYPES) {
            int channel = 0;
            List<Type> inputTypes = List.of(type);
            List<Page> inputPages = List.of(new Page(createBlock(type, 64, nullsProvider)));
            assertGpuMatchesCpu(inputPages, inputTypes, toUnixTime(field(channel, type)), Set.of(channel));
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

            assertGpuMatchesCpu(List.of(new Page(builder.build())), List.of(type), toUnixTime(field(0, type)), Set.of(0));
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
        Map<Symbol, Integer> layout = layoutFor(List.of(DOUBLE));
        PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
        CompiledExpression gpuExpression = compileExpression(expression, layout, SQL_STANDARD, UTC_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertGpuMatchesCpu(inputPages, List.of(DOUBLE), expression, cpuProcessor, gpuExpression, false, UTC_CONNECTOR_SESSION);
    }

    private static Expression fromUnixTime(Expression argument)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("from_unixtime", fromTypes(argument.type())),
                ImmutableList.of(argument));
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

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testStringLength(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = createVarcharPages(VARCHAR, List.of(64), nullsProvider);

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("length", fromTypes(VARCHAR)),
                ImmutableList.of(field(varcharChannel, VARCHAR)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSubstring(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        int startChannel = 1;
        int positionsCount = 64;
        List<Type> inputTypes = List.of(VARCHAR, BIGINT);

        List<Page> inputPages = new ArrayList<>();
        inputPages.add(new Page(
                positionsCount,
                createBlock(VARCHAR, positionsCount, nullsProvider),
                createBigintBlock(positionsCount, nullsProvider, -20, 20)));
        // Edge cases that random data may not cover: start=0 (empty), negative start beyond string length, start past end
        for (long start : List.of(0L, 1L, -1L, -100L, 100L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE)) {
            inputPages.add(new Page(
                    writeNativeValue(VARCHAR, Slices.utf8Slice("hello")),
                    writeNativeValue(BIGINT, start)));
        }

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("substring", fromTypes(VARCHAR, BIGINT)),
                ImmutableList.of(field(varcharChannel, VARCHAR), field(startChannel, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel, startChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSubstringWithLength(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        int startChannel = 1;
        int lengthChannel = 2;
        int positionsCount = 64;
        List<Type> inputTypes = List.of(VARCHAR, BIGINT, BIGINT);

        List<Page> inputPages = new ArrayList<>();
        inputPages.add(new Page(
                positionsCount,
                createBlock(VARCHAR, positionsCount, nullsProvider),
                createBigintBlock(positionsCount, nullsProvider, -20, 20),
                createBigintBlock(positionsCount, nullsProvider, -5, 20)));
        // Edge cases that random data may not cover: start=0, negative start, length<=0, start/length past end
        for (long start : List.of(0L, 1L, -1L, -100L, 100L)) {
            for (long length : List.of(-1L, 0L, 1L, 100L)) {
                inputPages.add(new Page(
                        writeNativeValue(VARCHAR, Slices.utf8Slice("hello")),
                        writeNativeValue(BIGINT, start),
                        writeNativeValue(BIGINT, length)));
            }
        }

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("substring", fromTypes(VARCHAR, BIGINT, BIGINT)),
                ImmutableList.of(field(varcharChannel, VARCHAR), field(startChannel, BIGINT), field(lengthChannel, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel, startChannel, lengthChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testReplace(NullsProvider nullsProvider)
    {
        for (String search : REPLACE_SEARCH_STRINGS) {
            testReplace(nullsProvider, search, Optional.empty());
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testReplaceWithReplacement(NullsProvider nullsProvider)
    {
        for (String search : REPLACE_SEARCH_STRINGS) {
            for (String replacement : List.of("", "X", "ab", "aa", "łąka", "ワ")) {
                testReplace(nullsProvider, search, Optional.of(replacement));
            }
        }
    }

    @Test
    void testReplaceWithNonConstantArgument()
    {
        // cuDF's literal string replace takes a scalar target and replacement, so both must be constants
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);

        Expression nonConstantSearch = new Call(
                FUNCTION_RESOLUTION.resolveFunction("replace", fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR)));
        assertThat(compileExpression(nonConstantSearch, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();

        Expression nonConstantReplacement = new Call(
                FUNCTION_RESOLUTION.resolveFunction("replace", fromTypes(VARCHAR, VARCHAR, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), new Constant(VARCHAR, Slices.utf8Slice("a")), field(1, VARCHAR)));
        assertThat(compileExpression(nonConstantReplacement, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private void testReplace(NullsProvider nullsProvider, String search, Optional<String> replacement)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = ImmutableList.<Page>builder()
                .addAll(createVarcharPages(VARCHAR, List.of(64), nullsProvider))
                // A null-free page of hand-picked values guarantees the search string really occurs,
                // whatever nulls the provider injects into the generated page
                .add(varcharPage(REPLACE_EDGE_CASE_STRINGS))
                .build();

        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        arguments.add(field(varcharChannel, VARCHAR));
        arguments.add(new Constant(VARCHAR, Slices.utf8Slice(search)));
        replacement.ifPresent(value -> arguments.add(new Constant(VARCHAR, Slices.utf8Slice(value))));
        List<Type> argumentTypes = Collections.nCopies(replacement.isPresent() ? 3 : 2, VARCHAR);

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("replace", fromTypes(argumentTypes)),
                arguments.build());

        if (search.isEmpty()) {
            // Trino wraps every code point in the replacement rather than replacing a substring,
            // and cuDF rejects an empty target outright
            assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
            return;
        }
        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testStartsWith(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = ImmutableList.<Page>builder()
                .addAll(createVarcharPages(VARCHAR, List.of(2 * TEST_STRINGS.size()), nullsProvider))
                .add(varcharPage(STARTS_WITH_EDGE_CASE_STRINGS))
                .build();

        for (String prefix : REPLACE_SEARCH_STRINGS) {
            Expression expression = new Call(
                    FUNCTION_RESOLUTION.resolveFunction("starts_with", fromTypes(VARCHAR, VARCHAR)),
                    ImmutableList.of(field(varcharChannel, VARCHAR), new Constant(VARCHAR, Slices.utf8Slice(prefix))));
            assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
        }
    }

    @Test
    void testStartsWithNonConstantPrefix()
    {
        // cuDF startsWith takes a scalar prefix, so it must be a constant
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("starts_with", fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR)));
        assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private static Page varcharPage(List<String> values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.size(), values.size() * 16);
        for (String value : values) {
            builder.writeEntry(Slices.utf8Slice(value));
        }
        return new Page(values.size(), builder.build());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testTrimWhitespace(NullsProvider nullsProvider)
    {
        for (TrimInput input : TRIM_INPUTS) {
            Type type = input.type();
            for (String functionName : List.of("trim", "ltrim", "rtrim")) {
                List<Type> inputTypes = List.of(type);
                Expression expression = new Call(
                        FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(type)),
                        ImmutableList.of(field(0, type)));

                assertGpuMatchesCpu(createTrimInputPages(input, nullsProvider), inputTypes, expression, Set.of(0));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testTrimCodePoints(NullsProvider nullsProvider)
    {
        for (TrimInput input : TRIM_INPUTS) {
            Type type = input.type();
            for (String functionName : List.of("trim", "ltrim", "rtrim")) {
                for (String codePointsToTrim : TRIM_CODE_POINT_SETS) {
                    List<Type> inputTypes = List.of(type);
                    Expression expression = new Call(
                            FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(type, CODE_POINTS)),
                            ImmutableList.of(field(0, type), new Constant(CODE_POINTS, codePointsToTrim.codePoints().toArray())));

                    assertGpuMatchesCpu(createTrimInputPages(input, nullsProvider), inputTypes, expression, Set.of(0));
                }
            }
        }
    }

    @Test
    void testTrimNonConstantCodePointsDoesNotCompile()
    {
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);
        // The characters to trim are only known per row, so the whole call falls back to the CPU
        Expression nonConstantCodePoints = new Call(
                FUNCTION_RESOLUTION.getCoercion(VARCHAR, CODE_POINTS),
                ImmutableList.of(field(1, VARCHAR)));
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("trim", fromTypes(VARCHAR, CODE_POINTS)),
                ImmutableList.of(field(0, VARCHAR), nonConstantCodePoints));

        assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
    }

    @Test
    void testTrimNullCodePointsDoesNotCompile()
    {
        List<Type> inputTypes = List.of(VARCHAR);
        // A null set of characters makes the call return null; that is left to the CPU
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("trim", fromTypes(VARCHAR, CODE_POINTS)),
                ImmutableList.of(field(0, VARCHAR), new Constant(CODE_POINTS, null)));

        assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
    }

    /// Trino's and cuDF's whitespace definitions differ, and choosing the wrong one is silently wrong
    /// rather than an error, so the agreement is checked for every code point instead of a sample.
    @Test
    void testTrimWhitespaceMatchesCpuForAllCodePoints()
    {
        for (CodePointShape shape : CodePointShape.values()) {
            List<Page> inputPages = createAllCodePointPages(shape);
            for (String functionName : List.of("trim", "ltrim", "rtrim")) {
                assertTrimMatchesCpuForAllCodePoints(functionName, shape, inputPages);
            }
        }
    }

    private void assertTrimMatchesCpuForAllCodePoints(String functionName, CodePointShape shape, List<Page> inputPages)
    {
        List<Type> inputTypes = List.of(VARCHAR);
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction(functionName, fromTypes(VARCHAR)),
                ImmutableList.of(field(0, VARCHAR)));
        Map<Symbol, Integer> layout = layoutFor(inputTypes);
        CompiledExpression gpuExpression = compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));

        List<Page> cpuResults = executeWithCpu(compileCpuExpression(expression, layout), inputPages);
        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, expression, gpuExpression);

        List<String> divergences = new ArrayList<>();
        Iterator<Slice> cpuValues = getOnlyColumn(cpuResults, VARCHAR);
        Iterator<Slice> gpuValues = getOnlyColumn(gpuResults, VARCHAR);
        for (int codePoint : allValidCodePoints) {
            Slice cpuValue = cpuValues.next();
            Slice gpuValue = gpuValues.next();
            if (!cpuValue.equals(gpuValue) && divergences.size() < 20) {
                divergences.add("U+%04X: input %s, CPU %s, GPU %s".formatted(
                        codePoint,
                        describeCodePoints(shape.buildInput(codePoint)),
                        describeCodePoints(cpuValue.toStringUtf8()),
                        describeCodePoints(gpuValue.toStringUtf8())));
            }
        }
        assertThat(cpuValues.hasNext()).isFalse();
        assertThat(gpuValues.hasNext()).isFalse();
        assertThat(divergences)
                .describedAs("%s(%s) differs between CPU and GPU (at most 20 shown)", functionName, shape)
                .isEmpty();
    }

    private List<Page> createAllCodePointPages(CodePointShape shape)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (int start = 0; start < allValidCodePoints.length; start += CODE_POINTS_PER_PAGE) {
            int end = min(start + CODE_POINTS_PER_PAGE, allValidCodePoints.length);
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, end - start, (end - start) * 12);
            for (int index = start; index < end; index++) {
                builder.writeEntry(Slices.utf8Slice(shape.buildInput(allValidCodePoints[index])));
            }
            pages.add(new Page(end - start, builder.build()));
        }
        return pages.build();
    }

    private static String describeCodePoints(String value)
    {
        return value.codePoints()
                .mapToObj("U+%04X"::formatted)
                .collect(joining(" ", "[", "]"));
    }

    /// The shapes every code point is tested in. Trimming must remove the code point from the ends when
    /// it is whitespace and never from the middle, a string made only of it must collapse to the empty
    /// string, and a run of it must be removed as a whole. The run appears at both ends because ltrim and
    /// rtrim are asymmetric, so one end alone would leave the other untested.
    private enum CodePointShape
    {
        SURROUNDED_BY_A,
        ALONE,
        REPEATED_BEFORE_A,
        REPEATED_AFTER_A;

        String buildInput(int codePoint)
        {
            String character = new String(Character.toChars(codePoint));
            return switch (this) {
                case SURROUNDED_BY_A -> character + "a" + character;
                case ALONE -> character;
                case REPEATED_BEFORE_A -> character + character + "a";
                case REPEATED_AFTER_A -> "a" + character + character;
            };
        }
    }

    private static List<Page> createTrimInputPages(TrimInput input, NullsProvider nullsProvider)
    {
        List<String> strings = input.strings();
        int positionsCount = strings.size();
        Optional<long[]> validity = nullsProvider.getValidityWords(positionsCount);
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionsCount, positionsCount * 16);
        for (int i = 0; i < positionsCount; i++) {
            if (validity.isPresent() && !Bitmap.isSet(validity.get(), 0, i)) {
                builder.appendNull();
            }
            else {
                String string = strings.get(i);
                Slice value = Slices.utf8Slice(string);
                if (input.type() instanceof CharType charType) {
                    verify(string.codePointCount(0, string.length()) <= charType.getLength(), "Value does not fit %s: %s", charType, string);
                    // A char(x) value is stored without the trailing spaces it is logically padded with
                    value = Chars.trimTrailingSpaces(value);
                }
                builder.writeEntry(value);
            }
        }
        return List.of(new Page(positionsCount, builder.build()));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testConcat(NullsProvider nullsProvider)
    {
        testConcat(2, nullsProvider);
        testConcat(4, nullsProvider);

        // concat is also defined for varbinary, which the GPU carries as LIST<INT8> rather than as strings
        assertThat(compileExpression(
                new Call(
                        FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(VARBINARY, VARBINARY)),
                        ImmutableList.of(field(0, VARBINARY), field(1, VARBINARY))),
                layoutFor(List.of(VARBINARY, VARBINARY)),
                SQL_STANDARD,
                TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
    }

    private void testConcat(int argumentCount, NullsProvider nullsProvider)
    {
        int positionsCount = 64;
        List<Type> inputTypes = Collections.nCopies(argumentCount, VARCHAR);

        Block[] randomBlocks = new Block[argumentCount];
        Block[] edgeCaseBlocks = new Block[argumentCount];
        ImmutableList.Builder<Expression> concatArguments = ImmutableList.builder();
        for (int channel = 0; channel < argumentCount; channel++) {
            randomBlocks[channel] = createBlock(VARCHAR, positionsCount, nullsProvider);

            // Rows the random data does not cover: empty strings, values that differ between arguments
            // (so that argument order is verified), multi-byte UTF-8, and a null in every argument position
            List<String> values = new ArrayList<>();
            values.add("");
            values.add("argument" + channel);
            values.add("Łania ワルシャワ Слава " + channel);
            for (int nullChannel = 0; nullChannel < argumentCount; nullChannel++) {
                values.add(nullChannel == channel ? null : "argument" + channel);
            }
            edgeCaseBlocks[channel] = createStringBlock(values);

            concatArguments.add(field(channel, VARCHAR));
        }

        List<Page> inputPages = List.of(
                new Page(positionsCount, randomBlocks),
                new Page(edgeCaseBlocks));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(inputTypes)),
                concatArguments.build());

        assertGpuMatchesCpu(inputPages, inputTypes, expression, IntStream.range(0, argumentCount).boxed().collect(toImmutableSet()));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCharConcat(NullsProvider nullsProvider)
    {
        CharType leftType = createCharType(5);
        CharType rightType = createCharType(7);
        int positionsCount = 64;
        List<Type> inputTypes = List.of(leftType, rightType);

        // Rows the random data does not cover: values shorter than the declared length, an empty right
        // argument (the only case where the result keeps no padding), multi-byte UTF-8, and nulls
        List<String> leftValues = Arrays.asList("", "", "ab", "abcde", "Łania", "ワル", null, "x");
        List<String> rightValues = Arrays.asList("", "xyz", "", "cd", "ワルシャワ", "Слава", "z", null);

        List<Page> inputPages = List.of(
                new Page(
                        positionsCount,
                        createBlock(leftType, positionsCount, nullsProvider),
                        createBlock(rightType, positionsCount, nullsProvider)),
                new Page(leftValues.size(), createStringBlock(leftValues), createStringBlock(rightValues)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(leftType, rightType)),
                ImmutableList.of(field(0, leftType), field(1, rightType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @Test
    void testConcatOutputLengthLimit()
    {
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR)));
        Set<Integer> inputChannels = Set.of(0, 1);

        int maxOutputLength = 1024 * 1024;
        String ascii = "a".repeat(maxOutputLength);
        // Pin where the limit is, which comparing GPU against CPU cannot do on its own: it passes when
        // neither engine rejects anything, so a wrong maxOutputLength would go unnoticed
        assertConcatOnCpuSucceeds(twoStringPage(ascii, ""), inputTypes, expression, maxOutputLength);
        assertTrinoExceptionThrownBy(() -> executeWithCpu(
                List.of(twoStringPage(ascii, "a")),
                List.of(expression),
                layoutFor(inputTypes)))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertGpuMatchesCpu(List.of(twoStringPage(ascii.substring(1), "")), inputTypes, expression, inputChannels);
        assertGpuMatchesCpu(List.of(twoStringPage(ascii, "")), inputTypes, expression, inputChannels);
        assertGpuMatchesCpu(List.of(twoStringPage(ascii, "a")), inputTypes, expression, inputChannels);

        // The cap counts bytes, not characters: this value is 3 bytes per character, so a character
        // count would stay far below the cap and let the oversized case through.
        String multiByte = "ワ".repeat(maxOutputLength / 3);
        assertGpuMatchesCpu(List.of(twoStringPage(multiByte, "")), inputTypes, expression, inputChannels);
        assertGpuMatchesCpu(List.of(twoStringPage(multiByte, "ワ")), inputTypes, expression, inputChannels);

        // Values that would overflow concat limit, except they are paired with a NULL entry
        List<Type> threeInputTypes = List.of(VARCHAR, VARCHAR, VARCHAR);
        Expression threeArgs = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR), field(2, VARCHAR)));
        Set<Integer> threeChannels = Set.of(0, 1, 2);
        String half = "a".repeat(maxOutputLength / 2 + 1);
        for (int nullChannel = 0; nullChannel < 3; nullChannel++) {
            List<String> values = new ArrayList<>(List.of(half, half, half));
            values.set(nullChannel, null);
            assertGpuMatchesCpu(
                    List.of(new Page(
                            1,
                            createStringBlock(asList(values.get(0))),
                            createStringBlock(asList(values.get(1))),
                            createStringBlock(asList(values.get(2))))),
                    threeInputTypes,
                    threeArgs,
                    threeChannels);
        }
    }

    private static void assertConcatOnCpuSucceeds(Page page, List<Type> inputTypes, Expression expression, int expectedLength)
    {
        List<Page> results = executeWithCpu(List.of(page), List.of(expression), layoutFor(inputTypes));
        assertThat(VARCHAR.getSlice(getOnlyElement(results).getBlock(0), 0).length()).isEqualTo(expectedLength);
    }

    private static Page twoStringPage(String first, String second)
    {
        return new Page(1, createStringBlock(asList(first)), createStringBlock(asList(second)));
    }

    private static Block createStringBlock(List<String> values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.size(), values.size() * 10);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(Slices.utf8Slice(value));
            }
        }
        return builder.build();
    }
}
