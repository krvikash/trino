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
import ai.rapids.cudf.HostColumnVector;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuDateTrunc.Field;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.scalar.StringFunctions;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.type.CharVarcharCoercion;
import io.trino.type.LikePattern;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.decimalBlock;
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
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.ir.TestingIr.nullIf;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static io.trino.type.CodePointsType.CODE_POINTS;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.lang.Math.clamp;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGpuExpressions
{
    // The nine {true, false, null} x {true, false, null} combinations, column-wise.
    private static final Boolean[] TRUTH_TABLE_LEFT = {true, true, true, false, false, false, null, null, null};
    private static final Boolean[] TRUTH_TABLE_RIGHT = {true, false, null, true, false, null, true, false, null};

    /// Search strings for `replace`, chosen to occur in `testStrings` and in `REPLACE_EDGE_CASE_STRINGS`:
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

    /**
     * Useful test strings, including interesting inputs and patterns for LIKE testing.
     */
    private final List<String> testStrings = ImmutableList.<String>builder()
            .add("test1", "other", "test2", "nothing", "testing", "%test%")
            .add("a", "xyz", "ab", "z", "yz", "abcd", "", "abcdefg", "xabc", "xyxw", "xaxxxbx", "abcdefghij")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab")
            .add("aabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabb")
            .add("aaaabbbbaaaabbbbaaaabbbb")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .add("aaaabbbbaaaabbbbaaaa", "aaaabbbbaaaabbbbcccc")
            .add("abababababacabababa", "bbbbbbbbxax", "bbbxxxxaz")
            .add("a".repeat(20) + "b".repeat(20) + "a".repeat(20) + "b".repeat(20) + "the quick brown fox jumps over the lazy dog")
            .add("ababaa", "papaya", "papapaya", "papapapaya", "papapapapaya", "papapapapapaya")
            .add("xyza1234567890123456")
            .add("%", "_", "-", "xxxxx_xxxxx")
            .add("__", "a%", "a_", "%a", "%z", "_z", "_%", "_a%", "_ab_", "_a%b_", "_%_%_%_%")
            .add("%a%a%a%a%a%a%", "%a%b%a%b%a%b%", "%aaaa%bbbb%aaaa%bbbb%aaaa%bbbb%")
            .add("%aaaaaaaaaaaaaaaaaaaaaaaaaa%", "%aab%bba%aab%bba%", "%abaca%")
            .add("%bcccccccca%", "%bbxxxxxa%", "%aaaaaaxaaaaaa%", "%abaaa%", "%paya%")
            .add("%a________________", "-%", "-_", "--", "%$_%")
            .add("Łania szła piękną łąką pod Warszawą")
            .add("ワルシャワ近郊の美しい草原を雌鹿が歩いていた。")
            .add("Слава Україні")
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
    /// `testStrings` cut to the char length, contributing multi-byte characters.
    private final List<Object> charCastValues = Stream.concat(
                    Stream.of("", " ", "        ", "a", "ab ", "abc", "abcdefgh", " abc", "a b c"),
                    testStrings.stream())
            .map(value -> (Object) truncateToLengthAndTrimSpaces(Slices.utf8Slice(value), CHAR_CAST_SOURCE_TYPE))
            .distinct()
            .collect(toImmutableList());

    @BeforeAll
    public static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeSmall(NullsProvider nullsProvider)
    {
        testLike(List.of(64), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeMany(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = Stream.generate(() -> 10_000).limit(42).toList();
        testLike(positionsCounts, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeRandomPages(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = randomInts(0, 200_00).limit(42).toList();
        testLike(positionsCounts, nullsProvider);
    }

    private void testLike(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = createVarcharPages(positionsCounts, nullsProvider);

        List<Optional<Character>> escapes = List.of(Optional.of('\\'), Optional.of('$'), Optional.empty());
        for (String pattern : testStrings) {
            for (Optional<Character> escape : escapes) {
                Expression expression = createLikeExpression(varcharChannel, pattern, escape);
                assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
            }
        }
    }

    @Test
    public void testBooleanConstant()
    {
        testConstant(new Constant(BOOLEAN, true));
        testConstant(new Constant(BOOLEAN, false));
        testConstant(new Constant(BOOLEAN, null));
    }

    @Test
    public void testTinyintConstant()
    {
        testConstant(new Constant(TINYINT, 42L));
        testConstant(new Constant(TINYINT, -1L));
        testConstant(new Constant(TINYINT, 0L));
        testConstant(new Constant(TINYINT, null));
    }

    @Test
    public void testSmallintConstant()
    {
        testConstant(new Constant(SMALLINT, 1234L));
        testConstant(new Constant(SMALLINT, -5678L));
        testConstant(new Constant(SMALLINT, null));
    }

    @Test
    public void testIntegerConstant()
    {
        testConstant(new Constant(INTEGER, 123456L));
        testConstant(new Constant(INTEGER, -789012L));
        testConstant(new Constant(INTEGER, 0L));
        testConstant(new Constant(INTEGER, null));
    }

    @Test
    public void testBigintConstant()
    {
        testConstant(new Constant(BIGINT, 1234567890123L));
        testConstant(new Constant(BIGINT, -9876543210L));
        testConstant(new Constant(BIGINT, 0L));
        testConstant(new Constant(BIGINT, null));
    }

    @Test
    public void testRealConstant()
    {
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(3.14f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(-2.5f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(0.0f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.POSITIVE_INFINITY)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.NEGATIVE_INFINITY)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.NaN)));
        testConstant(new Constant(REAL, null));
    }

    @Test
    public void testDoubleConstant()
    {
        testConstant(new Constant(DOUBLE, 3.14159265359));
        testConstant(new Constant(DOUBLE, -2.71828));
        testConstant(new Constant(DOUBLE, 0.0));
        testConstant(new Constant(DOUBLE, Double.POSITIVE_INFINITY));
        testConstant(new Constant(DOUBLE, Double.NEGATIVE_INFINITY));
        testConstant(new Constant(DOUBLE, Double.NaN));
        testConstant(new Constant(DOUBLE, null));
    }

    @Test
    public void testVarcharConstant()
    {
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("hello")));
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("")));
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("Łania szła piękną łąką pod Warszawą")));
        testConstant(new Constant(VARCHAR, null));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testReference(NullsProvider nullsProvider)
    {
        Expression expression = new Reference(BIGINT, "ref0");
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -1000, 1000)));
        assertGpuMatchesCpu(inputPages, List.of(BIGINT), expression, Set.of(0));
    }

    @Test
    public void testRealToIntegerCastPrecision()
    {
        // Odd integers in [2^23, 2^24) are exactly representable in float32 but adding 0.5
        // requires 25 significand bits, exceeding float32's 24. IEEE 754 round-to-nearest-even
        // rounds the sum up to value+1 (because the last kept bit is 1), producing an off-by-one
        // after truncation. The GPU must widen to float64 for the rounding arithmetic to match
        // CPU semantics.
        int[] values = {
                (1 << 23) + 1,
                (1 << 24) - 1,
                -((1 << 23) + 1),
                -((1 << 24) - 1),
        };

        Expression expression = new Cast(field(0, REAL), INTEGER);
        Map<Symbol, Integer> layout = layoutFor(List.of(REAL));
        CompiledExpression compiledGpu = compileExpression(expression, layout, SQL_STANDARD).orElseThrow();
        PageProcessor cpuProcessor = compileCpuExpression(expression, layout);

        for (int value : values) {
            long realBits = Float.floatToIntBits((float) value);
            Page input = new Page(writeNativeValue(REAL, realBits));
            assertGpuMatchesCpuForCast(List.of(input), List.of(REAL), expression, cpuProcessor, compiledGpu);
        }
    }

    @Test
    public void testNumericOperatorsCorrectnessSmoke()
    {
        List<Type> testedTypes = List.of(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                createDecimalType(1, 0),
                createDecimalType(3, 0), // can hold max tinyint
                createDecimalType(5, 0), // can hold max smallint
                createDecimalType(10, 0), // can hold max integer
                createDecimalType(19, 0), // can hold max bigint
                createDecimalType(13, 0),
                createDecimalType(13, 2),
                createDecimalType(27, 0),
                createDecimalType(27, 5),
                createDecimalType(38),
                NUMBER,
                createVarcharType(1),
                createVarcharType(3),
                createVarcharType(5),
                createVarcharType(20),
                VARCHAR);
        List<TrinoNumber> testedNumbers = numericValuesToTest();

        LoadingCache<Type, List<@Nullable Object>> testedTypeValues = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(type -> {
                    List<@Nullable Object> values = tryCastToAndFilter(NUMBER, type, testedNumbers);
                    verify(!values.isEmpty());
                    return values;
                }));

        LoadingCache<Type, List<Page>> unaryInputs = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(type -> testedTypeValues.getUnchecked(type).stream()
                        .map(value -> new Page(writeNativeValue(type, value)))
                        .collect(toImmutableList())));

        LoadingCache<Pair<Type, Type>, List<Page>> binaryInputs = EvictableCacheBuilder.newBuilder()
                // Unbounded. This is only to memoize.
                .maximumSize(Long.MAX_VALUE)
                .build(CacheLoader.from(pair -> cartesianProduct(
                        pair.first(),
                        testedTypeValues.getUnchecked(pair.first()),
                        pair.second(),
                        testedTypeValues.getUnchecked(pair.second()))));

        Set<String> testedOperators = new HashSet<>();
        Set<String> gpuEnabledOperators = new HashSet<>();

        // Unary operators
        for (Type type : testedTypes) {
            for (OperatorType operator : OperatorType.values()) {
                if (operator.getArgumentCount() != 1) {
                    continue;
                }
                if (operator == OperatorType.CAST || operator == OperatorType.SATURATED_FLOOR_CAST) {
                    // Casts tested below in the (fromType, toType) cross-product.
                    continue;
                }

                ResolvedFunction function;
                try {
                    function = FUNCTION_RESOLUTION.resolveOperator(operator, List.of(type));
                }
                catch (OperatorNotFoundException e) {
                    continue;
                }

                // Inspect coercions if any
                Type coerced = getOnlyElement(function.signature().getArgumentTypes());
                if (type.equals(coerced)) {
                    // Let's test, this is what we're here for
                }
                else if (testedTypes.contains(coerced)) {
                    // Will be tested explicitly on separate round
                    continue;
                }
                else {
                    // Might not be covered by explicit tests; expect this only for decimal coercions
                    verify(type instanceof DecimalType,
                            "Unexpected coercion for unary %s on %s: argument types %s",
                            operator,
                            type,
                            function.signature().getArgumentTypes());
                }

                testedOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                Expression expression = buildUnaryOperatorExpression(operator, function, field(0, coerced));
                Map<Symbol, Integer> layout = layoutFor(List.of(coerced));
                Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout, SQL_STANDARD);
                if (gpuExpression.isEmpty()) {
                    // Not supported for GPU execution
                    continue;
                }
                gpuEnabledOperators.add("%s %s".formatted(operator.getOperator(), coerced.getDisplayName()));

                try {
                    CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                    assertThat(compiledGpu.inputChannels().getInputChannels()).containsExactly(0);
                    PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                    for (Page input : unaryInputs.getUnchecked(coerced)) {
                        assertThat(input.getPositionCount()).isEqualTo(1);
                        assertGpuMatchesCpu(List.of(input), List.of(coerced), expression, cpuProcessor, compiledGpu, false);
                    }
                }
                catch (AssertionError failure) {
                    failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                    throw failure;
                }
            }
        }

        // Cast operators: cross-product over (fromType, toType).
        for (Type fromType : testedTypes) {
            for (Type toType : testedTypes) {
                // CPU's REAL→BIGINT silently saturates for ±Inf and finite OOR (uses (long) cast which clamps),
                // while GPU correctly throws INVALID_CAST_ARGUMENT. Skip until CPU is fixed.
                // TODO: re-enable once CPU REAL→BIGINT throws consistently.
                if (fromType.equals(REAL) && toType.equals(BIGINT)) {
                    continue;
                }
                try {
                    FUNCTION_RESOLUTION.getCoercion(fromType, toType);
                }
                catch (OperatorNotFoundException e) {
                    continue;
                }

                String label = "CAST %s AS %s".formatted(fromType.getDisplayName(), toType.getDisplayName());
                testedOperators.add(label);

                Expression expression = new Cast(field(0, fromType), toType);
                Map<Symbol, Integer> layout = layoutFor(List.of(fromType));
                Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout, SQL_STANDARD);
                if (gpuExpression.isEmpty()) {
                    continue;
                }
                gpuEnabledOperators.add(label);

                try {
                    CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                    assertThat(compiledGpu.inputChannels().getInputChannels()).containsExactly(0);
                    PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                    for (Page input : unaryInputs.getUnchecked(fromType)) {
                        assertThat(input.getPositionCount()).isEqualTo(1);
                        assertGpuMatchesCpuForCast(List.of(input), List.of(fromType), expression, cpuProcessor, compiledGpu);
                    }
                }
                catch (AssertionError failure) {
                    failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                    throw failure;
                }
            }
        }

        // Binary operators: cross-product over (leftType, rightType)
        for (Type leftType : testedTypes) {
            for (Type rightType : testedTypes) {
                for (OperatorType operator : OperatorType.values()) {
                    if (operator.getArgumentCount() != 2) {
                        continue;
                    }
                    if (operator == OperatorType.EQUAL || operator == OperatorType.LESS_THAN || operator == OperatorType.LESS_THAN_OR_EQUAL) {
                        continue;
                    }

                    ResolvedFunction function;
                    try {
                        function = FUNCTION_RESOLUTION.resolveOperator(operator, List.of(leftType, rightType));
                    }
                    catch (OperatorNotFoundException e) {
                        continue;
                    }

                    switch (operator) {
                        case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO, NEGATION, SUBSCRIPT -> {
                            // Binary operators tested here
                        }
                        case EQUAL, COMPARISON_UNORDERED_LAST, COMPARISON_UNORDERED_FIRST, LESS_THAN, LESS_THAN_OR_EQUAL, IDENTICAL -> {
                            // Covered by testComparison
                            assertThat(Set.copyOf(function.signature().getArgumentTypes()))
                                    .as(
                                            """
                                            Expect comparison operators to require pre-coerced inputs. If this is the case, comparisons have test coverage in testComparison.
                                            Otherwise test coverage needs to be revisited.
                                            """)
                                    .hasSize(1);
                        }
                        case CAST, SATURATED_FLOOR_CAST, HASH_CODE, XX_HASH_64, INDETERMINATE, READ_VALUE -> throw new AssertionError("Unreachable, not a binary operator");
                    }

                    // Inspect coercions if any
                    if (List.of(leftType, rightType).equals(function.signature().getArgumentTypes())) {
                        // Let's test, this is what we're here for
                    }
                    else if (testedTypes.containsAll(function.signature().getArgumentTypes())) {
                        // Will be tested explicitly on separate round
                        continue;
                    }
                    else {
                        // Might not be covered by explicit tests, so let's test this.
                        // This should be the case only for decimal types.
                        verify(leftType instanceof DecimalType || rightType instanceof DecimalType, "Neither is decimal: %s, %s", leftType, rightType);
                    }

                    assertThat(function.signature().getArgumentTypes()).hasSize(2);
                    Type leftCoerced = function.signature().getArgumentTypes().get(0);
                    Type rightCoerced = function.signature().getArgumentTypes().get(1);

                    testedOperators.add("%s %s %s".formatted(leftCoerced.getDisplayName(), operator.getOperator(), rightCoerced.getDisplayName()));

                    Expression expression = new Call(function, ImmutableList.of(field(0, leftCoerced), field(1, rightCoerced)));
                    Map<Symbol, Integer> layout = layoutFor(List.of(leftCoerced, rightCoerced));
                    Optional<CompiledExpression> gpuExpression = compileExpression(expression, layout, SQL_STANDARD);
                    if (gpuExpression.isEmpty()) {
                        // Not supported for GPU execution
                        continue;
                    }
                    gpuEnabledOperators.add("%s %s %s".formatted(leftCoerced.getDisplayName(), operator.getOperator(), rightCoerced.getDisplayName()));

                    try {
                        CompiledExpression compiledGpu = gpuExpression.orElseThrow();
                        assertThat(compiledGpu.inputChannels().getInputChannels())
                                .containsExactlyInAnyOrderElementsOf(Set.of(0, 1));
                        PageProcessor cpuProcessor = compileCpuExpression(expression, layout);
                        for (Page input : binaryInputs.getUnchecked(new Pair<>(leftCoerced, rightCoerced))) {
                            assertThat(input.getPositionCount()).isEqualTo(1);
                            assertGpuMatchesCpu(List.of(input), List.of(leftCoerced, rightCoerced), expression, cpuProcessor, compiledGpu, false);
                        }
                    }
                    catch (AssertionError failure) {
                        failure.addSuppressed(new Exception("GPU expression: " + gpuExpression.orElseThrow().expression()));
                        throw failure;
                    }
                }
            }
        }

        // Self-test
        assertThat(testedOperators)
                .contains(
                        "- bigint",
                        "- decimal(13,2)",
                        "HASH CODE bigint",
                        "bigint + bigint",
                        "decimal(3,0) + decimal(3,0)",
                        "decimal(27,5) + decimal(27,5)",
                        "decimal(27,0) - decimal(3,0)",
                        "CAST bigint AS integer",
                        "CAST double AS bigint",
                        "CAST real AS tinyint");

        // Self-test
        assertThat(gpuEnabledOperators)
                .contains(
                        "bigint + bigint",
                        "decimal(3,0) + decimal(3,0)",
                        "decimal(13,0) + decimal(13,0)",
                        "decimal(13,0) + decimal(13,2)",
                        "decimal(3,0) - decimal(3,0)",
                        "decimal(13,0) - decimal(13,0)",
                        "decimal(13,0) - decimal(13,2)",
                        "decimal(3,0) * decimal(3,0)",
                        "decimal(13,0) * decimal(13,0)",
                        "decimal(13,0) * decimal(13,2)",
                        "CAST bigint AS integer",
                        "CAST double AS bigint",
                        "CAST real AS tinyint",
                        "CAST double AS real",
                        "decimal(27,5) + decimal(27,5)",
                        "decimal(27,0) - decimal(3,0)",
                        "decimal(27,0) * decimal(3,0)")
                .doesNotContain(
                        "- bigint",
                        "- decimal(13,2)",
                        "HASH CODE bigint");
    }

    private static List<@Nullable TrinoNumber> numericValuesToTest()
    {
        List<Long> initial = List.of(
                0L,
                -1L,
                1L,
                (long) Byte.MIN_VALUE,
                (long) Byte.MAX_VALUE,
                (long) Short.MIN_VALUE,
                (long) Short.MAX_VALUE,
                (long) Integer.MIN_VALUE,
                (long) Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MAX_VALUE);
        List<BigDecimal> offsets = Stream.of("0", "1", "-1", "0.33", "0.5", "0.66", "-0.33", "-0.5", "-0.66")
                .map(BigDecimal::new)
                .toList();
        List<TrinoNumber> testedNumbers = new ArrayList<>();
        testedNumbers.add(null);
        for (long value : initial) {
            BigDecimal asBigDecimal = BigDecimal.valueOf(value);
            for (BigDecimal offset : offsets) {
                testedNumbers.add(TrinoNumber.from(asBigDecimal.add(offset)));
            }
        }
        testedNumbers.add(TrinoNumber.from(BigDecimal.valueOf(Math.PI)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.Infinity(false)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.Infinity(true)));
        testedNumbers.add(TrinoNumber.from(new TrinoNumber.NotANumber()));
        return testedNumbers;
    }

    private List<@Nullable Object> tryCastToAndFilter(Type sourceType, Type targetType, List<@Nullable ?> sourceNativeValues)
    {
        List<Object> nativeValues = new ArrayList<>();
        Expression castExpression = new Cast(field(0, sourceType), targetType);
        Map<Symbol, Integer> layout = layoutFor(List.of(sourceType));
        PageProcessor pageProcessor = compileCpuExpression(castExpression, layout);
        for (@Nullable Object sourceValue : sourceNativeValues) {
            Page sourcePage = new Page(writeNativeValue(sourceType, sourceValue));
            List<Page> result;
            try {
                result = executeWithCpu(pageProcessor, List.of(sourcePage));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode()) || e.getErrorCode().equals(INVALID_CAST_ARGUMENT.toErrorCode())) {
                    continue;
                }
                throw e;
            }
            assertThat(result).hasSize(1);
            Page resultPage = getOnlyElement(result);
            assertThat(resultPage.getPositionCount()).isEqualTo(1);
            assertThat(resultPage.getChannelCount()).isEqualTo(1);
            @Nullable Object targetValue = readNativeValue(targetType, resultPage.getBlock(0), 0);
            nativeValues.add(targetValue);
        }
        return nativeValues;
    }

    private List<Page> cartesianProduct(Type leftType, List<?> leftValues, Type rightType, List<?> rightValues)
    {
        List<Page> pages = new ArrayList<>();
        for (Object leftValue : leftValues) {
            for (Object rightValue : rightValues) {
                pages.add(new Page(
                        writeNativeValue(leftType, leftValue),
                        writeNativeValue(rightType, rightValue)));
            }
        }
        return pages;
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testArithmeticOperators(NullsProvider nullsProvider)
    {
        testArithmetic(OperatorType.ADD, nullsProvider);
        testArithmetic(OperatorType.SUBTRACT, nullsProvider);
        testArithmetic(OperatorType.MULTIPLY, nullsProvider);
        testArithmetic(OperatorType.DIVIDE, nullsProvider);
        testArithmetic(OperatorType.MODULO, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testShortDecimalAdd(NullsProvider nullsProvider)
    {
        testShortDecimalAdd(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalAdd(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalAdd(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalAdd(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (integral + scale + 1 > 18)
        testShortDecimalAdd(createDecimalType(18, 0), createDecimalType(18, 18), nullsProvider);
        testShortDecimalAdd(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalAdd(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.ADD, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testShortDecimalSubtract(NullsProvider nullsProvider)
    {
        testShortDecimalSubtract(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalSubtract(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalSubtract(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalSubtract(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (integral + scale + 1 > 18)
        testShortDecimalSubtract(createDecimalType(18, 0), createDecimalType(18, 18), nullsProvider);
        testShortDecimalSubtract(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalSubtract(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.SUBTRACT, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testShortDecimalMultiply(NullsProvider nullsProvider)
    {
        testShortDecimalMultiply(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testShortDecimalMultiply(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testShortDecimalMultiply(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testShortDecimalMultiply(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        // result overflows into DECIMAL128 (p1 + p2 > 18)
        testShortDecimalMultiply(createDecimalType(10, 2), createDecimalType(10, 2), nullsProvider);
        testShortDecimalMultiply(createDecimalType(13, 0), createDecimalType(13, 0), nullsProvider);
        testShortDecimalMultiply(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
    }

    private void testShortDecimalMultiply(DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.MULTIPLY, leftType, rightType, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLongDecimalAdd(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(19, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.ADD, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLongDecimalSubtract(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(19, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.SUBTRACT, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLongDecimalMultiply(NullsProvider nullsProvider)
    {
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(20, 2), createDecimalType(18, 2), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(27, 0), createDecimalType(3, 0), nullsProvider);
        testDecimalBinaryOp(OperatorType.MULTIPLY, createDecimalType(19, 0), createDecimalType(19, 0), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testShortDecimalDivide(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(5, 2), createDecimalType(5, 2), nullsProvider);
        testDecimalDivide(createDecimalType(3, 0), createDecimalType(4, 0), nullsProvider);
        testDecimalDivide(createDecimalType(8, 4), createDecimalType(9, 5), nullsProvider);
        testDecimalDivide(createDecimalType(1, 0), createDecimalType(13, 2), nullsProvider);
        testDecimalDivide(createDecimalType(18, 0), createDecimalType(18, 0), nullsProvider);
        testDecimalDivide(createDecimalType(18, 18), createDecimalType(18, 0), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLongDecimalDivide(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(38, 4), createDecimalType(38, 4), nullsProvider);
        testDecimalDivide(createDecimalType(38, 0), createDecimalType(38, 0), nullsProvider);
        testDecimalDivide(createDecimalType(38, 38), createDecimalType(38, 0), nullsProvider);
        testDecimalDivide(createDecimalType(27, 5), createDecimalType(27, 5), nullsProvider);
        testDecimalDivide(createDecimalType(19, 0), createDecimalType(19, 0), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testMixedWidthDecimalDivide(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(25, 4), createDecimalType(13, 2), nullsProvider);
        testDecimalDivide(createDecimalType(13, 2), createDecimalType(25, 4), nullsProvider);
        testDecimalDivide(createDecimalType(38, 6), createDecimalType(5, 2), nullsProvider);
        testDecimalDivide(createDecimalType(5, 2), createDecimalType(38, 6), nullsProvider);
    }

    /// The largest rescale factor the long division supports is 12, where `3 * rescaleFactor <= 38` is
    /// tightest. These four sit at exactly that factor: the first still rescales the dividend up front;
    /// the second divides first but never reaches the estimating branch, because no divisor in
    /// `decimal(6,6)` reaches `10^24`; the third has `divisorPrecision + rescaleFactor` one above 38, the
    /// point at which a rescaled remainder stops fitting DECIMAL128 and the estimating branch becomes
    /// necessary; and the fourth is well past it.
    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testDecimalDivideAtLargestRescaleFactor(NullsProvider nullsProvider)
    {
        testDecimalDivide(createDecimalType(26, 0), createDecimalType(6, 6), nullsProvider);
        testDecimalDivide(createDecimalType(27, 0), createDecimalType(6, 6), nullsProvider);
        testDecimalDivide(createDecimalType(27, 0), createDecimalType(27, 6), nullsProvider);
        testDecimalDivide(createDecimalType(27, 0), createDecimalType(30, 6), nullsProvider);
    }

    @Test
    public void testDecimalDivideByZero()
    {
        DecimalType type = createDecimalType(38, 4);
        Page input = new Page(decimalBlock(type, List.of(BigInteger.valueOf(12345))), decimalBlock(type, List.of(BigInteger.ZERO)));
        assertGpuMatchesCpu(List.of(input), List.of(type, type), divide(type, type));

        DecimalType shortType = createDecimalType(9, 2);
        Page shortInput = new Page(decimalBlock(shortType, List.of(BigInteger.valueOf(12345))), decimalBlock(shortType, List.of(BigInteger.ZERO)));
        assertGpuMatchesCpu(List.of(shortInput), List.of(shortType, shortType), divide(shortType, shortType));
    }

    @Test
    public void testDecimalDivideNullOperands()
    {
        // A null dividend yields null even for a zero divisor, so the division by zero must not be reported.
        // A null divisor yields null too, which relies on cuDF selecting the false branch of an ifElse
        // wherever the predicate is null: the null divisor's zero test is null, so nonZeroMagnitude keeps
        // the null magnitude instead of substituting one.
        DecimalType type = createDecimalType(38, 4);
        BlockBuilder dividends = type.createBlockBuilder(null, 6);
        dividends.appendNull();
        dividends.appendNull();
        dividends.appendNull();
        type.writeObject(dividends, Int128.valueOf(700));
        type.writeObject(dividends, Int128.valueOf(700));
        dividends.appendNull();
        BlockBuilder divisors = type.createBlockBuilder(null, 6);
        type.writeObject(divisors, Int128.valueOf(0));
        type.writeObject(divisors, Int128.valueOf(-3));
        type.writeObject(divisors, Int128.valueOf(1));
        type.writeObject(divisors, Int128.valueOf(4));
        divisors.appendNull();
        divisors.appendNull();
        Page input = new Page(dividends.build(), divisors.build());
        assertGpuMatchesCpu(List.of(input), List.of(type, type), divide(type, type));
    }

    @Test
    public void testDecimalDivideOverflow()
    {
        DecimalType type = createDecimalType(38, 0);
        // decimal(38,0) / decimal(38,0) is decimal(38,6) and takes the long-division path, so dividing by
        // one leaves a quotient of 10^38 - 1 against a limit of 10^(38 - 6): this trips the guard on the
        // quotient, before it is rescaled, rather than any check on the finished result.
        Page input = new Page(
                decimalBlock(type, List.of(BigInteger.TEN.pow(38).subtract(BigInteger.ONE))),
                decimalBlock(type, List.of(BigInteger.ONE)));
        assertGpuMatchesCpu(List.of(input), List.of(type, type), divide(type, type));
    }

    @Test
    public void testDecimalDivideUnsupportedRescale()
    {
        // decimal(30,0) / decimal(20,10) is decimal(38,6), rescaling the dividend by 10^16; neither the
        // rescaled dividend nor the long-division intermediates fit in DECIMAL128
        DecimalType dividendType = createDecimalType(30, 0);
        DecimalType divisorType = createDecimalType(20, 10);
        assertThat(compileExpression(divide(dividendType, divisorType), layoutFor(List.of(dividendType, divisorType)), SQL_STANDARD))
                .isEmpty();
    }

    private void testDecimalDivide(DecimalType dividendType, DecimalType divisorType, NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(dividendType, divisorType);
        Expression expression = divide(dividendType, divisorType);
        DecimalType resultType = (DecimalType) expression.type();
        int rescaleFactor = resultType.getScale() - dividendType.getScale() + divisorType.getScale();
        assertGpuMatchesCpu(divideInputs(dividendType, divisorType, rescaleFactor, nullsProvider), inputTypes, expression, Set.of(0, 1));
    }

    private static Expression divide(DecimalType dividendType, DecimalType divisorType)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(dividendType, divisorType)),
                ImmutableList.of(field(0, dividendType), field(1, divisorType)));
    }

    /// Dividend and divisor pairs covering both signs, remainders exactly on the HALF_UP boundary and
    /// their neighbours, and divisors on both sides of `10^(2 * rescaleFactor)`, where the GPU switches
    /// between rescaling the remainder directly and estimating the quotient. Dividends are capped so
    /// that no row overflows the result; overflow is covered separately.
    private static List<Page> divideInputs(DecimalType dividendType, DecimalType divisorType, int rescaleFactor, NullsProvider nullsProvider)
    {
        BigInteger dividendBound = BigInteger.TEN.pow(dividendType.getPrecision());
        BigInteger divisorBound = BigInteger.TEN.pow(divisorType.getPrecision());
        BigInteger rescale = BigInteger.TEN.pow(rescaleFactor);
        BigInteger resultBound = BigInteger.TEN.pow(38);
        List<BigInteger> dividends = new ArrayList<>();
        List<BigInteger> divisors = new ArrayList<>();

        // A divisor of 2^(rescaleFactor + 1) * m leaves the rescaled dividend m * 10^rescaleFactor at exactly
        // half of it: 10^rescaleFactor is 2^rescaleFactor * 5^rescaleFactor and 5^rescaleFactor is odd, so the
        // remainder is 2^rescaleFactor * m. The dividend's neighbours m - 1 and m + 1 are not ties.
        int ties = 0;
        for (BigInteger multiplier : tieMultipliers(rescaleFactor, dividendBound, divisorBound)) {
            BigInteger divisor = BigInteger.TWO.pow(rescaleFactor + 1).multiply(multiplier);
            verify(multiplier.multiply(rescale).mod(divisor).equals(divisor.divide(BigInteger.TWO)),
                    "Not a rounding tie: %s / %s",
                    multiplier,
                    divisor);
            ties++;
            for (long offset : new long[] {-1, 0, 1}) {
                for (int dividendSign : new int[] {1, -1}) {
                    for (int divisorSign : new int[] {1, -1}) {
                        dividends.add(multiplier.add(BigInteger.valueOf(offset)).multiply(BigInteger.valueOf(dividendSign)));
                        divisors.add(divisor.multiply(BigInteger.valueOf(divisorSign)));
                    }
                }
            }
        }
        verify(ties > 0, "No rounding tie fits %s / %s", dividendType, divisorType);

        Random random = new Random(29);
        BigInteger branchBoundary = rescale.multiply(rescale);
        // Just above the branch boundary, the divisor's low part is as large as it can be relative to its
        // high part, which is where the estimated quotient most often comes out one too high. Sample this
        // divisor repeatedly, since a single dividend would hit the correction only about half the time.
        BigInteger correctionProne = branchBoundary.add(rescale).subtract(BigInteger.ONE);
        if (correctionProne.compareTo(divisorBound) < 0) {
            for (int sample = 0; sample < 8; sample++) {
                addDividePair(dividends, divisors, correctionProne, dividendBound, resultBound, rescale, random);
            }
        }
        for (BigInteger branchPoint : List.of(branchBoundary, BigInteger.ONE, divisorBound.subtract(BigInteger.ONE))) {
            for (long offset : new long[] {-1, 0, 1}) {
                BigInteger divisor = branchPoint.add(BigInteger.valueOf(offset));
                if (divisor.signum() <= 0 || divisor.compareTo(divisorBound) >= 0) {
                    continue;
                }
                addDividePair(dividends, divisors, divisor, dividendBound, resultBound, rescale, random);
            }
        }
        while (dividends.size() < 256) {
            BigInteger divisor = randomBelow(random, divisorBound.subtract(BigInteger.ONE)).add(BigInteger.ONE);
            addDividePair(dividends, divisors, divisor, dividendBound, resultBound, rescale, random);
        }

        return List.of(new Page(
                decimalBlock(dividendType, dividends, nullsProvider),
                decimalBlock(divisorType, divisors, nullsProvider)));
    }

    /// Multipliers `m` whose tie divisor `2^(rescaleFactor + 1) * m` and tie dividend `m` both fit the
    /// tested precisions. The last candidate is the smallest `m` that lifts the divisor to
    /// `10^(2 * rescaleFactor)`, which is what puts a tie on the estimating branch of the long division.
    private static List<BigInteger> tieMultipliers(int rescaleFactor, BigInteger dividendBound, BigInteger divisorBound)
    {
        BigInteger step = BigInteger.TWO.pow(rescaleFactor + 1);
        BigInteger estimatingBranch = BigInteger.TEN.pow(2 * rescaleFactor).add(step).subtract(BigInteger.ONE).divide(step);
        ImmutableList.Builder<BigInteger> multipliers = ImmutableList.builder();
        for (BigInteger multiplier : List.of(BigInteger.ONE, BigInteger.valueOf(7), BigInteger.valueOf(9999), estimatingBranch)) {
            if (step.multiply(multiplier).compareTo(divisorBound) < 0 && multiplier.add(BigInteger.ONE).compareTo(dividendBound) < 0) {
                multipliers.add(multiplier);
            }
        }
        return multipliers.build();
    }

    private static void addDividePair(
            List<BigInteger> dividends,
            List<BigInteger> divisors,
            BigInteger divisor,
            BigInteger dividendBound,
            BigInteger resultBound,
            BigInteger rescale,
            Random random)
    {
        BigInteger cap = resultBound.subtract(BigInteger.ONE).multiply(divisor).divide(rescale).add(BigInteger.ONE).min(dividendBound);
        // What stresses the rescaling in the direct branch is a large remainder, not a large dividend: a
        // dividend one below the divisor leaves the largest remainder that divisor admits. The largest
        // permitted dividend is also worth a row, though against the largest divisor it divides exactly
        // and leaves nothing. Drawing only at random misses both: for decimal(27,0) / decimal(30,6) the
        // remainder has to reach 88 of its 90 available bits before the rescaled remainder stops fitting.
        BigInteger largest = cap.subtract(BigInteger.ONE).max(BigInteger.ZERO);
        for (BigInteger magnitude : List.of(randomBelow(random, cap), largest, divisor.subtract(BigInteger.ONE).min(largest).max(BigInteger.ZERO))) {
            for (int dividendSign : new int[] {1, -1}) {
                for (int divisorSign : new int[] {1, -1}) {
                    dividends.add(magnitude.multiply(BigInteger.valueOf(dividendSign)));
                    divisors.add(divisor.multiply(BigInteger.valueOf(divisorSign)));
                }
            }
        }
    }

    private static BigInteger randomBelow(Random random, BigInteger boundExclusive)
    {
        if (boundExclusive.compareTo(BigInteger.ONE) <= 0) {
            return BigInteger.ZERO;
        }
        // Draw the bit length first so that magnitudes spread over the whole range instead of clustering at the top
        return new BigInteger(1 + random.nextInt(boundExclusive.bitLength()), random).mod(boundExclusive);
    }

    private void testDecimalBinaryOp(OperatorType operatorType, DecimalType leftType, DecimalType rightType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(leftType, rightType);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(leftType, positionsCount, nullsProvider),
                createBlock(rightType, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveOperator(operatorType, List.of(leftType, rightType)),
                ImmutableList.of(field(channelA, leftType), field(channelB, rightType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @MethodSource("comparisonTestCases")
    public void testComparison(Type type, ComparisonOperator operator, NullsProvider nullsProvider)
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
        Set<Integer> inputChannels = Set.of(channelA, channelB);

        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        if (operator == ComparisonOperator.IDENTICAL && (type == REAL || type == DOUBLE)) {
            // operator currently not supported
            assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD))
                    .isEmpty();
            return;
        }
        assertGpuMatchesCpu(inputPages, inputTypes, expression, inputChannels);
    }

    public static Stream<Object[]> comparisonTestCases()
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
    @EnumSource(NullsProvider.class)
    public void testAnd(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // AND of two comparisons: a < 50 AND b > 0
        Expression left = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression right = comparison(
                ComparisonOperator.LESS_THAN,
                new Constant(BIGINT, 0L),
                field(channelB, BIGINT));
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(left, right));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testOr(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // OR of two comparisons: a < 50 OR b > 0
        Expression left = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression right = comparison(
                ComparisonOperator.LESS_THAN,
                new Constant(BIGINT, 0L),
                field(channelB, BIGINT));
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(left, right));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testAndNullSemantics()
    {
        List<Type> inputTypes = List.of(BOOLEAN, BOOLEAN);
        List<Page> inputPages = List.of(new Page(booleanBlock(TRUTH_TABLE_LEFT), booleanBlock(TRUTH_TABLE_RIGHT)));
        Expression expression = new Logical(Logical.Operator.AND, ImmutableList.of(field(0, BOOLEAN), field(1, BOOLEAN)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @Test
    public void testOrNullSemantics()
    {
        List<Type> inputTypes = List.of(BOOLEAN, BOOLEAN);
        List<Page> inputPages = List.of(new Page(booleanBlock(TRUTH_TABLE_LEFT), booleanBlock(TRUTH_TABLE_RIGHT)));
        Expression expression = new Logical(Logical.Operator.OR, ImmutableList.of(field(0, BOOLEAN), field(1, BOOLEAN)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testNot(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // NOT of a comparison: NOT(a < 50)
        Expression comparison = comparison(
                ComparisonOperator.LESS_THAN,
                field(channelA, BIGINT),
                new Constant(BIGINT, 50L));
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIsNull(NullsProvider nullsProvider)
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
    public void testBetween(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // a BETWEEN 10 AND 50
        Expression expression = between(
                field(channelA, BIGINT),
                new Constant(BIGINT, 10L),
                new Constant(BIGINT, 50L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testBetweenNonTrivialValue(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // (a + b) BETWEEN 10 AND 50 — the non-trivial value is bound with a Let so it is evaluated once
        Expression expression = between(
                new Call(
                        FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                        ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT))),
                new Constant(BIGINT, 10L),
                new Constant(BIGINT, 50L));
        assertThat(expression).isInstanceOf(Let.class);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testNestedLet(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // Let s1 = ref0 + ref1 in (Let s2 = s1 + ref2 in (s1 + s2))
        // Body references s1 (de Bruijn index 1) and s2 (index 0); the inner value references s1 (index 0).
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
    public void testSiblingLets(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // (Let s1 = ref0 + ref1 in s1 + s1) + (Let s2 = ref0 - ref1 in s2 + s2)
        // Sibling bindings are never live simultaneously, so both reuse the same tail slot (index 0).
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
    @MethodSource("coalesceTestCases")
    public void testCoalesce(Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Coalesce(ImmutableList.of(field(channelA, type), field(channelB, type)));

        if (type == VARBINARY) {
            // cudf replaceNulls has no implementation for the LIST<UINT8> column carrying varbinary
            assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD)).isEmpty();
            return;
        }
        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    public static Stream<Object[]> coalesceTestCases()
    {
        return Lists.cartesianProduct(TESTED_GPU_TYPES, ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testCoalesceManyOperands(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // The trailing constant is never NULL, so with ALL_NULLS inputs the last operand is the one selected.
        Expression expression = new Coalesce(ImmutableList.of(
                field(0, BIGINT),
                field(1, BIGINT),
                field(2, BIGINT),
                field(3, BIGINT),
                new Constant(BIGINT, 42L)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1, 2, 3));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testCoalesceWithCoercedOperand(NullsProvider nullsProvider)
    {
        // COALESCE(bigint, integer) is coerced by the analyzer, so the IR carries a Cast operand.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, INTEGER);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(BIGINT, positionsCount, nullsProvider),
                createBlock(INTEGER, positionsCount, nullsProvider)));

        Expression expression = new Coalesce(ImmutableList.of(
                field(channelA, BIGINT),
                new Cast(field(channelB, INTEGER), BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testCoalesceSelectsFirstNonNullOperand()
    {
        int positionsCount = 60;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        int firstSelected = 0;
        int secondSelected = 0;
        int neitherSelected = 0;
        for (int i = 0; i < positionsCount; i++) {
            boolean aIsNull = i % 3 == 0;
            boolean bIsNull = i % 2 == 0;
            if (aIsNull) {
                aBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(aBuilder, i);
            }
            if (bIsNull) {
                bBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(bBuilder, 1000 + i);
            }
            if (!aIsNull) {
                firstSelected++;
            }
            else if (!bIsNull) {
                secondSelected++;
            }
            else {
                neitherSelected++;
            }
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        // test's self-test: the NULL pattern above has to make COALESCE pick the first operand on some
        // rows, the second operand on others, and neither on the rest. Without all three outcomes the
        // assertion below would still pass while never exercising operands past the first.
        assertThat(firstSelected).as("rows taking the first operand").isEqualTo(40);
        assertThat(secondSelected).as("rows taking the second operand").isEqualTo(10);
        assertThat(neitherSelected).as("rows where both operands are NULL").isEqualTo(10);

        Expression expression = new Coalesce(ImmutableList.of(field(0, BIGINT), field(1, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @ParameterizedTest
    @MethodSource("leastTestCases")
    public void testLeast(Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(type, type)),
                ImmutableList.of(field(channelA, type), field(channelB, type)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    public static Stream<Object[]> leastTestCases()
    {
        return Lists.cartesianProduct(
                        List.of(
                                BOOLEAN,
                                TINYINT,
                                SMALLINT,
                                INTEGER,
                                BIGINT,
                                createDecimalType(3, 0),
                                createDecimalType(13, 0),
                                createDecimalType(27, 0),
                                createDecimalType(27, 5),
                                createDecimalType(38),
                                createCharType(8),
                                VARCHAR,
                                createVarcharType(20),
                                DATE,
                                createTimestampType(0),
                                createTimestampType(3),
                                createTimestampType(6),
                                createTimestampType(9)),
                        ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLeastManyArguments(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        int channelC = 2;
        int channelD = 3;
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(BIGINT, BIGINT, BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT), field(channelC, BIGINT), field(channelD, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB, channelC, channelD));
    }

    @Test
    public void testLeastWithNullConstant()
    {
        // A NULL operand makes every row NULL, regardless of the other operands.
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), new Constant(BIGINT, null)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testLeastCharPaddedSemantics()
    {
        // U+0001 is less than space (U+0020), so least() would return wrong results without proper CHAR padding
        Type charType = createCharType(2);
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(charType, charType);
        List<Page> inputPages = List.of(new Page(
                charBlock(charType, "a", "a\u0001", "b", "a"),
                charBlock(charType, "a\u0001", "a", "a", "ab")));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(charType, charType)),
                ImmutableList.of(field(channelA, charType), field(channelB, charType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(ComparisonOperator.class)
    public void testComparisonCharPaddedSemantics(ComparisonOperator operator)
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

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    private static Block charBlock(Type charType, String... values)
    {
        BlockBuilder builder = charType.createBlockBuilder(null, values.length);
        for (String value : values) {
            charType.writeSlice(builder, Slices.utf8Slice(value));
        }
        return builder.build();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIf(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // IF(a < 50, a, b)
        Expression condition = comparison(ComparisonOperator.LESS_THAN, field(channelA, BIGINT), new Constant(BIGINT, 50L));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(condition, field(channelA, BIGINT))),
                field(channelB, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testIfLazyEvaluation()
    {
        // IF(b != 0, a / b, a) must not throw division-by-zero on rows where b = 0,
        // because the CPU short-circuits and never evaluates the divide for those rows.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, random.nextLong(-1000, 1000));
            // Mix zero, negative, and positive divisors so both branches are exercised.
            BIGINT.writeLong(bBuilder, switch (i % 3) {
                case 0 -> 0L;
                case 1 -> random.nextLong(1, 100);
                default -> -random.nextLong(1, 100);
            });
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(bNotZero, aDivB)),
                field(channelA, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIfLazyEvaluationDefeatsInputMasking(NullsProvider nullsProvider)
    {
        // IF(b != 0, 10 / coalesce(b, 0), 42): the true branch reads b, but coalesce
        // resurrects a non-null 0 from a NULL b. CPU short-circuits and never invokes the
        // divide for rows with b = 0 or b IS NULL, so the result is 42 there. The GPU must
        // not evaluate the true branch on those rows either — input-NULL masking would
        // be defeated by coalesce and trigger DIVISION_BY_ZERO.
        int channelB = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        long[] isValid = nullsProvider.getValidityWords(positionsCount).orElse(Bitmap.allocateWords(positionsCount, true));
        for (int i = 0; i < positionsCount; i++) {
            if (!Bitmap.isSet(isValid, 0, i)) {
                bBuilder.appendNull();
            }
            else {
                // Mix zero and non-zero divisors so both branches are exercised.
                BIGINT.writeLong(bBuilder, i % 3 == 0 ? 0L : random.nextLong(1, 100));
            }
        }
        List<Page> inputPages = List.of(new Page(positionsCount, bBuilder.build()));

        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression coalesceB = new Coalesce(ImmutableList.of(field(channelB, BIGINT), new Constant(BIGINT, 0L)));
        Expression divide = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(new Constant(BIGINT, 10L), coalesceB));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(bNotZero, divide)),
                new Constant(BIGINT, 42L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelB));
    }

    @Test
    public void testCaseEmptyInput()
    {
        List<Type> inputTypes = List.of(BIGINT);
        List<Page> inputPages = List.of(new Page(
                0,
                createBigintBlock(0, NullsProvider.NO_NULLS, -100, 100)));

        int channelA = 0;
        Expression aLtZero = comparison(ComparisonOperator.LESS_THAN, field(channelA, BIGINT), new Constant(BIGINT, 0L));
        Expression aLtFifty = comparison(ComparisonOperator.LESS_THAN, field(channelA, BIGINT), new Constant(BIGINT, 50L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aLtZero, new Constant(BIGINT, -1L)),
                        new WhenClause(aLtFifty, new Constant(BIGINT, 0L))),
                new Constant(BIGINT, 1L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testCaseNoMatch()
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        // CASE WHEN a = 999 THEN 1 WHEN a = 998 THEN 2 ELSE 42 END
        Expression aEq999 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 999L));
        Expression aEq998 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 998L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aEq999, new Constant(BIGINT, 1L)),
                        new WhenClause(aEq998, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 42L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testCaseNoMatchNoElse()
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        // CASE WHEN a = 999 THEN 1 WHEN a = 998 THEN 2 END → all rows get null
        Expression aEq999 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 999L));
        Expression aEq998 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 998L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aEq999, new Constant(BIGINT, 1L)),
                        new WhenClause(aEq998, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testCaseNullCondition()
    {
        // CASE WHEN null THEN 1 WHEN null THEN 2 ELSE 3 END
        // Both conditions are literal NULLs, treated as FALSE → all rows hit default.
        int positionsCount = 64;
        List<Type> inputTypes = List.of(BIGINT);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, null), new Constant(BIGINT, 1L)),
                        new WhenClause(new Constant(BOOLEAN, null), new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    public void testCaseNullConditionGuardsFallibleResult()
    {
        // CASE WHEN b > 0 THEN a / coalesce(b, 0) WHEN true THEN 7 ELSE -3 END
        // When b is NULL, the condition (b > 0) evaluates to NULL → treated as FALSE.
        // The result a / coalesce(b, 0) must NOT be evaluated on those rows — coalesce
        // would resurrect 0 from the NULL and trigger DIVISION_BY_ZERO.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, 100L);
            if (i % 2 == 0) {
                bBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(bBuilder, i);
            }
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression coalesceB = new Coalesce(ImmutableList.of(field(channelB, BIGINT), new Constant(BIGINT, 0L)));
        Expression aDivCoalesceB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), coalesceB));
        Expression bPositive = comparison(ComparisonOperator.GREATER_THAN, field(channelB, BIGINT), new Constant(BIGINT, 0L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(bPositive, aDivCoalesceB),
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 7L))),
                new Constant(BIGINT, -3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testCaseNoInputColumns()
    {
        // CASE WHEN true THEN 1 WHEN false THEN 2 ELSE 3 END
        int positionsCount = 64;
        List<Type> inputTypes = List.of(BIGINT);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 1L)),
                        new WhenClause(new Constant(BOOLEAN, false), new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testCaseManyBranches(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, 0, 15)));

        // CASE WHEN a = 0 THEN 0 WHEN a = 1 THEN 1 ... WHEN a = 9 THEN 9 ELSE -1 END
        ImmutableList.Builder<WhenClause> whenClauses = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            whenClauses.add(new WhenClause(
                    comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, (long) i)),
                    new Constant(BIGINT, (long) i)));
        }
        Expression expression = new Case(whenClauses.build(), new Constant(BIGINT, -1L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testCaseLazyCondition()
    {
        // CASE WHEN b = 0 THEN 1 WHEN (a / b) > 5 THEN 2 ELSE 3 END
        // Rows where b = 0 match the first branch. The second branch's condition (a / b)
        // is never evaluated on those rows, avoiding DIVISION_BY_ZERO.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, random.nextLong(-1000, 1000));
            BIGINT.writeLong(bBuilder, switch (i % 3) {
                case 0 -> 0L;
                case 1 -> random.nextLong(1, 100);
                default -> -random.nextLong(1, 100);
            });
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression bEqZero = comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L));
        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression aDivBGt5 = comparison(ComparisonOperator.GREATER_THAN, aDivB, new Constant(BIGINT, 5L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(bEqZero, new Constant(BIGINT, 1L)),
                        new WhenClause(aDivBGt5, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testCaseLazyResult()
    {
        // CASE WHEN b != 0 THEN a / b WHEN true THEN 0 END
        // a / b is only evaluated on rows where b != 0.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, random.nextLong(-1000, 1000));
            BIGINT.writeLong(bBuilder, switch (i % 3) {
                case 0 -> 0L;
                case 1 -> random.nextLong(1, 100);
                default -> -random.nextLong(1, 100);
            });
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(bNotZero, aDivB),
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 0L))),
                new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testCaseFailingCondition()
    {
        // CASE WHEN false THEN 1 WHEN (a / b) > 5 THEN 2 ELSE 3 END
        // First branch doesn't match, so the second condition (a / b) IS evaluated with b = 0.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(aBuilder, 10L);
        BIGINT.writeLong(bBuilder, 0L);
        List<Page> inputPages = List.of(new Page(1, aBuilder.build(), bBuilder.build()));

        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression aDivBGt5 = comparison(ComparisonOperator.GREATER_THAN, aDivB, new Constant(BIGINT, 5L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, false), new Constant(BIGINT, 1L)),
                        new WhenClause(aDivBGt5, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testCaseFailingResult()
    {
        // CASE WHEN true THEN a / b WHEN true THEN 0 END
        // First branch matches and evaluates a / b with b = 0 → DIVISION_BY_ZERO.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(aBuilder, 10L);
        BIGINT.writeLong(bBuilder, 0L);
        List<Page> inputPages = List.of(new Page(1, aBuilder.build(), bBuilder.build()));

        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, true), aDivB),
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 0L))),
                new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    @Test
    public void testCaseComplexResultExpressions()
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        // CASE WHEN a > 0 THEN a + 1 WHEN a < 0 THEN a * 2 ELSE a END
        Expression aRef = field(channelA, BIGINT);
        Expression aGtZero = comparison(ComparisonOperator.GREATER_THAN, aRef, new Constant(BIGINT, 0L));
        Expression aLtZero = comparison(ComparisonOperator.LESS_THAN, aRef, new Constant(BIGINT, 0L));
        Expression aPlus1 = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(aRef, new Constant(BIGINT, 1L)));
        Expression aTimes2 = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(BIGINT, BIGINT)),
                ImmutableList.of(aRef, new Constant(BIGINT, 2L)));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aGtZero, aPlus1),
                        new WhenClause(aLtZero, aTimes2)),
                aRef);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @MethodSource("nullIfTestCases")
    public void testNullIf(Type type, List<Object> values, NullsProvider nullsProvider)
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

    public static Stream<Arguments> nullIfTestCases()
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
    public void testNullIfWithNullSecondOperand(NullsProvider nullsProvider)
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
    public void testNullIfWithNonTrivialValue(NullsProvider nullsProvider)
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
    public void testNullIfWithIntegerZero(Type type, List<Object> values, NullsProvider nullsProvider)
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

    public static Stream<Arguments> nullIfWithIntegerZeroTestCases()
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
    public void testNullIfWithMixedOperandTypes(NullsProvider nullsProvider)
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

    /// Builds a block repeating each of `values` `repeats` times in turn, cycling until
    /// `positionsCount` positions are filled, with NULLs wherever `nullsProvider` asks for one.
    private static Block createRepeatingValuesBlock(Type type, int positionsCount, NullsProvider nullsProvider, List<Object> values, int repeats)
    {
        Optional<long[]> validity = nullsProvider.getValidityWords(positionsCount);
        BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            if (validity.isPresent() && !Bitmap.isSet(validity.get(), 0, i)) {
                builder.appendNull();
            }
            else {
                writeNativeValue(type, builder, values.get((i / repeats) % values.size()));
            }
        }
        return builder.build();
    }

    @ParameterizedTest
    @MethodSource("charToVarcharCastTestCases")
    public void testCastCharToVarchar(Type targetType, CharVarcharCoercion charVarcharCoercion, NullsProvider nullsProvider)
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

    public static Stream<Object[]> charToVarcharCastTestCases()
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
    public void testTypeOnlyCast(NullsProvider nullsProvider)
    {
        // VARCHAR(10) -> VARCHAR(20) shares the same cudf STRING dtype, so visitCast should
        // skip emitting GpuCast and forward the inner expression directly.
        int channelA = 0;
        Type sourceType = createVarcharType(10);
        Type targetType = createVarcharType(20);
        List<Type> inputTypes = List.of(sourceType);
        List<Page> inputPages = createVarcharPages(List.of(64), nullsProvider);

        Expression expression = new Cast(field(channelA, sourceType), targetType);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testDateTimeExtract(NullsProvider nullsProvider)
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
    public void testYearExtract(NullsProvider nullsProvider)
    {
        // cuDF's year() returns INT16, so it can only represent years in the range [-32768, 32767].
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
    public void testYearExtractOutOfRangeThrows()
    {
        // Year 100000 is outside cuDF's INT16 year range, so GPU must throw. CPU would happily return 100000.
        int channelA = 0;
        Type type = DATE;
        long outOfRangeDays = LocalDate.of(100_000, 6, 15).toEpochDay();
        List<Page> inputPages = List.of(new Page(writeNativeValue(type, outOfRangeDays)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)), SQL_STANDARD)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Year out of range supported by GPU: must be in [-32768, 32767]");
    }

    private static Block createInRangeDateTimeBlock(Type type, int positionsCount, NullsProvider nullsProvider)
    {
        // Constrain to roughly years [-10000, +10000] so cuDF's INT16 year extraction does not overflow.
        Random random = new Random(42);
        Optional<Bitmap> isValid = nullsProvider.getValidityWords(positionsCount).map(valid -> new Bitmap(valid, 0, positionsCount));
        BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            if (isValid.isPresent() && !isValid.get().isSet(i)) {
                builder.appendNull();
            }
            else if (type == DATE) {
                // Days since epoch in ±10000 years (~3.65M days).
                type.writeLong(builder, random.nextInt(7_300_001) - 3_650_000);
            }
            else if (type instanceof TimestampType timestampType && timestampType.isShort()) {
                // Trino short TimestampType stores epochMicros; pick microsecond values in ±10000 years.
                long maxMicros = 10_000L * 365L * 24L * 60L * 60L * 1_000_000L;
                long micros = (random.nextLong() % maxMicros);
                // Precision p < 6 requires the last (6-p) decimal digits to be zero.
                long scale = 1L;
                for (int p = timestampType.getPrecision(); p < TimestampType.MAX_SHORT_PRECISION; p++) {
                    scale *= 10;
                }
                type.writeLong(builder, (micros / scale) * scale);
            }
            else if (type instanceof TimestampType timestampType) {
                // Limit to values that can be represented in 64-bit with nanosecond precision, also after e.g. date_trunc(day)
                long epochNanos = clamp(random.nextLong(), Long.MIN_VALUE + NANOSECONDS_PER_DAY, Long.MAX_VALUE);
                // Align to the declared precision (zero the last 9-p decimal digits of nanos).
                epochNanos = Timestamps.round(epochNanos, 9 - timestampType.getPrecision());
                long epochMicros = floorDiv(epochNanos, NANOSECONDS_PER_MICROSECOND);
                int picosOfMicro = floorMod(epochNanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                type.writeObject(builder, new LongTimestamp(epochMicros, picosOfMicro));
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
        return builder.build();
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
    public void testDateTrunc(NullsProvider nullsProvider)
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

        Type unitType = createVarcharType(unit.length());
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_trunc", fromTypes(unitType, timestampType)),
                ImmutableList.of(new Constant(unitType, Slices.utf8Slice(unit)), field(channelA, timestampType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testDateTruncWithUnitInDifferentCases()
    {
        for (String unit : List.of("minute", "MINUTE", "MiNuTe")) {
            testDateTrunc(unit, createTimestampType(6), NullsProvider.RANDOM_NULLS);
        }
    }

    @Test
    public void testLower()
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
    public void testLowerRejectedCodePoints()
    {
        Type type = VARCHAR;
        for (int codePoint : UNSUPPORTED_CODE_POINTS) {
            List<Page> inputPages = List.of(new Page(writeNativeValue(type, Slices.utf8Slice(new String(Character.toChars(codePoint))))));
            Expression expression = createLowerExpression(type);
            CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)), SQL_STANDARD)
                    .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
            assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                    .describedAs("U+%04X", codePoint)
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("lower() on GPU does not support code points whose lowercase mapping differs between cuDF and Trino");
        }
    }

    @Test
    public void testUnsupportedCodePointsDisagreeBetweenCudfAndTrino()
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
    public void testLowerReconcilesAndHandlesNulls()
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

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLastDayOfMonth(NullsProvider nullsProvider)
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
        // dates with years outside [-32768, 32767] produce incorrect results.
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createInRangeDateTimeBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("last_day_of_month", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testLastDayOfMonthCornerCases()
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
                    assertThatThrownBy(() -> assertGpuMatchesCpu(outOfRangePages, inputTypes, expression, Set.of(channelA)))
                            .isInstanceOf(ArithmeticException.class);
                    continue;
                }
                type.writeObject(builder, new LongTimestamp(epochMicros, 0));
            }
        }
        List<Page> inputPages = List.of(new Page(builder.build()));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    public void testLastDayOfMonthOutOfRangeThrows()
    {
        int channelA = 0;
        Type type = DATE;
        long outOfRangeDays = LocalDate.of(100_000, 6, 15).toEpochDay();
        List<Page> inputPages = List.of(new Page(writeNativeValue(type, outOfRangeDays)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("last_day_of_month", fromTypes(type)),
                ImmutableList.of(field(channelA, type)));

        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)), SQL_STANDARD)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Year out of range supported by GPU: must be in [-32768, 32767]");
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testStringLength(NullsProvider nullsProvider)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = createVarcharPages(List.of(64), nullsProvider);

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("length", fromTypes(VARCHAR)),
                ImmutableList.of(field(varcharChannel, VARCHAR)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testSubstring(NullsProvider nullsProvider)
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
    public void testSubstringWithLength(NullsProvider nullsProvider)
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
    public void testReplace(NullsProvider nullsProvider)
    {
        for (String search : REPLACE_SEARCH_STRINGS) {
            testReplace(nullsProvider, search, Optional.empty());
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testReplaceWithReplacement(NullsProvider nullsProvider)
    {
        for (String search : REPLACE_SEARCH_STRINGS) {
            for (String replacement : List.of("", "X", "ab", "aa", "łąka", "ワ")) {
                testReplace(nullsProvider, search, Optional.of(replacement));
            }
        }
    }

    @Test
    public void testReplaceWithNonConstantArgument()
    {
        // cuDF's literal string replace takes a scalar target and replacement, so both must be constants
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);

        Expression nonConstantSearch = new Call(
                FUNCTION_RESOLUTION.resolveFunction("replace", fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR)));
        assertThat(compileExpression(nonConstantSearch, layoutFor(inputTypes), SQL_STANDARD)).isEmpty();

        Expression nonConstantReplacement = new Call(
                FUNCTION_RESOLUTION.resolveFunction("replace", fromTypes(VARCHAR, VARCHAR, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), new Constant(VARCHAR, Slices.utf8Slice("a")), field(1, VARCHAR)));
        assertThat(compileExpression(nonConstantReplacement, layoutFor(inputTypes), SQL_STANDARD)).isEmpty();
    }

    private void testReplace(NullsProvider nullsProvider, String search, Optional<String> replacement)
    {
        int varcharChannel = 0;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = ImmutableList.<Page>builder()
                .addAll(createVarcharPages(List.of(64), nullsProvider))
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
            assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD)).isEmpty();
            return;
        }
        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(varcharChannel));
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
    public void testTrimWhitespace(NullsProvider nullsProvider)
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
    public void testTrimCodePoints(NullsProvider nullsProvider)
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
    public void testTrimNonConstantCodePointsDoesNotCompile()
    {
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);
        // The characters to trim are only known per row, so the whole call falls back to the CPU
        Expression nonConstantCodePoints = new Call(
                FUNCTION_RESOLUTION.getCoercion(VARCHAR, CODE_POINTS),
                ImmutableList.of(field(1, VARCHAR)));
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("trim", fromTypes(VARCHAR, CODE_POINTS)),
                ImmutableList.of(field(0, VARCHAR), nonConstantCodePoints));

        assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD))
                .isEmpty();
    }

    @Test
    public void testTrimNullCodePointsDoesNotCompile()
    {
        List<Type> inputTypes = List.of(VARCHAR);
        // A null set of characters makes the call return null; that is left to the CPU
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("trim", fromTypes(VARCHAR, CODE_POINTS)),
                ImmutableList.of(field(0, VARCHAR), new Constant(CODE_POINTS, null)));

        assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD))
                .isEmpty();
    }

    /// Trino's and cuDF's whitespace definitions differ, and choosing the wrong one is silently wrong
    /// rather than an error, so the agreement is checked for every code point instead of a sample.
    @Test
    public void testTrimWhitespaceMatchesCpuForAllCodePoints()
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
        CompiledExpression gpuExpression = compileExpression(expression, layout, SQL_STANDARD)
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
    public void testConcat(NullsProvider nullsProvider)
    {
        testConcat(2, nullsProvider);
        testConcat(4, nullsProvider);

        // concat is also defined for varbinary, which the GPU carries as LIST<INT8> rather than as strings
        assertThat(compileExpression(
                new Call(
                        FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(VARBINARY, VARBINARY)),
                        ImmutableList.of(field(0, VARBINARY), field(1, VARBINARY))),
                layoutFor(List.of(VARBINARY, VARBINARY)),
                SQL_STANDARD))
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
    public void testCharConcat(NullsProvider nullsProvider)
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
    public void testConcatOutputLengthLimit()
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

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testIn(NullsProvider nullsProvider)
    {
        testIn(BIGINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(INTEGER, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(SMALLINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(TINYINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(DOUBLE, List.of(-50.0, 0.0, 25.5, 50.0, 75.5), nullsProvider);
        testIn(REAL, List.of(
                (long) Float.floatToIntBits(-50.0f),
                (long) Float.floatToIntBits(0.0f),
                (long) Float.floatToIntBits(25.5f),
                (long) Float.floatToIntBits(50.0f),
                (long) Float.floatToIntBits(75.5f)), nullsProvider);
        testIn(VARCHAR, List.of(
                Slices.utf8Slice("test1"),
                Slices.utf8Slice("other"),
                Slices.utf8Slice("xyz")), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testInWithNull(NullsProvider nullsProvider)
    {
        testIn(BIGINT, Arrays.asList(10L, null, 50L), nullsProvider);
        testIn(BIGINT, Collections.singletonList(null), nullsProvider);
    }

    private void testIn(Type type, List<Object> inValues, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider)));

        ImmutableList.Builder<Expression> valueListBuilder = ImmutableList.builder();
        for (Object value : inValues) {
            valueListBuilder.add(new Constant(type, value));
        }

        Expression expression = new In(field(channelA, type), valueListBuilder.build());

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    private void testArithmetic(OperatorType operatorType, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -1000, 1000),
                createBigintBlock(positionsCount, nullsProvider, 1, 100)));  // non-zero to avoid division by zero

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveOperator(operatorType, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    private void testConstant(Expression constantExpression)
    {
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, 0, 100)));

        CompiledExpression gpuExpression = compileExpression(constantExpression, layoutFor(List.of()), SQL_STANDARD)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + constantExpression));
        assertThat(gpuExpression.inputChannels().getInputChannels()).isEmpty();

        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, constantExpression, gpuExpression);
        List<Page> cpuResults = executeWithCpu(inputPages, List.of(constantExpression), Map.of());
        assertSameDataInOrder(gpuResults, cpuResults, List.of(constantExpression.type()));
    }

    /**
     * Like {@link GpuTestUtils#assertGpuMatchesCpu(List, List, Expression, PageProcessor, CompiledExpression, boolean)},
     * but accepts {@code INVALID_CAST_ARGUMENT} from GPU when CPU returns {@code NUMERIC_VALUE_OUT_OF_RANGE}.
     * Used in the cast smoke loop.
     */
    // TODO: remove once CPU is fixed to consistently throw INVALID_CAST_ARGUMENT for ±Inf and finite OOR
    //  in float→int casts; switch the call site to plain assertGpuMatchesCpu.
    private void assertGpuMatchesCpuForCast(List<Page> inputPages, List<Type> inputTypes, Expression expression, PageProcessor cpuProcessor, CompiledExpression gpuExpression)
    {
        List<Page> cpuResults;
        try {
            cpuResults = executeWithCpu(cpuProcessor, inputPages);
        }
        catch (TrinoException cpuExecutionException) {
            assertThat(inputPages.stream().mapToLong(Page::getPositionCount).sum())
                    .describedAs("Cast smoke loop is single-row per page")
                    .isEqualTo(1);
            ErrorCodeSupplier[] acceptable = NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().equals(cpuExecutionException.getErrorCode())
                    ? new ErrorCodeSupplier[] {NUMERIC_VALUE_OUT_OF_RANGE, INVALID_CAST_ARGUMENT}
                    : new ErrorCodeSupplier[] {cpuExecutionException::getErrorCode};
            try {
                assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                        .hasErrorCode(acceptable);
            }
            catch (AssertionError failure) {
                failure.addSuppressed(new Exception("expression: " + expression));
                failure.addSuppressed(new Exception("inputTypes: " + inputTypes));
                failure.addSuppressed(new Exception("CPU execution exception", cpuExecutionException));
                throw failure;
            }
            return;
        }
        List<Page> gpuResults = executeWithGpu(inputPages, inputTypes, expression, gpuExpression);
        assertSameDataInOrder(gpuResults, cpuResults, List.of(expression.type()));
    }

    private Expression createLikeExpression(int channel, String pattern, Optional<Character> escape)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                ImmutableList.of(
                        field(channel, VARCHAR),
                        new Constant(LIKE_PATTERN, LikePattern.compile(pattern, escape))));
    }

    private List<Page> createVarcharPages(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Iterator<String> strings = generateInputStrings().iterator();
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<long[]> isValid = nullsProvider.getValidityWords(positionsCount);
                    VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionsCount, positionsCount * 10);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isValid.isPresent() && !Bitmap.isSet(isValid.get(), 0, i)) {
                            builder.appendNull();
                        }
                        else {
                            builder.writeEntry(Slices.utf8Slice(strings.next()));
                        }
                    }
                    return new Page(positionsCount, builder.build());
                })
                .collect(toImmutableList());
    }

    private Stream<String> generateInputStrings()
    {
        Random random = new Random(42);
        Stream<String> randomStrings = Stream.generate(() -> {
            int length = random.nextInt(21);
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = (char) random.nextInt(0, Character.MIN_SURROGATE - 1);
            }
            return new String(chars);
        });

        return Streams.zip(
                        Stream.generate(() -> testStrings).flatMap(List::stream),
                        randomStrings,
                        List::of)
                .flatMap(List::stream);
    }

    private static Stream<Integer> randomInts(int minInclusive, int maxExclusive)
    {
        Random random = new Random(42); // Fixed seed for reproducibility
        return IntStream.generate(() -> random.nextInt(minInclusive, maxExclusive))
                .boxed();
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

    private static Block booleanBlock(Boolean[] values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, values.length);
        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, value);
            }
        }
        return builder.build();
    }

    private static Expression buildUnaryOperatorExpression(OperatorType operator, ResolvedFunction function, Expression argument)
    {
        if (operator == OperatorType.CAST) {
            return new Cast(argument, function.signature().getReturnType());
        }
        // Note: SATURATED_FLOOR_CAST is still expressed as a Call (operator function), not a Cast IR node.
        return new Call(function, ImmutableList.of(argument));
    }

    private record Pair<F, S>(F first, S second)
    {
        Pair
        {
            requireNonNull(first, "first is null");
            requireNonNull(second, "second is null");
        }
    }
}
