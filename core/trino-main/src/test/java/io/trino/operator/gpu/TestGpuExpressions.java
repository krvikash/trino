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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuUpper;
import io.trino.operator.scalar.StringFunctions;
import io.trino.spi.Page;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.TEST_STRINGS;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createVarcharPages;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.getOnlyColumn;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.GpuTestUtils.varcharsBlock;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static io.trino.type.CodePointsType.CODE_POINTS;
import static java.lang.Math.min;
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
