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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.spi.Page;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.getOnlyColumn;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static io.trino.type.CodePointsType.CODE_POINTS;
import static java.lang.Math.min;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuTrim
{
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

    private static final List<TrimInput> TRIM_INPUTS = ImmutableList.of(
            new TrimInput(VARCHAR, TRIM_TEST_STRINGS),
            // Wide enough that every value keeps implicit trailing padding, so trailing space handling runs
            new TrimInput(createCharType(64), TRIM_TEST_STRINGS),
            // Tight, so each value's last character is genuinely last
            new TrimInput(createCharType(TRIM_CHAR_LENGTH), TRIM_EXACT_LENGTH_CHAR_STRINGS));

    /// Every code point that can appear in a varchar, computed once per test class instance. The
    /// surrogate block is excluded because unpaired surrogates are not valid UTF-8.
    private final int[] allValidCodePoints = allValidCodePoints();

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

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

                assertGpuMatchesCpu(createTrimInputPages(input, nullsProvider), inputTypes, expression);
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

                    assertGpuMatchesCpu(createTrimInputPages(input, nullsProvider), inputTypes, expression);
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

        List<Page> cpuResults = executeWithCpu(TEST_SESSION, compileCpuExpression(expression, layout), inputPages);
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
}
