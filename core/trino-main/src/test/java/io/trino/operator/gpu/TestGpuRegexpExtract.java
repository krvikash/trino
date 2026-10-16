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
import com.google.common.collect.ImmutableMap;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.spi.Page;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import jakarta.annotation.Nullable;
import org.assertj.core.api.AssertProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuRegexpExtract
{
    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(new Symbol(VARCHAR, "ref0"), 0);

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testWholeMatch()
    {
        assertThat(regexpExtract("[0-9]+"))
                .executesCorrectly("abc 123 def 456", "no digits", "42", "");
        assertThat(regexpExtract("[0-9]+", 0))
                .executesCorrectly("abc 123 def 456", "no digits", "42", "");
        assertThat(regexpExtract("(a)(b)", 0))
                .executesCorrectly("ab", "xaby", "ba", "");
    }

    @Test
    void testGroup()
    {
        assertThat(regexpExtract("([0-9]+)-([0-9]+)-([0-9]+)", 1))
                .executesCorrectly("2024-01-15", "date: 2023-12-31!", "no date", "1-2-3 4-5-6");
        assertThat(regexpExtract("([0-9]+)-([0-9]+)-([0-9]+)", 2))
                .executesCorrectly("2024-01-15", "date: 2023-12-31!", "no date", "1-2-3 4-5-6");
        assertThat(regexpExtract("([0-9]+)-([0-9]+)-([0-9]+)", 3))
                .executesCorrectly("2024-01-15", "date: 2023-12-31!", "no date", "1-2-3 4-5-6");
    }

    @Test
    void testNestedGroups()
    {
        for (int group = 0; group <= 4; group++) {
            assertThat(regexpExtract("((a)(b(c)))", group))
                    .executesCorrectly("abc", "xabcx", "ab", "abcabc");
        }
    }

    @Test
    void testNonCapturingGroupNotCounted()
    {
        for (int group = 0; group <= 2; group++) {
            assertThat(regexpExtract("(?:x(a))(?:y)(b)", group))
                    .executesCorrectly("xayb", "zxaybz", "xab", "");
        }
        assertThat(regexpExtract("(?:x(a))(?:y)(b)", 3)).doesNotCompile();
    }

    @Test
    void testNonParticipatingGroup()
    {
        for (int group = 0; group <= 2; group++) {
            assertThat(regexpExtract("(a)|(b)", group))
                    .executesCorrectly("a", "b", "ab", "ba", "c", "");
            assertThat(regexpExtract("(a)*(b)", group))
                    .executesCorrectly("b", "ab", "aab", "a", "");
        }
        for (int group = 0; group <= 1; group++) {
            assertThat(regexpExtract("x(a)?y", group))
                    .executesCorrectly("xy", "xay", "xaay", "y", "");
        }
    }

    @Test
    void testRepeatedGroup()
    {
        for (int group = 0; group <= 1; group++) {
            assertThat(regexpExtract("([a-z])+", group))
                    .executesCorrectly("abc", "a1bc", "123", "");
            assertThat(regexpExtract("(?:(a)|b)+", group))
                    .executesCorrectly("ab", "ba", "aba", "bab", "bb", "");
            assertThat(regexpExtract("(a+)+", group))
                    .executesCorrectly("b", "aab", "aa", "");
        }
        for (int group = 0; group <= 2; group++) {
            assertThat(regexpExtract("(?:(a)|(b))+", group))
                    .executesCorrectly("ab", "ba", "aba", "bab", "bb", "");
            assertThat(regexpExtract("(a|ab)(c|bcd)", group))
                    .executesCorrectly("abcd", "abc", "acd", "");
        }
    }

    @Test
    void testDoesNotCompileRepeatOfPossiblyEmpty()
    {
        // cuDF's regex compiler rejects these
        assertThat(regexpExtract("(a*)+", 1)).doesNotCompile();
        assertThat(regexpExtract("(a?)+", 1)).doesNotCompile();
        assertThat(regexpExtract("(a*)*", 1)).doesNotCompile();
        assertThat(regexpExtract("(?:a*b*)+")).doesNotCompile();
        assertThat(regexpExtract("(?:(a)|b?)+", 1)).doesNotCompile();
        assertThat(regexpExtract("(^)*", 1)).doesNotCompile();
        assertThat(regexpExtract("(^)?", 1)).doesNotCompile();
        assertThat(regexpExtract("(?:x$)+")).executesCorrectly("x", "xx", "");
        assertThat(regexpExtract("(?:a+)?")).executesCorrectly("", "b", "aab", "ba");
        assertThat(regexpExtract("(?:a+)+")).executesCorrectly("", "b", "aab", "ba");
    }

    @Test
    void testOptionalOfPossiblyEmpty()
    {
        for (int group = 0; group <= 1; group++) {
            assertThat(regexpExtract("(a?)?", group))
                    .executesCorrectly("", "a", "aa", "b", "ab", "ba");
            assertThat(regexpExtract("(a*)?", group))
                    .executesCorrectly("", "a", "aa", "b", "aab", "ba");
            assertThat(regexpExtract("x(a*)?y", group))
                    .executesCorrectly("xy", "xay", "xaay", "xby", "y");
        }
        assertThat(regexpExtract("(?:a?)?"))
                .executesCorrectly("", "a", "aa", "b", "ab", "ba");
    }

    @Test
    void testOptionalGroup()
    {
        for (int group = 0; group <= 1; group++) {
            assertThat(regexpExtract("(a)?", group))
                    .executesCorrectly("", "a", "aa", "b", "ab", "ba");
            assertThat(regexpExtract("(.)?", group))
                    .executesCorrectly("", "a", "ab", "\nb");
        }
        for (int group = 0; group <= 3; group++) {
            assertThat(regexpExtract("(www\\.)?([A-Za-z0-9_]+)\\.(com|gov|pl)", group))
                    .executesCorrectly("www.example.com", "example.gov", "www.foo_bar9.pl", "mail.www.example.com", "example.org", "www.com", "WWW.Example.COM", "");
        }
        // \w is not supported by the transpiler
        assertThat(regexpExtract("(www\\.)?(\\w+)\\.(com|gov|pl)", 2)).doesNotCompile();
    }

    @Test
    void testEmptyMatch()
    {
        assertThat(regexpExtract("a*"))
                .executesCorrectly("", "b", "ab", "ba", "aab");
        assertThat(regexpExtract("(a*)b", 1))
                .executesCorrectly("", "b", "ab", "ba", "aab", "cb");
        assertThat(regexpExtract("x(a?)", 1))
                .executesCorrectly("", "x", "xa", "ax", "xb");
        assertThat(regexpExtract("^", 0))
                .executesCorrectly("", "a", "ab");
        assertThat(regexpExtract("(^)", 1))
                .executesCorrectly("", "a", "ab");
        assertThat(regexpExtract("^(a?)", 1))
                .executesCorrectly("", "a", "ab", "ba");
        assertThat(regexpExtract("", 0)).doesNotCompile();
        assertThat(regexpExtract("()", 1)).doesNotCompile();
    }

    @Test
    void testAnchors()
    {
        assertThat(regexpExtract("^(a+)", 1))
                .executesCorrectly("aab", "baa", "a\naa", "");
        assertThat(regexpExtract("(a+)$", 1))
                .executesCorrectly("baa", "aab", "aa\na", "aa\n", "");
    }

    @Test
    void testClickBenchPattern()
    {
        assertThat(regexpExtract("^https?://(?:www\\.)?([^/]+)/.*$", 1))
                .executesCorrectly("https://www.example.com/path/to/page", "http://example.com/page", "not a url", "http://example.com");
    }

    @Test
    void testUnicode()
    {
        assertThat(regexpExtract("(ł[a-ząę]*)", 1))
                .executesCorrectly("Łania łąka szła", "łą", "abc", "");
        assertThat(regexpExtract("草(.)を", 1))
                .executesCorrectly("美しい草原を", "草を", "草𝄞を", "");
        assertThat(regexpExtract("a(.)b", 0))
                .executesCorrectly("a𝄞b", "xaąbx", "ab", "");
        assertThat(regexpExtract("(𝄞)", 1)).doesNotCompile();
    }

    @Test
    void testNullPropagation()
    {
        assertThat(regexpExtract("(a)", 1))
                .executesCorrectly(null, "a", null, "b");
    }

    @Test
    void testGroupOutOfRange()
    {
        // The CPU raises an error for these, so they must not run on the GPU
        assertThat(regexpExtract("abc", 1)).doesNotCompile();
        assertThat(regexpExtract("(a)(b)", 3)).doesNotCompile();
        assertThat(regexpExtract("(a)(b)", -1)).doesNotCompile();
        assertThat(regexpExtract("(a)(b)", Long.MAX_VALUE)).doesNotCompile();
        assertThat(regexpExtract("(a)(b)", Long.MIN_VALUE)).doesNotCompile();
    }

    @Test
    void testDoesNotCompileUnsupportedPattern()
    {
        assertThat(regexpExtract("(?<name>x)", 1)).doesNotCompile();
        assertThat(regexpExtract("(?i)(abc)", 1)).doesNotCompile();
        assertThat(regexpExtract("(a+?)", 1)).doesNotCompile();
        assertThat(regexpExtract("(\\d+)", 1)).doesNotCompile();
        assertThat(regexpExtract("(?=(a))", 1)).doesNotCompile();
    }

    @Test
    void testDoesNotCompileBackreferenceInPattern()
    {
        // Dropping the groups not extracted would renumber the groups a backreference refers to
        for (int group = 0; group <= 2; group++) {
            assertThat(regexpExtract("(a)(b)\\1", group)).doesNotCompile();
            assertThat(regexpExtract("(a)(b)\\2", group)).doesNotCompile();
        }
        assertThat(regexpExtract("(a)\\1", 1)).doesNotCompile();
        assertThat(regexpExtract("(?:a)(b)\\1", 1)).doesNotCompile();
        assertThat(regexpExtract("(?<x>a)(b)\\k<x>", 2)).doesNotCompile();
    }

    @Test
    void testDoesNotCompileNonConstantGroup()
    {
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("regexp_extract", fromTypes(VARCHAR, JONI_REGEXP, BIGINT)),
                ImmutableList.of(
                        new Reference(VARCHAR, "ref0"),
                        new Constant(JONI_REGEXP, joniRegexp(utf8Slice("(a)"))),
                        new Reference(BIGINT, "ref1")));
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(VARCHAR, "ref0"), 0,
                new Symbol(BIGINT, "ref1"), 1);
        assertThat(compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    @Test
    void testDoesNotCompileNullGroup()
    {
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("regexp_extract", fromTypes(VARCHAR, JONI_REGEXP, BIGINT)),
                ImmutableList.of(
                        new Reference(VARCHAR, "ref0"),
                        new Constant(JONI_REGEXP, joniRegexp(utf8Slice("(a)"))),
                        new Constant(BIGINT, null)));
        assertThat(compileExpression(expression, LAYOUT, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    @Test
    void testDoesNotCompileNonConstantPattern()
    {
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("regexp_extract", fromTypes(VARCHAR, JONI_REGEXP, BIGINT)),
                ImmutableList.of(
                        new Reference(VARCHAR, "ref0"),
                        new Reference(JONI_REGEXP, "ref1"),
                        new Constant(BIGINT, 1L)));
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(VARCHAR, "ref0"), 0,
                new Symbol(JONI_REGEXP, "ref1"), 1);
        assertThat(compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private static AssertProvider<RegexpExtractAssert> regexpExtract(String pattern)
    {
        return () -> new RegexpExtractAssert(pattern, OptionalLong.empty());
    }

    private static AssertProvider<RegexpExtractAssert> regexpExtract(String pattern, long group)
    {
        return () -> new RegexpExtractAssert(pattern, OptionalLong.of(group));
    }

    private static class RegexpExtractAssert
    {
        private final String pattern;
        private final String expressionString;
        private final Expression expression;

        RegexpExtractAssert(String pattern, OptionalLong group)
        {
            this.pattern = requireNonNull(pattern, "pattern is null");
            requireNonNull(group, "group is null");
            if (group.isPresent()) {
                this.expressionString = "regexp_extract('%s', %s)".formatted(pattern, group.orElseThrow());
                this.expression = new Call(
                        FUNCTION_RESOLUTION.resolveFunction("regexp_extract", fromTypes(VARCHAR, JONI_REGEXP, BIGINT)),
                        ImmutableList.of(
                                new Reference(VARCHAR, "ref0"),
                                new Constant(JONI_REGEXP, joniRegexp(utf8Slice(pattern))),
                                new Constant(BIGINT, group.orElseThrow())));
            }
            else {
                this.expressionString = "regexp_extract('%s')".formatted(pattern);
                this.expression = new Call(
                        FUNCTION_RESOLUTION.resolveFunction("regexp_extract", fromTypes(VARCHAR, JONI_REGEXP)),
                        ImmutableList.of(
                                new Reference(VARCHAR, "ref0"),
                                new Constant(JONI_REGEXP, joniRegexp(utf8Slice(pattern)))));
            }
        }

        void doesNotCompile()
        {
            assertThat(compileExpression(expression, LAYOUT, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .describedAs("Expected GPU compilation to fail for %s", expressionString)
                    .isEmpty();
        }

        void executesCorrectly(@Nullable String... inputs)
        {
            List<String> values = new ArrayList<>(asList(inputs));
            values.add(null);
            values.add(pattern);
            values.add(pattern + pattern);
            Page inputPage = createNullableVarcharPage(values);

            CompiledExpression gpuExpression = compileExpression(expression, LAYOUT, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                    .orElseThrow(() -> new AssertionError("GPU compilation failed for " + expressionString));
            assertThat(gpuExpression.inputChannels().getInputChannels())
                    .containsExactly(0);

            List<Type> outputTypes = List.of(expression.type());
            List<Page> cpuResults = executeWithCpu(List.of(inputPage), List.of(expression), LAYOUT);
            List<Page> gpuResults = executeGpuOperation(
                    List.of(inputPage),
                    List.of(VARCHAR),
                    outputTypes,
                    (context, copyToDevice) -> new GpuProject(context, copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression))));
            assertSameDataInOrder(gpuResults, cpuResults, outputTypes);
        }
    }

    private static Page createNullableVarcharPage(List</* nullable */ String> values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.size(), values.size() * 32);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(utf8Slice(value));
            }
        }
        return new Page(values.size(), builder.build());
    }
}
