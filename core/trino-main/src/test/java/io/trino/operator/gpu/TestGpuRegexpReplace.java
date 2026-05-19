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
import io.trino.FullConnectorSession;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuExpressionCompiler;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.planner.Symbol;
import io.trino.testing.TestingSession;
import org.assertj.core.api.AssertProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Streams.stream;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuRegexpReplace
{
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final GpuExpressionCompiler gpuCompiler = new GpuExpressionCompiler();

    @BeforeAll
    public static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testSimpleLiteralReplacement()
    {
        assertThat(regexpReplace("world", "earth"))
                .executesCorrectly("hello world", "no match here", "world world");
    }

    @Test
    void testCharacterClassReplacement()
    {
        assertThat(regexpReplace("[a-z]", "X"))
                .executesCorrectly("a1b2c3", "123", "abc");
    }

    @Test
    void testDoesNotCompileAlternation()
    {
        assertThat(regexpReplace("foo|bar", "X")).doesNotCompile();
        assertThat(regexpReplace("a|", "X")).doesNotCompile();
        assertThat(regexpReplace("|a", "X")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileBraceQuantifier()
    {
        assertThat(regexpReplace("a{3}", "X")).doesNotCompile();
        assertThat(regexpReplace("a{2,}", "X")).doesNotCompile();
        assertThat(regexpReplace("a{2,3}", "X")).doesNotCompile();
    }

    @Test
    void testLiteralWhitespaceInPattern()
    {
        assertThat(regexpReplace(" ", "_"))
                .executesCorrectly("a b", "a  b", "abc");
        assertThat(regexpReplace("a b", "X"))
                .executesCorrectly("a b", "ab", "a  b");
        assertThat(regexpReplace("\n", "_"))
                .executesCorrectly("a\nb", "a\n\nb", "abc");
        assertThat(regexpReplace("\t", "_"))
                .executesCorrectly("a\tb", "a\t\tb", "abc");
    }

    @Test
    void testDoesNotCompileEscapeSequenceInPattern()
    {
        assertThat(regexpReplace("\\n", "X")).doesNotCompile();
        assertThat(regexpReplace("\\t", "X")).doesNotCompile();
        assertThat(regexpReplace("\\s", "X")).doesNotCompile();
        assertThat(regexpReplace("\\d", "X")).doesNotCompile();
        assertThat(regexpReplace("\\w", "X")).doesNotCompile();
    }

    @Test
    void testEscapedMetacharacters()
    {
        assertThat(regexpReplace("\\.", "X"))
                .executesCorrectly("a.b", "a..b", "abc");
        assertThat(regexpReplace("\\\\", "X"))
                .executesCorrectly("a\\b", "\\", "abc");
        assertThat(regexpReplace("\\(", "X"))
                .executesCorrectly("a(b", "(", "abc");
    }

    @Test
    void testDot()
    {
        assertThat(regexpReplace(".", "X"))
                .executesCorrectly("abc", "", "a");
    }

    @Test
    void testQuantifiers()
    {
        assertThat(regexpReplace("a+", "A"))
                .executesCorrectly("aaa", "aabaa", "bbb");
    }

    @Test
    void testDoesNotCompileLazyQuantifier()
    {
        assertThat(regexpReplace("a.*?b", "X")).doesNotCompile();
        assertThat(regexpReplace("a+?", "X")).doesNotCompile();
        assertThat(regexpReplace("a??", "X")).doesNotCompile();
    }

    @Test
    void testNonCapturingGroup()
    {
        assertThat(regexpReplace("(?:abc)+", "X"))
                .executesCorrectly("abcabc", "abc", "xabcx");
    }

    @Test
    void testAnchors()
    {
        assertThat(regexpReplace("^a", "X"))
                .executesCorrectly("abc", "bac", "aaa");
        assertThat(regexpReplace("z$", "X"))
                .executesCorrectly("xyz", "zyx", "zzz");
    }

    @Test
    void testMultipleMatches()
    {
        assertThat(regexpReplace("a", "bb"))
                .executesCorrectly("aaa", "aba", "bbb");
    }

    @Test
    void testNoMatch()
    {
        assertThat(regexpReplace("ZZZZZ", "X"))
                .executesCorrectly("hello", "", "ZZZZ");
    }

    @Test
    void testReplacementShorterThanMatch()
    {
        assertThat(regexpReplace("abc", "x"))
                .executesCorrectly("abcdef", "abcabc", "xabcx");
    }

    @Test
    void testEmptyReplacement()
    {
        assertThat(regexpReplace("abc", ""))
                .executesCorrectly("xabcx", "abcabc", "xyz");
    }

    @Test
    void testNegatedCharClass()
    {
        assertThat(regexpReplace("[^a-z]", "_"))
                .executesCorrectly("a1b2", "abc", "123", "a\rb");
    }

    @Test
    void testNegatedCharClassWithCarriageReturn()
    {
        assertThat(regexpReplace("[^a]", "_"))
                .executesCorrectly("a\ra", "\r", "a");
        assertThat(regexpReplace("[^a\n]", "_"))
                .executesCorrectly("a\r\na", "\r\n", "a");
        assertThat(regexpReplace("[^\r]", "X"))
                .executesCorrectly("a\rb", "\r\r", "abc");
        assertThat(regexpReplace("[^\r\n]", "X"))
                .executesCorrectly("a\r\nb", "\r\n", "abc");
    }

    @Test
    void testDoesNotCompileEscapedCarriageReturn()
    {
        assertThat(regexpReplace("[^\\r]", "X")).doesNotCompile();
    }

    @Test
    void testNegatedCharClassWithCarriageReturnInsideRange()
    {
        assertThat(regexpReplace("[^-]", "X"))
                .executesCorrectly("a\rb", "\r");
    }

    @Test
    void testDoesNotCompileShorthandEscape()
    {
        assertThat(regexpReplace("[^\\d]", "X")).doesNotCompile();
        assertThat(regexpReplace("[\\d]", "X")).doesNotCompile();
        assertThat(regexpReplace("[\\w]+", "X")).doesNotCompile();
        assertThat(regexpReplace("[\\s]", "X")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileCharEscape()
    {
        assertThat(regexpReplace("[\\n]", "X")).doesNotCompile();
        assertThat(regexpReplace("[\\t]", "X")).doesNotCompile();
        assertThat(regexpReplace("[\\r]", "X")).doesNotCompile();
        assertThat(regexpReplace("[\\b]", "X")).doesNotCompile();
    }

    @Test
    void testEscapedMetacharactersInCharClass()
    {
        assertThat(regexpReplace("[\\]]", "X"))
                .executesCorrectly("]", "a]b", "abc");
        assertThat(regexpReplace("[\\\\]", "X"))
                .executesCorrectly("\\", "a\\b", "abc");
        assertThat(regexpReplace("[\\^]", "X"))
                .executesCorrectly("^", "a^b", "abc");
    }

    @Test
    void testCaretAsLiteralInCharClass()
    {
        assertThat(regexpReplace("[a^b]", "X")).doesNotCompile();
        assertThat(regexpReplace("[^^]", "X")).doesNotCompile();
    }

    @Test
    void testInvalidPatterns()
    {
        // According to https://docs.rapids.ai/api/cudf/stable/libcudf_docs/md_regex/
        // cuDF behavior for the following cases is undefined, so we either fall back to CPU
        // or verify that they are rejected before they reach the transpiler.

        assertThat(regexpReplace("[a-]", "X")).doesNotCompile();
        assertThat(regexpReplace("[-z]", "X")).doesNotCompile();
        assertThat(regexpReplace("[-]", "X")).doesNotCompile();

        assertThat(regexpReplace("{", "x")).doesNotCompile();
        assertThat(regexpReplace("a{b", "x")).doesNotCompile();
        assertThat(regexpReplace("|", "x")).doesNotCompile();

        assertInvalidPattern("(abc", "x");
        assertInvalidPattern("abc)", "x");
        assertInvalidPattern("[abc", "x");

        assertThat(regexpReplace("()", "x")).doesNotCompile();
        assertInvalidPattern("[]", "x");
        assertThat(regexpReplace("(?:)", "x")).doesNotCompile();

        assertInvalidPattern("*a", "x");
        assertInvalidPattern("+a", "x");
        assertInvalidPattern("?a", "x");
        assertInvalidPattern("(+)", "x");
    }

    @Test
    void testMultipleRangesInCharClass()
    {
        assertThat(regexpReplace("[a-zA-Z0-9]", "X"))
                .executesCorrectly("aB3!", "hello", "!@#");
    }

    @Test
    void testDoesNotCompileNestedCharClass()
    {
        assertThat(regexpReplace("[a[b]]", "x")).doesNotCompile();
    }

    @Test
    void testNoReplacement()
    {
        assertThat(regexpReplace("[0-9]+"))
                .executesCorrectly("hello 123 world", "abc", "123");
    }

    @Test
    void testSingleGroupBackreference()
    {
        assertThat(regexpReplace("(a)", "[$1]"))
                .executesCorrectly("abc", "aaa", "xyz");
    }

    @Test
    void testMultiGroupReorder()
    {
        assertThat(regexpReplace("([0-9]+)-([0-9]+)-([0-9]+)", "$3/$2/$1"))
                .executesCorrectly("2024-01-15", "date: 2023-12-31!", "no date");
    }

    @Test
    void testFullMatchBackreference()
    {
        assertThat(regexpReplace("[0-9]+", "[$0]"))
                .executesCorrectly("abc 123 def 456", "no digits", "42");
    }

    @Test
    void testNestedGroups()
    {
        assertThat(regexpReplace("((a)(b))", "$1-$2-$3"))
                .executesCorrectly("abc", "ab", "xyz");
    }

    @Test
    void testMixedLiteralAndBackreference()
    {
        assertThat(regexpReplace("(a)(b)", "x$2y$1z"))
                .executesCorrectly("abc", "ab ab", "xyz");
    }

    @Test
    void testAdaptiveGroupNumbering()
    {
        assertThat(regexpReplace("(a)(b)(c)", "$01"))
                .executesCorrectly("abc", "xabcx");
    }

    @Test
    void testAdaptiveGroupNumberingWithTrailingDigit()
    {
        assertThat(regexpReplace("(a)(b)(c)", "$10"))
                .executesCorrectly("abc", "xabcx");
        assertThat(regexpReplace("(a)(b)(c)", "$11"))
                .executesCorrectly("abc", "xabcx");
    }

    @Test
    void testMaxGroupReference()
    {
        // $99 is the highest group reference cuDF supports
        // Build a pattern with 99 capturing groups: (a)(a)...(a)
        String pattern = "(a)".repeat(99);
        assertThat(regexpReplace(pattern, "$99"))
                .executesCorrectly("a".repeat(99));
    }

    @Test
    void testDoesNotCompileNonexistentGroupReference()
    {
        assertThat(regexpReplace("(h)", "$2")).doesNotCompile();
    }

    @Test
    void testGroupReferenceExceedsCudfLimit()
    {
        // $100 with 100 groups: cuDF only supports ${0}-${99}
        String pattern = "(a)".repeat(100);
        assertThat(regexpReplace(pattern, "$100")).doesNotCompile();
    }

    @Test
    void testAdaptiveNumberingFallsWithinLimit()
    {
        // Trino's adaptive group numbering: $31 with 3 groups resolves to
        // group 3 + literal "1" (since 31 > 3, stop at $3)
        assertThat(regexpReplace("(a)(b)(c)", "$31"))
                .executesCorrectly("abc", "xabcx");
    }

    @Test
    void testEscapedDollarInReplacement()
    {
        assertThat(regexpReplace("x", "\\$1"))
                .executesCorrectly("x", "xx", "abc");
    }

    @Test
    void testEscapedBackslashInReplacement()
    {
        assertThat(regexpReplace("x", "\\\\"))
                .executesCorrectly("x", "xx", "abc");
    }

    @Test
    void testUnicodePolishDiacritics()
    {
        assertThat(regexpReplace("[a-z]", "X"))
                .executesCorrectly("Łania szła");
        assertThat(regexpReplace("ł", "l"))
                .executesCorrectly("łąka", "Łania szła");
    }

    @Test
    void testUnicodeEastAsian()
    {
        assertThat(regexpReplace("草原", "field"))
                .executesCorrectly("美しい草原を");
    }

    @Test
    void testDoesNotCompileSupplementaryUnicode()
    {
        // cuDF regex matching is limited to BMP (U+0000 to U+FFFF)
        assertThat(regexpReplace("𝄞", "X")).doesNotCompile();
        assertThat(regexpReplace("[𝄞]", "X")).doesNotCompile();
        assertThat(regexpReplace("[𝄞-𝄢]", "X")).doesNotCompile();
        assertThat(regexpReplace("[a-𝄞]", "X")).doesNotCompile();
    }

    @Test
    void testDotMatchesSupplementaryUnicode()
    {
        assertThat(regexpReplace("a.a", "X"))
                .executesCorrectly("a𝄞a", "a𝄞b", "𝄞𝄞");
    }

    @Test
    void testEntireSourceMatches()
    {
        assertThat(regexpReplace(".*", "X"))
                .executesCorrectly("abc", "");
    }

    @Test
    void testNullPropagation()
    {
        assertThat(regexpReplace("abc", "X"))
                .executesCorrectlyWithNulls(null, "hello", null, "abc");
    }

    @Test
    void testClickBenchPattern()
    {
        assertThat(regexpReplace("^https?://(?:www\\.)?([^/]+)/.*$", "$1"))
                .executesCorrectly("https://www.example.com/path/to/page", "http://example.com/page", "not a url");
    }

    @Test
    void testDoesNotCompileBackreferenceInPattern()
    {
        assertThat(regexpReplace("(a)\\1", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileLookahead()
    {
        assertThat(regexpReplace("a(?=b)", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileLookbehind()
    {
        assertThat(regexpReplace("(?<=a)b", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileNamedGroup()
    {
        assertThat(regexpReplace("(?<name>x)", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompilePossessiveQuantifier()
    {
        assertThat(regexpReplace("a*+", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileAtomicGroup()
    {
        assertThat(regexpReplace("(?>ab)", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileUnicodeProperty()
    {
        assertThat(regexpReplace("\\p{Lu}", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileInlineFlags()
    {
        assertThat(regexpReplace("(?i)abc", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileWordBoundary()
    {
        assertThat(regexpReplace("\\bword\\b", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileZAnchorLowercase()
    {
        assertThat(regexpReplace("abc\\z", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileZAnchorUppercase()
    {
        assertThat(regexpReplace("abc\\Z", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileEmptyPattern()
    {
        assertThat(regexpReplace("", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileClosingBracketAsFirstCharInClass()
    {
        assertThat(regexpReplace("[]]", "X")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileCharClassIntersection()
    {
        assertThat(regexpReplace("[a-z&&[^m-p]]", "x")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileBackslashWithBackreference()
    {
        // cuDF has no escape mechanism for literal backslash in
        // stringReplaceWithBackrefs — \<digit> is always a backreference
        assertThat(regexpReplace("(x)", "\\\\$1")).doesNotCompile();
        assertThat(regexpReplace("(x)", "$1\\\\")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileNamedGroupInReplacement()
    {
        assertThat(regexpReplace("(x)", "${name}")).doesNotCompile();
        assertThat(regexpReplace("(x)", "${0}")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileUnknownEscapeInReplacement()
    {
        assertThat(regexpReplace("x", "\\n")).doesNotCompile();
        assertThat(regexpReplace("x", "\\t")).doesNotCompile();
        assertThat(regexpReplace("x", "\\a")).doesNotCompile();
        assertThat(regexpReplace("x", "\\0")).doesNotCompile();
        assertThat(regexpReplace("x", "\\1")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileTrailingDollarInReplacement()
    {
        assertThat(regexpReplace("x", "abc$")).doesNotCompile();
    }

    @Test
    void testDoesNotCompileNonConstantPattern()
    {
        Expression expression = new Call(
                functionResolution.resolveFunction("regexp_replace", fromTypes(VARCHAR, JONI_REGEXP, VARCHAR)),
                ImmutableList.of(
                        new Reference(VARCHAR, "ref0"),
                        new Reference(JONI_REGEXP, "ref1"),
                        new Constant(VARCHAR, utf8Slice("X"))));
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(VARCHAR, "ref0"), 0,
                new Symbol(JONI_REGEXP, "ref1"), 1);
        assertThat(gpuCompiler.compileExpression(expression, layout)).isEmpty();
    }

    @Test
    void testDoesNotCompileNonConstantReplacement()
    {
        Expression expression = new Call(
                functionResolution.resolveFunction("regexp_replace", fromTypes(VARCHAR, JONI_REGEXP, VARCHAR)),
                ImmutableList.of(
                        new Reference(VARCHAR, "ref0"),
                        new Constant(JONI_REGEXP, joniRegexp(utf8Slice("x"))),
                        new Reference(VARCHAR, "ref1")));
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(VARCHAR, "ref0"), 0,
                new Symbol(VARCHAR, "ref1"), 1);
        assertThat(gpuCompiler.compileExpression(expression, layout)).isEmpty();
    }

    private void assertInvalidPattern(String pattern, String replacement)
    {
        assertThatThrownBy(() -> regexpReplaceExpression(pattern, replacement))
                .isInstanceOf(TrinoException.class);
    }

    private AssertProvider<RegexpReplaceAssert> regexpReplace(String pattern, String replacement)
    {
        return () -> new RegexpReplaceAssert(regexpReplaceExpression(pattern, replacement), pattern, replacement);
    }

    private AssertProvider<RegexpReplaceAssert> regexpReplace(String pattern)
    {
        return () -> new RegexpReplaceAssert(regexpReplaceExpression(pattern), pattern, null);
    }

    private Expression regexpReplaceExpression(String pattern, String replacement)
    {
        return new Call(
                functionResolution.resolveFunction("regexp_replace", fromTypes(VARCHAR, JONI_REGEXP, VARCHAR)),
                ImmutableList.of(
                        new Reference(VARCHAR, "ref0"),
                        new Constant(JONI_REGEXP, joniRegexp(utf8Slice(pattern))),
                        new Constant(VARCHAR, utf8Slice(replacement))));
    }

    private Expression regexpReplaceExpression(String pattern)
    {
        return new Call(
                functionResolution.resolveFunction("regexp_replace", fromTypes(VARCHAR, JONI_REGEXP)),
                ImmutableList.of(
                        new Reference(VARCHAR, "ref0"),
                        new Constant(JONI_REGEXP, joniRegexp(utf8Slice(pattern)))));
    }

    private class RegexpReplaceAssert
    {
        private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(new Symbol(VARCHAR, "ref0"), 0);

        private final String expressionString;
        private final Expression expression;

        RegexpReplaceAssert(Expression expression, String pattern, String replacement)
        {
            this.expressionString = replacement == null
                            ? "regexp_replace('%s')".formatted(pattern)
                    : "regexp_replace('%s', '%s')".formatted(pattern, replacement);
            this.expression = requireNonNull(expression, "expression is null");
        }

        void doesNotCompile()
        {
            assertThat(gpuCompiler.compileExpression(expression, LAYOUT))
                    .describedAs("Expected GPU compilation to fail for %s", expressionString)
                    .isEmpty();
        }

        void executesCorrectly(String... inputs)
        {
            assertGpuMatchesCpu(List.of(createVarcharPage(inputs)));
        }

        void executesCorrectlyWithNulls(String... inputs)
        {
            assertGpuMatchesCpu(List.of(createNullableVarcharPage(inputs)));
        }

        private void assertGpuMatchesCpu(List<Page> inputPages)
        {
            List<Type> inputTypes = List.of(VARCHAR);

            CompiledExpression gpuExpression = gpuCompiler.compileExpression(expression, LAYOUT)
                    .orElseThrow(() -> new AssertionError("GPU compilation failed for " + expressionString));

            assertThat(gpuExpression.inputChannels().getInputChannels())
                    .containsExactly(0);

            PageProcessor cpuProcessor = compileCpuExpression(expression, LAYOUT);
            List<Page> cpuResults = executeWithCpu(cpuProcessor, inputPages);
            List<Page> gpuResults = executeGpuOperation(
                    inputPages,
                    inputTypes,
                    List.of(expression.type()),
                    copyToDevice -> new GpuProject(copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression))));

            assertSameDataInOrder(gpuResults, cpuResults, List.of(expression.type()));
        }
    }

    private PageProcessor compileCpuExpression(Expression expression, Map<Symbol, Integer> layout)
    {
        return functionResolution.getExpressionCompiler().compilePageProcessor(
                        false,
                        true,
                        false,
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        List.of(expression),
                        layout,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(InternalDynamicFilter.EMPTY);
    }

    private static List<Page> executeWithCpu(PageProcessor compiledProcessor, List<Page> inputPages)
    {
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        for (Page inputPage : inputPages) {
            Iterator<Optional<Page>> processed = compiledProcessor.process(FULL_CONNECTOR_SESSION, new DriverYieldSignal(), context, SourcePage.create(inputPage));
            stream(processed)
                    .flatMap(Optional::stream)
                    .forEachOrdered(outputPages::add);
        }
        return outputPages.build();
    }

    private static Page createVarcharPage(String... values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.length, values.length * 32);
        for (String value : values) {
            builder.writeEntry(utf8Slice(value));
        }
        return new Page(values.length, builder.build());
    }

    private static Page createNullableVarcharPage(String... values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.length, values.length * 32);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(utf8Slice(value));
            }
        }
        return new Page(values.length, builder.build());
    }
}
