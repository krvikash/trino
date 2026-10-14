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
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.TEST_STRINGS;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createVarcharPages;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Arrays.asList;
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
