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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBlock;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.GpuTestUtils.varcharsBlock;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuConcatWs
{
    private static final List<String> SEPARATORS = List.of(",", "", "م", " -- ");

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testConcatWs(NullsProvider nullsProvider)
    {
        for (String separator : SEPARATORS) {
            testConcatWs(separator, 1, nullsProvider);
            testConcatWs(separator, 2, nullsProvider);
            testConcatWs(separator, 4, nullsProvider);
        }
    }

    private void testConcatWs(String separator, int valueCount, NullsProvider nullsProvider)
    {
        int positionsCount = 64;
        List<Type> inputTypes = Collections.nCopies(valueCount, VARCHAR);

        Block[] randomBlocks = new Block[valueCount];
        Block[] edgeCaseBlocks = new Block[valueCount];
        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        arguments.add(new Constant(VARCHAR, utf8Slice(separator)));
        for (int channel = 0; channel < valueCount; channel++) {
            randomBlocks[channel] = createBlock(VARCHAR, positionsCount, nullsProvider);

            // Rows the random data does not cover: empty strings, values that differ between arguments
            // (so that argument order is verified), multi-byte UTF-8, a null in every argument position,
            // and every argument null
            List<String> values = new ArrayList<>();
            values.add("");
            values.add("value" + channel);
            values.add("Łania ワルシャワ Слава " + channel);
            for (int nullChannel = 0; nullChannel < valueCount; nullChannel++) {
                values.add(nullChannel == channel ? null : "value" + channel);
            }
            values.add(channel == 0 ? "" : null);
            values.add(null);
            edgeCaseBlocks[channel] = varcharsBlock(values);

            arguments.add(field(channel, VARCHAR));
        }

        List<Page> inputPages = List.of(
                new Page(positionsCount, randomBlocks),
                new Page(edgeCaseBlocks));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat_ws", fromTypes(ImmutableList.<Type>builder().add(VARCHAR).addAll(inputTypes).build())),
                arguments.build());

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testConcatWsWithCoalescedValues(NullsProvider nullsProvider)
    {
        // The shape of a row hash, where separators are also passed as values
        int positionsCount = 64;
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);
        Constant separator = new Constant(VARCHAR, utf8Slice("م"));
        Constant empty = new Constant(VARCHAR, utf8Slice(""));
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat_ws", fromTypes(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR)),
                ImmutableList.of(
                        separator,
                        new Coalesce(field(0, VARCHAR), empty),
                        separator,
                        new Coalesce(field(1, VARCHAR), empty),
                        separator));

        assertGpuMatchesCpu(
                List.of(new Page(
                        positionsCount,
                        createBlock(VARCHAR, positionsCount, nullsProvider),
                        createBlock(VARCHAR, positionsCount, nullsProvider))),
                inputTypes,
                expression);
    }

    @Test
    void testConcatWsOutputLengthLimit()
    {
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR, VARCHAR);
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat_ws", fromTypes(VARCHAR, VARCHAR, VARCHAR, VARCHAR)),
                ImmutableList.of(new Constant(VARCHAR, utf8Slice("ワ")), field(0, VARCHAR), field(1, VARCHAR), field(2, VARCHAR)));

        int maxOutputLength = 1024 * 1024;
        int separatorLength = utf8Slice("ワ").length();
        // Two values and one separator, right at the limit and one byte past it
        String atLimit = "a".repeat(maxOutputLength - separatorLength - 1);
        assertGpuMatchesCpu(List.of(threeStringPage(atLimit, "a", null)), inputTypes, expression);
        assertGpuMatchesCpu(List.of(threeStringPage(atLimit, "aa", null)), inputTypes, expression);
        // A NULL value adds no separator
        assertGpuMatchesCpu(List.of(threeStringPage(atLimit, null, "a")), inputTypes, expression);
        // An empty value does add one
        assertGpuMatchesCpu(List.of(threeStringPage(atLimit, "", "a")), inputTypes, expression);
        assertGpuMatchesCpu(List.of(threeStringPage("a".repeat(maxOutputLength), null, null)), inputTypes, expression);
        assertGpuMatchesCpu(List.of(threeStringPage("a".repeat(maxOutputLength + 1), null, null)), inputTypes, expression);
    }

    @Test
    void testConcatWsFallback()
    {
        // A non-constant separator
        List<Type> inputTypes = List.of(VARCHAR, VARCHAR);
        assertThat(compileExpression(
                new Call(
                        FUNCTION_RESOLUTION.resolveFunction("concat_ws", fromTypes(VARCHAR, VARCHAR)),
                        ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR))),
                layoutFor(inputTypes),
                SQL_STANDARD,
                TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();

        // A NULL separator
        assertThat(compileExpression(
                new Call(
                        FUNCTION_RESOLUTION.resolveFunction("concat_ws", fromTypes(VARCHAR, VARCHAR)),
                        ImmutableList.of(new Constant(VARCHAR, null), field(0, VARCHAR))),
                layoutFor(List.of(VARCHAR)),
                SQL_STANDARD,
                TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();

        // The array(varchar) overload
        ArrayType arrayType = new ArrayType(VARCHAR);
        assertThat(compileExpression(
                new Call(
                        FUNCTION_RESOLUTION.resolveFunction("concat_ws", fromTypes(VARCHAR, arrayType)),
                        ImmutableList.of(new Constant(VARCHAR, utf8Slice(",")), field(0, arrayType))),
                layoutFor(List.of(arrayType)),
                SQL_STANDARD,
                TEST_SESSION_TIME_ZONE_KEY))
                .isEmpty();
    }

    private static Page threeStringPage(String first, String second, String third)
    {
        return new Page(1, varcharsBlock(asList(first)), varcharsBlock(asList(second)), varcharsBlock(asList(third)));
    }
}
