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
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBlock;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuPad
{
    private static final List<Long> TARGET_LENGTHS = List.of(0L, 1L, 3L, 4L, 5L, 20L, 100L);
    private static final List<String> PAD_STRINGS = List.of("0", " ", "ą", "ワ");

    /// Values shorter than, equal to and longer than the target lengths, empty, and multi-byte UTF-8
    private static final List<String> PAD_EDGE_CASE_STRINGS = Arrays.asList(
            null,
            "",
            "7",
            "123",
            "1234",
            "12345",
            "Łania",
            "ワルシャワ近郊",
            "Слава Україні",
            "𐐀abc",
            "éclair");

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testPad(NullsProvider nullsProvider)
    {
        int positionsCount = 64;
        List<Type> inputTypes = List.of(VARCHAR);
        List<Page> inputPages = List.of(
                new Page(positionsCount, createBlock(VARCHAR, positionsCount, nullsProvider)),
                new Page(createStringBlock(PAD_EDGE_CASE_STRINGS)));

        for (String function : List.of("lpad", "rpad")) {
            for (long targetLength : TARGET_LENGTHS) {
                for (String padString : PAD_STRINGS) {
                    assertGpuMatchesCpu(inputPages, inputTypes, pad(function, field(0, VARCHAR), targetLength, padString));
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLeftPadIntegerCastToVarchar(NullsProvider nullsProvider)
    {
        int positionsCount = 64;
        List<Type> inputTypes = List.of(INTEGER);
        List<Page> inputPages = List.of(new Page(positionsCount, createBlock(INTEGER, positionsCount, nullsProvider)));
        Expression expression = pad("lpad", new Cast(field(0, INTEGER), VARCHAR), 4, "0");
        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {"lpad", "rpad"})
    void testPadUnsupportedArguments(String function)
    {
        // cuDF pads every row to one width with one character, so both must be constant
        assertNotCompiled(List.of(VARCHAR, BIGINT), new Call(
                FUNCTION_RESOLUTION.resolveFunction(function, fromTypes(VARCHAR, BIGINT, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), field(1, BIGINT), new Constant(VARCHAR, Slices.utf8Slice("0")))));
        assertNotCompiled(List.of(VARCHAR, VARCHAR), new Call(
                FUNCTION_RESOLUTION.resolveFunction(function, fromTypes(VARCHAR, BIGINT, VARCHAR)),
                ImmutableList.of(field(0, VARCHAR), new Constant(BIGINT, 4L), field(1, VARCHAR))));
        assertNotCompiled(List.of(VARCHAR), pad(function, field(0, VARCHAR), 4, "ab"));
        assertNotCompiled(List.of(VARCHAR), pad(function, field(0, VARCHAR), 4, "𐐀"));
        // The CPU raises an error for these
        assertNotCompiled(List.of(VARCHAR), pad(function, field(0, VARCHAR), 4, ""));
        assertNotCompiled(List.of(VARCHAR), pad(function, field(0, VARCHAR), -1, "0"));
        assertNotCompiled(List.of(VARCHAR), pad(function, field(0, VARCHAR), Integer.MAX_VALUE + 1L, "0"));
    }

    @Test
    void testLeftPadChar()
    {
        assertNotCompiled(List.of(createCharType(5)), new Call(
                FUNCTION_RESOLUTION.resolveFunction("lpad", fromTypes(createCharType(5), BIGINT, VARCHAR)),
                ImmutableList.of(field(0, createCharType(5)), new Constant(BIGINT, 4L), new Constant(VARCHAR, Slices.utf8Slice("0")))));
    }

    private static Call pad(String function, Expression source, long targetLength, String padString)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction(function, fromTypes(VARCHAR, BIGINT, VARCHAR)),
                ImmutableList.of(source, new Constant(BIGINT, targetLength), new Constant(VARCHAR, Slices.utf8Slice(padString))));
    }

    private static void assertNotCompiled(List<Type> inputTypes, Expression expression)
    {
        assertThat(compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private static Block createStringBlock(List<String> values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.size(), values.size() * 16);
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
