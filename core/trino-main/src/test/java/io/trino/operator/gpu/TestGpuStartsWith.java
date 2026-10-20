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
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.TEST_STRINGS;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createVarcharPages;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuStartsWith
{
    /// Search strings for `replace`, chosen to occur in `TEST_STRINGS` and in `REPLACE_EDGE_CASE_STRINGS`:
    /// single and multi character, ASCII and multi-byte, plus an empty and a never-matching one.
    private static final List<String> REPLACE_SEARCH_STRINGS = ImmutableList.of(
            "", "a", "b", "z", "ab", "aa", "papa", "abcdefghij", "%", "test", "ą", "łąką", "ワ", "а", "no such substring");

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
            assertGpuMatchesCpu(inputPages, inputTypes, expression);
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
}
