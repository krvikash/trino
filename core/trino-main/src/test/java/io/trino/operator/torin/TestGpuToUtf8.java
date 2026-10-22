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
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.List;

import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBlock;
import static io.trino.operator.torin.GpuTestUtils.createVarcharPages;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.GpuTestUtils.varcharsBlock;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuToUtf8
{
    private static final List<String> EDGE_CASE_STRINGS = Arrays.asList(
            "",
            null,
            " ",
            "a ",
            "ąę",
            "ワルシャワ",
            "😀",
            "éclair",
            "\0");

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testToUtf8(NullsProvider nullsProvider)
    {
        List<Page> inputPages = ImmutableList.<Page>builder()
                .addAll(createVarcharPages(VARCHAR, List.of(64, 128), nullsProvider))
                .add(new Page(varcharsBlock(EDGE_CASE_STRINGS)))
                .build();
        assertGpuMatchesCpu(inputPages, List.of(VARCHAR), toUtf8(VARCHAR));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testToUtf8Char(NullsProvider nullsProvider)
    {
        Type type = createCharType(8);
        List<Page> inputPages = List.of(new Page(createBlock(type, 64, nullsProvider)));
        assertGpuMatchesCpu(inputPages, List.of(type), toUtf8(type));
    }

    @Test
    void testToUtf8CharPadding()
    {
        Type type = createCharType(3);
        List<Page> inputPages = List.of(new Page(varcharsBlock(Arrays.asList("", null, "a", "ab", "abc", "ą", "😀"))));
        assertGpuMatchesCpu(inputPages, List.of(type), toUtf8(type));
    }

    private static Expression toUtf8(Type type)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("to_utf8", fromTypes(type)),
                ImmutableList.of(field(0, type)));
    }
}
