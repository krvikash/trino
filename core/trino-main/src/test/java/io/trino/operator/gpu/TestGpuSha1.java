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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createVarcharPages;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuSha1
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSha1(NullsProvider nullsProvider)
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(VARBINARY, 64, nullsProvider)),
                new Page(createBlock(VARBINARY, 128, nullsProvider)));
        assertGpuMatchesCpu(inputPages, List.of(VARBINARY), sha1(field(0, VARBINARY)));
    }

    @Test
    void testSha1EdgeCases()
    {
        // Lengths around the 64-byte SHA-1 block and the 56-byte padding boundary
        Block block = varbinaryBlock(Arrays.asList(
                new byte[0],
                null,
                new byte[] {0x00},
                new byte[55],
                new byte[56],
                new byte[63],
                new byte[64],
                new byte[65],
                new byte[1000],
                null,
                new byte[0]));
        assertGpuMatchesCpu(List.of(new Page(block)), List.of(VARBINARY), sha1(field(0, VARBINARY)));
    }

    @Test
    void testSha1OfConstant()
    {
        List<Page> inputPages = List.of(new Page(createBlock(VARBINARY, 4, NullsProvider.NO_NULLS)));
        assertGpuMatchesCpu(TEST_SESSION, inputPages, List.of(VARBINARY), sha1(new Constant(VARBINARY, utf8Slice("abc"))), Set.of());
        assertGpuMatchesCpu(TEST_SESSION, inputPages, List.of(VARBINARY), sha1(new Constant(VARBINARY, null)), Set.of());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSha1OfConcatenatedHex(NullsProvider nullsProvider)
    {
        // The shape of a row-hashing expression: sha1(to_utf8(concat(..., COALESCE(to_hex(to_utf8(x)), ''), ...)))
        Expression hex = new Coalesce(ImmutableList.of(
                toHex(toUtf8(field(0, VARCHAR))),
                new Constant(VARCHAR, utf8Slice(""))));
        Constant separator = new Constant(VARCHAR, utf8Slice("م"));
        Expression concatenated = new Call(
                FUNCTION_RESOLUTION.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR, VARCHAR)),
                ImmutableList.of(separator, hex, separator));
        assertGpuMatchesCpu(createVarcharPages(VARCHAR, List.of(64), nullsProvider), List.of(VARCHAR), sha1(toUtf8(concatenated)));
    }

    private static Expression sha1(Expression argument)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("sha1", fromTypes(VARBINARY)),
                ImmutableList.of(argument));
    }

    private static Expression toHex(Expression argument)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("to_hex", fromTypes(VARBINARY)),
                ImmutableList.of(argument));
    }

    private static Expression toUtf8(Expression argument)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("to_utf8", fromTypes(VARCHAR)),
                ImmutableList.of(argument));
    }

    private static Block varbinaryBlock(List<byte[]> values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.size(), values.size() * 16);
        for (byte[] value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(Slices.wrappedBuffer(value));
            }
        }
        return builder.build();
    }
}
