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
import java.util.stream.IntStream;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.createVarcharPages;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuToHex
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testToHex(NullsProvider nullsProvider)
    {
        List<Page> inputPages = List.of(
                new Page(createBlock(VARBINARY, 64, nullsProvider)),
                new Page(createBlock(VARBINARY, 128, nullsProvider)));
        assertGpuMatchesCpu(inputPages, List.of(VARBINARY), toHex(field(0, VARBINARY)));
    }

    @Test
    void testToHexEdgeCases()
    {
        byte[] everyByte = new byte[256];
        IntStream.range(0, everyByte.length).forEach(value -> everyByte[value] = (byte) value);
        Block block = varbinaryBlock(Arrays.asList(
                new byte[0],
                null,
                new byte[] {0x00},
                new byte[] {0x0F},
                new byte[] {(byte) 0xF0},
                new byte[] {(byte) 0xFF, 0x00, (byte) 0xAB},
                everyByte,
                new byte[0]));
        assertGpuMatchesCpu(List.of(new Page(block)), List.of(VARBINARY), toHex(field(0, VARBINARY)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testToHexOfToUtf8(NullsProvider nullsProvider)
    {
        Expression toUtf8 = new Call(
                FUNCTION_RESOLUTION.resolveFunction("to_utf8", fromTypes(VARCHAR)),
                ImmutableList.of(field(0, VARCHAR)));
        assertGpuMatchesCpu(createVarcharPages(VARCHAR, List.of(64), nullsProvider), List.of(VARCHAR), toHex(toUtf8));
    }

    @Test
    void testToHexOfConstant()
    {
        // The constant needs a column to set the row count
        List<Page> inputPages = List.of(new Page(createBlock(BIGINT, 4, NullsProvider.NO_NULLS)));
        Expression expression = toHex(new Constant(VARBINARY, Slices.wrappedBuffer(new byte[] {0x01, (byte) 0xEF})));
        assertGpuMatchesCpu(TEST_SESSION, inputPages, List.of(BIGINT), expression, Set.of());
    }

    private static Expression toHex(Expression argument)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("to_hex", fromTypes(VARBINARY)),
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
