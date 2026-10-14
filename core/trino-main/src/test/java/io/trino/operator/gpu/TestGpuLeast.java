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
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuLeast
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @MethodSource("leastTestCases")
    void testLeast(Type type, NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(type, type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider),
                createBlock(type, positionsCount, nullsProvider)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(type, type)),
                ImmutableList.of(field(channelA, type), field(channelB, type)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    private static Stream<Object[]> leastTestCases()
    {
        return Lists.cartesianProduct(
                        List.of(
                                BOOLEAN,
                                TINYINT,
                                SMALLINT,
                                INTEGER,
                                BIGINT,
                                createDecimalType(3, 0),
                                createDecimalType(13, 0),
                                createDecimalType(27, 0),
                                createDecimalType(27, 5),
                                createDecimalType(38),
                                createCharType(8),
                                VARCHAR,
                                createVarcharType(20),
                                DATE,
                                createTimestampType(0),
                                createTimestampType(3),
                                createTimestampType(6),
                                createTimestampType(9)),
                        ImmutableList.copyOf(NullsProvider.values()))
                .stream()
                .map(List::toArray);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLeastManyArguments(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        int channelC = 2;
        int channelD = 3;
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(BIGINT, BIGINT, BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT), field(channelC, BIGINT), field(channelD, BIGINT)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB, channelC, channelD));
    }

    @Test
    void testLeastWithNullConstant()
    {
        // A NULL operand makes every row NULL, regardless of the other operands.
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), new Constant(BIGINT, null)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA));
    }

    @Test
    void testLeastCharPaddedSemantics()
    {
        // U+0001 is less than space (U+0020), so least() would return wrong results without proper CHAR padding
        Type charType = createCharType(2);
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(charType, charType);
        List<Page> inputPages = List.of(new Page(
                charBlock(charType, "a", "a\u0001", "b", "a"),
                charBlock(charType, "a\u0001", "a", "a", "ab")));

        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("least", fromTypes(charType, charType)),
                ImmutableList.of(field(channelA, charType), field(channelB, charType)));

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(channelA, channelB));
    }

    private static Block charBlock(Type charType, String... values)
    {
        BlockBuilder builder = charType.createBlockBuilder(null, values.length);
        for (String value : values) {
            charType.writeSlice(builder, Slices.utf8Slice(value));
        }
        return builder.build();
    }
}
