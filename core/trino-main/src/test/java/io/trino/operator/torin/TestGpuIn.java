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
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBlock;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuIn
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIn(NullsProvider nullsProvider)
    {
        testIn(BIGINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(INTEGER, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(SMALLINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(TINYINT, List.of(-50L, 0L, 25L, 50L, 75L), nullsProvider);
        testIn(DOUBLE, List.of(-50.0, 0.0, 25.5, 50.0, 75.5), nullsProvider);
        testIn(REAL, List.of(
                (long) Float.floatToIntBits(-50.0f),
                (long) Float.floatToIntBits(0.0f),
                (long) Float.floatToIntBits(25.5f),
                (long) Float.floatToIntBits(50.0f),
                (long) Float.floatToIntBits(75.5f)), nullsProvider);
        testIn(VARCHAR, List.of(
                Slices.utf8Slice("test1"),
                Slices.utf8Slice("other"),
                Slices.utf8Slice("xyz")), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testInWithNull(NullsProvider nullsProvider)
    {
        testIn(BIGINT, Arrays.asList(10L, null, 50L), nullsProvider);
        testIn(BIGINT, Collections.singletonList(null), nullsProvider);
    }

    private void testIn(Type type, List<Object> inValues, NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(type);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(type, positionsCount, nullsProvider)));

        ImmutableList.Builder<Expression> valueListBuilder = ImmutableList.builder();
        for (Object value : inValues) {
            valueListBuilder.add(new Constant(type, value));
        }

        Expression expression = new In(field(channelA, type), valueListBuilder.build());

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }
}
