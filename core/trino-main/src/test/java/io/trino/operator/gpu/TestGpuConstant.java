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

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Set;

import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
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
final class TestGpuConstant
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testBooleanConstant()
    {
        testConstant(new Constant(BOOLEAN, true));
        testConstant(new Constant(BOOLEAN, false));
        testConstant(new Constant(BOOLEAN, null));
    }

    @Test
    void testTinyintConstant()
    {
        testConstant(new Constant(TINYINT, 42L));
        testConstant(new Constant(TINYINT, -1L));
        testConstant(new Constant(TINYINT, 0L));
        testConstant(new Constant(TINYINT, null));
    }

    @Test
    void testSmallintConstant()
    {
        testConstant(new Constant(SMALLINT, 1234L));
        testConstant(new Constant(SMALLINT, -5678L));
        testConstant(new Constant(SMALLINT, null));
    }

    @Test
    void testIntegerConstant()
    {
        testConstant(new Constant(INTEGER, 123456L));
        testConstant(new Constant(INTEGER, -789012L));
        testConstant(new Constant(INTEGER, 0L));
        testConstant(new Constant(INTEGER, null));
    }

    @Test
    void testBigintConstant()
    {
        testConstant(new Constant(BIGINT, 1234567890123L));
        testConstant(new Constant(BIGINT, -9876543210L));
        testConstant(new Constant(BIGINT, 0L));
        testConstant(new Constant(BIGINT, null));
    }

    @Test
    void testRealConstant()
    {
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(3.14f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(-2.5f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(0.0f)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.POSITIVE_INFINITY)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.NEGATIVE_INFINITY)));
        testConstant(new Constant(REAL, (long) Float.floatToIntBits(Float.NaN)));
        testConstant(new Constant(REAL, null));
    }

    @Test
    void testDoubleConstant()
    {
        testConstant(new Constant(DOUBLE, 3.14159265359));
        testConstant(new Constant(DOUBLE, -2.71828));
        testConstant(new Constant(DOUBLE, 0.0));
        testConstant(new Constant(DOUBLE, Double.POSITIVE_INFINITY));
        testConstant(new Constant(DOUBLE, Double.NEGATIVE_INFINITY));
        testConstant(new Constant(DOUBLE, Double.NaN));
        testConstant(new Constant(DOUBLE, null));
    }

    @Test
    void testVarcharConstant()
    {
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("hello")));
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("")));
        testConstant(new Constant(VARCHAR, Slices.utf8Slice("Łania szła piękną łąką pod Warszawą")));
        testConstant(new Constant(VARCHAR, null));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testReference(NullsProvider nullsProvider)
    {
        Expression expression = new Reference(BIGINT, "ref0");
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -1000, 1000)));
        assertGpuMatchesCpu(inputPages, List.of(BIGINT), expression, Set.of(0));
    }

    private void testConstant(Expression constantExpression)
    {
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, 0, 100)));
        assertGpuMatchesCpu(inputPages, List.of(BIGINT), constantExpression, Set.of());
    }
}
