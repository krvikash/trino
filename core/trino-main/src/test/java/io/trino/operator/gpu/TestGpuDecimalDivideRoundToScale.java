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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.Page;
import io.trino.spi.type.DecimalType;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.bigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.longDecimalBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.scalar.DivideRoundToScale.NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * GPU-gated tests for the {@code $divide_round_to_scale} scalar
 * ({@link io.trino.operator.gpu.expression.GpuDivideRoundToScale}). Each case runs on CPU and GPU
 * and asserts they agree — equal output, or both throwing.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGpuDecimalDivideRoundToScale
{
    private static final DecimalType DECIMAL_38_2 = createDecimalType(38, 2);

    @BeforeAll
    public static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testDivideRoundToScale(NullsProvider nullsProvider)
    {
        for (int scale : new int[] {0, 2, 6}) {
            DecimalType dividendType = createDecimalType(38, scale);
            int positionsCount = 1024;
            List<Page> inputPages = List.of(new Page(
                    positionsCount,
                    createBlock(dividendType, positionsCount, nullsProvider),
                    // strictly positive divisors keep this on the normal (non-throwing) path
                    createBigintBlock(positionsCount, nullsProvider, 1, 1_000_001)));
            assertGpuMatchesCpu(inputPages, List.of(dividendType, BIGINT), divideRoundToScale(dividendType));
        }
    }

    @Test
    public void testNullOperands()
    {
        // A null dividend yields null even with a 0 or negative divisor: the guard skips null dividends.
        List<Page> inputPages = List.of(new Page(
                longDecimalBlock(DECIMAL_38_2, null, null, 700L, 500L),
                bigintBlock(0L, -3L, null, 4L)));
        assertGpuMatchesCpu(inputPages, List.of(DECIMAL_38_2, BIGINT), divideRoundToScale(DECIMAL_38_2));
    }

    @Test
    public void testDivisionByZero()
    {
        List<Page> inputPages = List.of(new Page(longDecimalBlock(DECIMAL_38_2, 12345L), bigintBlock(0L)));
        assertGpuMatchesCpu(inputPages, List.of(DECIMAL_38_2, BIGINT), divideRoundToScale(DECIMAL_38_2));
    }

    @Test
    public void testNegativeDivisor()
    {
        List<Page> inputPages = List.of(new Page(longDecimalBlock(DECIMAL_38_2, 12345L), bigintBlock(-3L)));
        assertGpuMatchesCpu(inputPages, List.of(DECIMAL_38_2, BIGINT), divideRoundToScale(DECIMAL_38_2));
    }

    private static Expression divideRoundToScale(DecimalType dividendType)
    {
        ResolvedFunction function = FUNCTION_RESOLUTION.resolveFunction(NAME, TypeDescriptorProvider.fromTypes(dividendType, BIGINT));
        return new Call(function, ImmutableList.of(field(0, dividendType), field(1, BIGINT)));
    }
}
