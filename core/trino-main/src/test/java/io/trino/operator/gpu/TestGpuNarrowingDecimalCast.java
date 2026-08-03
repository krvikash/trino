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

import io.trino.spi.Page;
import io.trino.spi.type.DecimalType;
import io.trino.sql.ir.Cast;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigInteger;
import java.util.List;

import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.longDecimalBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * GPU-gated tests for the same-scale decimal narrowing cast
 * ({@link io.trino.operator.gpu.expression.GpuNarrowingDecimalCast}). Each case runs on CPU and GPU
 * and asserts they agree — equal output, or both throwing.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGpuNarrowingDecimalCast
{
    private static final DecimalType DECIMAL_38_2 = createDecimalType(38, 2);

    @BeforeAll
    public static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    public void testToShortDecimal()
    {
        // decimal(38, 2) -> decimal(10, 2): DECIMAL128 to DECIMAL64, values in range
        List<Page> inputPages = List.of(new Page(longDecimalBlock(DECIMAL_38_2, 0L, 500L, -500L, 999999L, -999999L)));
        assertGpuMatchesCpu(inputPages, List.of(DECIMAL_38_2), new Cast(field(0, DECIMAL_38_2), createDecimalType(10, 2)));
    }

    @Test
    public void testToLongDecimal()
    {
        // decimal(38, 2) -> decimal(20, 2): stays DECIMAL128, values in range
        List<Page> inputPages = List.of(new Page(longDecimalBlock(DECIMAL_38_2, 0L, 500L, -500L, 1_000_000_000_000_000_000L)));
        assertGpuMatchesCpu(inputPages, List.of(DECIMAL_38_2), new Cast(field(0, DECIMAL_38_2), createDecimalType(20, 2)));
    }

    @Test
    public void testOverflow()
    {
        // 10^12 does not fit decimal(10, 2) (short target)
        assertGpuMatchesCpu(
                List.of(new Page(longDecimalBlock(DECIMAL_38_2, 1_000_000_000_000L))),
                List.of(DECIMAL_38_2),
                new Cast(field(0, DECIMAL_38_2), createDecimalType(10, 2)));
        // 10^20 does not fit decimal(20, 2) (long target)
        assertGpuMatchesCpu(
                List.of(new Page(longDecimalBlock(DECIMAL_38_2, BigInteger.TEN.pow(20)))),
                List.of(DECIMAL_38_2),
                new Cast(field(0, DECIMAL_38_2), createDecimalType(20, 2)));
    }
}
