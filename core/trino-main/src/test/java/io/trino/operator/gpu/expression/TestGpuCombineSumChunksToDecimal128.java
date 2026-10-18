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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.expression.SumChunksTestUtils.ChunkSums;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.SumChunksTestUtils.MAX_38_DIGITS;
import static io.trino.operator.gpu.expression.SumChunksTestUtils.MAX_ROWS;
import static io.trino.operator.gpu.expression.SumChunksTestUtils.MAX_UNSIGNED_CHUNK;
import static io.trino.operator.gpu.expression.SumChunksTestUtils.chunkSumColumns;
import static io.trino.operator.gpu.expression.SumChunksTestUtils.sumOfRepeated;
import static io.trino.operator.gpu.expression.SumChunksTestUtils.sumOfValues;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuCombineSumChunksToDecimal128
{
    private static final BigInteger TWO_TO_127 = BigInteger.ONE.shiftLeft(127);

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testSums()
    {
        DecimalType type = createDecimalType(38, 2);
        long maxChunkSum = MAX_ROWS * MAX_UNSIGNED_CHUNK;
        assertSums(type, ImmutableList.of(
                sumOfValues(5, 7),
                sumOfValues(-5, -7),
                sumOfValues(-5, 7),
                sumOfValues(5, -7),
                sumOfValues(0),
                // one row of -1 has chunks (2^32 - 1, 2^32 - 1, 2^32 - 1, -1)
                sumOfValues(-1),
                // values using all four chunks, with their sums and differences
                sumOfValues(ImmutableList.of(BigInteger.TEN.pow(37), BigInteger.TEN.pow(37).multiply(BigInteger.valueOf(3)))),
                sumOfValues(ImmutableList.of(BigInteger.TEN.pow(37).negate(), BigInteger.TEN.pow(37).multiply(BigInteger.valueOf(-3)))),
                sumOfValues(ImmutableList.of(MAX_38_DIGITS, MAX_38_DIGITS.negate(), BigInteger.valueOf(-3))),
                // many rows of one value
                sumOfRepeated(BigInteger.TEN.pow(28).negate(), MAX_ROWS),
                sumOfRepeated(BigInteger.TEN.pow(28), MAX_ROWS),
                // unsigned chunk sums that carry into the chunks above them
                new ChunkSums(maxChunkSum, maxChunkSum, 0, 0),
                // large unsigned chunk sums cancelled by a negative top chunk sum
                new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, -MAX_ROWS)));
    }

    @Test
    void testMaxPrecision()
    {
        for (int scale : new int[] {0, 6, 38}) {
            DecimalType type = createDecimalType(38, scale);
            assertSums(type, ImmutableList.of(
                    sumOfValues(ImmutableList.of(MAX_38_DIGITS)),
                    sumOfValues(ImmutableList.of(MAX_38_DIGITS.negate())),
                    sumOfValues(ImmutableList.of(MAX_38_DIGITS.subtract(BigInteger.TEN), BigInteger.TEN)),
                    sumOfValues(ImmutableList.of(MAX_38_DIGITS.subtract(BigInteger.TEN).negate(), BigInteger.TEN.negate()))));
        }
    }

    @Test
    void testExceedsPrecision()
    {
        // Sums of at least 10^38 that fit 128 bits
        DecimalType type = createDecimalType(38, 0);
        BigInteger sixTimesTenTo37 = BigInteger.TEN.pow(37).multiply(BigInteger.valueOf(6));
        BigInteger fiveTimesTenTo37 = BigInteger.TEN.pow(37).multiply(BigInteger.valueOf(5));
        assertOverflow(type, sumOfValues(ImmutableList.of(MAX_38_DIGITS, BigInteger.ONE)));
        assertOverflow(type, sumOfValues(ImmutableList.of(MAX_38_DIGITS.negate(), BigInteger.ONE.negate())));
        assertOverflow(type, sumOfValues(ImmutableList.of(sixTimesTenTo37, fiveTimesTenTo37)));
        assertOverflow(type, sumOfValues(ImmutableList.of(sixTimesTenTo37.negate(), fiveTimesTenTo37.negate())));
        assertOverflow(createDecimalType(38, 6), sumOfValues(ImmutableList.of(sixTimesTenTo37, fiveTimesTenTo37)));
    }

    @Test
    void testExceeds128Bits()
    {
        DecimalType type = createDecimalType(38, 0);
        BigInteger nineTimesTenTo37 = BigInteger.TEN.pow(37).multiply(BigInteger.valueOf(9));
        assertThat(nineTimesTenTo37.multiply(BigInteger.TWO)).isGreaterThan(TWO_TO_127);
        assertOverflow(type, sumOfValues(ImmutableList.of(nineTimesTenTo37, nineTimesTenTo37)));
        assertOverflow(type, sumOfValues(ImmutableList.of(nineTimesTenTo37.negate(), nineTimesTenTo37.negate())));
        assertOverflow(type, sumOfRepeated(MAX_38_DIGITS, MAX_ROWS));
        assertOverflow(type, sumOfRepeated(MAX_38_DIGITS.negate(), MAX_ROWS));
        assertOverflow(type, new ChunkSums(0, 0, 0, Long.MAX_VALUE));
        assertOverflow(type, new ChunkSums(0, 0, 0, Long.MIN_VALUE));
    }

    @Test
    void testNulls()
    {
        DecimalType type = createDecimalType(38, 2);
        GpuExpression expression = expression(type);
        // NULL chunk sums produce NULL rather than an overflow, as do the frames GpuIf masks out
        try (ColumnVector chunk0 = ColumnVector.fromBoxedLongs(null, 7L);
                ColumnVector chunk1 = ColumnVector.fromBoxedLongs(null, 0L);
                ColumnVector chunk2 = ColumnVector.fromBoxedLongs(null, 0L);
                ColumnVector chunk3 = ColumnVector.fromBoxedLongs(null, 0L);
                ColumnVector result = expression.evaluate(2, ImmutableList.of(chunk0, chunk1, chunk2, chunk3), new Bindings());
                HostColumnVector host = result.copyToHost()) {
            assertThat(expression.fallibility()).isEqualTo(Fallibility.FALLIBLE_EXCEPT_ALL_NULLS);
            assertThat(expression.nullOnNullInputs()).isTrue();
            assertThat(result.getType()).isEqualTo(DType.create(DType.DTypeEnum.DECIMAL128, -2));
            assertThat(host.isNull(0)).isTrue();
            assertThat(host.getBigDecimal(1)).isEqualTo(new BigDecimal("0.07"));
        }
    }

    private static void assertSums(DecimalType type, List<ChunkSums> cases)
    {
        try (ColumnVector result = evaluate(type, cases);
                HostColumnVector host = result.copyToHost()) {
            assertThat(result.getType()).isEqualTo(DType.create(DType.DTypeEnum.DECIMAL128, -type.getScale()));
            for (int row = 0; row < cases.size(); row++) {
                ChunkSums chunkSums = cases.get(row);
                assertThat(host.getBigDecimal(row))
                        .as("%s", chunkSums)
                        .isEqualTo(new BigDecimal(chunkSums.sum(), type.getScale()));
            }
        }
    }

    private static void assertOverflow(DecimalType type, ChunkSums chunkSums)
    {
        assertThat(chunkSums.sum().abs()).isGreaterThan(MAX_38_DIGITS);
        assertTrinoExceptionThrownBy(() -> evaluate(type, ImmutableList.of(chunkSums)).close())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    private static ColumnVector evaluate(DecimalType type, List<ChunkSums> cases)
    {
        List<ColumnVector> columns = new ArrayList<>();
        try {
            for (int chunkIndex = 0; chunkIndex < 4; chunkIndex++) {
                int index = chunkIndex;
                columns.add(ColumnVector.fromLongs(cases.stream().mapToLong(chunkSum -> chunkSum.chunk(index)).toArray()));
            }
            return expression(type).evaluate(cases.size(), columns, new Bindings());
        }
        finally {
            columns.forEach(ColumnVector::close);
        }
    }

    private static GpuExpression expression(DecimalType type)
    {
        return new GpuCombineSumChunksToDecimal128(chunkSumColumns(), type);
    }
}
