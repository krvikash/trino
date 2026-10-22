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
package io.trino.operator.torin.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import io.trino.operator.torin.expression.SumChunksTestUtils.ChunkSums;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.expression.SumChunksTestUtils.MAX_38_DIGITS;
import static io.trino.operator.torin.expression.SumChunksTestUtils.MAX_ROWS;
import static io.trino.operator.torin.expression.SumChunksTestUtils.MAX_UNSIGNED_CHUNK;
import static io.trino.operator.torin.expression.SumChunksTestUtils.chunkSumColumns;
import static io.trino.operator.torin.expression.SumChunksTestUtils.sumOf;
import static io.trino.operator.torin.expression.SumChunksTestUtils.sumOfRepeated;
import static io.trino.operator.torin.expression.SumChunksTestUtils.sumOfValues;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.math.RoundingMode.HALF_UP;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuDivideSumChunksRoundToScale
{
    private static final BigInteger MAX_29_DIGITS = BigInteger.TEN.pow(29).subtract(BigInteger.ONE);

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testRounding()
    {
        DecimalType type = createDecimalType(38, 2);
        assertDivision(type, ImmutableList.of(
                averageOf(6, 3),
                averageOf(-6, 3),
                averageOf(7, 2),
                averageOf(-7, 2),
                averageOf(5, 2),
                averageOf(-5, 2),
                averageOf(4, 3),
                averageOf(-4, 3),
                averageOf(5, 3),
                averageOf(-5, 3),
                averageOf(0, 7),
                averageOf(1, MAX_ROWS),
                averageOf(-1, MAX_ROWS)));
    }

    @Test
    void testRemainderCloseToHalf()
    {
        DecimalType type = createDecimalType(38, 0);
        // divisor 2k + 1: remainder k is just below one half, remainder k + 1 just above
        long divisor = MAX_ROWS;
        long half = divisor / 2;
        BigInteger base = BigInteger.TEN.pow(30).multiply(BigInteger.valueOf(divisor));
        BigInteger evenBase = BigInteger.TEN.pow(37);
        assertDivision(type, ImmutableList.of(
                averageOf(base.add(BigInteger.valueOf(half)), divisor),
                averageOf(base.add(BigInteger.valueOf(half + 1)), divisor),
                averageOf(base.add(BigInteger.valueOf(half)).negate(), divisor),
                averageOf(base.add(BigInteger.valueOf(half + 1)).negate(), divisor),
                // even divisor, remainder exactly one half
                averageOf(evenBase.multiply(BigInteger.TWO).add(BigInteger.ONE), 2),
                averageOf(evenBase.multiply(BigInteger.TWO).add(BigInteger.ONE).negate(), 2)));
    }

    @Test
    void testSumBeyondSigned128Bits()
    {
        DecimalType type = createDecimalType(38, 6);
        BigInteger belowMax = MAX_38_DIGITS.subtract(BigInteger.ONE);
        assertDivision(type, ImmutableList.of(
                averageOf(MAX_38_DIGITS.multiply(BigInteger.TWO), 2),
                averageOf(MAX_38_DIGITS.multiply(BigInteger.TWO).negate(), 2),
                averageOf(MAX_38_DIGITS.add(belowMax), 2),
                averageOf(MAX_38_DIGITS.add(belowMax).negate(), 2),
                averageOf(MAX_38_DIGITS.multiply(BigInteger.valueOf(MAX_ROWS)), MAX_ROWS),
                averageOf(MAX_38_DIGITS.multiply(BigInteger.valueOf(MAX_ROWS)).negate(), MAX_ROWS),
                averageOf(MAX_38_DIGITS.multiply(BigInteger.valueOf(MAX_ROWS)).subtract(BigInteger.ONE), MAX_ROWS)));
    }

    @Test
    void testLimbCarries()
    {
        DecimalType type = createDecimalType(38, 0);
        long maxChunkSum = MAX_ROWS * MAX_UNSIGNED_CHUNK;
        assertDivision(type, ImmutableList.of(
                // each unsigned chunk sum carries into the chunks above it
                new Average(new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, 0), MAX_ROWS),
                new Average(new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, MAX_ROWS * 1_000_000_000L), MAX_ROWS),
                new Average(new ChunkSums(maxChunkSum, 0, 0, 0), 1),
                new Average(new ChunkSums(0, maxChunkSum, 0, 0), 3),
                new Average(new ChunkSums(0, 0, maxChunkSum, 0), MAX_ROWS),
                new Average(new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, -MAX_ROWS * 1_000_000_000L), MAX_ROWS)));
    }

    @Test
    void testCancellation()
    {
        DecimalType type = createDecimalType(38, 4);
        long maxChunkSum = MAX_ROWS * MAX_UNSIGNED_CHUNK;
        assertDivision(type, ImmutableList.of(
                // one row of -1 has chunks (2^32 - 1, 2^32 - 1, 2^32 - 1, -1)
                new Average(new ChunkSums(MAX_UNSIGNED_CHUNK, MAX_UNSIGNED_CHUNK, MAX_UNSIGNED_CHUNK, -1), 1),
                // +x and -x in the same frame
                averageOfValues(ImmutableList.of(MAX_38_DIGITS, MAX_38_DIGITS.negate())),
                averageOfValues(ImmutableList.of(MAX_38_DIGITS, MAX_38_DIGITS.negate(), BigInteger.valueOf(-3))),
                // large unsigned chunk sums cancelled by a negative top chunk sum
                new Average(new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, -MAX_ROWS), MAX_ROWS)));
    }

    @Test
    void testPrecision29WorstCase()
    {
        for (int scale : new int[] {0, 2, 6, 29}) {
            DecimalType type = createDecimalType(29, scale);
            assertDivision(type, ImmutableList.of(
                    averageOfRepeated(MAX_29_DIGITS, MAX_ROWS),
                    averageOfRepeated(MAX_29_DIGITS.negate(), MAX_ROWS),
                    averageOfRepeated(MAX_29_DIGITS, 1),
                    averageOfRepeated(MAX_29_DIGITS.negate(), 1)));
        }
    }

    @Test
    void testUnsignedChunkSums()
    {
        DecimalType type = createDecimalType(38, 2);
        List<Average> cases = ImmutableList.of(
                averageOfRepeated(MAX_38_DIGITS, MAX_ROWS),
                averageOfRepeated(MAX_38_DIGITS.negate(), 3),
                averageOf(-5, 2));
        try (ColumnVector result = evaluate(type, cases, DType.UINT64);
                HostColumnVector host = result.copyToHost()) {
            assertResults(type, cases, host);
        }
    }

    @Test
    void testNulls()
    {
        DecimalType type = createDecimalType(38, 2);
        GpuExpression expression = expression(type);
        // a frame with only nulls has null chunk sums and a zero count
        try (ColumnVector chunk0 = ColumnVector.fromBoxedLongs(null, 7L);
                ColumnVector chunk1 = ColumnVector.fromBoxedLongs(null, 0L);
                ColumnVector chunk2 = ColumnVector.fromBoxedLongs(null, 0L);
                ColumnVector chunk3 = ColumnVector.fromBoxedLongs(null, 0L);
                ColumnVector count = ColumnVector.fromBoxedLongs(0L, 2L);
                ColumnVector result = expression.evaluate(2, ImmutableList.of(chunk0, chunk1, chunk2, chunk3, count), new Bindings());
                HostColumnVector host = result.copyToHost()) {
            assertThat(expression.fallibility()).isEqualTo(Fallibility.FALLIBLE_EXCEPT_ALL_NULLS);
            assertThat(expression.nullOnNullInputs()).isTrue();
            assertThat(result.getType()).isEqualTo(DType.create(DType.DTypeEnum.DECIMAL128, -2));
            assertThat(host.isNull(0)).isTrue();
            assertThat(host.getBigDecimal(1)).isEqualTo(new BigDecimal("0.04"));
        }
    }

    @Test
    void testOverflow()
    {
        assertOverflow(createDecimalType(29, 0), averageOf(BigInteger.TEN.pow(29), 1));
        assertOverflow(createDecimalType(29, 0), averageOf(BigInteger.TEN.pow(29).negate(), 1));
        // rounds up past the precision
        assertOverflow(createDecimalType(29, 2), averageOf(MAX_29_DIGITS.multiply(BigInteger.TWO).add(BigInteger.ONE), 2));
        assertOverflow(createDecimalType(29, 2), averageOf(MAX_29_DIGITS.multiply(BigInteger.TWO).add(BigInteger.ONE).negate(), 2));
        assertOverflow(createDecimalType(38, 0), averageOf(MAX_38_DIGITS.add(BigInteger.ONE), 1));
        // quotients far beyond 128 bits must fail rather than wrap
        assertOverflow(createDecimalType(38, 0), averageOfRepeated(MAX_38_DIGITS, MAX_ROWS).withCount(1));
        assertOverflow(createDecimalType(38, 0), averageOfRepeated(MAX_38_DIGITS.negate(), MAX_ROWS).withCount(1));
        assertOverflow(createDecimalType(38, 0), new Average(new ChunkSums(0, 0, 0, Long.MAX_VALUE), 1));
        assertOverflow(createDecimalType(38, 0), new Average(new ChunkSums(0, 0, 0, Long.MIN_VALUE), 1));
    }

    @Test
    void testInvalidDivisor()
    {
        DecimalType type = createDecimalType(38, 2);
        assertTrinoExceptionThrownBy(() -> evaluateAndClose(type, ImmutableList.of(averageOf(5, 0))))
                .hasErrorCode(DIVISION_BY_ZERO);
        assertTrinoExceptionThrownBy(() -> evaluateAndClose(type, ImmutableList.of(averageOf(5, -1))))
                .hasErrorCode(NOT_SUPPORTED);
    }

    private static void assertDivision(DecimalType type, List<Average> cases)
    {
        try (ColumnVector result = evaluate(type, cases, DType.INT64);
                HostColumnVector host = result.copyToHost()) {
            assertThat(result.getType()).isEqualTo(DType.create(DType.DTypeEnum.DECIMAL128, -type.getScale()));
            assertResults(type, cases, host);
        }
    }

    private static void assertResults(DecimalType type, List<Average> cases, HostColumnVector host)
    {
        for (int row = 0; row < cases.size(); row++) {
            Average average = cases.get(row);
            assertThat(host.getBigDecimal(row))
                    .as("%s", average)
                    .isEqualTo(average.expected(type));
        }
    }

    private static void assertOverflow(DecimalType type, Average average)
    {
        assertTrinoExceptionThrownBy(() -> evaluateAndClose(type, ImmutableList.of(average)))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    private static void evaluateAndClose(DecimalType type, List<Average> cases)
    {
        evaluate(type, cases, DType.INT64).close();
    }

    private static ColumnVector evaluate(DecimalType type, List<Average> cases, DType unsignedChunkSumType)
    {
        List<ColumnVector> columns = new ArrayList<>();
        try {
            for (int chunkIndex = 0; chunkIndex < 4; chunkIndex++) {
                int index = chunkIndex;
                try (ColumnVector chunkSums = ColumnVector.fromLongs(cases.stream().mapToLong(average -> average.chunkSums().chunk(index)).toArray())) {
                    DType chunkSumType = chunkIndex < 3 ? unsignedChunkSumType : DType.INT64;
                    columns.add(chunkSums.castTo(chunkSumType));
                }
            }
            columns.add(ColumnVector.fromLongs(cases.stream().mapToLong(Average::count).toArray()));
            return expression(type).evaluate(cases.size(), columns, new Bindings());
        }
        finally {
            columns.forEach(ColumnVector::close);
        }
    }

    private static GpuExpression expression(DecimalType type)
    {
        return new GpuDivideSumChunksRoundToScale(
                chunkSumColumns(),
                new GetColumn(4),
                type);
    }

    private static Average averageOf(long sum, long count)
    {
        return averageOf(BigInteger.valueOf(sum), count);
    }

    // average of a single-row 160-bit sum, not limited to 128 bits
    private static Average averageOf(BigInteger sum, long count)
    {
        return new Average(sumOf(sum), count);
    }

    private static Average averageOfRepeated(BigInteger value, long rows)
    {
        return new Average(sumOfRepeated(value, rows), rows);
    }

    private static Average averageOfValues(List<BigInteger> values)
    {
        return new Average(sumOfValues(values), values.size());
    }

    private record Average(ChunkSums chunkSums, long count)
    {
        Average withCount(long newCount)
        {
            return new Average(chunkSums, newCount);
        }

        BigDecimal expected(DecimalType type)
        {
            BigDecimal average = new BigDecimal(chunkSums.sum(), type.getScale()).divide(BigDecimal.valueOf(count), type.getScale(), HALF_UP);
            assertThat(average.unscaledValue().abs()).isLessThanOrEqualTo(BigInteger.TEN.pow(type.getPrecision()).subtract(BigInteger.ONE));
            return average;
        }
    }
}
