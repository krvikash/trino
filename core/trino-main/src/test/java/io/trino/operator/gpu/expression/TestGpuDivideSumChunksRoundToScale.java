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
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.math.RoundingMode.HALF_UP;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuDivideSumChunksRoundToScale
{
    private static final long MAX_ROWS = Integer.MAX_VALUE;
    private static final long MAX_UNSIGNED_CHUNK = 0xFFFF_FFFFL;
    private static final BigInteger MAX_38_DIGITS = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
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
                sumOf(6, 3),
                sumOf(-6, 3),
                sumOf(7, 2),
                sumOf(-7, 2),
                sumOf(5, 2),
                sumOf(-5, 2),
                sumOf(4, 3),
                sumOf(-4, 3),
                sumOf(5, 3),
                sumOf(-5, 3),
                sumOf(0, 7),
                sumOf(1, MAX_ROWS),
                sumOf(-1, MAX_ROWS)));
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
                sumOf(base.add(BigInteger.valueOf(half)), divisor),
                sumOf(base.add(BigInteger.valueOf(half + 1)), divisor),
                sumOf(base.add(BigInteger.valueOf(half)).negate(), divisor),
                sumOf(base.add(BigInteger.valueOf(half + 1)).negate(), divisor),
                // even divisor, remainder exactly one half
                sumOf(evenBase.multiply(BigInteger.TWO).add(BigInteger.ONE), 2),
                sumOf(evenBase.multiply(BigInteger.TWO).add(BigInteger.ONE).negate(), 2)));
    }

    @Test
    void testSumBeyondSigned128Bits()
    {
        DecimalType type = createDecimalType(38, 6);
        BigInteger belowMax = MAX_38_DIGITS.subtract(BigInteger.ONE);
        assertDivision(type, ImmutableList.of(
                sumOf(MAX_38_DIGITS.multiply(BigInteger.TWO), 2),
                sumOf(MAX_38_DIGITS.multiply(BigInteger.TWO).negate(), 2),
                sumOf(MAX_38_DIGITS.add(belowMax), 2),
                sumOf(MAX_38_DIGITS.add(belowMax).negate(), 2),
                sumOf(MAX_38_DIGITS.multiply(BigInteger.valueOf(MAX_ROWS)), MAX_ROWS),
                sumOf(MAX_38_DIGITS.multiply(BigInteger.valueOf(MAX_ROWS)).negate(), MAX_ROWS),
                sumOf(MAX_38_DIGITS.multiply(BigInteger.valueOf(MAX_ROWS)).subtract(BigInteger.ONE), MAX_ROWS)));
    }

    @Test
    void testLimbCarries()
    {
        DecimalType type = createDecimalType(38, 0);
        long maxChunkSum = MAX_ROWS * MAX_UNSIGNED_CHUNK;
        assertDivision(type, ImmutableList.of(
                // each unsigned chunk sum carries into the chunks above it
                new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, 0, MAX_ROWS),
                new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, MAX_ROWS * 1_000_000_000L, MAX_ROWS),
                new ChunkSums(maxChunkSum, 0, 0, 0, 1),
                new ChunkSums(0, maxChunkSum, 0, 0, 3),
                new ChunkSums(0, 0, maxChunkSum, 0, MAX_ROWS),
                new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, -MAX_ROWS * 1_000_000_000L, MAX_ROWS)));
    }

    @Test
    void testCancellation()
    {
        DecimalType type = createDecimalType(38, 4);
        long maxChunkSum = MAX_ROWS * MAX_UNSIGNED_CHUNK;
        assertDivision(type, ImmutableList.of(
                // one row of -1 has chunks (2^32 - 1, 2^32 - 1, 2^32 - 1, -1)
                new ChunkSums(MAX_UNSIGNED_CHUNK, MAX_UNSIGNED_CHUNK, MAX_UNSIGNED_CHUNK, -1, 1),
                // +x and -x in the same frame
                sumOfValues(ImmutableList.of(MAX_38_DIGITS, MAX_38_DIGITS.negate())),
                sumOfValues(ImmutableList.of(MAX_38_DIGITS, MAX_38_DIGITS.negate(), BigInteger.valueOf(-3))),
                // large unsigned chunk sums cancelled by a negative top chunk sum
                new ChunkSums(maxChunkSum, maxChunkSum, maxChunkSum, -MAX_ROWS, MAX_ROWS)));
    }

    @Test
    void testPrecision29WorstCase()
    {
        for (int scale : new int[] {0, 2, 6, 29}) {
            DecimalType type = createDecimalType(29, scale);
            assertDivision(type, ImmutableList.of(
                    sumOfRepeated(MAX_29_DIGITS, MAX_ROWS),
                    sumOfRepeated(MAX_29_DIGITS.negate(), MAX_ROWS),
                    sumOfRepeated(MAX_29_DIGITS, 1),
                    sumOfRepeated(MAX_29_DIGITS.negate(), 1)));
        }
    }

    @Test
    void testUnsignedChunkSums()
    {
        DecimalType type = createDecimalType(38, 2);
        List<ChunkSums> cases = ImmutableList.of(
                sumOfRepeated(MAX_38_DIGITS, MAX_ROWS),
                sumOfRepeated(MAX_38_DIGITS.negate(), 3),
                sumOf(-5, 2));
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
        assertOverflow(createDecimalType(29, 0), sumOf(BigInteger.TEN.pow(29), 1));
        assertOverflow(createDecimalType(29, 0), sumOf(BigInteger.TEN.pow(29).negate(), 1));
        // rounds up past the precision
        assertOverflow(createDecimalType(29, 2), sumOf(MAX_29_DIGITS.multiply(BigInteger.TWO).add(BigInteger.ONE), 2));
        assertOverflow(createDecimalType(29, 2), sumOf(MAX_29_DIGITS.multiply(BigInteger.TWO).add(BigInteger.ONE).negate(), 2));
        assertOverflow(createDecimalType(38, 0), sumOf(MAX_38_DIGITS.add(BigInteger.ONE), 1));
        // quotients far beyond 128 bits must fail rather than wrap
        assertOverflow(createDecimalType(38, 0), sumOfRepeated(MAX_38_DIGITS, MAX_ROWS).withCount(1));
        assertOverflow(createDecimalType(38, 0), sumOfRepeated(MAX_38_DIGITS.negate(), MAX_ROWS).withCount(1));
        assertOverflow(createDecimalType(38, 0), new ChunkSums(0, 0, 0, Long.MAX_VALUE, 1));
        assertOverflow(createDecimalType(38, 0), new ChunkSums(0, 0, 0, Long.MIN_VALUE, 1));
    }

    @Test
    void testInvalidDivisor()
    {
        DecimalType type = createDecimalType(38, 2);
        assertTrinoExceptionThrownBy(() -> evaluateAndClose(type, ImmutableList.of(sumOf(5, 0))))
                .hasErrorCode(DIVISION_BY_ZERO);
        assertTrinoExceptionThrownBy(() -> evaluateAndClose(type, ImmutableList.of(sumOf(5, -1))))
                .hasErrorCode(NOT_SUPPORTED);
    }

    private static void assertDivision(DecimalType type, List<ChunkSums> cases)
    {
        try (ColumnVector result = evaluate(type, cases, DType.INT64);
                HostColumnVector host = result.copyToHost()) {
            assertThat(result.getType()).isEqualTo(DType.create(DType.DTypeEnum.DECIMAL128, -type.getScale()));
            assertResults(type, cases, host);
        }
    }

    private static void assertResults(DecimalType type, List<ChunkSums> cases, HostColumnVector host)
    {
        for (int row = 0; row < cases.size(); row++) {
            ChunkSums chunkSums = cases.get(row);
            assertThat(host.getBigDecimal(row))
                    .as("%s", chunkSums)
                    .isEqualTo(chunkSums.expectedAverage(type));
        }
    }

    private static void assertOverflow(DecimalType type, ChunkSums chunkSums)
    {
        assertTrinoExceptionThrownBy(() -> evaluateAndClose(type, ImmutableList.of(chunkSums)))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    private static void evaluateAndClose(DecimalType type, List<ChunkSums> cases)
    {
        evaluate(type, cases, DType.INT64).close();
    }

    private static ColumnVector evaluate(DecimalType type, List<ChunkSums> cases, DType unsignedChunkSumType)
    {
        List<ColumnVector> columns = new ArrayList<>();
        try {
            for (int chunkIndex = 0; chunkIndex < 4; chunkIndex++) {
                int index = chunkIndex;
                try (ColumnVector chunkSums = ColumnVector.fromLongs(cases.stream().mapToLong(chunkSum -> chunkSum.chunk(index)).toArray())) {
                    DType chunkSumType = chunkIndex < 3 ? unsignedChunkSumType : DType.INT64;
                    columns.add(chunkSums.castTo(chunkSumType));
                }
            }
            columns.add(ColumnVector.fromLongs(cases.stream().mapToLong(ChunkSums::count).toArray()));
            return expression(type).evaluate(cases.size(), columns, new Bindings());
        }
        finally {
            columns.forEach(ColumnVector::close);
        }
    }

    private static GpuExpression expression(DecimalType type)
    {
        return new GpuDivideSumChunksRoundToScale(
                ImmutableList.of(new GetColumn(0), new GetColumn(1), new GetColumn(2), new GetColumn(3)),
                new GetColumn(4),
                type);
    }

    private static ChunkSums sumOf(long sum, long count)
    {
        return sumOf(BigInteger.valueOf(sum), count);
    }

    // chunk sums of a single-row 160-bit sum, not limited to 128 bits
    private static ChunkSums sumOf(BigInteger sum, long count)
    {
        return new ChunkSums(unsignedChunk(sum, 0), unsignedChunk(sum, 1), unsignedChunk(sum, 2), sum.shiftRight(96).longValueExact(), count);
    }

    // chunk sums of `rows` rows of the same DECIMAL128 value, as the rolling sums of its 32-bit chunks produce
    private static ChunkSums sumOfRepeated(BigInteger value, long rows)
    {
        return new ChunkSums(
                unsignedChunk(value, 0) * rows,
                unsignedChunk(value, 1) * rows,
                unsignedChunk(value, 2) * rows,
                value.shiftRight(96).intValueExact() * rows,
                rows);
    }

    private static ChunkSums sumOfValues(List<BigInteger> values)
    {
        long[] chunks = new long[4];
        for (BigInteger value : values) {
            chunks[0] += unsignedChunk(value, 0);
            chunks[1] += unsignedChunk(value, 1);
            chunks[2] += unsignedChunk(value, 2);
            chunks[3] += value.shiftRight(96).intValueExact();
        }
        return new ChunkSums(chunks[0], chunks[1], chunks[2], chunks[3], values.size());
    }

    private static long unsignedChunk(BigInteger value, int chunkIndex)
    {
        // BigInteger shifts and masks act on the two's complement representation, also for negative values
        return value.shiftRight(32 * chunkIndex).and(BigInteger.valueOf(MAX_UNSIGNED_CHUNK)).longValueExact();
    }

    private record ChunkSums(long chunk0, long chunk1, long chunk2, long chunk3, long count)
    {
        long chunk(int index)
        {
            return switch (index) {
                case 0 -> chunk0;
                case 1 -> chunk1;
                case 2 -> chunk2;
                case 3 -> chunk3;
                default -> throw new IllegalArgumentException("Invalid chunk index: " + index);
            };
        }

        ChunkSums withCount(long newCount)
        {
            return new ChunkSums(chunk0, chunk1, chunk2, chunk3, newCount);
        }

        BigInteger sum()
        {
            return BigInteger.valueOf(chunk3).shiftLeft(96)
                    .add(BigInteger.valueOf(chunk2).shiftLeft(64))
                    .add(BigInteger.valueOf(chunk1).shiftLeft(32))
                    .add(BigInteger.valueOf(chunk0));
        }

        BigDecimal expectedAverage(DecimalType type)
        {
            BigDecimal average = new BigDecimal(sum(), type.getScale()).divide(BigDecimal.valueOf(count), type.getScale(), HALF_UP);
            assertThat(average.unscaledValue().abs()).isLessThanOrEqualTo(BigInteger.TEN.pow(type.getPrecision()).subtract(BigInteger.ONE));
            return average;
        }
    }
}
