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
import ai.rapids.cudf.Scalar;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.type.DecimalType;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.torin.expression.CudfUtils.anyTrue;
import static io.trino.operator.torin.expression.CudfUtils.combineSumChunks;
import static io.trino.operator.torin.expression.GpuExpressions.anyNullOnNullInputs;
import static io.trino.operator.torin.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;

/// Reassembles a sum into a value of the result type. The sum is given as the four 64-bit sums of the 32-bit chunks
/// [GpuExtractInt32Chunk] extracts from DECIMAL128 values, and equals
/// `chunk0 + chunk1 * 2^32 + chunk2 * 2^64 + chunk3 * 2^96`.
///
/// Raises an error for a sum that does not fit 128 bits, and for a sum that does but exceeds the precision of the
/// result type, as `sum(decimal)` does on the CPU.
///
/// In every row, the four chunk sums must be either all NULL or all non-NULL. cuDF decides which rows of the result
/// are NULL by looking at the first chunk sum only, so a row where just another chunk sum is NULL would come out non-NULL,
/// assembled from whatever value that chunk sum holds under its NULL.
public final class GpuCombineSumChunksToDecimal128
        extends GpuExpression
{
    private final List<GpuExpression> chunkSums;
    private final DecimalType resultType;

    public GpuCombineSumChunksToDecimal128(List<GpuExpression> chunkSums, DecimalType resultType)
    {
        checkArgument(chunkSums.size() == 4, "Expected 4 chunk sums, got %s", chunkSums.size());
        this.chunkSums = ImmutableList.copyOf(chunkSums);
        checkArgument(!resultType.isShort(), "Expected a long decimal result type, got %s", resultType);
        this.resultType = resultType;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        // Trino scale s means unscaled / 10^s; cuDF scale convention is unscaled * 10^scale, so negate
        DType decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, -resultType.getScale());
        try (ColumnVector chunk0 = chunkSums.get(0).evaluate(positionCount, inputColumns, bindings);
                ColumnVector chunk1 = chunkSums.get(1).evaluate(positionCount, inputColumns, bindings);
                ColumnVector chunk2 = chunkSums.get(2).evaluate(positionCount, inputColumns, bindings);
                ColumnVector chunk3 = chunkSums.get(3).evaluate(positionCount, inputColumns, bindings);
                ColumnVector sum = combineSumChunks(List.of(chunk0, chunk1, chunk2, chunk3), decimal128Type)) {
            failOnOutOfRange(sum, decimal128Type);
            return sum.incRefCount();
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(chunkSums);
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return anyNullOnNullInputs(chunkSums);
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuCombineSumChunksToDecimal128 other
                && chunkSums.equals(other.chunkSums)
                && resultType.equals(other.resultType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), chunkSums, resultType);
    }

    private void failOnOutOfRange(@Borrow ColumnVector sum, DType decimal128Type)
    {
        BigInteger maxUnscaled = BigInteger.TEN.pow(resultType.getPrecision()).subtract(BigInteger.ONE);
        try (Scalar min = Scalar.fromDecimal(maxUnscaled.negate(), decimal128Type);
                Scalar max = Scalar.fromDecimal(maxUnscaled, decimal128Type);
                ColumnVector belowMin = sum.binaryOp(LESS, min, DType.BOOL8);
                ColumnVector aboveMax = sum.binaryOp(GREATER, max, DType.BOOL8);
                ColumnVector outOfRange = belowMin.binaryOp(NULL_LOGICAL_OR, aboveMax, DType.BOOL8)) {
            if (anyTrue(outOfRange)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
        }
    }
}
