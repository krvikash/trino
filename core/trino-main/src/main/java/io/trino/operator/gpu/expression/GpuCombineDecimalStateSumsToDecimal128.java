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

import ai.rapids.cudf.Aggregation128Utils;
import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.util.Objects.requireNonNull;

/**
 * Reassembles four INT64 chunk sums into a DECIMAL128 sum, throwing on overflow. Pair with
 * {@link GpuExtractDecimalStateChunk} on the input side to implement the GPU FINAL step of
 * {@code sum(decimal)}: the chunk sums combine the {@code low}/{@code high} 32-bit chunks of the
 * intermediate {@link io.trino.operator.aggregation.state.LongDecimalWithOverflowState} value, and
 * a fifth INT64 column carries the cumulative {@code overflow} field that the CPU PARTIAL
 * serializer emits when the per-state sum already overflowed 128 bits.
 *
 * <p>An overflow is signalled if either:
 * <ul>
 *   <li>{@link Aggregation128Utils#combineInt64SumChunks} reports the chunked recombination
 *       overflowed 128 bits, or</li>
 *   <li>any group's accumulated {@code overflow} sum is non-zero.</li>
 * </ul>
 */
public final class GpuCombineDecimalStateSumsToDecimal128
        extends GpuExpression
{
    private final DType decimal128Type;

    public GpuCombineDecimalStateSumsToDecimal128(DType decimal128Type)
    {
        this.decimal128Type = requireNonNull(decimal128Type, "decimal128Type is null");
        checkState(decimal128Type.getTypeId() == DType.DTypeEnum.DECIMAL128,
                "decimal128Type must be DECIMAL128, got %s",
                decimal128Type);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, @Borrow List<ColumnVector> inputColumns)
    {
        checkState(inputColumns.size() == 5,
                "Expected 4 chunk-sum columns plus an overflow-sum column, got %s",
                inputColumns.size());
        @Borrow ColumnVector overflowSum = inputColumns.get(4);

        try (Table chunks = new Table(
                inputColumns.get(0),
                inputColumns.get(1),
                inputColumns.get(2),
                inputColumns.get(3));
                Table assembled = Aggregation128Utils.combineInt64SumChunks(chunks, decimal128Type)) {
            checkState(assembled.getNumberOfColumns() == 2, "Expected 2-column result, got %s", assembled.getNumberOfColumns());
            if (anyTrue(assembled.getColumn(0)) || anyOverflowNonZero(overflowSum)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            // The Table owns its columns and will close them; bump the ref count so the DECIMAL128
            // column survives the table's close. The caller owns the returned column.
            return assembled.getColumn(1).incRefCount();
        }
    }

    private static boolean anyOverflowNonZero(@Borrow ColumnVector overflowSum)
    {
        try (Scalar zero = Scalar.fromLong(0L);
                ColumnVector nonZero = overflowSum.binaryOp(BinaryOp.NOT_EQUAL, zero, DType.BOOL8)) {
            return anyTrue(nonZero);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuCombineDecimalStateSumsToDecimal128 other && decimal128Type.equals(other.decimal128Type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), decimal128Type);
    }
}
