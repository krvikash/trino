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
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Packs a DECIMAL128 sum column and an INT64 count column into a 32-byte VARBINARY
 * compatible with {@link io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateSerializer}.
 * Always produces the full 4-long form: {@code [sum_low(8), sum_high(8), count(8), overflow(8)]}.
 * Overflow is always zero because short-decimal sums cannot overflow DECIMAL128.
 */
public final class GpuPackAvgDecimalState
        extends GpuExpression
{
    @Override
    public @Move ColumnVector evaluate(int positionCount, @Borrow List<ColumnVector> inputColumns)
    {
        checkState(inputColumns.size() == 2, "Expected 2 input columns (sum, count), got %s", inputColumns.size());
        @Borrow ColumnVector sumColumn = inputColumns.get(0);
        @Borrow ColumnVector countColumn = inputColumns.get(1);

        checkState(sumColumn.getType().getTypeId() == DType.DTypeEnum.DECIMAL128, "Expected DECIMAL128 sum, got %s", sumColumn.getType());
        checkState(countColumn.getType().equals(DType.INT64), "Expected INT64 count, got %s", countColumn.getType());

        try (ColumnVector sumBytes = GpuCombineSumChunksToVarbinary.decimal128ToBytes(sumColumn);
                ColumnVector countBytes = countColumn.asByteList(false);
                Scalar zeroScalar = Scalar.fromLong(0);
                ColumnVector overflowColumn = ColumnVector.fromScalar(zeroScalar, positionCount);
                ColumnVector overflowBytes = overflowColumn.asByteList(false)) {
            return ColumnVector.listConcatenateByRow(sumBytes, countBytes, overflowBytes);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuPackAvgDecimalState;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
