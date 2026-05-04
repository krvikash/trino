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
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Convert a DECIMAL128 column to LIST&lt;INT8&gt; with 16 native-byte-order bytes per row, so a
 * short-decimal sum result can land in a Trino VARBINARY block. Used after a short-decimal SUM
 * (precision &le; 18 input cast up to DECIMAL128) where overflow detection isn't required —
 * max input magnitude &lt; 10^18, max DECIMAL128 &gt; 10^38, max rows per cuDF batch ~ 2^31.
 *
 * <p>Implementation reuses {@link GpuCombineSumChunksToVarbinary#decimal128ToBytes} since
 * cuDF's {@code byte_cast} rejects fixed-point inputs directly.
 */
public class GpuDecimal128AsVarbinary
        implements GpuExpression
{
    @Override
    public @Move ColumnVector evaluate(int positionCount, @Borrow List<@Borrow ColumnVector> inputColumns)
    {
        checkState(inputColumns.size() == 1, "Expected exactly one input column, got %s", inputColumns.size());
        @Borrow ColumnVector input = inputColumns.getFirst();
        checkState(input.getType().getTypeId() == DType.DTypeEnum.DECIMAL128,
                "Expected DECIMAL128 input, got %s", input.getType());
        return GpuCombineSumChunksToVarbinary.decimal128ToBytes(input);
    }
}
