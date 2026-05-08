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
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;

import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.DType.DTypeEnum.DECIMAL128;
import static java.util.Objects.requireNonNull;

public class GpuWideningShortDecimalMultiply
        implements GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;
    private final DType outputType;

    public GpuWideningShortDecimalMultiply(GpuExpression left, GpuExpression right, DType outputType)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        // Widen inputs to DECIMAL128 so the product doesn't overflow DECIMAL64.
        // No overflow detection needed: both inputs have at most 18 digits,
        // so the product needs at most 36 digits, which fits in DECIMAL128 (38 digits).
        try (ClosingOnce<ColumnVector> leftResult = ClosingOnce.own(left.evaluate(positionCount, inputColumns));
                ColumnVector leftWide = leftResult.borrow().castTo(DType.create(DECIMAL128, leftResult.borrow().getType().getScale()))) {
            leftResult.close();
            try (ClosingOnce<ColumnVector> rightResult = ClosingOnce.own(right.evaluate(positionCount, inputColumns));
                    ColumnVector rightWide = rightResult.borrow().castTo(DType.create(DECIMAL128, rightResult.borrow().getType().getScale()))) {
                rightResult.close();
                return leftWide.binaryOp(MUL, rightWide, outputType);
            }
        }
    }
}
