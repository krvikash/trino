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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.DType.DTypeEnum.DECIMAL128;
import static java.util.Objects.requireNonNull;

public final class GpuWideningShortDecimalArithmetic
        extends GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;
    private final BinaryOp op;
    private final DType outputType;

    public GpuWideningShortDecimalArithmetic(GpuExpression left, GpuExpression right, BinaryOp op, DType outputType)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.op = requireNonNull(op, "op is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        // Widen inputs to DECIMAL128 so the result doesn't overflow DECIMAL64.
        try (ClosingOnce<ColumnVector> leftResult = ClosingOnce.own(left.evaluate(positionCount, inputColumns));
                ColumnVector leftWide = leftResult.borrow().castTo(DType.create(DECIMAL128, leftResult.borrow().getType().getScale()))) {
            leftResult.close();
            try (ClosingOnce<ColumnVector> rightResult = ClosingOnce.own(right.evaluate(positionCount, inputColumns));
                    ColumnVector rightWide = rightResult.borrow().castTo(DType.create(DECIMAL128, rightResult.borrow().getType().getScale()))) {
                rightResult.close();
                return leftWide.binaryOp(op, rightWide, outputType);
            }
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuWideningShortDecimalArithmetic other
                && left.equals(other.left)
                && right.equals(other.right)
                && op == other.op
                && outputType.equals(other.outputType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), left, right, op, outputType);
    }
}
