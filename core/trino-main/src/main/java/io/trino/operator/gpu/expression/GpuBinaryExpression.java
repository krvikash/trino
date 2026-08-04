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
import ai.rapids.cudf.BinaryOperable;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Wrapper around cuDF {@link ColumnVector#binaryOp(BinaryOp, BinaryOperable, DType)}.
 */
public final class GpuBinaryExpression
        extends GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;
    private final BinaryOp operation;
    private final DType outputType;

    public GpuBinaryExpression(GpuExpression left, GpuExpression right, BinaryOp operation, DType outputType)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.operation = requireNonNull(operation, "operation is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector leftResult = left.evaluate(positionCount, inputColumns);
                ColumnVector rightResult = right.evaluate(positionCount, inputColumns)) {
            return leftResult.binaryOp(operation, rightResult, outputType);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuBinaryExpression other
                && left.equals(other.left)
                && right.equals(other.right)
                && operation == other.operation
                && outputType.equals(other.outputType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), left, right, operation, outputType);
    }
}
