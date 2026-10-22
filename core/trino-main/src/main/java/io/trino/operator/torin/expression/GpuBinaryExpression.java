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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.BinaryOperable;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import com.google.common.collect.ImmutableList;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static io.trino.operator.torin.expression.GpuExpressions.anyNullOnNullInputs;
import static io.trino.operator.torin.expression.GpuExpressions.worstOf;
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
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector leftResult = left.evaluate(positionCount, inputColumns, bindings);
                ColumnVector rightResult = right.evaluate(positionCount, inputColumns, bindings)) {
            return leftResult.binaryOp(operation, rightResult, outputType);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return worstOf(ImmutableList.of(left, right));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return switch (operation) {
            case ADD, SUB, MUL, DIV, TRUE_DIV, FLOOR_DIV, MOD, PMOD, PYMOD, POW, INT_POW, LOG_BASE, ATAN2,
                 SHIFT_LEFT, SHIFT_RIGHT, SHIFT_RIGHT_UNSIGNED, BITWISE_AND, BITWISE_OR, BITWISE_XOR,
                 LOGICAL_AND, LOGICAL_OR, EQUAL, NOT_EQUAL, LESS, GREATER, LESS_EQUAL, GREATER_EQUAL -> anyNullOnNullInputs(ImmutableList.of(left, right));
            // The NULL_* operations produce a value from NULL operands
            case NULL_EQUALS, NULL_NOT_EQUALS, NULL_MAX, NULL_MIN, NULL_LOGICAL_AND, NULL_LOGICAL_OR -> false;
        };
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
