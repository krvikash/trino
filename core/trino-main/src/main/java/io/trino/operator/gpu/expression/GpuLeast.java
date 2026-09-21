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
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.expression.GpuExpressions.anyNullOnNullInputs;
import static io.trino.operator.gpu.expression.GpuExpressions.worstOf;
import static java.util.Objects.requireNonNull;

/// `LEAST(a, b, ...)`: the smallest operand, or NULL when any operand is NULL.
public final class GpuLeast
        extends GpuExpression
{
    private final List<GpuExpression> operands;
    private final DType outputType;

    public GpuLeast(List<GpuExpression> operands, DType outputType)
    {
        checkArgument(operands.size() >= 2, "LEAST requires at least 2 operands");
        this.operands = ImmutableList.copyOf(operands);
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ClosingRef<ColumnVector> result = ClosingRef.own(operands.getFirst().evaluate(positionCount, inputColumns))) {
            for (int i = 1; i < operands.size(); i++) {
                try (ColumnVector current = result.take();
                        ColumnVector next = operands.get(i).evaluate(positionCount, inputColumns);
                        ColumnVector minimum = current.binaryOp(BinaryOp.NULL_MIN, next, outputType)) {
                    // NULL_MIN treats NULL as the identity element instead of propagating it, so
                    // restore the NULL by intersecting the operand validity masks: a row is NULL
                    // when any operand seen so far is NULL.
                    result.set(minimum.mergeAndSetValidity(BinaryOp.BITWISE_AND, current, next));
                }
            }
            return result.take();
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return worstOf(operands);
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return anyNullOnNullInputs(operands);
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuLeast other
                && operands.equals(other.operands)
                && outputType.equals(other.outputType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), operands, outputType);
    }
}
