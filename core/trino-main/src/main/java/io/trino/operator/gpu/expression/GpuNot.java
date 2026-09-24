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
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GpuNot
        extends GpuExpression
{
    private final GpuExpression operand;

    public GpuNot(GpuExpression operand)
    {
        this.operand = requireNonNull(operand, "operand is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector result = operand.evaluate(positionCount, inputColumns, bindings)) {
            return result.not();
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return operand.fallibility();
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return operand.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuNot other && operand.equals(other.operand);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), operand);
    }
}
