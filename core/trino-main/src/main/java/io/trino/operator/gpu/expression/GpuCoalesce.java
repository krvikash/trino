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
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.expression.GpuExpressions.allNullOnNullInputs;
import static io.trino.operator.gpu.expression.GpuExpressions.worstOf;

/// `COALESCE(v1, v2, ...)`: the value of the first operand that is not NULL, per row.
public final class GpuCoalesce
        extends GpuExpression
{
    private final List<GpuExpression> operands;

    public GpuCoalesce(List<GpuExpression> operands)
    {
        checkArgument(operands.size() >= 2, "COALESCE requires at least 2 operands");
        this.operands = ImmutableList.copyOf(operands);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        // TODO (https://starburstdata.atlassian.net/browse/ENG-26721) evaluate the operands lazily,
        //  the way GpuIf evaluates its branches. Every operand is evaluated over every row here, while
        //  the CPU (CoalesceCodeGenerator) stops at the first non-NULL one, so a fallible later operand
        //  can fail a query the CPU would have answered: COALESCE(a, 10 / b) raises division by zero on
        //  rows where a is not NULL and b is 0.
        try (ClosingRef<ColumnVector> result = ClosingRef.own(operands.getFirst().evaluate(positionCount, inputColumns, bindings))) {
            for (int i = 1; i < operands.size(); i++) {
                try (ColumnVector current = result.take();
                        ColumnVector replacement = operands.get(i).evaluate(positionCount, inputColumns, bindings)) {
                    result.set(current.replaceNulls(replacement));
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
        return allNullOnNullInputs(operands);
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuCoalesce other && operands.equals(other.operands);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), operands);
    }
}
