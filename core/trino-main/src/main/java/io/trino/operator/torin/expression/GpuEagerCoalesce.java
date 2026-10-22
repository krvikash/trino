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

import ai.rapids.cudf.ColumnVector;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.torin.expression.GpuExpressions.allNullOnNullInputs;
import static io.trino.operator.torin.expression.GpuExpressions.worstOf;

/// `COALESCE(v1, v2, ...)`: the value of the first operand that is not NULL, per row, with every
/// operand evaluated over every row. The CPU stops at the first non-NULL operand, so an operand that
/// can fail belongs in the lazy form [GpuIf] provides instead.
public final class GpuEagerCoalesce
        extends GpuExpression
{
    private final List<GpuExpression> operands;

    public GpuEagerCoalesce(List<GpuExpression> operands)
    {
        checkArgument(operands.size() >= 2, "COALESCE requires at least 2 operands");
        this.operands = ImmutableList.copyOf(operands);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
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
        return obj instanceof GpuEagerCoalesce other && operands.equals(other.operands);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), operands);
    }
}
