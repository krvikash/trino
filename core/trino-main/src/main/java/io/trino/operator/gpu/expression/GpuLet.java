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
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.gpu.expression.Fallibility.worst;
import static io.trino.operator.gpu.expression.GpuExpressions.allNullOnNullInputs;
import static java.util.Objects.requireNonNull;

/// Realizes [io.trino.sql.ir.Let] on the GPU: evaluates `value` once and appends the resulting column to
/// the input list while `body` is evaluated, so references to the bound symbol ([GpuBoundReference]) read
/// it there.
public final class GpuLet
        extends GpuExpression
{
    private final GpuExpression value;
    private final GpuExpression body;

    public GpuLet(GpuExpression value, GpuExpression body)
    {
        this.value = requireNonNull(value, "value is null");
        this.body = requireNonNull(body, "body is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector bound = value.evaluate(positionCount, inputColumns, bindings)) {
            List<ColumnVector> extended = ImmutableList.<ColumnVector>builderWithExpectedSize(inputColumns.size() + 1)
                    .addAll(inputColumns)
                    .add(bound)
                    .build();
            return body.evaluate(positionCount, extended, bindings);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        Fallibility bodyFallibility = body.fallibility();
        if (bodyFallibility == FALLIBLE_EXCEPT_ALL_NULLS && !value.nullOnNullInputs()) {
            // The bound column is one of the body's inputs, and it is not NULL on an all-NULL row
            bodyFallibility = FALLIBLE;
        }
        return worst(value.fallibility(), bodyFallibility);
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return allNullOnNullInputs(ImmutableList.of(value, body));
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuLet other
                && value.equals(other.value)
                && body.equals(other.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), value, body);
    }
}
