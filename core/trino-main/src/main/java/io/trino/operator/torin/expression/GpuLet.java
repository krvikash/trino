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
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static io.trino.operator.torin.expression.Fallibility.FALLIBLE;
import static io.trino.operator.torin.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.torin.expression.Fallibility.worst;
import static io.trino.operator.torin.expression.GpuExpressions.allNullOnNullInputs;
import static java.util.Objects.requireNonNull;

/// Evaluates a LET value once and extends the body's binding scope without changing input channels.
public final class GpuLet
        extends GpuExpression
{
    private final int bindingId;
    private final GpuExpression value;
    private final GpuExpression body;

    public GpuLet(int bindingId, GpuExpression value, GpuExpression body)
    {
        this.bindingId = bindingId;
        this.value = requireNonNull(value, "value is null");
        this.body = requireNonNull(body, "body is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector bound = value.evaluate(positionCount, inputColumns, bindings)) {
            bindings.put(bindingId, bound);
            try {
                return body.evaluate(positionCount, inputColumns, bindings);
            }
            finally {
                bindings.remove(bindingId);
            }
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
                && bindingId == other.bindingId
                && value.equals(other.value)
                && body.equals(other.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), bindingId, value, body);
    }
}
