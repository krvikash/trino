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
import ai.rapids.cudf.Scalar;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GpuStartsWith
        extends GpuExpression
{
    private final GpuExpression source;
    private final String prefix;

    public GpuStartsWith(GpuExpression source, String prefix)
    {
        this.source = requireNonNull(source, "source is null");
        this.prefix = requireNonNull(prefix, "prefix is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector sourceColumn = source.evaluate(positionCount, inputColumns, bindings);
                Scalar prefixScalar = Scalar.fromString(prefix)) {
            return sourceColumn.startsWith(prefixScalar);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return source.fallibility();
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return source.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuStartsWith other
                && source.equals(other.source)
                && prefix.equals(other.prefix);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, prefix);
    }
}
