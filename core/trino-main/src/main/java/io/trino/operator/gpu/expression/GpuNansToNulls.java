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
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GpuNansToNulls
        extends GpuExpression
{
    private final GpuExpression source;

    public GpuNansToNulls(GpuExpression source)
    {
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector input = source.evaluate(positionCount, inputColumns);
                ColumnVector isNotNan = input.isNotNan();
                Scalar nullScalar = Scalar.fromNull(input.getType())) {
            return isNotNan.ifElse(input, nullScalar);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuNansToNulls other && source.equals(other.source);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source);
    }
}
