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

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GpuInColumn
        extends GpuExpression
{
    private final GpuExpression source;
    private final GpuExpression values;

    public GpuInColumn(GpuExpression source, GpuExpression values)
    {
        this.source = requireNonNull(source, "source is null");
        this.values = requireNonNull(values, "values is null");
    }

    @Override
    public ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector source = this.source.evaluate(positionCount, inputColumns);
                ColumnVector values = this.values.evaluate(positionCount, inputColumns)) {
            return source.contains(values);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuInColumn other
                && source.equals(other.source)
                && values.equals(other.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, values);
    }
}
