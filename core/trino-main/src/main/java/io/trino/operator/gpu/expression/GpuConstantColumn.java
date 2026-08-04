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

public final class GpuConstantColumn
        extends GpuExpression
{
    private final @Borrow ColumnVector columnVector;

    public GpuConstantColumn(@Borrow ColumnVector columnVector)
    {
        this.columnVector = requireNonNull(columnVector, "columnVector is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        return columnVector.incRefCount();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuConstantColumn other
                // reference equality. there is no fast value-based equality for column vectors
                && columnVector == other.columnVector;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), System.identityHashCode(columnVector));
    }
}
