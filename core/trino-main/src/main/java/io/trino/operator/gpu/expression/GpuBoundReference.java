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

import static com.google.common.base.Preconditions.checkArgument;

/// Reads a [GpuLet]-bound column. Each enclosing [GpuLet] appends its bound value to the end of the input
/// list, so a binding is `deBruijnIndex` positions from the end, where 0 is the innermost enclosing
/// [GpuLet]. Indexing from the end keeps the reference independent of the number of source columns, so
/// binding resolves in a single compilation pass.
public final class GpuBoundReference
        extends GpuExpression
{
    private final int deBruijnIndex;

    public GpuBoundReference(int deBruijnIndex)
    {
        checkArgument(deBruijnIndex >= 0, "deBruijnIndex must be non-negative: %s", deBruijnIndex);
        this.deBruijnIndex = deBruijnIndex;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        return inputColumns.get(inputColumns.size() - 1 - deBruijnIndex).incRefCount();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuBoundReference other && deBruijnIndex == other.deBruijnIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), deBruijnIndex);
    }
}
