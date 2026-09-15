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
import ai.rapids.cudf.ColumnView;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/// Implements `concat(char(t), char(u)) → char(t + u)`.
///
/// @see GpuVarcharConcat
public final class GpuCharConcat
        extends GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;

    public GpuCharConcat(GpuExpression left, GpuExpression right)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector leftColumn = left.evaluate(positionCount, inputColumns);
                ColumnVector rightColumn = right.evaluate(positionCount, inputColumns)) {
            // cuDF makes a row null whenever any of its inputs is null, matching Trino's null propagation
            return ColumnVector.stringConcatenate(new ColumnView[] {leftColumn, rightColumn});
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuCharConcat other
                && left.equals(other.left)
                && right.equals(other.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), left, right);
    }
}
