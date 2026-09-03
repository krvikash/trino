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
import ai.rapids.cudf.PadSide;
import ai.rapids.cudf.Scalar;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// Implements `concat(char(t), char(u)) → char(t + u)`.
///
/// @see GpuVarcharConcat
public final class GpuCharConcat
        extends GpuExpression
{
    private static final String SPACE = " ";

    private final GpuExpression left;
    private final GpuExpression right;
    private final int leftLength;

    public GpuCharConcat(GpuExpression left, GpuExpression right, int leftLength)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        checkArgument(leftLength > 0, "leftLength must be positive, got %s", leftLength);
        this.leftLength = leftLength;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector concatenated = concatenatePaddedLeft(positionCount, inputColumns);
                Scalar space = Scalar.fromString(SPACE)) {
            // Trino stores char(n) without its trailing spaces, so the padding added above must not
            // survive into the result. It only can when the right argument is an empty string, because
            // a non-empty char(n) argument is itself stored without trailing spaces.
            return concatenated.rstrip(space);
        }
    }

    private @Move ColumnVector concatenatePaddedLeft(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ClosingOnce<ColumnVector> leftColumn = ClosingOnce.own(left.evaluate(positionCount, inputColumns));
                ColumnVector paddedLeft = leftColumn.borrow().pad(leftLength, PadSide.RIGHT, SPACE)) {
            leftColumn.close();
            try (ColumnVector rightColumn = right.evaluate(positionCount, inputColumns)) {
                // cuDF makes a row null whenever any of its inputs is null, matching Trino's null propagation
                return ColumnVector.stringConcatenate(new ColumnView[] {paddedLeft, rightColumn});
            }
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuCharConcat other
                && left.equals(other.left)
                && right.equals(other.right)
                && leftLength == other.leftLength;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), left, right, leftLength);
    }
}
