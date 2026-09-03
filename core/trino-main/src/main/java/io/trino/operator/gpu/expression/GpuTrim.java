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
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// Removes the longest prefix and/or suffix built from a fixed set of code points, implementing
/// Trino's `trim`, `ltrim` and `rtrim`.
public final class GpuTrim
        extends GpuExpression
{
    /// Code points removed by the single-argument `trim`, `ltrim` and `rtrim`.
    public static final String WHITESPACE_CODE_POINTS = whitespaceCodePoints();

    private final GpuExpression source;
    private final String codePointsToTrim;
    private final Side side;
    private final boolean removeTrailingSpaces;

    public GpuTrim(GpuExpression source, String codePointsToTrim, Side side, boolean removeTrailingSpaces)
    {
        this.source = requireNonNull(source, "source is null");
        this.codePointsToTrim = requireNonNull(codePointsToTrim, "codePointsToTrim is null");
        // An empty set would make cuDF fall back to its own whitespace definition
        checkArgument(!codePointsToTrim.isEmpty(), "codePointsToTrim is empty");
        this.side = requireNonNull(side, "side is null");
        this.removeTrailingSpaces = removeTrailingSpaces;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        // The code points are always passed explicitly: cuDF's no-argument strip() removes everything up
        // to and including the space character, which is neither a subset nor a superset of Trino's
        // whitespace, so relying on it would silently produce wrong results.
        try (ClosingOnce<ColumnVector> sourceColumn = ClosingOnce.own(source.evaluate(positionCount, inputColumns));
                Scalar codePoints = Scalar.fromString(codePointsToTrim);
                ClosingRef<ColumnVector> stripped = ClosingRef.own(strip(sourceColumn.borrow(), codePoints))) {
            sourceColumn.close();
            if (!removeTrailingSpaces) {
                return stripped.take();
            }
            // Only to match the CPU: StringFunctions.charTrim and charRightTrim apply
            // Chars.trimTrailingSpaces after removing the code points. It does not follow from the result
            // type, which is varchar(x), and upstream considers the behavior a bug --
            // https://github.com/trinodb/trino/issues/31004 -- so this mirrors it deliberately.
            try (Scalar space = Scalar.fromString(" ")) {
                return stripped.borrow().rstrip(space);
            }
        }
    }

    private @Move ColumnVector strip(@Borrow ColumnVector column, @Borrow Scalar codePoints)
    {
        return switch (side) {
            case BOTH -> column.strip(codePoints);
            case LEADING -> column.lstrip(codePoints);
            case TRAILING -> column.rstrip(codePoints);
        };
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuTrim other
                && source.equals(other.source)
                && codePointsToTrim.equals(other.codePointsToTrim)
                && side == other.side
                && removeTrailingSpaces == other.removeTrailingSpaces;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, codePointsToTrim, side, removeTrailingSpaces);
    }

    private static String whitespaceCodePoints()
    {
        // SliceUtf8, which backs the single-argument trim functions, classifies a code point as
        // whitespace exactly when Character.isWhitespace does
        StringBuilder codePoints = new StringBuilder();
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isWhitespace(codePoint)) {
                codePoints.appendCodePoint(codePoint);
            }
        }
        return codePoints.toString();
    }

    /// The end(s) of the string the code points are removed from.
    public enum Side
    {
        BOTH,
        LEADING,
        TRAILING,
    }
}
