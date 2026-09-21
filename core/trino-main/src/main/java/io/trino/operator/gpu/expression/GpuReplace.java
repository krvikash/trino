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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// Replaces every occurrence of a literal substring, implementing Trino's `replace` function.
public final class GpuReplace
        extends GpuExpression
{
    private final GpuExpression source;
    private final String search;
    private final String replacement;

    public GpuReplace(GpuExpression source, String search, String replacement)
    {
        this.source = requireNonNull(source, "source is null");
        this.search = requireNonNull(search, "search is null");
        this.replacement = requireNonNull(replacement, "replacement is null");
        // cuDF rejects an empty target, and Trino's behavior for it differs from a plain substring replace
        checkArgument(!search.isEmpty(), "search is empty");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector sourceColumn = source.evaluate(positionCount, inputColumns);
                Scalar searchScalar = Scalar.fromString(search);
                Scalar replacementScalar = Scalar.fromString(replacement)) {
            // cuDF scans left to right and resumes after each match, so occurrences never overlap.
            // This matches Trino, which advances past the whole match too. Null rows stay null.
            return sourceColumn.stringReplace(searchScalar, replacementScalar);
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
        return obj instanceof GpuReplace other
                && source.equals(other.source)
                && search.equals(other.search)
                && replacement.equals(other.replacement);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, search, replacement);
    }
}
