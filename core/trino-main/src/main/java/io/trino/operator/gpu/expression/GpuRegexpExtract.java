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
import ai.rapids.cudf.RegexProgram;
import ai.rapids.cudf.Table;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static ai.rapids.cudf.CaptureGroups.EXTRACT;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.expression.CudfUtils.cudfAcceptsRegex;
import static java.util.Objects.requireNonNull;

public final class GpuRegexpExtract
        extends GpuExpression
{
    private final GpuExpression source;
    private final String cudfPattern;

    private GpuRegexpExtract(GpuExpression source, String cudfPattern)
    {
        this.source = requireNonNull(source, "source is null");
        this.cudfPattern = requireNonNull(cudfPattern, "cudfPattern is null");
    }

    /// Returns `Optional.empty()` when cuDF rejects the pattern, which it would do for every batch
    ///
    /// @param cudfPattern cuDF pattern whose only capturing group is the one to extract
    public static Optional<GpuRegexpExtract> create(GpuExpression source, String cudfPattern)
    {
        if (!cudfAcceptsRegex(cudfPattern, input -> extract(input, cudfPattern))) {
            return Optional.empty();
        }
        return Optional.of(new GpuRegexpExtract(source, cudfPattern));
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector sourceColumn = source.evaluate(positionCount, inputColumns, bindings)) {
            return extract(sourceColumn, cudfPattern);
        }
    }

    private static @Move ColumnVector extract(@Borrow ColumnVector source, String cudfPattern)
    {
        try (Table extracted = source.extractRe(new RegexProgram(cudfPattern, EXTRACT))) {
            checkState(extracted.getNumberOfColumns() == 1, "Expected exactly one capturing group, got %s columns for pattern %s", extracted.getNumberOfColumns(), cudfPattern);
            // cuDF yields NULL both for a row without a match and for a group that did not participate in the match
            return extracted.getColumn(0).incRefCount();
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
        return obj instanceof GpuRegexpExtract other
                && source.equals(other.source)
                && cudfPattern.equals(other.cudfPattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, cudfPattern);
    }
}
