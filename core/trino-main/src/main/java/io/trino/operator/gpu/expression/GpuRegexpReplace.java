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
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static ai.rapids.cudf.CaptureGroups.EXTRACT;
import static ai.rapids.cudf.CaptureGroups.NON_CAPTURE;
import static io.trino.operator.gpu.expression.CudfUtils.cudfAcceptsRegex;
import static java.util.Objects.requireNonNull;

public final class GpuRegexpReplace
        extends GpuExpression
{
    private final GpuExpression source;
    private final String cudfPattern;
    private final String cudfReplacement;
    private final boolean hasBackreferences;

    private GpuRegexpReplace(GpuExpression source, String cudfPattern, String cudfReplacement, boolean hasBackreferences)
    {
        this.source = requireNonNull(source, "source is null");
        this.cudfPattern = requireNonNull(cudfPattern, "cudfPattern is null");
        this.cudfReplacement = requireNonNull(cudfReplacement, "cudfReplacement is null");
        this.hasBackreferences = hasBackreferences;
    }

    /// Returns `Optional.empty()` when cuDF rejects the pattern, which it would do for every batch
    public static Optional<GpuRegexpReplace> create(GpuExpression source, String cudfPattern, String cudfReplacement, boolean hasBackreferences)
    {
        if (!cudfAcceptsRegex(cudfPattern, input -> replace(input, cudfPattern, cudfReplacement, hasBackreferences))) {
            return Optional.empty();
        }
        return Optional.of(new GpuRegexpReplace(source, cudfPattern, cudfReplacement, hasBackreferences));
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector sourceColumn = source.evaluate(positionCount, inputColumns, bindings)) {
            return replace(sourceColumn, cudfPattern, cudfReplacement, hasBackreferences);
        }
    }

    private static @Move ColumnVector replace(@Borrow ColumnVector source, String cudfPattern, String cudfReplacement, boolean hasBackreferences)
    {
        if (hasBackreferences) {
            RegexProgram program = new RegexProgram(cudfPattern, EXTRACT);
            return source.stringReplaceWithBackrefs(program, cudfReplacement);
        }
        RegexProgram program = new RegexProgram(cudfPattern, NON_CAPTURE);
        try (Scalar replacementScalar = Scalar.fromString(cudfReplacement)) {
            return source.replaceRegex(program, replacementScalar);
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
        return obj instanceof GpuRegexpReplace other
                && source.equals(other.source)
                && cudfPattern.equals(other.cudfPattern)
                && cudfReplacement.equals(other.cudfReplacement)
                && hasBackreferences == other.hasBackreferences;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, cudfPattern, cudfReplacement, hasBackreferences);
    }
}
