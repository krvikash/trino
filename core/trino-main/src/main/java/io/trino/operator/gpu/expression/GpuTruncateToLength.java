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
import static java.util.Objects.requireNonNull;

/// Keeps at most `length` leading code points of each string, leaving shorter strings unchanged.
public final class GpuTruncateToLength
        extends GpuExpression
{
    private final GpuExpression argument;
    private final int length;

    public GpuTruncateToLength(GpuExpression argument, int length)
    {
        this.argument = requireNonNull(argument, "argument is null");
        checkArgument(length >= 0, "length is negative: %s", length);
        this.length = length;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector value = argument.evaluate(positionCount, inputColumns, bindings)) {
            // cuDF substring positions are code point offsets, and the end position is clamped to the string length
            return value.substring(0, length);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return argument.fallibility();
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuTruncateToLength other
                && argument.equals(other.argument)
                && length == other.length;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument, length);
    }
}
