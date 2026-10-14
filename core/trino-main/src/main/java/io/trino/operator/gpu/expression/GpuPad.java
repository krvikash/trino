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
import ai.rapids.cudf.PadSide;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// Adds `fill` on the given side of each string until it is `length` code points long, leaving longer strings unchanged.
public final class GpuPad
        extends GpuExpression
{
    private final GpuExpression argument;
    private final int length;
    private final PadSide side;
    private final String fill;

    public GpuPad(GpuExpression argument, int length, PadSide side, String fill)
    {
        this.argument = requireNonNull(argument, "argument is null");
        checkArgument(length >= 0, "length is negative: %s", length);
        this.length = length;
        this.side = requireNonNull(side, "side is null");
        requireNonNull(fill, "fill is null");
        // cuDF takes a single Java char, which JNI passes as modified UTF-8; that differs from UTF-8 for NUL and surrogates
        checkArgument(fill.length() == 1 && fill.charAt(0) != 0 && !Character.isSurrogate(fill.charAt(0)), "fill is not a single non-NUL BMP character: %s", fill);
        this.fill = fill;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector value = argument.evaluate(positionCount, inputColumns, bindings)) {
            // cuDF pad widths are code point counts
            return value.pad(length, side, fill);
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
        return obj instanceof GpuPad other
                && argument.equals(other.argument)
                && length == other.length
                && side == other.side
                && fill.equals(other.fill);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument, length, side, fill);
    }
}
