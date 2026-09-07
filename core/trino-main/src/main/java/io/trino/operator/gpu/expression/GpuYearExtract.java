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

import static io.trino.operator.gpu.expression.CudfUtils.checkYearWithinInt16Range;
import static java.util.Objects.requireNonNull;

/**
 * Extracts the year of a date/timestamp.
 *
 * @see GpuDateTimeExtract
 */
public final class GpuYearExtract
        extends GpuExpression
{
    private final GpuExpression argument;

    public GpuYearExtract(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector timestamp = argument.evaluate(positionCount, inputColumns)) {
            // cuDF's `ColumnView.year()` returns INT16, silently overflows for years outside [-32768, 32767],
            // which is possible to date and low precision timestamp values.
            checkYearWithinInt16Range(timestamp);
            return timestamp.year();
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuYearExtract other && argument.equals(other.argument);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument);
    }
}
