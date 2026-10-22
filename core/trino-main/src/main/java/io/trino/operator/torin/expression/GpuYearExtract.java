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
package io.trino.operator.torin.expression;

import ai.rapids.cudf.ColumnVector;
import com.google.common.collect.ImmutableList;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;
import java.util.Objects;

import static io.trino.operator.torin.expression.CudfUtils.checkYearWithinCudfCalendar;
import static io.trino.operator.torin.expression.GpuExpressions.nullSkippingCheck;
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
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector timestamp = argument.evaluate(positionCount, inputColumns, bindings)) {
            // cuDF's `ColumnView.year()` carries the year as an INT16 and silently returns a wrong
            // value outside cuDF's calendar, which date and low precision timestamp values can reach.
            checkYearWithinCudfCalendar(timestamp);
            return timestamp.year();
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(ImmutableList.of(argument));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
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
