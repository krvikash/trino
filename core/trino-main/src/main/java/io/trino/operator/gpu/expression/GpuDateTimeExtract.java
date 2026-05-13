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
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class GpuDateTimeExtract
        implements GpuExpression
{
    private final GpuExpression argument;
    private final Field field;

    public GpuDateTimeExtract(GpuExpression argument, Field field)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.field = requireNonNull(field, "field is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector timestamp = argument.evaluate(positionCount, inputColumns)) {
            return field.extract(timestamp);
        }
    }

    public enum Field
    {
        DAY(ColumnVector::day),
        HOUR(ColumnVector::hour),
        MINUTE(ColumnVector::minute),
        SECOND(ColumnVector::second);
        // TODO millisecond, week, month, quarter, year, day_of_week, day_of_year

        private final Function<ColumnVector, ColumnVector> extractor;

        Field(Function<ColumnVector, ColumnVector> extractor)
        {
            this.extractor = requireNonNull(extractor, "extractor is null");
        }

        public @Move ColumnVector extract(ColumnVector dateTime)
        {
            return extractor.apply(dateTime);
        }
    }
}
