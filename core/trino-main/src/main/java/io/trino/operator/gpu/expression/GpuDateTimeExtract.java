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
import ai.rapids.cudf.DType;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        try (@Own ClosingOnce<ColumnVector> timestamp = ClosingOnce.own(argument.evaluate(positionCount, inputColumns));
                @Own ColumnVector extracted = field.extract(timestamp.borrow())) {
            timestamp.close();
            return extracted.castTo(field.resultDType());
        }
    }

    public enum Field
    {
        // cuDF extraction returns INT16; Trino returns BIGINT (INT64) for all of these.
        DAY("day", ColumnVector::day, DType.INT64),
        HOUR("hour", ColumnVector::hour, DType.INT64),
        MINUTE("minute", ColumnVector::minute, DType.INT64),
        SECOND("second", ColumnVector::second, DType.INT64);
        // TODO millisecond, week, month, quarter, year, day_of_week, day_of_year

        private static final Map<String, Field> BY_TRINO_FUNCTION_NAME;

        static {
            ImmutableMap.Builder<String, Field> builder = ImmutableMap.builder();
            for (Field field : values()) {
                builder.put(field.trinoFunctionName, field);
            }
            BY_TRINO_FUNCTION_NAME = builder.buildOrThrow();
        }

        private final String trinoFunctionName;
        private final Function<ColumnVector, ColumnVector> extractor;
        private final DType resultDType;

        Field(String trinoFunctionName, Function<ColumnVector, ColumnVector> extractor, DType resultDType)
        {
            this.trinoFunctionName = requireNonNull(trinoFunctionName, "trinoFunctionName is null");
            this.extractor = requireNonNull(extractor, "extractor is null");
            this.resultDType = requireNonNull(resultDType, "resultDType is null");
        }

        public @Move ColumnVector extract(ColumnVector dateTime)
        {
            return extractor.apply(dateTime);
        }

        public DType resultDType()
        {
            return resultDType;
        }

        public static Optional<Field> forTrinoFunctionName(String trinoFunctionName)
        {
            return Optional.ofNullable(BY_TRINO_FUNCTION_NAME.get(trinoFunctionName));
        }
    }
}
