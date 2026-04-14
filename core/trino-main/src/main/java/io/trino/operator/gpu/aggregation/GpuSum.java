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
package io.trino.operator.gpu.aggregation;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.GroupByAggregation;
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.Type;

import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GpuSum
        implements GpuAggregateFunction
{
    private final int inputChannel;
    private final Type outputType;
    private final DType outputDType;

    public GpuSum(int inputChannel, Type outputType, DType outputDType)
    {
        checkArgument(inputChannel >= 0, "inputChannel must be non-negative");
        this.inputChannel = inputChannel;
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.outputDType = requireNonNull(outputDType, "outputDType is null");
    }

    @Override
    public OptionalInt inputChannel()
    {
        return OptionalInt.of(inputChannel);
    }

    @Override
    public @Move Scalar emptyResult()
    {
        return Scalar.fromNull(outputDType);
    }

    @Override
    public @Move Scalar reduce(@Borrow ColumnVector column, long rowCount)
    {
        return column.sum(outputDType);
    }

    @Override
    public GroupByAggregation groupByAggregation()
    {
        return GroupByAggregation.sum();
    }

    @Override
    public Type outputType()
    {
        return outputType;
    }

    @Override
    public DType outputDType()
    {
        return outputDType;
    }
}
