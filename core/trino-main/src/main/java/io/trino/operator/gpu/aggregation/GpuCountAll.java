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
import ai.rapids.cudf.NullPolicy;
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.Type;

import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class GpuCountAll
        implements GpuAggregateFunction
{
    private final Type outputType;
    private final DType outputDType;

    public GpuCountAll(Type outputType, DType outputDType)
    {
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.outputDType = requireNonNull(outputDType, "outputDType is null");
    }

    @Override
    public OptionalInt inputChannel()
    {
        return OptionalInt.empty();
    }

    @Override
    public @Move Scalar emptyResult()
    {
        return Scalar.fromLong(0);
    }

    @Override
    public @Move Scalar reduce(long rowCount)
    {
        return Scalar.fromLong(rowCount);
    }

    @Override
    public @Move Scalar mergeReduce(@Borrow ColumnVector column, long rowCount)
    {
        return column.sum(outputDType());
    }

    @Override
    public GroupByAggregation groupByAggregation()
    {
        return GroupByAggregation.count(NullPolicy.INCLUDE);
    }

    @Override
    public GroupByAggregation mergeAggregation()
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

    @Override
    public ColumnVector postProcessGroupByResult(ColumnVector column)
    {
        // cuDF returns INT32 for count but we need INT64.
        // This may cause overflow if a single group exceeds ~2.1B rows.
        // TODO: https://starburstdata.atlassian.net/browse/ENG-10569
        return column.castTo(outputDType());
    }
}
