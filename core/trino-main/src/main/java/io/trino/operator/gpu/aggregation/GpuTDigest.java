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
import ai.rapids.cudf.ReductionAggregation;
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.DoubleType.DOUBLE;

/// A cuDF t-digest of the non-null input values of each group, as a
/// `STRUCT<centroids LIST<STRUCT<mean FLOAT64, weight FLOAT64>>, min FLOAT64, max FLOAT64>` column,
/// with an empty centroid list for a group without values.
///
/// With raw input, the input column holds the values. Otherwise it holds t-digests to merge.
public class GpuTDigest
        implements GpuAggregateFunction
{
    /// Same as airlift `TDigest.DEFAULT_COMPRESSION`, which the CPU `approx_percentile` uses
    public static final int COMPRESSION = 100;

    /// The cuDF t-digest column is consumed by a post-projection and never copied to a Trino block,
    /// so the Trino type only has to describe it
    private static final Type OUTPUT_TYPE = RowType.anonymousRow(
            new ArrayType(RowType.anonymousRow(DOUBLE, DOUBLE)),
            DOUBLE,
            DOUBLE);

    private final int inputChannel;

    public GpuTDigest(int inputChannel)
    {
        checkArgument(inputChannel >= 0, "inputChannel must be non-negative");
        this.inputChannel = inputChannel;
    }

    @Override
    public OptionalInt inputChannel()
    {
        return OptionalInt.of(inputChannel);
    }

    @Override
    public @Move Scalar emptyResult()
    {
        // cuDF reduces an input without values to an empty t-digest
        try (ColumnVector noValues = ColumnVector.fromDoubles()) {
            return reduce(noValues, 0);
        }
    }

    @Override
    public @Move Scalar reduce(@Borrow ColumnVector column, long rowCount)
    {
        return column.reduce(ReductionAggregation.createTDigest(COMPRESSION), DType.STRUCT);
    }

    @Override
    public @Move Scalar mergeReduce(@Borrow ColumnVector column, long rowCount)
    {
        return column.reduce(ReductionAggregation.mergeTDigest(COMPRESSION), DType.STRUCT);
    }

    @Override
    public GroupByAggregation groupByAggregation()
    {
        return GroupByAggregation.createTDigest(COMPRESSION);
    }

    @Override
    public GroupByAggregation mergeAggregation()
    {
        return GroupByAggregation.mergeTDigest(COMPRESSION);
    }

    @Override
    public Type outputType()
    {
        return OUTPUT_TYPE;
    }

    @Override
    public DType outputDType()
    {
        return DType.STRUCT;
    }
}
