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

/**
 * Experimental: This API may change as we add support for more complex aggregates.
 * TODO: https://starburstdata.atlassian.net/browse/ENG-10570
 */
public interface GpuAggregateFunction
{
    /**
     * Input channel this aggregate operates on.
     * <p>
     * Returns empty if the aggregate doesn't require any input column (e.g., COUNT(*)).
     * <p>
     * Limited to one input column because cuDF's {@code GroupByAggregation.onColumn(int)}
     * binds each aggregation to a single column. Multi-argument aggregates (e.g., covar)
     * are not yet supported.
     */
    OptionalInt inputChannel();

    /**
     * Create result scalar for empty input (0 rows).
     *
     * @return scalar result for empty input (caller takes ownership)
     */
    @Move
    Scalar emptyResult();

    /**
     * Perform reduction aggregation on raw input without a column.
     * <p>
     * Called for global aggregations (no GROUP BY) when {@link #inputChannel()} is empty.
     * Used for SINGLE and PARTIAL steps.
     *
     * @param rowCount total number of rows
     * @return scalar result (caller takes ownership)
     */
    @Move
    default Scalar reduce(long rowCount)
    {
        throw new UnsupportedOperationException(getClass() + " requires an input column");
    }

    /**
     * Perform reduction aggregation on raw input with a column.
     * <p>
     * Called for global aggregations (no GROUP BY) when {@link #inputChannel()} is present.
     * Used for SINGLE and PARTIAL steps.
     *
     * @param column input column
     * @param rowCount total number of rows
     * @return scalar result (caller takes ownership)
     */
    @Move
    default Scalar reduce(@Borrow ColumnVector column, long rowCount)
    {
        throw new UnsupportedOperationException(getClass() + " does not use an input column");
    }

    /**
     * Perform reduction aggregation on intermediate state.
     * <p>
     * Called for global aggregations (no GROUP BY).
     * Used for FINAL and INTERMEDIATE steps to merge partial results.
     *
     * @param column input column containing intermediate state values
     * @param rowCount total number of rows
     * @return scalar result (caller takes ownership)
     */
    @Move
    default Scalar mergeReduce(@Borrow ColumnVector column, long rowCount)
    {
        return reduce(column, rowCount);
    }

    /**
     * Get GroupByAggregation for GROUP BY queries on raw input.
     * <p>
     * Used for SINGLE and PARTIAL steps.
     */
    GroupByAggregation groupByAggregation();

    /**
     * Get GroupByAggregation for merging partial aggregation results.
     * <p>
     * Used for FINAL and INTERMEDIATE steps.
     * For most aggregates this is the same as {@link #groupByAggregation()}, but
     * for example COUNT requires SUM, not COUNT for merging partial results.
     */
    default GroupByAggregation mergeAggregation()
    {
        return groupByAggregation();
    }

    /**
     * Output type of the aggregation result.
     */
    Type outputType();

    /**
     * Output DType for GPU representation of the result.
     */
    DType outputDType();

    /**
     * Transform the result column from a group-by aggregation.
     *
     * @param column the result column from cuDF group-by aggregation (borrowed)
     * @return transformed column (caller takes ownership)
     */
    @Move
    default ColumnVector postProcessGroupByResult(@Borrow ColumnVector column)
    {
        if (!column.getType().equals(outputDType())) {
            throw new IllegalStateException("Expected %s but got %s".formatted(outputDType(), column.getType()));
        }
        return column.incRefCount();
    }
}
