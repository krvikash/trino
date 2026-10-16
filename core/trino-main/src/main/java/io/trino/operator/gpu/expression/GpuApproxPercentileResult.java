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
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.EQUAL;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.GREATER_EQUAL;
import static ai.rapids.cudf.BinaryOp.LESS_EQUAL;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.expression.CudfUtils.Ties.TOWARD_POSITIVE_INFINITY;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.roundToIntegral;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.gpu.GpuTypeConversion.nullScalar;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

/// Post-projection of a SINGLE or FINAL `approx_percentile(x, percentile)`: answers each group's percentile from
/// its cuDF t-digest, or NULL for a group without values.
///
/// cuDF and airlift build and query t-digests differently, so the result is an approximation of the same quality
/// as on the CPU, but not necessarily the same value.
public final class GpuApproxPercentileResult
        extends GpuExpression
{
    private final DType outputDType;

    public GpuApproxPercentileResult(DType outputDType)
    {
        this.outputDType = requireNonNull(outputDType, "outputDType is null");
        checkArgument(outputDType.equals(DType.INT64) || outputDType.equals(DType.FLOAT32) || outputDType.equals(DType.FLOAT64), "Unsupported type: %s", outputDType);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        checkState(inputColumns.size() == 2, "Expected 2 input columns (t-digest, percentile), got %s", inputColumns.size());
        @Borrow ColumnVector digests = inputColumns.get(0);
        checkState(digests.getType().equals(DType.STRUCT), "Expected t-digest column of type STRUCT, got %s", digests.getType());
        @Borrow ColumnVector percentiles = inputColumns.get(1);
        checkState(percentiles.getType().equals(DType.FLOAT64), "Expected percentile column of type FLOAT64, got %s", percentiles.getType());

        try (ColumnVector hasValues = hasValues(digests);
                ColumnVector percentilesOfGroupsWithValues = maskPercentiles(percentiles, hasValues)) {
            verifyPercentiles(percentilesOfGroupsWithValues);
            try (ColumnVector values = valuesAt(digests, percentilesOfGroupsWithValues, positionCount)) {
                return toOutputType(values);
            }
        }
    }

    private static @Move ColumnVector hasValues(@Borrow ColumnVector digests)
    {
        try (ColumnView centroids = digests.getChildColumnView(0);
                ColumnVector centroidCounts = centroids.countElements();
                Scalar noCentroids = Scalar.fromInt(0);
                ColumnVector nonEmpty = centroidCounts.binaryOp(GREATER, noCentroids, DType.BOOL8);
                Scalar falseScalar = Scalar.fromBool(false)) {
            // A NULL t-digest is the empty result of a global aggregation
            return nonEmpty.replaceNulls(falseScalar);
        }
    }

    private static @Move ColumnVector maskPercentiles(@Borrow ColumnVector percentiles, @Borrow ColumnVector hasValues)
    {
        try (Scalar nullPercentile = nullScalar(DOUBLE)) {
            return hasValues.ifElse(percentiles, nullPercentile);
        }
    }

    private static void verifyPercentiles(@Borrow ColumnVector percentiles)
    {
        // Like the CPU, check only the percentile of a group that has values. A NaN percentile fails both comparisons.
        try (Scalar zero = Scalar.fromDouble(0);
                Scalar one = Scalar.fromDouble(1);
                ColumnVector atLeastZero = percentiles.binaryOp(GREATER_EQUAL, zero, DType.BOOL8);
                ColumnVector atMostOne = percentiles.binaryOp(LESS_EQUAL, one, DType.BOOL8);
                ColumnVector withinRange = atLeastZero.binaryOp(NULL_LOGICAL_AND, atMostOne, DType.BOOL8);
                ColumnVector outOfRange = withinRange.not()) {
            if (anyTrue(outOfRange)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            }
        }
    }

    /// cuDF answers the same percentiles for every t-digest, so answer each distinct percentile, usually just one,
    /// and pick each group's own
    private static @Move ColumnVector valuesAt(@Borrow ColumnVector digests, @Borrow ColumnVector percentiles, int positionCount)
    {
        try (Scalar nullValue = nullScalar(DOUBLE);
                ClosingRef<ColumnVector> result = ClosingRef.own(ColumnVector.fromScalar(nullValue, positionCount))) {
            for (double percentile : distinctPercentiles(percentiles)) {
                try (ColumnVector percentileColumn = ColumnVector.fromDoubles(percentile);
                        ColumnVector valueLists = digests.approxPercentile(percentileColumn);
                        ColumnVector values = valueLists.extractListElement(0);
                        Scalar percentileScalar = Scalar.fromDouble(percentile);
                        ColumnVector selected = percentiles.binaryOp(EQUAL, percentileScalar, DType.BOOL8);
                        Scalar falseScalar = Scalar.fromBool(false);
                        ColumnVector selectedNotNull = selected.replaceNulls(falseScalar);
                        ColumnVector previous = result.take()) {
                    result.set(selectedNotNull.ifElse(values, previous));
                }
            }
            return result.take();
        }
    }

    private static double[] distinctPercentiles(@Borrow ColumnVector percentiles)
    {
        try (Table table = new Table(percentiles);
                Table distinct = table.dropDuplicates(new int[] {0}, Table.DuplicateKeepOption.KEEP_ANY, true);
                HostColumnVector host = distinct.getColumn(0).copyToHost()) {
            int nonNullCount = (int) (host.getRowCount() - host.getNullCount());
            double[] result = new double[nonNullCount];
            int index = 0;
            for (int row = 0; row < host.getRowCount(); row++) {
                if (!host.isNull(row)) {
                    result[index] = host.getDouble(row);
                    index++;
                }
            }
            return result;
        }
    }

    private @Move ColumnVector toOutputType(@Borrow ColumnVector values)
    {
        if (outputDType.equals(DType.FLOAT64)) {
            return values.incRefCount();
        }
        if (outputDType.equals(DType.FLOAT32)) {
            return values.castTo(DType.FLOAT32);
        }
        // Like `Math.round` in the CPU implementation. The value lies between the minimum and the maximum input, so
        // only Long.MAX_VALUE, which is 2^63 as a double, is out of the long range, where `Math.round` saturates.
        try (ColumnVector rounded = roundToIntegral(values, TOWARD_POSITIVE_INFINITY);
                Scalar twoToThe63 = Scalar.fromDouble(0x1p63);
                ColumnVector outOfRange = rounded.binaryOp(GREATER_EQUAL, twoToThe63, DType.BOOL8);
                ColumnVector converted = rounded.castTo(DType.INT64);
                Scalar maxLong = Scalar.fromLong(Long.MAX_VALUE)) {
            return outOfRange.ifElse(maxLong, converted);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return true;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuApproxPercentileResult other
                && outputDType.equals(other.outputDType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), outputDType);
    }
}
