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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.SegmentedReductionAggregation;
import io.trino.operator.torin.aggregation.GpuTDigest;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.torin.GpuTypeConversion.nullScalar;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

/// Post-projection of a PARTIAL `approx_percentile`: encodes each group's cuDF t-digest and percentile as the
/// CPU intermediate state, so that either a CPU or a GPU FINAL step can merge it.
///
/// The state is what `TDigestAndPercentileStateSerializer` writes, with the t-digest as serialized by airlift
/// `TDigest.serialize`, all little-endian:
/// `percentile double | digest length int | format byte (0) | min double | max double | compression double |
/// total weight double | centroid count int | centroid means double[] | centroid weights double[]`.
/// A group without values has a NULL state, like on the CPU.
///
/// The cuDF t-digest has its centroids sorted by mean, as airlift `TDigest.deserialize` requires.
public final class GpuEncodeApproxPercentileState
        extends GpuExpression
{
    static final int PERCENTILE_OFFSET = 0;
    static final int DIGEST_OFFSET = Double.BYTES + Integer.BYTES;
    static final int FORMAT_OFFSET = DIGEST_OFFSET;
    static final int MIN_OFFSET = FORMAT_OFFSET + Byte.BYTES;
    static final int MAX_OFFSET = MIN_OFFSET + Double.BYTES;
    static final int CENTROID_COUNT_OFFSET = MAX_OFFSET + 3 * Double.BYTES;
    static final int CENTROIDS_OFFSET = CENTROID_COUNT_OFFSET + Integer.BYTES;
    static final int CENTROID_BYTES = 2 * Double.BYTES;

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        checkState(inputColumns.size() == 2, "Expected 2 input columns (t-digest, percentile), got %s", inputColumns.size());
        @Borrow ColumnVector digests = inputColumns.get(0);
        checkState(digests.getType().equals(DType.STRUCT), "Expected t-digest column of type STRUCT, got %s", digests.getType());
        @Borrow ColumnVector percentiles = inputColumns.get(1);
        checkState(percentiles.getType().equals(DType.FLOAT64), "Expected percentile column of type FLOAT64, got %s", percentiles.getType());

        try (ColumnView centroids = digests.getChildColumnView(0);
                ColumnView means = centroidField(centroids, 0);
                ColumnView weights = centroidField(centroids, 1)) {
            if (means.getRowCount() == 0) {
                // No group has values
                try (Scalar nullState = nullScalar(VARBINARY)) {
                    return ColumnVector.fromScalar(nullState, positionCount);
                }
            }
            try (ColumnVector centroidCounts = centroids.countElements();
                    ClosingRef<ColumnVector> states = ClosingRef.own(header(digests, percentiles, centroids, weights, centroidCounts, positionCount))) {
                // Append the centroid arrays, the bulk of the state, one at a time, to keep peak memory low
                append(states, perDigestBytes(means, centroids));
                append(states, perDigestBytes(weights, centroids));
                try (Scalar noCentroids = Scalar.fromInt(0);
                        ColumnVector hasValues = centroidCounts.binaryOp(BinaryOp.GREATER, noCentroids, DType.BOOL8);
                        Scalar nullState = nullScalar(VARBINARY)) {
                    return hasValues.ifElse(states.borrow(), nullState);
                }
            }
        }
    }

    /// The state up to and including the centroid count
    private static @Move ColumnVector header(
            @Borrow ColumnVector digests,
            @Borrow ColumnVector percentiles,
            @Borrow ColumnView centroids,
            @Borrow ColumnView weights,
            @Borrow ColumnVector centroidCounts,
            int positionCount)
    {
        try (ColumnView min = digests.getChildColumnView(1);
                ColumnView max = digests.getChildColumnView(2);
                ColumnVector totalWeights = weightSums(centroids, weights);
                ColumnVector digestLengths = digestLengths(centroidCounts);
                ColumnVector percentileBytes = percentiles.asByteList(false);
                ColumnVector digestLengthBytes = digestLengths.asByteList(false);
                ColumnVector formatBytes = constantBytes(Scalar.fromByte((byte) 0), positionCount);
                ColumnVector minBytes = min.asByteList(false);
                ColumnVector maxBytes = max.asByteList(false);
                ColumnVector compressionBytes = constantBytes(Scalar.fromDouble(GpuTDigest.COMPRESSION), positionCount);
                ColumnVector totalWeightBytes = totalWeights.asByteList(false);
                ColumnVector centroidCountBytes = centroidCounts.asByteList(false)) {
            return ColumnVector.listConcatenateByRow(
                    percentileBytes,
                    digestLengthBytes,
                    formatBytes,
                    minBytes,
                    maxBytes,
                    compressionBytes,
                    totalWeightBytes,
                    centroidCountBytes);
        }
    }

    private static void append(ClosingRef<ColumnVector> states, @Move ColumnVector bytes)
    {
        try (bytes;
                ColumnVector previous = states.take()) {
            states.set(ColumnVector.listConcatenateByRow(previous, bytes));
        }
    }

    private static @Move ColumnView centroidField(@Borrow ColumnView centroids, int field)
    {
        try (ColumnView centroid = centroids.getChildColumnView(0)) {
            return centroid.getChildColumnView(field);
        }
    }

    private static @Move ColumnVector weightSums(@Borrow ColumnView centroids, @Borrow ColumnView weights)
    {
        try (ColumnView digestWeights = centroids.replaceListChild(weights)) {
            return digestWeights.listReduce(SegmentedReductionAggregation.sum(), DType.FLOAT64);
        }
    }

    private static @Move ColumnVector digestLengths(@Borrow ColumnVector centroidCounts)
    {
        // Everything after the digest length
        try (Scalar centroidBytes = Scalar.fromInt(CENTROID_BYTES);
                Scalar fixedBytes = Scalar.fromInt(CENTROIDS_OFFSET - DIGEST_OFFSET);
                ColumnVector variableBytes = centroidCounts.binaryOp(BinaryOp.MUL, centroidBytes, DType.INT32)) {
            return variableBytes.binaryOp(BinaryOp.ADD, fixedBytes, DType.INT32);
        }
    }

    private static @Move ColumnVector constantBytes(@Move Scalar value, int positionCount)
    {
        try (value;
                ColumnVector column = ColumnVector.fromScalar(value, positionCount)) {
            return column.asByteList(false);
        }
    }

    /// The bytes of the `FLOAT64` centroid field of each digest, all means and then all weights in airlift's layout
    private static @Move ColumnVector perDigestBytes(@Borrow ColumnView field, @Borrow ColumnView centroids)
    {
        try (ColumnVector valueBytes = field.asByteList(false);
                ColumnView digestValueBytes = centroids.replaceListChild(valueBytes)) {
            return digestValueBytes.flattenLists();
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return Fallibility.INFALLIBLE;
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return false;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuEncodeApproxPercentileState;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
