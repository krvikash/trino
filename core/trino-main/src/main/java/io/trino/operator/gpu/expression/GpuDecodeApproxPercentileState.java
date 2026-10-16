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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.aggregation.GpuTDigest;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.GpuEncodeApproxPercentileState.CENTROIDS_OFFSET;
import static io.trino.operator.gpu.expression.GpuEncodeApproxPercentileState.CENTROID_BYTES;
import static io.trino.operator.gpu.expression.GpuEncodeApproxPercentileState.CENTROID_COUNT_OFFSET;
import static io.trino.operator.gpu.expression.GpuEncodeApproxPercentileState.DIGEST_OFFSET;
import static io.trino.operator.gpu.expression.GpuEncodeApproxPercentileState.MAX_OFFSET;
import static io.trino.operator.gpu.expression.GpuEncodeApproxPercentileState.MIN_OFFSET;
import static io.trino.operator.gpu.expression.GpuEncodeApproxPercentileState.PERCENTILE_OFFSET;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/// Decodes the `approx_percentile` intermediate state, see [GpuEncodeApproxPercentileState], for a FINAL step.
/// Produces either the cuDF t-digest, empty for a NULL state, or the percentile, NULL for a NULL state.
public final class GpuDecodeApproxPercentileState
        extends GpuExpression
{
    public enum Part
    {
        DIGEST,
        PERCENTILE,
    }

    private final GpuExpression source;
    private final Part part;

    public GpuDecodeApproxPercentileState(GpuExpression source, Part part)
    {
        this.source = requireNonNull(source, "source is null");
        this.part = requireNonNull(part, "part is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ClosingRef<ColumnVector> states = ClosingRef.own(source.evaluate(positionCount, inputColumns, bindings));
                ColumnVector filled = fillNulls(states.borrow())) {
            verifyFixedFieldsPresent(filled);
            return switch (part) {
                case DIGEST -> {
                    // Only the percentile needs the NULLs of the original states
                    states.close();
                    yield digests(filled, positionCount);
                }
                case PERCENTILE -> {
                    try (ColumnVector percentiles = doubleField(filled, PERCENTILE_OFFSET, positionCount)) {
                        yield percentiles.mergeAndSetValidity(BinaryOp.BITWISE_AND, states.borrow());
                    }
                }
            };
        }
    }

    /// A NULL state becomes the state of an empty t-digest, so that every row has the fixed fields
    private static @Move ColumnVector fillNulls(@Borrow ColumnVector states)
    {
        try (ColumnVector present = states.isNotNull();
                Scalar emptyState = emptyStateScalar()) {
            return present.ifElse(states, emptyState);
        }
    }

    private static @Move ColumnVector digests(@Borrow ColumnVector states, int positionCount)
    {
        try (ColumnVector centroidCounts = fixedWidthField(states, CENTROID_COUNT_OFFSET, DType.INT32, positionCount)) {
            verifyLengths(states, centroidCounts);
            // Close each copy of the centroid data as soon as the next one is built, to keep peak memory low
            try (Scalar doubleBytes = Scalar.fromInt(Double.BYTES);
                    ColumnVector arrayBytes = centroidCounts.binaryOp(BinaryOp.MUL, doubleBytes, DType.INT32);
                    Scalar centroidsOffset = Scalar.fromInt(CENTROIDS_OFFSET);
                    ColumnVector meansStart = ColumnVector.fromScalar(centroidsOffset, positionCount);
                    ColumnVector weightsStart = meansStart.binaryOp(BinaryOp.ADD, arrayBytes, DType.INT32);
                    ClosingRef<ColumnVector> means = ClosingRef.own(doubles(states, meansStart, arrayBytes));
                    ClosingRef<ColumnVector> weights = ClosingRef.own(doubles(states, weightsStart, arrayBytes));
                    ClosingRef<ColumnVector> centroid = ClosingRef.own(ColumnVector.makeStruct(means.borrow(), weights.borrow()))) {
                means.close();
                weights.close();
                try (ColumnVector centroidOffsets = offsets(centroidCounts);
                        ColumnVector centroids = centroid.borrow().makeListFromOffsets(positionCount, centroidOffsets)) {
                    centroid.close();
                    try (ColumnVector min = doubleField(states, MIN_OFFSET, positionCount);
                            ColumnVector max = doubleField(states, MAX_OFFSET, positionCount)) {
                        return ColumnVector.makeStruct(positionCount, centroids, min, max);
                    }
                }
            }
        }
    }

    private static void verifyFixedFieldsPresent(@Borrow ColumnVector states)
    {
        try (ColumnVector lengths = states.countElements();
                Scalar fixedBytes = Scalar.fromInt(CENTROIDS_OFFSET);
                ColumnVector tooShort = lengths.binaryOp(BinaryOp.LESS, fixedBytes, DType.BOOL8)) {
            verifyNone(tooShort);
        }
    }

    private static void verifyLengths(@Borrow ColumnVector states, @Borrow ColumnVector centroidCounts)
    {
        try (ColumnVector lengths = states.countElements();
                Scalar centroidBytes = Scalar.fromInt(CENTROID_BYTES);
                Scalar fixedBytes = Scalar.fromInt(CENTROIDS_OFFSET);
                ColumnVector variableBytes = centroidCounts.binaryOp(BinaryOp.MUL, centroidBytes, DType.INT32);
                ColumnVector expectedLengths = variableBytes.binaryOp(BinaryOp.ADD, fixedBytes, DType.INT32);
                ColumnVector unexpected = lengths.binaryOp(BinaryOp.NOT_EQUAL, expectedLengths, DType.BOOL8)) {
            verifyNone(unexpected);
        }
    }

    private static void verifyNone(@Borrow ColumnVector malformed)
    {
        if (anyTrue(malformed)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected approx_percentile intermediate state length");
        }
    }

    /// Exclusive prefix sums of the counts, with the total appended
    private static @Move ColumnVector offsets(@Borrow ColumnVector counts)
    {
        try (Scalar zero = Scalar.fromInt(0);
                ColumnVector first = ColumnVector.fromScalar(zero, 1);
                ColumnVector inclusive = counts.prefixSum()) {
            return ColumnVector.concatenate(first, inclusive);
        }
    }

    private static @Move ColumnVector doubleField(@Borrow ColumnVector states, int offset, int positionCount)
    {
        return fixedWidthField(states, offset, DType.FLOAT64, positionCount);
    }

    private static @Move ColumnVector fixedWidthField(@Borrow ColumnVector states, int offset, DType type, int positionCount)
    {
        try (Scalar offsetScalar = Scalar.fromInt(offset);
                Scalar sizeScalar = Scalar.fromInt(type.getSizeInBytes());
                ColumnVector start = ColumnVector.fromScalar(offsetScalar, positionCount);
                ColumnVector size = ColumnVector.fromScalar(sizeScalar, positionCount)) {
            return reinterpret(states, start, size, type);
        }
    }

    private static @Move ColumnVector doubles(@Borrow ColumnVector states, @Borrow ColumnVector start, @Borrow ColumnVector size)
    {
        return reinterpret(states, start, size, DType.FLOAT64);
    }

    /// Gathers bytes `[start, start + size)` of each state and reinterprets them, concatenated, as values of `type`
    private static @Move ColumnVector reinterpret(@Borrow ColumnVector states, @Borrow ColumnVector start, @Borrow ColumnVector size, DType type)
    {
        try (ColumnVector indices = ColumnVector.sequence(start, size);
                ColumnVector gathered = states.segmentedGather(indices);
                ColumnView bytes = gathered.getChildColumnView(0)) {
            long valueCount = bytes.getRowCount() / type.getSizeInBytes();
            try (ColumnView values = new ColumnView(type, valueCount, Optional.of(0L), bytes.getData(), null)) {
                return values.copyToColumnVector();
            }
        }
    }

    private static @Move Scalar emptyStateScalar()
    {
        Slice state = Slices.allocate(CENTROIDS_OFFSET);
        SliceOutput output = state.getOutput();
        output.writeDouble(0);
        output.writeInt(CENTROIDS_OFFSET - DIGEST_OFFSET);
        output.writeByte(0);
        output.writeDouble(0);
        output.writeDouble(0);
        output.writeDouble(GpuTDigest.COMPRESSION);
        output.writeDouble(0);
        output.writeInt(0);
        try (ColumnVector bytes = ColumnVector.fromUnsignedBytes(state.getBytes())) {
            return Scalar.listFromColumnView(bytes);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        // Only an internal error on a malformed state
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
        return obj instanceof GpuDecodeApproxPercentileState other
                && source.equals(other.source)
                && part == other.part;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, part);
    }
}
