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

import ai.rapids.cudf.Aggregation128Utils;
import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.expression.CudfUtils.allTrue;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;

/**
 * Unpacks one component of the {@code sum(decimal)} VARBINARY intermediate state
 * (see {@link io.trino.operator.aggregation.state.LongDecimalWithOverflowStateSerializer}) into a
 * fixed-width column. Inputs are 8 / 16 / 24 byte rows of a {@code LIST<UINT8>} column.
 *
 * <p>Components, indexed 0..4:
 * <pre>
 *   idx  bytes    output dtype  meaning
 *   0    [0..3]   UINT32        chunk0 of low 64 bits (least-significant)
 *   1    [4..7]   UINT32        chunk1 of low 64 bits
 *   2    [8..11]  UINT32        chunk2 of high 64 bits        (0 when row length &lt; 16)
 *   3    [12..15] INT32         chunk3 of high 64 bits        (0 when row length &lt; 16)
 *   4    [16..23] INT64         overflow long                 (0 when row length &lt; 24)
 * </pre>
 * cuDF SUM widens UINT32 / INT32 inputs to INT64 with the right (zero / sign) extension; downstream
 * {@link GpuCombineDecimalStateSumsToDecimal128} reassembles the sums into a DECIMAL128.
 */
public final class GpuExtractDecimalStateChunk
        implements GpuExpression
{
    public static final int COMPONENT_COUNT = 5;
    private static final int[] BYTE_OFFSET = {0, 4, 8, 12, 16};
    private static final int[] BYTE_COUNT = {4, 4, 4, 4, 8};
    private static final DType[] OUTPUT_DTYPE = {DType.UINT32, DType.UINT32, DType.UINT32, DType.INT32, DType.INT64};

    private final int componentIdx;

    public GpuExtractDecimalStateChunk(int componentIdx)
    {
        checkArgument(componentIdx >= 0 && componentIdx < COMPONENT_COUNT,
                "componentIdx must be in [0, %s), got %s",
                COMPONENT_COUNT,
                componentIdx);
        this.componentIdx = componentIdx;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, @Borrow List<ColumnVector> inputColumns)
    {
        checkState(inputColumns.size() == 1, "Expected exactly one input column, got %s", inputColumns.size());
        @Borrow ColumnVector input = inputColumns.getFirst();
        checkState(input.getType().getTypeId() == DType.DTypeEnum.LIST,
                "Expected LIST input, got %s",
                input.getType());

        // Fast path: the entire input is uniform 16-byte rows (the GPU PARTIAL output format —
        // overflow never written, high always written). Skip the byte-by-byte unpacking and
        // reinterpret the underlying byte buffer as a DECIMAL128 view, then use cuDF's existing
        // Int128 chunk extractor. The check is O(1) per page — much cheaper than the slow path.
        if (isUniform16ByteRows(input)) {
            return fastPathUniform16(input);
        }

        validateRowLengths(input);

        int firstByte = BYTE_OFFSET[componentIdx];
        int byteCount = BYTE_COUNT[componentIdx];
        DType outputType = OUTPUT_DTYPE[componentIdx];

        // Slow path: handle CPU PARTIAL's variable-length 8 / 16 / 24 byte intermediate states.
        // Build the value as INT64 by OR'ing each byte (zero-extended) shifted into place. Bytes
        // past the row's length are NULL after extractListElement; replaceNulls(0) zero-pads so
        // the OR ignores them. The parent's null mask is reapplied at the end so NULL rows stay
        // NULL rather than collapsing to 0 (which would otherwise be summed as a real 0).
        try (Scalar zeroByte = Scalar.fromUnsignedByte((byte) 0);
                ClosingRef<ColumnVector> accumulator = ClosingRef.own(computeShiftedByte(input, firstByte, 0, zeroByte))) {
            for (int i = 1; i < byteCount; i++) {
                try (ColumnVector previous = accumulator.take();
                        ColumnVector contribution = computeShiftedByte(input, firstByte + i, i * 8, zeroByte)) {
                    accumulator.set(previous.binaryOp(BinaryOp.BITWISE_OR, contribution, DType.INT64));
                }
            }

            try (ColumnVector raw = accumulator.take()) {
                // cuDF binary ops drop the row-level NULL mask after replaceNulls; reattach the
                // parent's mask so NULL accumulator states survive as NULL, not 0.
                if (outputType.equals(DType.INT64)) {
                    return raw.mergeAndSetValidity(BinaryOp.BITWISE_AND, input);
                }
                try (ColumnVector narrowed = raw.castTo(outputType)) {
                    return narrowed.mergeAndSetValidity(BinaryOp.BITWISE_AND, input);
                }
            }
        }
    }

    /**
     * O(1) check that every row is exactly 16 bytes. {@code child.rowCount == 16 * input.rowCount}
     * is only sufficient if the rows are uniform; the equality could otherwise hold by accident
     * (e.g. one 8-byte row balancing one 24-byte row). We also require {@code positionCount > 0}
     * to dodge the empty-input ambiguity. For non-uniform input or empty input we fall through to
     * the slow path which validates row lengths properly.
     */
    private static boolean isUniform16ByteRows(@Borrow ColumnVector input)
    {
        long inputRows = input.getRowCount();
        if (inputRows == 0) {
            return false;
        }
        try (ColumnView child = input.getChildColumnView(0)) {
            if (child.getRowCount() != 16 * inputRows) {
                return false;
            }
        }
        // Necessary condition holds; verify uniformity by checking every length equals 16. NULL
        // rows produce NULL lengths (replaced with TRUE since they don't violate the invariant).
        try (ColumnVector lengths = input.countElements();
                Scalar sixteen = Scalar.fromInt(16);
                ColumnVector eqSixteen = lengths.binaryOp(BinaryOp.EQUAL, sixteen, DType.BOOL8);
                Scalar trueScalar = Scalar.fromBool(true);
                ColumnVector validIncludingNulls = eqSixteen.replaceNulls(trueScalar)) {
            return allTrue(validIncludingNulls);
        }
    }

    private @Move ColumnVector fastPathUniform16(@Borrow ColumnVector input)
    {
        long inputRows = input.getRowCount();

        // Component 4 is the overflow long. With 16-byte rows every overflow trailer is implicitly
        // 0 — emit a constant zero column carrying the parent's null mask.
        if (componentIdx == 4) {
            try (Scalar zero = Scalar.fromLong(0L);
                    ColumnVector zeros = ColumnVector.fromScalar(zero, toIntExact(inputRows))) {
                return zeros.mergeAndSetValidity(BinaryOp.BITWISE_AND, input);
            }
        }

        // Components 0..3 are 32-bit chunks of the 128-bit decimal sum. Reinterpret the LIST<UINT8>
        // child buffer as DECIMAL128 (zero-copy) and use cuDF's existing chunk extractor. Scale
        // doesn't affect chunk extraction; pick 0 arbitrarily.
        DType chunkType = OUTPUT_DTYPE[componentIdx];
        DType decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, 0);
        try (ColumnView child = input.getChildColumnView(0)) {
            @Borrow BaseDeviceMemoryBuffer dataBuffer = child.getData();
            @Borrow BaseDeviceMemoryBuffer validityBuffer = input.getValid();
            Optional<Long> nullCount = Optional.of(input.getNullCount());
            try (ColumnView decimal128View = new ColumnView(decimal128Type, inputRows, nullCount, dataBuffer, validityBuffer)) {
                return Aggregation128Utils.extractInt32Chunk(decimal128View, chunkType, componentIdx);
            }
        }
    }

    private static @Move ColumnVector computeShiftedByte(@Borrow ColumnVector input, int byteOffset, int shift, @Borrow Scalar zeroByte)
    {
        try (ColumnVector byteCol = input.extractListElement(byteOffset);
                ColumnVector byteFilled = byteCol.replaceNulls(zeroByte);
                ColumnVector byteAsInt64 = byteFilled.castTo(DType.INT64)) {
            if (shift == 0) {
                return byteAsInt64.copyToColumnVector();
            }
            try (Scalar shiftAmount = Scalar.fromInt(shift)) {
                return byteAsInt64.binaryOp(BinaryOp.SHIFT_LEFT, shiftAmount, DType.INT64);
            }
        }
    }

    private static void validateRowLengths(@Borrow ColumnVector input)
    {
        // Allowed lengths match LongDecimalWithOverflowStateSerializer's three encodings:
        //   8B  → low only      (high == 0 && overflow == 0)
        //   16B → low + high    (overflow == 0)  — what GPU PARTIAL emits
        //   24B → low + high + overflow
        try (ColumnVector lengths = input.countElements();
                Scalar eight = Scalar.fromInt(8);
                Scalar sixteen = Scalar.fromInt(16);
                Scalar twentyFour = Scalar.fromInt(24);
                ColumnVector eqEight = lengths.binaryOp(BinaryOp.EQUAL, eight, DType.BOOL8);
                ColumnVector eqSixteen = lengths.binaryOp(BinaryOp.EQUAL, sixteen, DType.BOOL8);
                ColumnVector eqTwentyFour = lengths.binaryOp(BinaryOp.EQUAL, twentyFour, DType.BOOL8);
                ColumnVector eight16 = eqEight.binaryOp(BinaryOp.BITWISE_OR, eqSixteen, DType.BOOL8);
                ColumnVector valid = eight16.binaryOp(BinaryOp.BITWISE_OR, eqTwentyFour, DType.BOOL8);
                Scalar trueScalar = Scalar.fromBool(true);
                ColumnVector validIncludingNulls = valid.replaceNulls(trueScalar)) {
            if (!allTrue(validIncludingNulls)) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "sum(decimal) FINAL on GPU expects 8, 16, or 24 byte intermediate states");
            }
        }
    }
}
