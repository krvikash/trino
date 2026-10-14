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

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.BITWISE_AND;
import static ai.rapids.cudf.BinaryOp.BITWISE_OR;
import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.SHIFT_LEFT;
import static ai.rapids.cudf.BinaryOp.SHIFT_RIGHT;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// Computes the SHA-1 digest of a varbinary, as 20 raw bytes.
public final class GpuSha1
        extends GpuExpression
{
    private static final int DIGEST_BYTES = 20;

    private final GpuExpression argument;

    public GpuSha1(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector input = argument.evaluate(positionCount, inputColumns, bindings)) {
            int rows = toIntExact(input.getRowCount());
            if (rows == 0) {
                return input.copyToColumnVector();
            }
            // cuDF sha1 rejects lists, but hashes the bytes of a string as they are, so view the varbinary as a string.
            // The view leaves out the validity, so every row gets a digest, and the nulls are restored at the end.
            try (ColumnView elements = input.getChildColumnView(0)) {
                verify(elements.getType().equals(DType.UINT8), "Unexpected list element type: %s", elements.getType());
                try (ColumnView strings = new ColumnView(DType.STRING, rows, Optional.of(0L), elements.getData(), null, input.getOffsets());
                        ColumnVector hexDigests = ColumnVector.sha1Hash(strings);
                        ColumnVector digestBytes = hexToBytes(hexDigests, rows);
                        ColumnVector offsets = digestOffsets(rows);
                        ColumnView digests = new ColumnView(DType.LIST, rows, Optional.of(0L), null, offsets.getData(), new ColumnView[] {digestBytes})) {
                    return digests.mergeAndSetValidity(BITWISE_AND, input);
                }
            }
        }
    }

    /// Decodes the 40 lowercase hex digits of each SHA-1 digest into a flat column of its 20 bytes.
    private static @Move ColumnVector hexToBytes(@Borrow ColumnVector hexDigests, int rows)
    {
        long digitCount = multiplyExact(2L * DIGEST_BYTES, rows);
        @Borrow BaseDeviceMemoryBuffer chars = hexDigests.getData();
        checkState(hexDigests.getNullCount() == 0 && chars.getLength() == digitCount, "Unexpected SHA-1 hex digests: %s bytes for %s rows", chars.getLength(), rows);
        try (ColumnView digits = new ColumnView(DType.UINT8, digitCount, Optional.of(0L), chars, null);
                ColumnVector nibbles = hexDigitValues(digits);
                // Little-endian, so the low byte of each UINT16 is the high nibble of the digest byte
                ColumnView nibblePairs = new ColumnView(DType.UINT16, digitCount / 2, Optional.of(0L), nibbles.getData(), null);
                ColumnVector highNibbles = binaryOp(nibblePairs, BITWISE_AND, 0x0F, DType.UINT16);
                ColumnVector highBits = binaryOp(highNibbles, SHIFT_LEFT, 4, DType.UINT16);
                ColumnVector lowBits = binaryOp(nibblePairs, SHIFT_RIGHT, 8, DType.UINT16)) {
            return highBits.binaryOp(BITWISE_OR, lowBits, DType.UINT8);
        }
    }

    /// The value of each of the hex `digits`, which are '0'-'9' or 'a'-'f'.
    private static @Move ColumnVector hexDigitValues(@Borrow ColumnView digits)
    {
        // The low four bits of '0'-'9' are the value, and of 'a'-'f' (where bit 6 is set) are the value minus 9
        try (ColumnVector lowBits = binaryOp(digits, BITWISE_AND, 0x0F, DType.UINT8);
                ColumnVector isLetter = binaryOp(digits, SHIFT_RIGHT, 6, DType.UINT8);
                ColumnVector letterAdjustment = binaryOp(isLetter, MUL, 9, DType.UINT8)) {
            return lowBits.binaryOp(ADD, letterAdjustment, DType.UINT8);
        }
    }

    private static @Move ColumnVector binaryOp(@Borrow ColumnView column, BinaryOp operator, int value, DType outputType)
    {
        try (Scalar operand = Scalar.fromInt(value)) {
            return column.binaryOp(operator, operand, outputType);
        }
    }

    private static @Move ColumnVector digestOffsets(int rows)
    {
        // Offsets are INT32
        toIntExact(multiplyExact((long) DIGEST_BYTES, rows));
        try (Scalar zero = Scalar.fromInt(0);
                Scalar step = Scalar.fromInt(DIGEST_BYTES)) {
            return ColumnVector.sequence(zero, step, rows + 1);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return argument.fallibility();
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuSha1 other && argument.equals(other.argument);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument);
    }
}
