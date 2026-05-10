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
import ai.rapids.cudf.Scalar;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Optional;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.BITWISE_AND;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.LESS_EQUAL;
import static ai.rapids.cudf.BinaryOp.LOGICAL_OR;
import static ai.rapids.cudf.BinaryOp.NULL_MIN;
import static ai.rapids.cudf.BinaryOp.SUB;
import static ai.rapids.cudf.DType.BOOL8;
import static ai.rapids.cudf.DType.INT32;
import static java.util.Objects.requireNonNull;

public class GpuSubstring
        implements GpuExpression
{
    private final GpuExpression source;
    private final GpuExpression start;
    private final Optional<GpuExpression> length;

    public GpuSubstring(GpuExpression source, GpuExpression start, Optional<GpuExpression> length)
    {
        this.source = requireNonNull(source, "source is null");
        this.start = requireNonNull(start, "start is null");
        this.length = requireNonNull(length, "length is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector sourceColumn = source.evaluate(positionCount, inputColumns);
                ColumnVector startColumn = start.evaluate(positionCount, inputColumns)) {
            if (length.isEmpty()) {
                return substring(sourceColumn, startColumn);
            }
            try (ColumnVector lengthColumn = length.get().evaluate(positionCount, inputColumns)) {
                return substring(sourceColumn, startColumn, lengthColumn);
            }
        }
    }

    private static @Move ColumnVector substring(@Borrow ColumnVector source, @Borrow ColumnVector start)
    {
        try (Scalar zero = Scalar.fromInt(0);
                ClosingOnce<ColumnVector> sourceLengths = ClosingOnce.own(source.getCharLengths())) {
            // Convert Trino's 1-based start to cuDF's 0-based, handling negative start (relative to end)
            try (ClosingOnce<ColumnVector> zeroBasedStart = ClosingOnce.own(toZeroBasedStart(sourceLengths.borrow(), start, zero))) {
                sourceLengths.close();
                // Detect out-of-range starts (e.g. start=0, or negative start beyond string length)
                // For valid rows use zeroBasedStart..-1 (cuDF convention: -1 means through end of string);
                // for invalid rows use 0..0 (producing empty string)
                try (ClosingOnce<ColumnVector> invalidStart = ClosingOnce.own(zeroBasedStart.borrow().binaryOp(LESS, zero, BOOL8));
                        Scalar negOne = Scalar.fromInt(-1);
                        ClosingOnce<ColumnVector> safeStart = ClosingOnce.own(toSafePosition(zeroBasedStart.borrow(), invalidStart.borrow(), zero));
                        ClosingOnce<ColumnVector> safeEnd = ClosingOnce.own(toSafePosition(negOne, invalidStart.borrow(), zero))) {
                    zeroBasedStart.close();
                    invalidStart.close();
                    // cuDF rejects nulls in start/end vectors, so toSafePosition replaced them
                    // with zeros. Restore null propagation: on CPU, substring takes primitive args
                    // so Trino's framework auto-returns null for null inputs.
                    try (ColumnVector rawResult = source.substring(safeStart.borrow(), safeEnd.borrow())) {
                        safeStart.close();
                        safeEnd.close();
                        return rawResult.mergeAndSetValidity(BITWISE_AND, rawResult, start);
                    }
                }
            }
        }
    }

    private static @Move ColumnVector substring(@Borrow ColumnVector source, @Borrow ColumnVector start, @Borrow ColumnVector length)
    {
        try (Scalar zero = Scalar.fromInt(0);
                ClosingOnce<ColumnVector> sourceLengths = ClosingOnce.own(source.getCharLengths())) {
            // Convert Trino's 1-based start to cuDF's 0-based, handling negative start (relative to end)
            // Compute end = start + min(length, charLength); detect invalid length (length <= 0)
            try (ClosingOnce<ColumnVector> zeroBasedStart = ClosingOnce.own(toZeroBasedStart(sourceLengths.borrow(), start, zero));
                    ClosingOnce<ColumnVector> invalidStart = ClosingOnce.own(zeroBasedStart.borrow().binaryOp(LESS, zero, BOOL8));
                    ClosingOnce<ColumnVector> clampedLength = ClosingOnce.own(saturatedCastToInt32(length));
                    ClosingOnce<ColumnVector> end = ClosingOnce.own(computeEndPosition(zeroBasedStart.borrow(), clampedLength.borrow(), sourceLengths.borrow()));
                    ClosingOnce<ColumnVector> invalidLength = ClosingOnce.own(clampedLength.borrow().binaryOp(LESS_EQUAL, zero, BOOL8));
                    ClosingOnce<ColumnVector> anyInvalid = ClosingOnce.own(invalidStart.borrow().binaryOp(LOGICAL_OR, invalidLength.borrow(), BOOL8))) {
                sourceLengths.close();
                clampedLength.close();
                invalidStart.close();
                invalidLength.close();
                // For invalid rows use 0..0 (producing empty string); replace nulls because cuDF substring rejects them
                try (ClosingOnce<ColumnVector> safeStart = ClosingOnce.own(toSafePosition(zeroBasedStart.borrow(), anyInvalid.borrow(), zero));
                        ClosingOnce<ColumnVector> safeEnd = ClosingOnce.own(toSafePosition(end.borrow(), anyInvalid.borrow(), zero))) {
                    zeroBasedStart.close();
                    end.close();
                    anyInvalid.close();
                    // cuDF rejects nulls in start/end vectors, so toSafePosition replaced them
                    // with zeros. Restore null propagation: on CPU, substring takes primitive args
                    // so Trino's framework auto-returns null for null inputs.
                    try (ColumnVector rawResult = source.substring(safeStart.borrow(), safeEnd.borrow())) {
                        safeStart.close();
                        safeEnd.close();
                        return rawResult.mergeAndSetValidity(BITWISE_AND, rawResult, start, length);
                    }
                }
            }
        }
    }

    private static @Move ColumnVector computeEndPosition(@Borrow ColumnVector zeroBasedStart, @Borrow ColumnVector length, @Borrow ColumnVector sourceLengths)
    {
        try (ColumnVector cappedLength = length.binaryOp(NULL_MIN, sourceLengths, INT32)) {
            return zeroBasedStart.binaryOp(ADD, cappedLength, INT32);
        }
    }

    private static @Move ColumnVector toSafePosition(@Borrow ColumnVector position, @Borrow ColumnVector invalid, @Borrow Scalar zero)
    {
        try (ColumnVector clamped = invalid.ifElse(zero, position)) {
            return clamped.replaceNulls(zero);
        }
    }

    private static @Move ColumnVector toSafePosition(@Borrow Scalar position, @Borrow ColumnVector invalid, @Borrow Scalar zero)
    {
        try (ColumnVector clamped = invalid.ifElse(zero, position)) {
            return clamped.replaceNulls(zero);
        }
    }

    private static @Move ColumnVector toZeroBasedStart(@Borrow ColumnVector sourceLengths, @Borrow ColumnVector start, @Borrow Scalar zero)
    {
        try (ClosingOnce<ColumnVector> clampedStart = ClosingOnce.own(saturatedCastToInt32(start));
                Scalar one = Scalar.fromInt(1);
                ColumnVector positiveStart = clampedStart.borrow().binaryOp(SUB, one, INT32);
                ColumnVector negativeStart = sourceLengths.binaryOp(ADD, clampedStart.borrow(), INT32);
                ColumnVector positive = clampedStart.borrow().binaryOp(GREATER, zero, BOOL8)) {
            clampedStart.close();
            return positive.ifElse(positiveStart, negativeStart);
        }
    }

    private static @Move ColumnVector saturatedCastToInt32(@Borrow ColumnVector int64Column)
    {
        try (Scalar intMin = Scalar.fromLong(Integer.MIN_VALUE);
                Scalar intMax = Scalar.fromLong(Integer.MAX_VALUE);
                ColumnVector clamped = int64Column.clamp(intMin, intMax)) {
            return clamped.castTo(INT32);
        }
    }
}
