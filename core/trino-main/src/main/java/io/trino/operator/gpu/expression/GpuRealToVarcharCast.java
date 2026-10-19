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
import ai.rapids.cudf.Scalar;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.BITWISE_AND;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.SHIFT_RIGHT;
import static ai.rapids.cudf.BinaryOp.SUB;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.type.RealOperators.castToVarchar;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/// Formats a REAL like [io.trino.type.RealOperators#castToVarchar]: `NaN`, `Infinity`, `-Infinity`,
/// `0E0`, `-0E0`, and otherwise `d.ddddE[-]x` with at least one fraction digit.
///
/// The result is the same as the CPU's: at most 7 significant digits, which do not always parse back
/// to the same real. See `ryu_f2s.cu`.
public final class GpuRealToVarcharCast
        extends GpuExpression
{
    /// Longest result of the cast: a sign, 7 significant digits, the point, `E`, and a negative two-digit exponent
    private static final int MAX_LENGTH = 13;

    private static final int FRACTION_LENGTH_MASK = 0x1F;
    private static final int LEAD_SHIFT = 5;
    private static final int LEAD_MASK = 0xF;
    private static final int EXPONENT_SHIFT = 9;
    // Keeps the packed exponent non-negative, so the shift right needs no sign handling
    private static final int EXPONENT_BIAS = 1024;

    private static final String RYU_SOURCE = loadSource("ryu_tables.cu") + loadSource("ryu_f2s.cu");
    // A REAL has at most 6 fraction digits, so they fit in a single UDF output of 8 digits
    private static final String FRACTION_DIGITS_UDF = ryuUdf(0);
    private static final String PACKED_UDF = ryuUdf(2);

    private final GpuExpression argument;
    private final VarcharType targetType;

    public GpuRealToVarcharCast(GpuExpression argument, VarcharType targetType)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.targetType = requireNonNull(targetType, "targetType is null");
    }

    private boolean checksLength()
    {
        return !targetType.isUnbounded() && targetType.getBoundedLength() < MAX_LENGTH;
    }

    private static String loadSource(String name)
    {
        try {
            return Resources.toString(Resources.getResource(GpuRealToVarcharCast.class, name), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String ryuUdf(int output)
    {
        return RYU_SOURCE
                .replace("RYU_OUTPUT", Integer.toString(output))
                .replace("RYU_EXPONENT_BIAS", Integer.toString(EXPONENT_BIAS))
                .replace("RYU_EXPONENT_SHIFT", Integer.toString(EXPONENT_SHIFT))
                .replace("RYU_LEAD_SHIFT", Integer.toString(LEAD_SHIFT));
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector value = argument.evaluate(positionCount, inputColumns, bindings)) {
            verify(value.getType().equals(DType.FLOAT32), "Expected %s input, got %s", DType.FLOAT32, value.getType());
            try (ClosingRef<ColumnVector> result = ClosingRef.own(format(value))) {
                if (checksLength()) {
                    checkLength(value, result.borrow());
                }
                return result.take();
            }
        }
    }

    private static @Move ColumnVector format(@Borrow ColumnVector value)
    {
        try (ColumnVector negative = signBit(value);
                ColumnVector magnitude = formatMagnitude(value)) {
            return replaceSpecialValues(value, negative, signed(negative, magnitude));
        }
    }

    /// Fails like the CPU, which formats the value before it checks the length, on the first row that is too long
    private void checkLength(@Borrow ColumnVector value, @Borrow ColumnVector formatted)
    {
        int bound = targetType.getBoundedLength();
        try (ColumnVector lengths = formatted.getCharLengths();
                Scalar boundScalar = Scalar.fromInt(bound);
                ColumnVector tooLong = lengths.binaryOp(GREATER, boundScalar, DType.BOOL8)) {
            if (!anyTrue(tooLong)) {
                return;
            }
            int row = firstTrueRow(tooLong);
            try (Scalar offending = value.getScalarElement(row)) {
                // The CPU result is never shorter than the GPU one, so the CPU fails on this row too
                castToVarchar(bound, floatToRawIntBits(offending.getFloat()));
            }
            throw new VerifyException("CPU cast of row %s succeeded where GPU result is longer than %s".formatted(row, bound));
        }
    }

    private static int firstTrueRow(@Borrow ColumnVector booleans)
    {
        try (Scalar zero = Scalar.fromInt(0);
                Scalar one = Scalar.fromInt(1);
                ColumnVector rows = ColumnVector.sequence(zero, one, toIntExact(booleans.getRowCount()));
                Scalar notFound = Scalar.fromInt(Integer.MAX_VALUE);
                ColumnVector candidates = booleans.ifElse(rows, notFound);
                Scalar first = candidates.min()) {
            return first.getInt();
        }
    }

    // Unlike a comparison with zero, also set for -0.0
    private static @Move ColumnVector signBit(@Borrow ColumnVector value)
    {
        try (ColumnView bits = value.bitCastTo(DType.INT32);
                Scalar zero = Scalar.fromInt(0)) {
            return bits.binaryOp(LESS, zero, DType.BOOL8);
        }
    }

    private static @Move ColumnVector formatMagnitude(@Borrow ColumnVector value)
    {
        try (ColumnVector packed = value.transform(PACKED_UDF, false);
                ColumnVector significand = formatSignificand(value, packed);
                ColumnVector exponent = decimalExponent(packed);
                ColumnVector exponentString = exponent.castTo(DType.STRING);
                Scalar separator = Scalar.fromString("E");
                Scalar nullString = Scalar.fromString(null)) {
            return ColumnVector.stringConcatenate(separator, nullString, new ColumnView[] {significand, exponentString});
        }
    }

    private static @Move ColumnVector formatSignificand(@Borrow ColumnVector value, @Borrow ColumnVector packed)
    {
        try (ColumnVector leadingDigit = bitField(packed, LEAD_SHIFT, LEAD_MASK);
                ColumnVector leadingDigitString = leadingDigit.castTo(DType.STRING);
                ColumnVector fractionDigits = formatFractionDigits(value, packed);
                Scalar separator = Scalar.fromString(".");
                Scalar nullString = Scalar.fromString(null)) {
            return ColumnVector.stringConcatenate(separator, nullString, new ColumnView[] {leadingDigitString, fractionDigits});
        }
    }

    private static @Move ColumnVector formatFractionDigits(@Borrow ColumnVector value, @Borrow ColumnVector packed)
    {
        try (ColumnVector eightDigits = formatEightDigits(value, FRACTION_DIGITS_UDF);
                ColumnVector fractionLength = bitField(packed, 0, FRACTION_LENGTH_MASK);
                Scalar zero = Scalar.fromInt(0);
                // cuDF rejects NULL substring bounds, and the row stays NULL through the digits anyway
                ColumnVector end = fractionLength.replaceNulls(zero);
                ColumnVector start = ColumnVector.fromScalar(zero, toIntExact(value.getRowCount()))) {
            return eightDigits.substring(start, end);
        }
    }

    private static @Move ColumnVector formatEightDigits(@Borrow ColumnVector value, String udf)
    {
        try (ColumnVector digits = value.transform(udf, false);
                ColumnVector unpadded = digits.castTo(DType.STRING)) {
            return unpadded.zfill(8);
        }
    }

    private static @Move ColumnVector decimalExponent(@Borrow ColumnVector packed)
    {
        try (Scalar shift = Scalar.fromInt(EXPONENT_SHIFT);
                ColumnVector biased = packed.binaryOp(SHIFT_RIGHT, shift, DType.INT32);
                Scalar bias = Scalar.fromInt(EXPONENT_BIAS)) {
            return biased.binaryOp(SUB, bias, DType.INT32);
        }
    }

    private static @Move ColumnVector bitField(@Borrow ColumnVector packed, int shift, int mask)
    {
        try (Scalar shiftScalar = Scalar.fromInt(shift);
                ColumnVector shifted = packed.binaryOp(SHIFT_RIGHT, shiftScalar, DType.INT32);
                Scalar maskScalar = Scalar.fromInt(mask)) {
            return shifted.binaryOp(BITWISE_AND, maskScalar, DType.INT32);
        }
    }

    private static @Move ColumnVector signed(@Borrow ColumnVector negative, @Borrow ColumnVector magnitude)
    {
        try (Scalar minus = Scalar.fromString("-");
                Scalar empty = Scalar.fromString("");
                ColumnVector sign = negative.ifElse(minus, empty)) {
            return ColumnVector.stringConcatenate(new ColumnView[] {sign, magnitude});
        }
    }

    private static @Move ColumnVector replaceSpecialValues(@Borrow ColumnVector value, @Borrow ColumnVector negative, @Move ColumnVector finite)
    {
        try (finite;
                Scalar zero = Scalar.fromFloat(0.0f);
                ColumnVector isZero = value.equalTo(zero);
                Scalar negativeZeroString = Scalar.fromString("-0E0");
                Scalar positiveZeroString = Scalar.fromString("0E0");
                ColumnVector zeroString = negative.ifElse(negativeZeroString, positiveZeroString);
                ColumnVector withZero = isZero.ifElse(zeroString, finite);
                Scalar positiveInfinity = Scalar.fromFloat(Float.POSITIVE_INFINITY);
                ColumnVector isPositiveInfinity = value.equalTo(positiveInfinity);
                Scalar positiveInfinityString = Scalar.fromString("Infinity");
                ColumnVector withPositiveInfinity = isPositiveInfinity.ifElse(positiveInfinityString, withZero);
                Scalar negativeInfinity = Scalar.fromFloat(Float.NEGATIVE_INFINITY);
                ColumnVector isNegativeInfinity = value.equalTo(negativeInfinity);
                Scalar negativeInfinityString = Scalar.fromString("-Infinity");
                ColumnVector withNegativeInfinity = isNegativeInfinity.ifElse(negativeInfinityString, withPositiveInfinity);
                ColumnVector isNan = value.isNan();
                Scalar nanString = Scalar.fromString("NaN")) {
            return isNan.ifElse(nanString, withNegativeInfinity);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        if (checksLength()) {
            return nullSkippingCheck(ImmutableList.of(argument));
        }
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
        return obj instanceof GpuRealToVarcharCast other
                && argument.equals(other.argument)
                && targetType.equals(other.targetType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument, targetType);
    }
}
