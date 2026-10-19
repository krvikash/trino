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
import io.trino.type.DoubleOperators;
import io.trino.type.RealOperators;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static ai.rapids.cudf.BinaryOp.BITWISE_AND;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.SHIFT_RIGHT;
import static ai.rapids.cudf.BinaryOp.SUB;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.floatingScalar;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/// Formats a DOUBLE like [io.trino.type.DoubleOperators#castToVarchar], or a REAL like
/// [io.trino.type.RealOperators#castToVarchar]: `NaN`, `Infinity`, `-Infinity`, `0E0`, `-0E0`, and
/// otherwise `d.ddddE[-]x` with at least one fraction digit.
///
/// For DOUBLE the digits are the shortest ones that parse back to the same double, which is what the
/// CPU prints on JDK 26+, where `DecimalFormat` uses the algorithm of `Double.toString`
/// (https://bugs.openjdk.org/browse/JDK-8362448). On JDK 25 `DecimalFormat` prints other digits for
/// some powers of two and the smallest subnormals, whose digits are therefore taken from the CPU, and
/// for some magnitudes in [2^54, 2^85), where it may print a 17th or 18th digit that is not needed, or
/// a last digit farther from the exact value. The GPU result still parses back to the same double there.
///
/// For REAL the result is the same as the CPU's on JDK 25: at most 7 significant digits, which do not
/// always parse back to the same real. JDK 26+ rounds some ties differently, see `ryu_f2s.cu`.
public final class GpuFloatingToVarcharCast
        extends GpuExpression
{
    private static final int FRACTION_LENGTH_MASK = 0x1F;
    private static final int LEAD_SHIFT = 5;
    private static final int LEAD_MASK = 0xF;
    private static final int EXPONENT_SHIFT = 9;
    // Keeps the packed exponent non-negative, so the shift right needs no sign handling
    private static final int EXPONENT_BIAS = 1024;

    // The subnormals with bits below 2^SMALL_SUBNORMAL_POWERS, and the powers of two above them, have
    // their digits taken from the CPU. See ryu_d2s.cu.
    private static final int SMALL_SUBNORMAL_POWERS = 5;
    private static final int SMALL_SUBNORMALS = 1 << SMALL_SUBNORMAL_POWERS;
    private static final int MAX_POWER_OF_TWO = 1023;
    // The fraction digits the UDF returns hold up to 17 significant digits
    private static final int MAX_TABLE_DIGITS = 17;

    private static final String RYU_TABLES = loadSource("ryu_tables.cu");

    private final GpuExpression argument;
    private final Source source;
    private final VarcharType targetType;

    public enum Source
    {
        /// Longest result: a sign, 17 significant digits, the point, `E`, and a negative three-digit exponent.
        /// The CPU result is no longer either: it has 18 digits only for exponents of two digits.
        DOUBLE(DType.FLOAT64, 24),
        /// Longest result: a sign, 7 significant digits, the point, `E`, and a negative two-digit exponent
        REAL(DType.FLOAT32, 13),
        /**/;

        private final DType dtype;
        private final int maxLength;

        Source(DType dtype, int maxLength)
        {
            this.dtype = dtype;
            this.maxLength = maxLength;
        }
    }

    private record Udfs(String highFractionDigits, Optional<String> lowFractionDigits, String packed) {}

    private static final String DOUBLE_SOURCE = RYU_TABLES + specialDigitsTables() + loadSource("ryu_d2s.cu");
    private static final String REAL_SOURCE = RYU_TABLES + loadSource("ryu_f2s.cu");
    private static final Udfs DOUBLE_UDFS = new Udfs(ryuUdf(DOUBLE_SOURCE, 0), Optional.of(ryuUdf(DOUBLE_SOURCE, 1)), ryuUdf(DOUBLE_SOURCE, 2));
    // A REAL has at most 6 fraction digits, so the last 8 of the 16 fraction digits are always zero
    private static final Udfs REAL_UDFS = new Udfs(ryuUdf(REAL_SOURCE, 0), Optional.empty(), ryuUdf(REAL_SOURCE, 2));

    public GpuFloatingToVarcharCast(GpuExpression argument, Source source, VarcharType targetType)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.source = requireNonNull(source, "source is null");
        this.targetType = requireNonNull(targetType, "targetType is null");
    }

    private boolean checksLength()
    {
        return !targetType.isUnbounded() && targetType.getBoundedLength() < source.maxLength;
    }

    private static String loadSource(String name)
    {
        try {
            return Resources.toString(Resources.getResource(GpuFloatingToVarcharCast.class, name), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /// The CPU's significant digits, as an integer, and decimal exponent for each value whose digits
    /// may not be the shortest ones: the smallest subnormals, where Java prints at least two digits,
    /// then the powers of two above them
    // TODO https://starburstdata.atlassian.net/browse/ENG-29636 remove the powers of two when JDK 25 is no longer
    //  supported: JDK 26+ DecimalFormat prints their shortest digits, https://bugs.openjdk.org/browse/JDK-8362448
    private static String specialDigitsTables()
    {
        StringBuilder digits = new StringBuilder("__device__ const unsigned long long RYU_SPECIAL_DIGITS[] = {\n");
        StringBuilder exponents = new StringBuilder("__device__ const int RYU_SPECIAL_EXPONENTS[] = {\n");
        // Bits 0 is zero, never looked up
        digits.append("0ull,\n");
        exponents.append("0,\n");
        for (long bits = 1; bits < SMALL_SUBNORMALS; bits++) {
            appendCpuDigits(digits, exponents, Double.longBitsToDouble(bits));
        }
        for (int power = Double.MIN_EXPONENT - 52 + SMALL_SUBNORMAL_POWERS; power <= MAX_POWER_OF_TWO; power++) {
            appendCpuDigits(digits, exponents, Math.scalb(1.0, power));
        }
        return digits.append("};\n").append(exponents).append("};\n").toString();
    }

    private static void appendCpuDigits(StringBuilder digits, StringBuilder exponents, double value)
    {
        BigDecimal cpu = new BigDecimal(DoubleOperators.castToVarchar(Integer.MAX_VALUE, value).toStringUtf8()).stripTrailingZeros();
        if (cpu.precision() <= MAX_TABLE_DIGITS) {
            digits.append(cpu.unscaledValue()).append("ull,\n");
            exponents.append(-cpu.scale()).append(",\n");
        }
        else {
            digits.append("0ull,\n");
            exponents.append("0,\n");
        }
    }

    private static String ryuUdf(String source, int output)
    {
        return source
                .replace("RYU_OUTPUT", Integer.toString(output))
                .replace("RYU_EXPONENT_BIAS", Integer.toString(EXPONENT_BIAS))
                .replace("RYU_EXPONENT_SHIFT", Integer.toString(EXPONENT_SHIFT))
                .replace("RYU_LEAD_SHIFT", Integer.toString(LEAD_SHIFT))
                .replace("RYU_SMALL_SUBNORMAL_POWERS", Integer.toString(SMALL_SUBNORMAL_POWERS))
                .replace("RYU_SMALL_SUBNORMALS", Integer.toString(SMALL_SUBNORMALS));
    }

    private Udfs udfs()
    {
        return switch (source) {
            case DOUBLE -> DOUBLE_UDFS;
            case REAL -> REAL_UDFS;
        };
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector value = argument.evaluate(positionCount, inputColumns, bindings)) {
            verify(value.getType().equals(source.dtype), "Expected %s input, got %s", source.dtype, value.getType());
            try (ClosingRef<ColumnVector> result = ClosingRef.own(format(value))) {
                if (checksLength()) {
                    checkLength(value, result.borrow());
                }
                return result.take();
            }
        }
    }

    private @Move ColumnVector format(@Borrow ColumnVector value)
    {
        try (ColumnVector negative = signBit(value, source);
                ColumnVector magnitude = formatMagnitude(value, udfs())) {
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
                switch (source) {
                    case DOUBLE -> DoubleOperators.castToVarchar(bound, offending.getDouble());
                    case REAL -> RealOperators.castToVarchar(bound, floatToRawIntBits(offending.getFloat()));
                }
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
    private static @Move ColumnVector signBit(@Borrow ColumnVector value, Source source)
    {
        return switch (source) {
            case DOUBLE -> {
                try (ColumnView bits = value.bitCastTo(DType.INT64);
                        Scalar zero = Scalar.fromLong(0)) {
                    yield bits.binaryOp(LESS, zero, DType.BOOL8);
                }
            }
            case REAL -> {
                try (ColumnView bits = value.bitCastTo(DType.INT32);
                        Scalar zero = Scalar.fromInt(0)) {
                    yield bits.binaryOp(LESS, zero, DType.BOOL8);
                }
            }
        };
    }

    private static @Move ColumnVector formatMagnitude(@Borrow ColumnVector value, Udfs udfs)
    {
        try (ColumnVector packed = value.transform(udfs.packed(), false);
                ColumnVector significand = formatSignificand(value, packed, udfs);
                ColumnVector exponent = decimalExponent(packed);
                ColumnVector exponentString = exponent.castTo(DType.STRING);
                Scalar separator = Scalar.fromString("E");
                Scalar nullString = Scalar.fromString(null)) {
            return ColumnVector.stringConcatenate(separator, nullString, new ColumnView[] {significand, exponentString});
        }
    }

    private static @Move ColumnVector formatSignificand(@Borrow ColumnVector value, @Borrow ColumnVector packed, Udfs udfs)
    {
        try (ColumnVector leadingDigit = bitField(packed, LEAD_SHIFT, LEAD_MASK);
                ColumnVector leadingDigitString = leadingDigit.castTo(DType.STRING);
                ColumnVector fractionDigits = formatFractionDigits(value, packed, udfs);
                Scalar separator = Scalar.fromString(".");
                Scalar nullString = Scalar.fromString(null)) {
            return ColumnVector.stringConcatenate(separator, nullString, new ColumnView[] {leadingDigitString, fractionDigits});
        }
    }

    private static @Move ColumnVector formatFractionDigits(@Borrow ColumnVector value, @Borrow ColumnVector packed, Udfs udfs)
    {
        try (ColumnVector allDigits = formatAllFractionDigits(value, udfs);
                ColumnVector fractionLength = bitField(packed, 0, FRACTION_LENGTH_MASK);
                Scalar zero = Scalar.fromInt(0);
                // cuDF rejects NULL substring bounds, and the row stays NULL through the digits anyway
                ColumnVector end = fractionLength.replaceNulls(zero);
                ColumnVector start = ColumnVector.fromScalar(zero, toIntExact(value.getRowCount()))) {
            return allDigits.substring(start, end);
        }
    }

    private static @Move ColumnVector formatAllFractionDigits(@Borrow ColumnVector value, Udfs udfs)
    {
        if (udfs.lowFractionDigits().isEmpty()) {
            return formatEightDigits(value, udfs.highFractionDigits());
        }
        try (ColumnVector high = formatEightDigits(value, udfs.highFractionDigits());
                ColumnVector low = formatEightDigits(value, udfs.lowFractionDigits().get())) {
            return ColumnVector.stringConcatenate(new ColumnView[] {high, low});
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
        DType type = value.getType();
        try (finite;
                Scalar zero = floatingScalar(type, 0.0);
                ColumnVector isZero = value.equalTo(zero);
                Scalar negativeZeroString = Scalar.fromString("-0E0");
                Scalar positiveZeroString = Scalar.fromString("0E0");
                ColumnVector zeroString = negative.ifElse(negativeZeroString, positiveZeroString);
                ColumnVector withZero = isZero.ifElse(zeroString, finite);
                Scalar positiveInfinity = floatingScalar(type, Double.POSITIVE_INFINITY);
                ColumnVector isPositiveInfinity = value.equalTo(positiveInfinity);
                Scalar positiveInfinityString = Scalar.fromString("Infinity");
                ColumnVector withPositiveInfinity = isPositiveInfinity.ifElse(positiveInfinityString, withZero);
                Scalar negativeInfinity = floatingScalar(type, Double.NEGATIVE_INFINITY);
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
        return obj instanceof GpuFloatingToVarcharCast other
                && argument.equals(other.argument)
                && source == other.source
                && targetType.equals(other.targetType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument, source, targetType);
    }
}
