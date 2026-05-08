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
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.UnaryOp;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.floatingScalar;
import static io.trino.operator.gpu.expression.CudfUtils.maxValue;
import static io.trino.operator.gpu.expression.CudfUtils.minValue;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Cast from a floating-point type (REAL, DOUBLE) to an integer type with HALF_UP rounding
 * (away from zero) and overflow detection.
 *
 * <p>NaN, ±Infinity, and finite out-of-range values all produce {@code INVALID_CAST_ARGUMENT}.
 * Once the CPU-side asymmetry between {@code NUMERIC_VALUE_OUT_OF_RANGE} and
 * {@code INVALID_CAST_ARGUMENT} for ±Inf and finite OOR is unified, the test harness's
 * one-way relaxation can be removed and this implementation will match CPU exactly.
 */
// TODO: revisit arithmetic overflow detection to use a shared range-check helper.
public class GpuFloatingToIntegerCast
        implements GpuExpression
{
    private final GpuExpression argument;
    private final DType sourceDType;
    private final DType targetDType;
    private final double lowerBoundExclusive;
    private final double upperBoundExclusive;
    private final String sourceTrinoTypeName;
    private final String targetTrinoTypeName;

    public GpuFloatingToIntegerCast(
            GpuExpression argument,
            DType sourceDType,
            DType targetDType,
            String sourceTrinoTypeName,
            String targetTrinoTypeName)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.sourceDType = requireNonNull(sourceDType, "sourceDType is null");
        this.targetDType = requireNonNull(targetDType, "targetDType is null");
        this.lowerBoundExclusive = exclusiveLowerBound(sourceDType, targetDType);
        this.upperBoundExclusive = exclusiveUpperBound(sourceDType, targetDType);
        this.sourceTrinoTypeName = requireNonNull(sourceTrinoTypeName, "sourceTrinoTypeName is null");
        this.targetTrinoTypeName = requireNonNull(targetTrinoTypeName, "targetTrinoTypeName is null");
    }

    /**
     * Largest source-type value that should be rejected as too low (i.e. would round below {@code targetDType}'s
     * minimum). When the half-step {@code targetMin - 0.5} is exactly representable in the source type, use it
     * directly. When it isn't (FLOAT32→INT32/INT64, FLOAT64→INT64), {@code targetMin} itself is exactly
     * representable as a power of two — use {@code nextDown} so source value {@code targetMin} still passes the
     * strict-greater check.
     */
    private static double exclusiveLowerBound(DType sourceDType, DType targetDType)
    {
        long targetMin = minValue(targetDType);
        return switch (sourceDType.getTypeId()) {
            case FLOAT32 -> switch (targetDType.getTypeId()) {
                case INT8, INT16 -> targetMin - 0.5;
                case INT32, INT64 -> Math.nextDown((float) targetMin);
                default -> throw new IllegalArgumentException("Unsupported target DType: " + targetDType);
            };
            case FLOAT64 -> switch (targetDType.getTypeId()) {
                case INT8, INT16, INT32 -> targetMin - 0.5;
                case INT64 -> Math.nextDown((double) targetMin);
                default -> throw new IllegalArgumentException("Unsupported target DType: " + targetDType);
            };
            default -> throw new IllegalArgumentException("Unsupported source DType: " + sourceDType);
        };
    }

    /**
     * Smallest source-type value that should be rejected as too high. When {@code targetMax + 0.5} is exactly
     * representable in the source type, use it directly. When it isn't, {@code (source) targetMax} rounds up to
     * the next power of two ({@code targetMax + 1}) — use that as the exclusive upper bound, which correctly
     * rejects any source value at or above that power of two.
     */
    private static double exclusiveUpperBound(DType sourceDType, DType targetDType)
    {
        long targetMax = maxValue(targetDType);
        return switch (sourceDType.getTypeId()) {
            case FLOAT32 -> switch (targetDType.getTypeId()) {
                case INT8, INT16 -> targetMax + 0.5;
                case INT32, INT64 -> (float) targetMax;
                default -> throw new IllegalArgumentException("Unsupported target DType: " + targetDType);
            };
            case FLOAT64 -> switch (targetDType.getTypeId()) {
                case INT8, INT16, INT32 -> targetMax + 0.5;
                case INT64 -> (double) targetMax;
                default -> throw new IllegalArgumentException("Unsupported target DType: " + targetDType);
            };
            default -> throw new IllegalArgumentException("Unsupported source DType: " + sourceDType);
        };
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own ClosingOnce<ColumnVector> source = ClosingOnce.own(argument.evaluate(positionCount, inputColumns))) {
            // Range check: input strictly within (lowerBoundExclusive, upperBoundExclusive).
            // IEEE comparisons against NaN return false, so NaN/±Inf/finite-OOR all end up out of range.
            // Null inputs propagate as null through the comparisons; anyTrue ignores nulls.
            try (@Own Scalar lower = floatingScalar(sourceDType, lowerBoundExclusive);
                    @Own Scalar upper = floatingScalar(sourceDType, upperBoundExclusive);
                    @Own ClosingOnce<ColumnVector> aboveLower = ClosingOnce.own(source.borrow().binaryOp(GREATER, lower, DType.BOOL8));
                    @Own ClosingOnce<ColumnVector> belowUpper = ClosingOnce.own(source.borrow().binaryOp(LESS, upper, DType.BOOL8));
                    @Own ClosingOnce<ColumnVector> inRange = ClosingOnce.own(aboveLower.borrow().binaryOp(NULL_LOGICAL_AND, belowUpper.borrow(), DType.BOOL8))) {
                aboveLower.close();
                belowUpper.close();
                try (@Own ColumnVector outOfRange = inRange.borrow().unaryOp(UnaryOp.NOT)) {
                    inRange.close();
                    if (anyTrue(outOfRange)) {
                        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast %s to %s", sourceTrinoTypeName, targetTrinoTypeName));
                    }
                }
            }
            // Round half-away-from-zero: trunc(x + sign(x) * 0.5), matching CPU's MathFunctions.round(double).
            // The rounding arithmetic is always performed in FLOAT64 to match CPU semantics and to avoid
            // precision loss for FLOAT32 inputs (e.g. float32 9999999.0f + 0.5f = 10000000.0f).
            try (@Own ClosingOnce<ColumnVector> wideSource = ClosingOnce.own(source.borrow().castTo(DType.FLOAT64))) {
                source.close();
                try (@Own Scalar zero = floatingScalar(DType.FLOAT64, 0.0);
                        @Own Scalar halfStepTowardNegativeInfinity = floatingScalar(DType.FLOAT64, -0.5);
                        @Own Scalar halfStepTowardPositiveInfinity = floatingScalar(DType.FLOAT64, 0.5);
                        @Own ClosingOnce<ColumnVector> negativeMask = ClosingOnce.own(wideSource.borrow().binaryOp(LESS, zero, DType.BOOL8));
                        @Own ClosingOnce<ColumnVector> halfStepAwayFromZero = ClosingOnce.own(negativeMask.borrow().ifElse(halfStepTowardNegativeInfinity, halfStepTowardPositiveInfinity));
                        @Own ColumnVector shifted = wideSource.borrow().binaryOp(ADD, halfStepAwayFromZero.borrow(), DType.FLOAT64)) {
                    wideSource.close();
                    negativeMask.close();
                    halfStepAwayFromZero.close();
                    return shifted.castTo(targetDType);
                }
            }
        }
    }
}
