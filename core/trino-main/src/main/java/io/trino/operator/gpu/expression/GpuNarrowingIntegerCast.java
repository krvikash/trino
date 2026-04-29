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
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static ai.rapids.cudf.BinaryOp.GREATER;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.integerScalar;
import static io.trino.operator.gpu.expression.CudfUtils.maxValue;
import static io.trino.operator.gpu.expression.CudfUtils.minValue;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Narrowing cast between integer types with overflow detection (e.g. BIGINT → INTEGER).
 */
// TODO: revisit arithmetic overflow detection to use a shared range-check helper.
public class GpuNarrowingIntegerCast
        implements GpuExpression
{
    private final GpuExpression argument;
    private final DType sourceDType;
    private final DType targetDType;
    private final String targetTrinoTypeName;

    public GpuNarrowingIntegerCast(
            GpuExpression argument,
            DType sourceDType,
            DType targetDType,
            String targetTrinoTypeName)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.sourceDType = requireNonNull(sourceDType, "sourceDType is null");
        this.targetDType = requireNonNull(targetDType, "targetDType is null");
        this.targetTrinoTypeName = requireNonNull(targetTrinoTypeName, "targetTrinoTypeName is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own ColumnVector source = argument.evaluate(positionCount, inputColumns)) {
            // Detect OOR directly: (source < min) OR (source > max). Nulls in source propagate to NULL on
            // both comparisons; NULL_LOGICAL_OR yields NULL when both operands are NULL, which anyTrue ignores.
            try (@Own Scalar lower = integerScalar(sourceDType, minValue(targetDType));
                    @Own Scalar upper = integerScalar(sourceDType, maxValue(targetDType));
                    @Own ClosingOnce<ColumnVector> belowLower = ClosingOnce.own(source.binaryOp(LESS, lower, DType.BOOL8));
                    @Own ClosingOnce<ColumnVector> aboveUpper = ClosingOnce.own(source.binaryOp(GREATER, upper, DType.BOOL8));
                    @Own ColumnVector outOfRange = belowLower.borrow().binaryOp(NULL_LOGICAL_OR, aboveUpper.borrow(), DType.BOOL8)) {
                belowLower.close();
                aboveUpper.close();
                if (anyTrue(outOfRange)) {
                    throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Out of range for %s", targetTrinoTypeName));
                }
            }
            return source.castTo(targetDType);
        }
    }
}
