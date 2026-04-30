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

import static ai.rapids.cudf.BinaryOp.DIV;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.minValue;
import static io.trino.operator.gpu.expression.CudfUtils.negativeOne;
import static io.trino.operator.gpu.expression.CudfUtils.zero;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Integer DIV with divide-by-zero and {@code MIN/-1} overflow detection.
 */
public class GpuIntegerDivide
        implements GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;
    private final DType operandType;
    private final String operandTrinoTypeName;

    public GpuIntegerDivide(GpuExpression left, GpuExpression right, DType operandType, String operandTrinoTypeName)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.operandType = requireNonNull(operandType, "operandType is null");
        this.operandTrinoTypeName = requireNonNull(operandTrinoTypeName, "operandTrinoTypeName is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own ColumnVector leftResult = left.evaluate(positionCount, inputColumns);
                @Own ColumnVector rightResult = right.evaluate(positionCount, inputColumns)) {
            // Trino's CPU evaluator never invokes the divide implementation when an operand is null —
            // it short-circuits to a null result. Masking the divisor-zero detection by
            // "left IS NOT NULL" restores the same shape: a non-null zero divisor only matters when
            // the dividend is also non-null.
            try (@Own Scalar zero = zero(operandType);
                    @Own ClosingOnce<ColumnVector> divisorIsZero = ClosingOnce.own(rightResult.equalTo(zero));
                    @Own ClosingOnce<ColumnVector> dividendNotNull = ClosingOnce.own(leftResult.isNotNull());
                    @Own ColumnVector divByZero = divisorIsZero.borrow().binaryOp(NULL_LOGICAL_AND, dividendNotNull.borrow(), DType.BOOL8)) {
                divisorIsZero.close();
                dividendNotNull.close();
                if (anyTrue(divByZero)) {
                    throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
                }
            }
            // For two's-complement integers, MIN_VALUE / -1 overflows because -MIN_VALUE is
            // unrepresentable in the same width. Detected via (left == MIN_VALUE) AND (right == -1);
            // nulls in either operand naturally propagate to FALSE under cuDF's null-aware AND.
            try (@Own Scalar min = minValue(operandType);
                    @Own Scalar negOne = negativeOne(operandType);
                    @Own ClosingOnce<ColumnVector> leftIsMin = ClosingOnce.own(leftResult.equalTo(min));
                    @Own ClosingOnce<ColumnVector> rightIsNegOne = ClosingOnce.own(rightResult.equalTo(negOne));
                    @Own ColumnVector bothMatch = leftIsMin.borrow().binaryOp(NULL_LOGICAL_AND, rightIsNegOne.borrow(), DType.BOOL8)) {
                leftIsMin.close();
                rightIsNegOne.close();
                if (anyTrue(bothMatch)) {
                    throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("%s division overflow", operandTrinoTypeName));
                }
            }
            return leftResult.binaryOp(DIV, rightResult, operandType);
        }
    }
}
