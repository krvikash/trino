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
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;

import static ai.rapids.cudf.BinaryOp.BITWISE_AND;
import static ai.rapids.cudf.BinaryOp.BITWISE_XOR;
import static ai.rapids.cudf.BinaryOp.LESS;
import static ai.rapids.cudf.BinaryOp.SUB;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.zero;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Integer SUB with overflow detection.
 */
public class GpuIntegerSubtract
        implements GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;
    private final DType operandType;
    private final String operandTrinoTypeName;

    public GpuIntegerSubtract(GpuExpression left, GpuExpression right, DType operandType, String operandTrinoTypeName)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.operandType = requireNonNull(operandType, "operandType is null");
        this.operandTrinoTypeName = requireNonNull(operandTrinoTypeName, "operandTrinoTypeName is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        // SUB overflows iff operands differ in sign and the result's sign differs
        // from the minuend's: ((left ^ right) & (left ^ result)) < 0. Sign bit of
        // (left ^ right) is set iff operands differ in sign; ANDing with (left ^
        // result) and testing < 0 checks that property on the sign bit only,
        // vectorized against the (possibly wrapped) cuDF SUB result.
        try (ClosingOnce<ColumnVector> leftResult = ClosingOnce.own(left.evaluate(positionCount, inputColumns));
                ClosingOnce<ColumnVector> rightResult = ClosingOnce.own(right.evaluate(positionCount, inputColumns));
                ClosingRef<ColumnVector> result = ClosingRef.own(leftResult.borrow().binaryOp(SUB, rightResult.borrow(), operandType));
                ClosingOnce<ColumnVector> signsDiffer = ClosingOnce.own(leftResult.borrow().binaryOp(BITWISE_XOR, rightResult.borrow(), operandType))) {
            rightResult.close();
            try (ClosingOnce<ColumnVector> resultSignDiffersFromLeft = ClosingOnce.own(leftResult.borrow().binaryOp(BITWISE_XOR, result.borrow(), operandType))) {
                leftResult.close();
                try (ClosingOnce<ColumnVector> signBits = ClosingOnce.own(signsDiffer.borrow().binaryOp(BITWISE_AND, resultSignDiffersFromLeft.borrow(), operandType))) {
                    signsDiffer.close();
                    resultSignDiffersFromLeft.close();
                    try (Scalar zero = zero(operandType);
                            ColumnVector overflow = signBits.borrow().binaryOp(LESS, zero, DType.BOOL8)) {
                        signBits.close();
                        if (anyTrue(overflow)) {
                            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("%s subtraction overflow", operandTrinoTypeName));
                        }
                    }
                }
            }
            return result.take();
        }
    }
}
