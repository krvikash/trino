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

import java.util.List;

import static ai.rapids.cudf.BinaryOp.MOD;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.CudfUtils.zero;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static java.util.Objects.requireNonNull;

/**
 * Integer MOD with divide-by-zero detection.
 */
public class GpuIntegerModulo
        implements GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;
    private final DType operandType;

    public GpuIntegerModulo(GpuExpression left, GpuExpression right, DType operandType)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.operandType = requireNonNull(operandType, "operandType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector leftResult = left.evaluate(positionCount, inputColumns);
                ColumnVector rightResult = right.evaluate(positionCount, inputColumns)) {
            // Trino's CPU evaluator short-circuits modulus when an operand is null. Mask the
            // divisor-zero detection by "left IS NOT NULL" so the GPU side matches.
            try (Scalar zero = zero(operandType);
                    ClosingOnce<ColumnVector> divisorIsZero = ClosingOnce.own(rightResult.equalTo(zero));
                    ClosingOnce<ColumnVector> dividendNotNull = ClosingOnce.own(leftResult.isNotNull());
                    ColumnVector divByZero = divisorIsZero.borrow().binaryOp(NULL_LOGICAL_AND, dividendNotNull.borrow(), DType.BOOL8)) {
                divisorIsZero.close();
                dividendNotNull.close();
                if (anyTrue(divByZero)) {
                    throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
                }
            }
            return leftResult.binaryOp(MOD, rightResult, operandType);
        }
    }
}
