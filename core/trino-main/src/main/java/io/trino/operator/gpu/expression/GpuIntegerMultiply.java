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
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static ai.rapids.cudf.BinaryOp.MUL;
import static ai.rapids.cudf.BinaryOp.NOT_EQUAL;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Integer MUL with overflow detection.
 */
// TODO (https://starburstdata.atlassian.net/browse/ENG-12005) Optimize GPU BIGINT multiply overflow detection
public class GpuIntegerMultiply
        implements GpuExpression
{
    private final GpuExpression left;
    private final GpuExpression right;
    private final DType operandType;
    private final DType widerType;
    private final String operandTrinoTypeName;

    public GpuIntegerMultiply(GpuExpression left, GpuExpression right, DType operandType, DType widerType, String operandTrinoTypeName)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.operandType = requireNonNull(operandType, "operandType is null");
        this.widerType = requireNonNull(widerType, "widerType is null");
        this.operandTrinoTypeName = requireNonNull(operandTrinoTypeName, "operandTrinoTypeName is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        // Multiply at widerType (which holds the full product), then check the wide
        // product round-trips through the narrow type. For BIGINT widerType is DECIMAL128;
        // cuDF discards the outType for decimal binary ops and picks scale=0+0=0 itself
        // (see BinaryOperable.implicitConversion), which is the DType we want.
        try (@Own ClosingOnce<ColumnVector> leftResult = ClosingOnce.own(left.evaluate(positionCount, inputColumns));
                @Own ClosingOnce<ColumnVector> rightResult = ClosingOnce.own(right.evaluate(positionCount, inputColumns));
                @Own ClosingOnce<ColumnVector> wideProduct = ClosingOnce.own(leftResult.borrow().binaryOp(MUL, rightResult.borrow(), widerType))) {
            leftResult.close();
            rightResult.close();
            try (@Own ClosingRef<ColumnVector> narrowed = ClosingRef.own(wideProduct.borrow().castTo(operandType))) {
                try (@Own ClosingOnce<ColumnVector> roundTripped = ClosingOnce.own(narrowed.borrow().castTo(widerType));
                        @Own ColumnVector mismatch = wideProduct.borrow().binaryOp(NOT_EQUAL, roundTripped.borrow(), DType.BOOL8)) {
                    wideProduct.close();
                    roundTripped.close();
                    if (anyTrue(mismatch)) {
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("%s multiplication overflow", operandTrinoTypeName));
                    }
                }
                return narrowed.take();
            }
        }
    }
}
