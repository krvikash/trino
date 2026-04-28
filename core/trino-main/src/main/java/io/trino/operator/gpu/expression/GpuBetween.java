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
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static ai.rapids.cudf.BinaryOp.GREATER_EQUAL;
import static ai.rapids.cudf.BinaryOp.LESS_EQUAL;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static java.util.Objects.requireNonNull;

public class GpuBetween
        implements GpuExpression
{
    private final GpuExpression value;
    private final GpuExpression min;
    private final GpuExpression max;

    public GpuBetween(GpuExpression value, GpuExpression min, GpuExpression max)
    {
        this.value = requireNonNull(value, "value is null");
        this.min = requireNonNull(min, "min is null");
        this.max = requireNonNull(max, "max is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own ClosingOnce<ColumnVector> valueResult = ClosingOnce.own(value.evaluate(positionCount, inputColumns))) {
            try (@Own ClosingOnce<ColumnVector> minResult = ClosingOnce.own(min.evaluate(positionCount, inputColumns))) {
                try (@Own ColumnVector greaterOrEqual = valueResult.borrow().binaryOp(GREATER_EQUAL, minResult.borrow(), DType.BOOL8)) {
                    minResult.close();
                    try (@Own ClosingOnce<ColumnVector> maxResult = ClosingOnce.own(max.evaluate(positionCount, inputColumns))) {
                        try (@Own ColumnVector lessOrEqual = valueResult.borrow().binaryOp(LESS_EQUAL, maxResult.borrow(), DType.BOOL8)) {
                            valueResult.close();
                            maxResult.close();
                            return greaterOrEqual.binaryOp(NULL_LOGICAL_AND, lessOrEqual, DType.BOOL8);
                        }
                    }
                }
            }
        }
    }
}
