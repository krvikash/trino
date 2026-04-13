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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import io.trino.operator.gpu.borrow.Borrow;
import io.trino.operator.gpu.borrow.Move;
import io.trino.operator.gpu.borrow.Own;

import java.util.List;

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
        try (@Own CloseOnce<ColumnVector> valueResult = CloseOnce.own(value.evaluate(positionCount, inputColumns))) {
            try (@Own CloseOnce<ColumnVector> minResult = CloseOnce.own(min.evaluate(positionCount, inputColumns))) {
                try (@Own ColumnVector greaterOrEqual = valueResult.value().binaryOp(BinaryOp.GREATER_EQUAL, minResult.value(), DType.BOOL8)) {
                    minResult.close();
                    try (@Own CloseOnce<ColumnVector> maxResult = CloseOnce.own(max.evaluate(positionCount, inputColumns))) {
                        try (@Own ColumnVector lessOrEqual = valueResult.value().binaryOp(BinaryOp.LESS_EQUAL, maxResult.value(), DType.BOOL8)) {
                            valueResult.close();
                            maxResult.close();
                            return greaterOrEqual.binaryOp(BinaryOp.NULL_LOGICAL_AND, lessOrEqual, DType.BOOL8);
                        }
                    }
                }
            }
        }
    }
}
