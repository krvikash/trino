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
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_AND;
import static ai.rapids.cudf.BinaryOp.NULL_LOGICAL_OR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GpuLogicalExpression
        implements GpuExpression
{
    private final List<GpuExpression> operands;
    private final BinaryOp operation;

    private GpuLogicalExpression(List<GpuExpression> operands, BinaryOp operation)
    {
        checkArgument(operands.size() >= 2, "AND requires at least 2 operands");
        this.operands = ImmutableList.copyOf(operands);
        this.operation = requireNonNull(operation, "operation is null");
    }

    public static GpuLogicalExpression and(List<GpuExpression> operands)
    {
        return new GpuLogicalExpression(operands, NULL_LOGICAL_AND);
    }

    public static GpuLogicalExpression or(List<GpuExpression> operands)
    {
        return new GpuLogicalExpression(operands, NULL_LOGICAL_OR);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own ClosingRef<ColumnVector> result = ClosingRef.own(operands.getFirst().evaluate(positionCount, inputColumns))) {
            for (int i = 1; i < operands.size(); i++) {
                try (@Own ColumnVector left = result.take();
                        @Own ColumnVector right = operands.get(i).evaluate(positionCount, inputColumns)) {
                    result.set(left.binaryOp(operation, right, DType.BOOL8));
                }
            }
            return result.take();
        }
    }
}
