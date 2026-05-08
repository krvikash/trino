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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GpuCast
        implements GpuExpression
{
    private final GpuExpression argument;
    private final DType toType;

    public GpuCast(GpuExpression argument, DType toType)
    {
        this.argument = requireNonNull(argument, "argument is null");
        this.toType = requireNonNull(toType, "toType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector result = argument.evaluate(positionCount, inputColumns)) {
            return result.castTo(toType);
        }
    }
}
