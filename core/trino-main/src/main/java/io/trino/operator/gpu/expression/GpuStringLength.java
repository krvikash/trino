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

import static java.util.Objects.requireNonNull;

public class GpuStringLength
        implements GpuExpression
{
    private final GpuExpression argument;

    public GpuStringLength(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own ClosingOnce<ColumnVector> input = ClosingOnce.own(argument.evaluate(positionCount, inputColumns));
                @Own ColumnVector charLengths = input.borrow().getCharLengths()) {
            input.close();
            // cuDF returns INT32; Trino's length() returns BIGINT.
            return charLengths.castTo(DType.INT64);
        }
    }
}
