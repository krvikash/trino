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
import ai.rapids.cudf.Scalar;
import io.trino.operator.gpu.GpuTypeConversion.ToScalar;
import io.trino.operator.gpu.borrow.Borrow;
import io.trino.operator.gpu.borrow.Move;
import io.trino.operator.gpu.borrow.Own;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GpuConstant
        implements GpuExpression
{
    // TODO (https://starburstdata.atlassian.net/browse/ENG-9846) should the Scalar be created once?
    private final ToScalar toScalar;
    private final Optional<Object> value;

    public GpuConstant(ToScalar toScalar, Optional<Object> value)
    {
        this.toScalar = requireNonNull(toScalar, "toScalar is null");
        this.value = requireNonNull(value, "value is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own Scalar scalar = toScalar.copyToScalar(value)) {
            return ColumnVector.fromScalar(scalar, positionCount);
        }
    }
}
