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
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.gpu.GpuTypeConversion.ToScalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class GpuConstant
        extends GpuExpression
{
    private final Type type;
    // TODO (https://starburstdata.atlassian.net/browse/ENG-9846) should the Scalar be created once?
    private final ToScalar toScalar;
    private final Optional<Object> value;

    public GpuConstant(Type type, Optional<Object> value)
    {
        this.type = requireNonNull(type, "type is null");
        this.toScalar = GpuTypeConversion.toGpuMapping(type)
                .orElseThrow(() -> new UnsupportedOperationException("Unsupported type: " + type))
                .toScalar();
        this.value = requireNonNull(value, "value is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (Scalar scalar = toScalar.copyToScalar(value)) {
            return ColumnVector.fromScalar(scalar, positionCount);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuConstant other
                && type.equals(other.type)
                // derived: && toScalar.equals(other.toScalar)
                && value.equals(other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getClass(),
                type,
                // derived: toScalar,
                value);
    }
}
