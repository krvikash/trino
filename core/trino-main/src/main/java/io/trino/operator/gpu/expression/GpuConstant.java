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
import io.airlift.slice.Slice;
import io.trino.operator.gpu.borrow.Borrow;
import io.trino.operator.gpu.borrow.Move;
import io.trino.operator.gpu.borrow.Own;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class GpuConstant
        implements GpuExpression
{
    private final Object value;
    private final Type type;

    public GpuConstant(@Nullable Object value, Type type)
    {
        this.value = value;
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own Scalar scalar = createScalar()) {
            return ColumnVector.fromScalar(scalar, positionCount);
        }
    }

    private @Move Scalar createScalar()
    {
        if (type == BOOLEAN) {
            return Scalar.fromBool((Boolean) value);
        }
        if (type == TINYINT) {
            return Scalar.fromByte(value == null ? null : ((Long) value).byteValue());
        }
        if (type == SMALLINT) {
            return Scalar.fromShort(value == null ? null : ((Long) value).shortValue());
        }
        if (type == INTEGER) {
            return Scalar.fromInt(value == null ? null : ((Long) value).intValue());
        }
        if (type == BIGINT) {
            return Scalar.fromLong((Long) value);
        }
        if (type == REAL) {
            return Scalar.fromFloat(value == null ? null : Float.intBitsToFloat(((Long) value).intValue()));
        }
        if (type == DOUBLE) {
            return Scalar.fromDouble((Double) value);
        }
        if (type instanceof VarcharType) {
            return Scalar.fromString(value == null ? null : ((Slice) value).toStringUtf8());
        }
        throw new UnsupportedOperationException("Unsupported constant type: " + type);
    }
}
