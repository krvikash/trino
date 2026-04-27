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
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

/**
 * Utilities helpful when working with rapids cudf library.
 */
final class CudfUtils
{
    private CudfUtils() {}

    static @Move Scalar zero(DType operandType)
    {
        if (operandType.equals(DType.INT8)) {
            return Scalar.fromByte((byte) 0);
        }
        if (operandType.equals(DType.INT16)) {
            return Scalar.fromShort((short) 0);
        }
        if (operandType.equals(DType.INT32)) {
            return Scalar.fromInt(0);
        }
        if (operandType.equals(DType.INT64)) {
            return Scalar.fromLong(0L);
        }
        throw new IllegalArgumentException("Unsupported integer DType: " + operandType);
    }

    static @Move Scalar negativeOne(DType operandType)
    {
        if (operandType.equals(DType.INT8)) {
            return Scalar.fromByte((byte) -1);
        }
        if (operandType.equals(DType.INT16)) {
            return Scalar.fromShort((short) -1);
        }
        if (operandType.equals(DType.INT32)) {
            return Scalar.fromInt(-1);
        }
        if (operandType.equals(DType.INT64)) {
            return Scalar.fromLong(-1L);
        }
        throw new IllegalArgumentException("Unsupported integer DType: " + operandType);
    }

    static @Move Scalar minValue(DType operandType)
    {
        if (operandType.equals(DType.INT8)) {
            return Scalar.fromByte(Byte.MIN_VALUE);
        }
        if (operandType.equals(DType.INT16)) {
            return Scalar.fromShort(Short.MIN_VALUE);
        }
        if (operandType.equals(DType.INT32)) {
            return Scalar.fromInt(Integer.MIN_VALUE);
        }
        if (operandType.equals(DType.INT64)) {
            return Scalar.fromLong(Long.MIN_VALUE);
        }
        throw new IllegalArgumentException("Unsupported integer DType: " + operandType);
    }

    static boolean anyTrue(@Borrow ColumnVector boolColumn)
    {
        try (@Own Scalar any = boolColumn.any()) {
            return any.isValid() && any.getBoolean();
        }
    }
}
