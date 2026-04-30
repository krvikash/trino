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
        return integerScalar(operandType, 0);
    }

    static @Move Scalar integerScalar(DType integerDType, long value)
    {
        return switch (integerDType.getTypeId()) {
            case INT8 -> Scalar.fromByte((byte) value);
            case INT16 -> Scalar.fromShort((short) value);
            case INT32 -> Scalar.fromInt((int) value);
            case INT64 -> Scalar.fromLong(value);
            default -> throw new IllegalArgumentException("Unsupported integer DType: " + integerDType);
        };
    }

    static long minValue(DType integerDType)
    {
        return switch (integerDType.getTypeId()) {
            case INT8 -> Byte.MIN_VALUE;
            case INT16 -> Short.MIN_VALUE;
            case INT32 -> Integer.MIN_VALUE;
            case INT64 -> Long.MIN_VALUE;
            default -> throw new IllegalArgumentException("Unsupported integer DType: " + integerDType);
        };
    }

    static boolean anyTrue(@Borrow ColumnVector boolColumn)
    {
        try (@Own Scalar any = boolColumn.any()) {
            return any.isValid() && any.getBoolean();
        }
    }

    static boolean allTrue(@Borrow ColumnVector boolColumn)
    {
        try (@Own Scalar all = boolColumn.all()) {
            return all.isValid() && all.getBoolean();
        }
    }
}
