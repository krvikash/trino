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
package io.trino.operator.gpu;

import ai.rapids.cudf.DType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

public final class GpuTypes
{
    private GpuTypes() {}

    // Keep in sync with CopyToDevice#copyToDevice
    // TODO: Consider unifying type mapping in a single place: https://starburstdata.atlassian.net/browse/ENG-10144
    public static DType toDType(Type type)
    {
        if (type == BOOLEAN) {
            return DType.BOOL8;
        }
        if (type == TINYINT) {
            return DType.INT8;
        }
        if (type == SMALLINT) {
            return DType.INT16;
        }
        if (type == INTEGER) {
            return DType.INT32;
        }
        if (type == BIGINT) {
            return DType.INT64;
        }
        if (type == REAL) {
            return DType.FLOAT32;
        }
        if (type == DOUBLE) {
            return DType.FLOAT64;
        }
        if (type instanceof VarcharType) {
            return DType.STRING;
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
