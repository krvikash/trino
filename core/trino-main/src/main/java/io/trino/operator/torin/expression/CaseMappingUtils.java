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
package io.trino.operator.torin.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.trino.spi.TrinoException;
import io.trino.spi.torin.borrow.Borrow;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.torin.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.nio.charset.StandardCharsets.UTF_8;

final class CaseMappingUtils
{
    private CaseMappingUtils() {}

    /// The maximum code point in `char_utf8` form, or 0 when the column is empty or all-null.
    /// Reinterprets the INT32 code points as UINT32 (a zero-copy view over the same buffer) so supplementary
    /// characters, which pack to negative ints, order as the largest values rather than the smallest.
    static int maxCharUtf8(@Borrow ColumnVector column)
    {
        try (ColumnVector characters = column.codePoints();
                ColumnView unsigned = characters.bitCastTo(DType.UINT32);
                Scalar max = unsigned.max()) {
            return max.isValid() ? max.getInt() : 0;
        }
    }

    /// Throws NOT_SUPPORTED with `message` when the column contains any of `rejectedCudfChars` (in `char_utf8` form).
    static void failOnRejectedCodePoints(@Borrow ColumnVector column, int[] rejectedCudfChars, String message)
    {
        try (ColumnVector characters = column.codePoints();
                ColumnVector rejected = ColumnVector.fromInts(rejectedCudfChars);
                ColumnVector matches = characters.contains(rejected)) {
            if (anyTrue(matches)) {
                throw new TrinoException(NOT_SUPPORTED, message);
            }
        }
    }

    static Iterator<Integer> codePointRange(int fromInclusive, int toInclusive)
    {
        return IntStream.rangeClosed(fromInclusive, toInclusive).boxed().iterator();
    }

    /// Pack the code point's UTF-8 bytes big-endian into an int, matching ColumnVector.codePoints().
    static int charUtf8(int codePoint)
    {
        byte[] utf8Bytes = Character.toString(codePoint).getBytes(UTF_8);
        checkArgument(utf8Bytes.length >= 1 && utf8Bytes.length <= 4, "Expected 1 to 4 UTF-8 bytes for code point %s, got %s", codePoint, utf8Bytes.length);
        // reads the UTF-8 bytes as a big-endian positive magnitude
        return new BigInteger(1, utf8Bytes).intValue();
    }
}
