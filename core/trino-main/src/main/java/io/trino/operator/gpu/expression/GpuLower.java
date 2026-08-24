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
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedSet;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Integer.compareUnsigned;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class GpuLower
        extends GpuExpression
{
    /// The uppercase dotted I (İ, U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE).
    ///
    /// cuDF's `lower()` maps İ to two code points, U+0069 (i) followed by U+0307 (combining dot above),
    /// while Trino's `lower()` maps it to the single code point U+0069 (i). Rather than reject the input,
    /// evaluate() replaces İ with i before lowering so the GPU matches Trino. (The 1:N form is Unicode's
    /// default mapping; Trino follows the Turkic single-code-point mapping.)
    private static final String DOTTED_CAPITAL_I = "İ";

    /// The code points whose lowercase mapping differs between cuDF and Trino
    ///(İ handled by [#DOTTED_CAPITAL_I], is reconciled instead and is not included here).
    @VisibleForTesting
    public static final Set<Integer> UNSUPPORTED_CODE_POINTS = buildUnsupportedLowerCodePoints();

    /// [#UNSUPPORTED_CODE_POINTS] in the `char_utf8` form cuDF uses.
    ///
    /// Each code point's UTF-8 bytes are packed big-endian into an int (e.g. U+1C89 -> 0x00E1B289,
    /// U+10400 -> 0xF0909080). This is what ColumnVector.codePoints() emits, so contains() matches them
    /// directly.
    private static final int[] REJECTED_CUDF_CHARS = UNSUPPORTED_CODE_POINTS.stream()
            .mapToInt(GpuLower::charUtf8)
            .toArray();

    private static final int DOTTED_CAPITAL_I_CHAR_UTF8 = charUtf8(DOTTED_CAPITAL_I.codePointAt(0));
    private static final int FIRST_UNSUPPORTED_CHAR_UTF8 = Arrays.stream(REJECTED_CUDF_CHARS).boxed()
            .min(Integer::compareUnsigned)
            .orElseThrow();

    private final GpuExpression argument;

    public GpuLower(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ClosingRef<ColumnVector> input = ClosingRef.own(argument.evaluate(positionCount, inputColumns))) {
            int maxChar = maxCharUtf8(input.borrow());

            if (compareUnsigned(maxChar, DOTTED_CAPITAL_I_CHAR_UTF8) < 0) {
                return input.borrow().lower();
            }

            if (compareUnsigned(maxChar, FIRST_UNSUPPORTED_CHAR_UTF8) >= 0) {
                failOnRejectedCodePoints(input.borrow());
            }

            try (ColumnVector withoutDottedCapitalI = replaceDottedCapitalI(input.borrow())) {
                input.close();
                return withoutDottedCapitalI.lower();
            }
        }
    }

    /// The maximum code point in `char_utf8` form, or 0 when the column is empty or all-null.
    /// Reinterprets the INT32 code points as UINT32 (a zero-copy view over the same buffer) so supplementary
    /// characters, which pack to negative ints, order as the largest values rather than the smallest.
    private static int maxCharUtf8(@Borrow ColumnVector column)
    {
        try (ColumnVector characters = column.codePoints();
                ColumnView unsigned = characters.bitCastTo(DType.UINT32);
                Scalar max = unsigned.max()) {
            return max.isValid() ? max.getInt() : 0;
        }
    }

    /// Replaces the dotted capital I (İ) with i so the subsequent lower() matches Trino. See [#DOTTED_CAPITAL_I]
    private static @Move ColumnVector replaceDottedCapitalI(@Borrow ColumnVector input)
    {
        try (Scalar dottedCapitalI = Scalar.fromString(DOTTED_CAPITAL_I);
                Scalar dotlessLowercaseI = Scalar.fromString("i")) {
            return input.stringReplace(dottedCapitalI, dotlessLowercaseI);
        }
    }

    private static void failOnRejectedCodePoints(ColumnVector column)
    {
        try (ColumnVector characters = column.codePoints();
                ColumnVector rejected = ColumnVector.fromInts(REJECTED_CUDF_CHARS);
                ColumnVector matches = characters.contains(rejected)) {
            if (anyTrue(matches)) {
                throw new TrinoException(NOT_SUPPORTED, "lower() on GPU does not support code points whose lowercase mapping differs between cuDF and Trino");
            }
        }
    }

    private static Set<Integer> buildUnsupportedLowerCodePoints()
    {
        return ImmutableSortedSet.<Integer>naturalOrder()
                // BMP letters cuDF leaves unchanged.
                .add(0x1C89, 0x2C2F, 0xA7C0, 0xA7C7, 0xA7C9, 0xA7CB, 0xA7CC, 0xA7D0, 0xA7D6, 0xA7D8, 0xA7DA, 0xA7DC, 0xA7F5)
                // Deseret (U+10400–U+10427)
                .addAll(codePointRange(0x10400, 0x10427))
                // Osage (U+104B0–U+104D3)
                .addAll(codePointRange(0x104B0, 0x104D3))
                // Vithkuqi (U+10570–U+10595, with unassigned code points at U+1057B, U+1058B and U+10593)
                .addAll(codePointRange(0x10570, 0x1057A))
                .addAll(codePointRange(0x1057C, 0x1058A))
                .addAll(codePointRange(0x1058C, 0x10592))
                .addAll(codePointRange(0x10594, 0x10595))
                // Old Hungarian (U+10C80–U+10CB2)
                .addAll(codePointRange(0x10C80, 0x10CB2))
                // Garay (U+10D50–U+10D65)
                .addAll(codePointRange(0x10D50, 0x10D65))
                // Warang Citi (U+118A0–U+118BF)
                .addAll(codePointRange(0x118A0, 0x118BF))
                // Medefaidrin (U+16E40–U+16E5F)
                .addAll(codePointRange(0x16E40, 0x16E5F))
                // Adlam (U+1E900–U+1E921)
                .addAll(codePointRange(0x1E900, 0x1E921))
                .build();
    }

    private static Iterator<Integer> codePointRange(int fromInclusive, int toInclusive)
    {
        return IntStream.rangeClosed(fromInclusive, toInclusive).boxed().iterator();
    }

    /// Pack the code point's UTF-8 bytes big-endian into an int, matching ColumnVector.codePoints().
    private static int charUtf8(int codePoint)
    {
        byte[] utf8Bytes = Character.toString(codePoint).getBytes(UTF_8);
        checkArgument(utf8Bytes.length >= 1 && utf8Bytes.length <= 4, "Expected 1 to 4 UTF-8 bytes for code point %s, got %s", codePoint, utf8Bytes.length);
        // reads the UTF-8 bytes as a big-endian positive magnitude
        return new BigInteger(1, utf8Bytes).intValue();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuLower other && argument.equals(other.argument);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument);
    }
}
