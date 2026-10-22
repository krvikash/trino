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
import ai.rapids.cudf.Scalar;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static io.trino.operator.torin.expression.CaseMappingUtils.charUtf8;
import static io.trino.operator.torin.expression.CaseMappingUtils.codePointRange;
import static io.trino.operator.torin.expression.CaseMappingUtils.failOnRejectedCodePoints;
import static io.trino.operator.torin.expression.CaseMappingUtils.maxCharUtf8;
import static io.trino.operator.torin.expression.GpuExpressions.nullSkippingCheck;
import static java.lang.Integer.compareUnsigned;
import static java.util.Objects.requireNonNull;

public final class GpuUpper
        extends GpuExpression
{
    /// The sharp S (ß, U+00DF LATIN SMALL LETTER SHARP S).
    ///
    /// cuDF's `upper()` maps ß to "SS", while Trino's `upper()` leaves it unchanged
    /// TODO: This PR maps the ß to "SS" https://github.com/trinodb/trino/issues/30995
    private static final String SHARP_S = "ß";

    /// Stands in for ß during `upper()`: ƛ (U+019B LATIN SMALL LETTER LAMBDA WITH STROKE, since Unicode 1.1).
    /// It is one of [#UNSUPPORTED_CODE_POINTS], so inputs containing it are rejected before the replacement, and it cannot
    /// be in the string. cuDF's `upper()` leaves it unchanged, so swapping it back restores only former ß.
    private static final String SHARP_S_PLACEHOLDER = "ƛ";

    @VisibleForTesting
    public static final Set<Integer> UNSUPPORTED_CODE_POINTS = ImmutableSortedSet.<Integer>naturalOrder()
            .addAll(remappedCodePoints())
            .addAll(expandedCodePoints())
            .build();

    private static final int[] REJECTED_CUDF_CHARS = UNSUPPORTED_CODE_POINTS.stream()
            .mapToInt(CaseMappingUtils::charUtf8)
            .toArray();

    private static final int SHARP_S_CHAR_UTF8 = charUtf8(SHARP_S.codePointAt(0));
    private static final int FIRST_UNSUPPORTED_CHAR_UTF8 = Arrays.stream(REJECTED_CUDF_CHARS).boxed()
            .min(Integer::compareUnsigned)
            .orElseThrow();

    private final GpuExpression argument;

    public GpuUpper(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ClosingRef<ColumnVector> input = ClosingRef.own(argument.evaluate(positionCount, inputColumns, bindings))) {
            int maxChar = maxCharUtf8(input.borrow());

            if (compareUnsigned(maxChar, SHARP_S_CHAR_UTF8) < 0) {
                return input.borrow().upper();
            }

            if (compareUnsigned(maxChar, FIRST_UNSUPPORTED_CHAR_UTF8) >= 0) {
                failOnRejectedCodePoints(input.borrow(), REJECTED_CUDF_CHARS, "upper() on GPU does not support code points whose uppercase mapping differs between cuDF and Trino");
            }

            // The SHARP_S_PLACEHOLDER is not in the input, because it's a part of UNSUPPORTED_CODE_POINTS
            try (Scalar sharpS = Scalar.fromString(SHARP_S);
                    Scalar sharpSPlaceholder = Scalar.fromString(SHARP_S_PLACEHOLDER);
                    ColumnVector withPlaceholder = input.borrow().stringReplace(sharpS, sharpSPlaceholder)) {
                input.close();
                try (ColumnVector upper = withPlaceholder.upper()) {
                    return upper.stringReplace(sharpSPlaceholder, sharpS);
                }
            }
        }
    }

    /// Code points cuDF maps to a different single code point than Trino, mostly leaving them unchanged.
    private static Set<Integer> remappedCodePoints()
    {
        return ImmutableSortedSet.<Integer>naturalOrder()
                // BMP letters cuDF leaves unchanged.
                .add(0x019B, 0x0264, 0x1C8A, 0x2C5F, 0xA7C1, 0xA7C8, 0xA7CA, 0xA7CD, 0xA7D1, 0xA7D7, 0xA7D9, 0xA7DB, 0xA7F6)
                // Unassigned code points cuDF maps to U+0000.
                .addAll(codePointRange(0xFB09, 0xFB0D))
                // Deseret (U+10428–U+1044F)
                .addAll(codePointRange(0x10428, 0x1044F))
                // Osage (U+104D8–U+104FB)
                .addAll(codePointRange(0x104D8, 0x104FB))
                // Vithkuqi (U+10597–U+105BC, with unassigned code points at U+105A2, U+105B2 and U+105BA)
                .addAll(codePointRange(0x10597, 0x105A1))
                .addAll(codePointRange(0x105A3, 0x105B1))
                .addAll(codePointRange(0x105B3, 0x105B9))
                .addAll(codePointRange(0x105BB, 0x105BC))
                // Old Hungarian (U+10CC0–U+10CF2)
                .addAll(codePointRange(0x10CC0, 0x10CF2))
                // Garay (U+10D70–U+10D85)
                .addAll(codePointRange(0x10D70, 0x10D85))
                // Warang Citi (U+118C0–U+118DF)
                .addAll(codePointRange(0x118C0, 0x118DF))
                // Medefaidrin (U+16E60–U+16E7F)
                .addAll(codePointRange(0x16E60, 0x16E7F))
                // Adlam (U+1E922–U+1E943)
                .addAll(codePointRange(0x1E922, 0x1E943))
                .build();
    }

    /// Code points cuDF expands to several code points (Unicode's full case mapping), which Trino leaves unchanged
    /// or maps to a single code point.
    /// TODO: This PR adds support for these cases, aligning trino with the cudf approach (https://github.com/trinodb/trino/issues/30995)
    private static Set<Integer> expandedCodePoints()
    {
        return ImmutableSortedSet.<Integer>naturalOrder()
                .add(0x0149, 0x01F0, 0x0390, 0x03B0, 0x0587, 0x1F50, 0x1F52, 0x1F54, 0x1F56, 0x1FBC, 0x1FCC, 0x1FFC)
                .addAll(codePointRange(0x1E96, 0x1E9A))
                .addAll(codePointRange(0x1F80, 0x1FAF))
                .addAll(codePointRange(0x1FB2, 0x1FB4))
                .addAll(codePointRange(0x1FB6, 0x1FB7))
                .addAll(codePointRange(0x1FC2, 0x1FC4))
                .addAll(codePointRange(0x1FC6, 0x1FC7))
                .addAll(codePointRange(0x1FD2, 0x1FD3))
                .addAll(codePointRange(0x1FD6, 0x1FD7))
                .addAll(codePointRange(0x1FE2, 0x1FE4))
                .addAll(codePointRange(0x1FE6, 0x1FE7))
                .addAll(codePointRange(0x1FF2, 0x1FF4))
                .addAll(codePointRange(0x1FF6, 0x1FF7))
                .addAll(codePointRange(0xFB00, 0xFB06))
                .addAll(codePointRange(0xFB13, 0xFB17))
                .build();
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(ImmutableList.of(argument));
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuUpper other && argument.equals(other.argument);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument);
    }
}
