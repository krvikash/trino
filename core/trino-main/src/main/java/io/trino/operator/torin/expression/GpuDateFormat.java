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

import io.trino.operator.torin.expression.GpuFormatDateTime.Translation;
import io.trino.operator.torin.expression.GpuFormatDateTime.YearRange;

import java.util.Optional;
import java.util.PrimitiveIterator;

import static io.trino.operator.torin.expression.GpuFormatDateTime.appendLiteral;

public final class GpuDateFormat
{
    private GpuDateFormat() {}

    /// `timestampMillis` is the value rounded to `TIMESTAMP_MILLISECONDS`, as the CPU rounds it before formatting.
    public static Optional<GpuFormatDateTime> create(GpuExpression timestampMillis, String mySqlFormat)
    {
        return translateMySqlToCudfFormat(mySqlFormat).map(translation -> new GpuFormatDateTime(timestampMillis, translation.cudfFormat(), translation.yearRange()));
    }

    static Optional<Translation> translateMySqlToCudfFormat(String mySqlFormat)
    {
        StringBuilder cudfFormat = new StringBuilder();
        YearRange yearRange = YearRange.UNCHECKED;
        PrimitiveIterator.OfInt codePoints = mySqlFormat.codePoints().iterator();
        while (codePoints.hasNext()) {
            int c = codePoints.nextInt();
            if (c != '%') {
                appendLiteral(c, cudfFormat);
                continue;
            }
            // The CPU drops a trailing '%'
            if (!codePoints.hasNext()) {
                break;
            }
            int specifier = codePoints.nextInt();
            // Text fields (names, AM/PM) depend on the session locale, and unpadded fields have no cuDF counterpart.
            // Any other character, which the CPU prints as is or rejects today, may become a specifier on the CPU.
            String cudfSpecifier = switch (specifier) {
                case 'd' -> "%d";
                case 'f' -> "%6f";
                case 'H' -> "%H";
                case 'h', 'I' -> "%I";
                case 'i' -> "%M";
                case 'j' -> "%j";
                case 'm' -> "%m";
                case 'S', 's' -> "%S";
                case 'T' -> "%H:%M:%S";
                case 'v' -> "%V";
                case 'x' -> "%G";
                case 'Y' -> "%Y";
                case 'y' -> "%y";
                case '%' -> "%%";
                default -> null;
            };
            if (cudfSpecifier == null) {
                return Optional.empty();
            }
            cudfFormat.append(cudfSpecifier);
            yearRange = yearRange.narrowest(switch (specifier) {
                case 'x' -> YearRange.FOUR_DIGIT_WEEK_YEAR;
                case 'Y', 'y' -> YearRange.FOUR_DIGIT;
                case 'v' -> YearRange.ISO_WEEK;
                case 'j' -> YearRange.CUDF_CALENDAR;
                default -> YearRange.UNCHECKED;
            });
        }
        // The CPU rejects a format that prints nothing, and so does cuDF
        if (cudfFormat.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Translation(cudfFormat.toString(), yearRange));
    }
}
