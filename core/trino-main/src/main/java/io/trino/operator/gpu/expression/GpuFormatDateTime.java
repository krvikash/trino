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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static ai.rapids.cudf.DType.TIMESTAMP_MILLISECONDS;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.gpu.expression.CudfUtils.MAX_CALENDAR_YEAR;
import static io.trino.operator.gpu.expression.CudfUtils.MIN_CALENDAR_YEAR;
import static io.trino.operator.gpu.expression.CudfUtils.checkYearWithinCudfCalendar;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public final class GpuFormatDateTime
        extends GpuExpression
{
    // cuDF's %Y prints only the last 4 digits of the year, and for a negative year %Y and %y leave the bytes they
    // reserved uninitialized, so both need years 0 to 9999
    private static final long YEAR_0_START_EPOCH_MILLIS = LocalDate.of(0, 1, 1).atStartOfDay().toInstant(UTC).toEpochMilli();
    private static final long YEAR_10000_START_EPOCH_MILLIS = LocalDate.of(10000, 1, 1).atStartOfDay().toInstant(UTC).toEpochMilli();
    // %G has the same limits on the year of the ISO week, which is -1 in the first days of year 0
    private static final long YEAR_1_START_EPOCH_MILLIS = LocalDate.of(1, 1, 1).atStartOfDay().toInstant(UTC).toEpochMilli();
    // cuDF computes %V from the year and its neighbors, and the year after the last calendar year overflows
    private static final long FIRST_CALENDAR_YEAR_START_EPOCH_MILLIS = LocalDate.of(MIN_CALENDAR_YEAR, 1, 1).atStartOfDay().toInstant(UTC).toEpochMilli();
    private static final long LAST_CALENDAR_YEAR_START_EPOCH_MILLIS = LocalDate.of(MAX_CALENDAR_YEAR, 1, 1).atStartOfDay().toInstant(UTC).toEpochMilli();

    private final GpuExpression source;
    private final String cudfFormat;
    private final YearRange yearRange;

    /// The years cuDF formats correctly, ordered from the widest range to the narrowest. Each range contains every
    /// narrower one, so for a format with several fields the narrowest of their ranges is enough.
    enum YearRange
    {
        /// Any year. cuDF derives none of the format's fields from the year.
        UNCHECKED,
        /// Years -32767 to 32767, cuDF's calendar, for `%j` (day of year)
        CUDF_CALENDAR,
        /// Years -32767 to 32766, for `%V` (ISO week)
        ISO_WEEK,
        /// Years 0 to 9999, for `%Y` and `%y`
        FOUR_DIGIT,
        /// Years 1 to 9999, for `%G` (year of the ISO week)
        FOUR_DIGIT_WEEK_YEAR,
    }

    private GpuFormatDateTime(GpuExpression source, String cudfFormat, YearRange yearRange)
    {
        this.source = requireNonNull(source, "source is null");
        this.cudfFormat = requireNonNull(cudfFormat, "cudfFormat is null");
        this.yearRange = requireNonNull(yearRange, "yearRange is null");
    }

    public static Optional<GpuFormatDateTime> create(GpuExpression source, String jodaPattern)
    {
        return translateJodaToCudfPattern(jodaPattern).map(translation -> new GpuFormatDateTime(source, translation.cudfFormat(), translation.yearRange()));
    }

    record Translation(String cudfFormat, YearRange yearRange) {}

    static Optional<Translation> translateJodaToCudfPattern(String jodaPattern)
    {
        StringBuilder cudfFormat = new StringBuilder();
        YearRange yearRange = YearRange.UNCHECKED;
        PeekingIterator<Integer> codePoints = Iterators.peekingIterator(jodaPattern.codePoints().iterator());
        while (codePoints.hasNext()) {
            int c = codePoints.next();
            if (c == '\'') {
                if (codePoints.hasNext() && codePoints.peek() == '\'') {
                    codePoints.next();
                    cudfFormat.append('\'');
                }
                else {
                    // Quoted text ends at the next quote that is not doubled
                    while (codePoints.hasNext()) {
                        int quoted = codePoints.next();
                        if (quoted == '\'') {
                            if (!codePoints.hasNext() || codePoints.peek() != '\'') {
                                break;
                            }
                            codePoints.next();
                        }
                        appendLiteral(quoted, cudfFormat);
                    }
                }
            }
            else if (isJodaFieldLetter(c)) {
                int count = 1;
                while (codePoints.hasNext() && codePoints.peek() == c) {
                    codePoints.next();
                    count++;
                }
                String token = Character.toString(c).repeat(count);
                Optional<String> specifier = toCudfSpecifier(token);
                if (specifier.isEmpty()) {
                    return Optional.empty();
                }
                cudfFormat.append(specifier.get());
                YearRange tokenYearRange = switch (token) {
                    case "xxxx" -> YearRange.FOUR_DIGIT_WEEK_YEAR;
                    case "yy", "yyyy" -> YearRange.FOUR_DIGIT;
                    case "ww" -> YearRange.ISO_WEEK;
                    case "DDD" -> YearRange.CUDF_CALENDAR;
                    default -> YearRange.UNCHECKED;
                };
                // The narrowest range covers all the others
                if (tokenYearRange.compareTo(yearRange) > 0) {
                    yearRange = tokenYearRange;
                }
            }
            else {
                appendLiteral(c, cudfFormat);
            }
        }
        if (cudfFormat.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Translation(cudfFormat.toString(), yearRange));
    }

    private static void appendLiteral(int codePoint, StringBuilder cudfFormat)
    {
        // '%' literal needs to be escaped
        if (codePoint == '%') {
            cudfFormat.append('%');
        }
        cudfFormat.appendCodePoint(codePoint);
    }

    /// Joda-Time reads any ASCII letter as a field and fails on the ones it does not know, and copies every other
    /// character unquoted. See `parseToken` and `parsePatternTo` in
    /// [DateTimeFormat](https://github.com/JodaOrg/joda-time/blob/v2.14.3/src/main/java/org/joda/time/format/DateTimeFormat.java).
    private static boolean isJodaFieldLetter(int codePoint)
    {
        return (codePoint >= 'A' && codePoint <= 'Z') || (codePoint >= 'a' && codePoint <= 'z');
    }

    private static Optional<String> toCudfSpecifier(String token)
    {
        String specifier = switch (token) {
            case "yy" -> "%y";
            case "yyyy" -> "%Y";
            case "MM" -> "%m";
            case "dd" -> "%d";
            case "DDD" -> "%j";
            case "HH" -> "%H";
            case "hh" -> "%I";
            case "mm" -> "%M";
            case "ss" -> "%S";
            case "ww" -> "%V";
            case "xxxx" -> "%G";
            case "e" -> "%u";
            case "ee" -> "0%u";
            case "S" -> "%1f";
            case "SS" -> "%2f";
            case "SSS" -> "%3f";
            case "SSSS" -> "%4f";
            case "SSSSS" -> "%5f";
            case "SSSSSS" -> "%6f";
            case "SSSSSSS" -> "%7f";
            case "SSSSSSSS" -> "%8f";
            case "SSSSSSSSS" -> "%9f";
            default -> null;
        };
        return Optional.ofNullable(specifier);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector timestampMillis = source.evaluate(positionCount, inputColumns, bindings)) {
            verify(timestampMillis.getType().equals(TIMESTAMP_MILLISECONDS), "Expected %s input, got %s", TIMESTAMP_MILLISECONDS, timestampMillis.getType());
            switch (yearRange) {
                case UNCHECKED -> {}
                case CUDF_CALENDAR -> checkYearWithinCudfCalendar(timestampMillis);
                case ISO_WEEK -> checkYearsWithin(timestampMillis, FIRST_CALENDAR_YEAR_START_EPOCH_MILLIS, LAST_CALENDAR_YEAR_START_EPOCH_MILLIS);
                case FOUR_DIGIT -> checkYearsWithin(timestampMillis, YEAR_0_START_EPOCH_MILLIS, YEAR_10000_START_EPOCH_MILLIS);
                case FOUR_DIGIT_WEEK_YEAR -> checkYearsWithin(timestampMillis, YEAR_1_START_EPOCH_MILLIS, YEAR_10000_START_EPOCH_MILLIS);
            }
            return timestampMillis.asStrings(cudfFormat);
        }
    }

    private static void checkYearsWithin(@Borrow ColumnVector timestampMillis, long startEpochMillis, long endEpochMillis)
    {
        try (Scalar min = timestampMillis.min();
                Scalar max = timestampMillis.max()) {
            // min and max are invalid only when every value is null so a single check is exhaustive
            if (!min.isValid()) {
                return;
            }
            if (min.getLong() < startEpochMillis || max.getLong() >= endEpochMillis) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "format_datetime() year outside of range supported by GPU");
            }
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        if (yearRange != YearRange.UNCHECKED) {
            return nullSkippingCheck(ImmutableList.of(source));
        }
        return source.fallibility();
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return source.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuFormatDateTime other
                && source.equals(other.source)
                && cudfFormat.equals(other.cudfFormat)
                && yearRange == other.yearRange;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), source, cudfFormat, yearRange);
    }
}
