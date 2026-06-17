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
package io.trino.plugin.elasticsearch.decoders;

import com.google.common.collect.ImmutableList;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.util.List;

import static java.time.format.ResolverStyle.STRICT;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

/**
 * Parses the date/time string representations produced by Elasticsearch into {@link Instant}s.
 * <p>
 * This is a focused reimplementation of the subset of {@code org.elasticsearch.common.time.DateFormatter} that the
 * connector relies on. The named-format definitions below are ported verbatim from {@code DateFormatters} to preserve
 * the exact parsing semantics for the supported formats. Formats not listed here are treated as custom
 * {@link java.time.format.DateTimeFormatter} patterns, matching Elasticsearch's fallback behaviour.
 */
final class ElasticsearchDateFormatter
{
    @FunctionalInterface
    private interface DateTimeParser
    {
        // Throws when the value cannot be parsed with this parser; callers fall through to the next parser.
        Instant parse(String value);
    }

    private final String pattern;
    private final List<DateTimeParser> parsers;

    static ElasticsearchDateFormatter forPattern(String pattern)
    {
        requireNonNull(pattern, "pattern is null");
        ImmutableList.Builder<DateTimeParser> parsers = ImmutableList.builder();
        for (String part : pattern.split("\\|\\|")) {
            if (part.isEmpty()) {
                throw new IllegalArgumentException("Cannot have empty element in multi date format pattern: " + pattern);
            }
            parsers.addAll(parsersFor(part));
        }
        return new ElasticsearchDateFormatter(pattern, parsers.build());
    }

    private ElasticsearchDateFormatter(String pattern, List<DateTimeParser> parsers)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.parsers = requireNonNull(parsers, "parsers is null");
    }

    public long parseMillis(String value)
    {
        return parseInstant(value).toEpochMilli();
    }

    public Instant parseInstant(String value)
    {
        for (DateTimeParser parser : parsers) {
            try {
                return parser.parse(value);
            }
            catch (RuntimeException _) {
                // try the next parser
            }
        }
        throw new IllegalArgumentException("Failed to parse date field [" + value + "] with format [" + pattern + "]");
    }

    private static List<DateTimeParser> parsersFor(String pattern)
    {
        return switch (pattern) {
            case "epoch_millis" -> ImmutableList.of(ElasticsearchDateFormatter::parseEpochMillis);
            case "epoch_second" -> ImmutableList.of(ElasticsearchDateFormatter::parseEpochSecond);
            case "strict_date_optional_time" -> formatters(STRICT_DATE_OPTIONAL_TIME_FORMATTER);
            case "strict_date_optional_time_nanos" -> formatters(STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS);
            case "date_optional_time" -> formatters(DATE_OPTIONAL_TIME_FORMATTER);
            case "strict_date_hour_minute_second" -> formatters(STRICT_DATE_HOUR_MINUTE_SECOND_FORMATTER);
            case "date_hour_minute_second_millis" -> formatters(DATE_HOUR_MINUTE_SECOND_MILLIS_FORMATTER);
            case "year_month_day" -> formatters(YEAR_MONTH_DAY_FORMATTER);
            case "basic_date" -> formatters(BASIC_DATE_FORMATTER);
            case "basic_date_time" -> formatters(BASIC_DATE_TIME_FORMATTER, BASIC_DATE_TIME_NO_COLON_FORMATTER);
            case "basic_date_time_no_millis" -> formatters(BASIC_DATE_TIME_NO_MILLIS_FORMATTER, BASIC_DATE_TIME_NO_MILLIS_NO_COLON_FORMATTER);
            case "weekyear_week_day" -> formatters(WEEKYEAR_WEEK_DAY_FORMATTER);
            case "week_date" -> formatters(WEEK_DATE_FORMATTER);
            case "t_time" -> formatters(T_TIME_FORMATTER, T_TIME_NO_COLON_FORMATTER);
            default -> formatters(custom(pattern));
        };
    }

    private static List<DateTimeParser> formatters(DateTimeFormatter... formatters)
    {
        ImmutableList.Builder<DateTimeParser> parsers = ImmutableList.builder();
        for (DateTimeFormatter formatter : formatters) {
            parsers.add(value -> toInstant(formatter.parse(value)));
        }
        return parsers.build();
    }

    private static DateTimeFormatter custom(String pattern)
    {
        try {
            return new DateTimeFormatterBuilder().appendPattern(pattern).toFormatter(ROOT).withResolverStyle(STRICT);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid format: [" + pattern + "]: " + e.getMessage(), e);
        }
    }

    private static Instant parseEpochMillis(String value)
    {
        int dot = value.indexOf('.');
        if (dot < 0) {
            return Instant.ofEpochMilli(Long.parseLong(value));
        }
        long millis = Long.parseLong(value.substring(0, dot));
        // The fractional part is the sub-millisecond component; up to 6 digits express the nanoseconds within the millisecond.
        return Instant.ofEpochMilli(millis).plusNanos(parseFractionalNanos(value.substring(dot + 1), 6));
    }

    private static Instant parseEpochSecond(String value)
    {
        int dot = value.indexOf('.');
        if (dot < 0) {
            return Instant.ofEpochSecond(Long.parseLong(value));
        }
        long seconds = Long.parseLong(value.substring(0, dot));
        // The fractional part is the sub-second component; up to 9 digits express the nanoseconds within the second.
        return Instant.ofEpochSecond(seconds, parseFractionalNanos(value.substring(dot + 1), 9));
    }

    /**
     * Interprets {@code fraction} as a fractional value with {@code maxDigits} digits of resolution and returns it as a
     * nanosecond count. {@code maxDigits} is 9 for a fraction of a second and 6 for a fraction of a millisecond, so
     * right-padding the digits to {@code maxDigits} always yields nanoseconds.
     */
    private static long parseFractionalNanos(String fraction, int maxDigits)
    {
        if (fraction.isEmpty() || fraction.length() > maxDigits) {
            throw new NumberFormatException("Invalid fractional part: " + fraction);
        }
        return Long.parseLong(fraction + "0".repeat(maxDigits - fraction.length()));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Conversion of a parsed accessor to a UTC-defaulted instant, ported from DateFormatters#from.
    // ------------------------------------------------------------------------------------------------------------

    private static final LocalDate LOCALDATE_EPOCH = LocalDate.of(1970, 1, 1);

    /**
     * Extends {@link TemporalQueries#localDate()} to also build local dates when {@code YEAR_OF_ERA} was parsed instead
     * of {@code YEAR}, retaining Elasticsearch (Joda) behaviour for patterns such as {@code yyyy-MM-dd}.
     */
    private static final TemporalQuery<LocalDate> LOCAL_DATE_QUERY = temporal -> {
        if (temporal.isSupported(ChronoField.EPOCH_DAY)) {
            return LocalDate.ofEpochDay(temporal.getLong(ChronoField.EPOCH_DAY));
        }
        if (temporal.isSupported(ChronoField.YEAR_OF_ERA) || temporal.isSupported(ChronoField.YEAR)) {
            int year = getYear(temporal);
            if (temporal.isSupported(MONTH_OF_YEAR) && temporal.isSupported(DAY_OF_MONTH)) {
                return LocalDate.of(year, temporal.get(MONTH_OF_YEAR), temporal.get(DAY_OF_MONTH));
            }
            if (temporal.isSupported(ChronoField.DAY_OF_YEAR)) {
                return LocalDate.ofYearDay(year, temporal.get(ChronoField.DAY_OF_YEAR));
            }
        }
        return null;
    };

    private static Instant toInstant(TemporalAccessor accessor)
    {
        if (accessor instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.toInstant();
        }

        ZoneId zoneId = accessor.query(TemporalQueries.zone());
        if (zoneId == null) {
            zoneId = ZoneOffset.UTC;
        }

        LocalDate localDate = accessor.query(LOCAL_DATE_QUERY);
        LocalTime localTime = accessor.query(TemporalQueries.localTime());

        if (localDate != null && localTime != null) {
            return ZonedDateTime.of(localDate, localTime, zoneId).toInstant();
        }
        if (accessor.isSupported(ChronoField.INSTANT_SECONDS) && accessor.isSupported(NANO_OF_SECOND)) {
            return Instant.from(accessor);
        }
        if (localDate != null) {
            return localDate.atStartOfDay(zoneId).toInstant();
        }
        if (localTime != null) {
            return ZonedDateTime.of(getLocalDate(accessor), localTime, zoneId).toInstant();
        }
        if (accessor.isSupported(ChronoField.YEAR) || accessor.isSupported(ChronoField.YEAR_OF_ERA)) {
            int year = getYear(accessor);
            if (accessor.isSupported(MONTH_OF_YEAR)) {
                return LocalDate.of(year, accessor.get(MONTH_OF_YEAR), 1).atStartOfDay(zoneId).toInstant();
            }
            return Year.of(year).atDay(1).atStartOfDay(zoneId).toInstant();
        }
        if (accessor.isSupported(MONTH_OF_YEAR)) {
            return getLocalDate(accessor).atStartOfDay(zoneId).toInstant();
        }

        throw new IllegalArgumentException("Temporal accessor [" + accessor + "] cannot be converted to an instant");
    }

    private static LocalDate getLocalDate(TemporalAccessor accessor)
    {
        if (accessor.isSupported(MONTH_OF_YEAR)) {
            int year = getYear(accessor);
            if (accessor.isSupported(DAY_OF_MONTH)) {
                return LocalDate.of(year, accessor.get(MONTH_OF_YEAR), accessor.get(DAY_OF_MONTH));
            }
            return LocalDate.of(year, accessor.get(MONTH_OF_YEAR), 1);
        }
        return LOCALDATE_EPOCH;
    }

    private static int getYear(TemporalAccessor accessor)
    {
        if (accessor.isSupported(ChronoField.YEAR)) {
            return accessor.get(ChronoField.YEAR);
        }
        if (accessor.isSupported(ChronoField.YEAR_OF_ERA)) {
            return accessor.get(ChronoField.YEAR_OF_ERA);
        }
        return 1970;
    }

    // ------------------------------------------------------------------------------------------------------------
    // Named-format definitions, ported verbatim from org.elasticsearch.common.time.DateFormatters.
    // Only the parsing formatters (not the printers) are reproduced, since this class never formats.
    // ------------------------------------------------------------------------------------------------------------

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_NO_COLON = new DateTimeFormatterBuilder()
            .appendOffset("+HHmm", "Z")
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter STRICT_YEAR_MONTH_DAY_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
            .optionalStart()
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalEnd()
            .optionalEnd()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .optionalStart()
            .appendLiteral('T')
            .optionalStart()
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(',')
            .appendFraction(NANO_OF_SECOND, 1, 9, false)
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS = new DateTimeFormatterBuilder()
            .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .optionalStart()
            .appendLiteral('T')
            .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(',')
            .appendFraction(NANO_OF_SECOND, 1, 9, false)
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .optionalEnd()
            .optionalEnd()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 1, 9, SignStyle.NORMAL)
            .optionalStart()
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalEnd()
            .optionalEnd()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .optionalStart()
            .appendLiteral('T')
            .optionalStart()
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalEnd()
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(',')
            .appendFraction(NANO_OF_SECOND, 1, 9, false)
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HHmm", "Z")
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter STRICT_DATE_HOUR_MINUTE_SECOND_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss", ROOT);

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendFraction(NANO_OF_SECOND, 1, 3, true)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter DATE_HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral("T")
            .append(HOUR_MINUTE_SECOND_MILLIS_FORMATTER)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter YEAR_MONTH_DAY_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR)
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR)
            .appendLiteral("-")
            .appendValue(DAY_OF_MONTH)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 1, 4, SignStyle.NORMAL)
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT)
            .withZone(ZoneOffset.UTC);

    private static final DateTimeFormatter BASIC_YEAR_MONTH_DAY_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL)
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_T_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendLiteral("T")
            .append(BASIC_TIME_FORMATTER)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_DATE_TIME_BASE = new DateTimeFormatterBuilder()
            .append(BASIC_YEAR_MONTH_DAY_FORMATTER)
            .append(BASIC_T_TIME_FORMATTER)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_TIME_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_DATE_TIME_NO_COLON_FORMATTER = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_TIME_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_DATE_T_FORMATTER = new DateTimeFormatterBuilder()
            .append(BASIC_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_DATE_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_T_FORMATTER)
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter BASIC_DATE_TIME_NO_MILLIS_NO_COLON_FORMATTER = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_T_FORMATTER)
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter WEEKYEAR_WEEK_DAY_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(IsoFields.WEEK_BASED_YEAR)
            .appendLiteral("-W")
            .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
            .appendLiteral("-")
            .appendValue(DAY_OF_WEEK)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral("-W")
            .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral('-')
            .appendValue(DAY_OF_WEEK, 1)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter TIME_PREFIX = new DateTimeFormatterBuilder()
            .append(TIME_NO_MILLIS_FORMATTER)
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter T_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendLiteral("T")
            .append(TIME_PREFIX)
            .appendZoneOrOffsetId()
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);

    private static final DateTimeFormatter T_TIME_NO_COLON_FORMATTER = new DateTimeFormatterBuilder()
            .appendLiteral("T")
            .append(TIME_PREFIX)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(ROOT)
            .withResolverStyle(STRICT);
}
