/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimeZoneNotSupportedException;
import io.trino.spi.type.UuidType;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.util.Base64;
import java.util.UUID;

import static com.fasterxml.jackson.databind.node.JsonNodeType.STRING;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

public final class StringColumnWriters
{
    public static final ColumnWriter BYTE_COLUMN_WRITER = new AbstractColumnWriter(VARBINARY, STRING)
    {
        @Override
        public String toString()
        {
            return "BYTE_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            final byte[] bytes;
            try {
                bytes = Base64.getDecoder().decode(node.textValue());
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "JSON STRING did not contain valid base64 encoded data",
                        e);
            }
            VARBINARY.writeSlice(
                    blockBuilder,
                    Slices.wrappedBuffer(bytes));
        }
    };

    public static final ColumnWriter UUID_COLUMN_WRITER = new AbstractColumnWriter(UuidType.UUID, STRING)
    {
        @Override
        public String toString()
        {
            return "UUID_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            final UUID uuid;
            try {
                uuid = UUID.fromString(node.textValue());
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "String was not a valid UUID",
                        e);
            }
            UuidType.UUID.writeSlice(blockBuilder, javaUuidToTrinoUuid(uuid));
        }
    };

    public static final ColumnWriter DATE_COLUMN_WRITER = new AbstractColumnWriter(DATE, STRING)
    {
        @Override
        public String toString()
        {
            return "DATE_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            try {
                DATE.writeLong(blockBuilder, LocalDate.parse(node.textValue(), RFC_3339_DATE).toEpochDay());
            }
            catch (DateTimeParseException e) {
                throw new TrinoException(OPENAPI_UNEXPECTED_RESPONSE_SCHEMA, "String %s was not a valid date".formatted(node.textValue()), e);
            }
        }
    };

    public static final ColumnWriter DATE_TIME_COLUMN_WRITER = new AbstractColumnWriter(TIMESTAMP_TZ_PICOS, STRING)
    {
        @Override
        public String toString()
        {
            return "DATE_TIME_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            // RFC 3339 allows unlimited fractional second digits; truncate to picosecond precision (12 digits).
            // Java DateTimeFormatter handles at most 9 digits (nanoseconds), so digits 10–12 are extracted manually.
            final String text = node.textValue();
            final int dotIndex = text.indexOf('.');
            final String parsableText;
            final long picosOfMilli;
            if (dotIndex >= 0) {
                int fractionEnd = dotIndex + 1;
                while (fractionEnd < text.length() && Character.isDigit(text.charAt(fractionEnd))) {
                    fractionEnd++;
                }
                String fractionText = text.substring(dotIndex + 1, fractionEnd);

                // NORMALIZE: Pad with zeros to exactly 12 digits
                // If input is .000123, padded becomes "000123000000"
                String paddedFraction = (fractionText + "000000000000").substring(0, 12);

                // Now parsing is safe because the position is fixed
                long totalPicos = Long.parseLong(paddedFraction);
                picosOfMilli = totalPicos % 1_000_000_000L;

                parsableText = text.substring(0, dotIndex + 1) + paddedFraction.substring(0, 9) + text.substring(fractionEnd);
            }
            else {
                parsableText = text;
                picosOfMilli = 0;
            }

            OffsetDateTime dateTime;
            try {
                dateTime = OffsetDateTime.parse(parsableText, RFC_3339_NANO_FORMATTER);
            }
            catch (DateTimeParseException e) {
                throw new TrinoException(OPENAPI_UNEXPECTED_RESPONSE_SCHEMA, "The provided string is not in a valid RFC 3339 format: " + node.textValue(), e);
            }

            TimeZoneKey timeZoneKey;
            try {
                timeZoneKey = TimeZoneKey.getTimeZoneKey(dateTime.getOffset().getId());
            }
            catch (TimeZoneNotSupportedException e) {
                throw new TrinoException(OPENAPI_UNEXPECTED_RESPONSE_SCHEMA, "The time zone offset in '%s' is not supported".formatted(node.textValue()), e);
            }

            long epochMillis = dateTime.toInstant().toEpochMilli();
            TIMESTAMP_TZ_PICOS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, (int) picosOfMilli, timeZoneKey));
        }
    };

    public static final DateTimeFormatter RFC_3339_DATE = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 4, 4, SignStyle.NOT_NEGATIVE)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
            .withChronology(IsoChronology.INSTANCE);

    private static final DateTimeFormatter RFC_3339_TIME = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    public static final DateTimeFormatter RFC_3339_NANO_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(RFC_3339_DATE)
            .appendLiteral('T')
            .append(RFC_3339_TIME)
            .parseLenient()
            .appendOffset("+HH:mm", "Z")
            .parseStrict()
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
            .withChronology(IsoChronology.INSTANCE);

    private StringColumnWriters() {}
}
