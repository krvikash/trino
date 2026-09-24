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
package io.trino.sql.analyzer;

import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.DateTimeUtils.parseDate;
import static org.assertj.core.api.Assertions.assertThat;

class TestIncrementalColumnWatermarkLiteral
{
    private static final NodeLocation LOCATION = new NodeLocation(1, 1);

    @Test
    void testIntegerAndBigint()
    {
        assertThat(literalValue(INTEGER, 42L)).isEqualTo("42");
        assertThat(literalValue(INTEGER, (long) Integer.MIN_VALUE)).isEqualTo("-2147483648");
        assertThat(literalValue(BIGINT, 9_000_000_000L)).isEqualTo("9000000000");
        assertThat(literalValue(BIGINT, Long.MIN_VALUE)).isEqualTo("-9223372036854775808");
    }

    @Test
    void testDate()
    {
        assertThat(genericLiteral(DATE, epochDay("2026-04-01"))).isEqualTo(new TypeAndValue("DATE", "2026-04-01"));
        assertThat(genericLiteral(DATE, epochDay("1965-06-14"))).isEqualTo(new TypeAndValue("DATE", "1965-06-14"));
        assertThat(genericLiteral(DATE, LocalDate.of(0, 1, 1).toEpochDay())).isEqualTo(new TypeAndValue("DATE", "0000-01-01"));
        assertThat(genericLiteral(DATE, epochDay("9999-12-31"))).isEqualTo(new TypeAndValue("DATE", "9999-12-31"));
    }

    /**
     * {@link SqlDate#toString()} renders five-digit and negative years in extended ISO form
     * ({@code +10000-01-01}, {@code -0001-12-31}), which misses {@code parseDate}'s fixed-width
     * fast path. Its Joda fallback still parses them, so no watermark is declined over its year.
     */
    @Test
    void testRenderedDateLiteralRoundTripsThroughAnalyzerParsing()
    {
        for (long epochDay : new long[] {
                LocalDate.of(-1, 12, 31).toEpochDay(),
                LocalDate.of(0, 1, 1).toEpochDay(),
                epochDay("1965-06-14"),
                epochDay("1969-12-31"),
                epochDay("1970-01-01"),
                epochDay("2026-04-01"),
                epochDay("9999-12-31"),
                LocalDate.of(10000, 1, 1).toEpochDay(),
        }) {
            Optional<Expression> literal = toLiteral(DATE, epochDay);
            assertThat(literal).isPresent();
            assertThat(parseDate(utf8Slice(((GenericLiteral) literal.get()).getValue()))).isEqualTo(epochDay);
        }
    }

    @Test
    void testDecimal()
    {
        assertThat(genericLiteral(createDecimalType(10, 2), 100L)).isEqualTo(new TypeAndValue("DECIMAL(10,2)", "1.00"));
        assertThat(genericLiteral(createDecimalType(10, 2), -50L)).isEqualTo(new TypeAndValue("DECIMAL(10,2)", "-0.50"));
        assertThat(genericLiteral(createDecimalType(38, 10), Int128.valueOf("1234567890123456781000000000")))
                .isEqualTo(new TypeAndValue("DECIMAL(38,10)", "123456789012345678.1000000000"));
    }

    @Test
    void testTime()
    {
        assertThat(genericLiteral(createTimeType(0), picosOfDay(1, 0, 0, 0))).isEqualTo(new TypeAndValue("TIME", "01:00:00"));
        assertThat(genericLiteral(createTimeType(3), picosOfDay(1, 0, 0, 100_000_000_000L))).isEqualTo(new TypeAndValue("TIME", "01:00:00.100"));
        // Trailing zeroes are padded, never trimmed: the analyzer reads the precision off the text.
        assertThat(genericLiteral(createTimeType(6), picosOfDay(1, 0, 0, 0))).isEqualTo(new TypeAndValue("TIME", "01:00:00.000000"));
    }

    @Test
    void testTimestamp()
    {
        assertThat(genericLiteral(createTimestampType(0), 0L)).isEqualTo(new TypeAndValue("TIMESTAMP", "1970-01-01 00:00:00"));
        assertThat(genericLiteral(createTimestampType(3), 100_000L)).isEqualTo(new TypeAndValue("TIMESTAMP", "1970-01-01 00:00:00.100"));
        assertThat(genericLiteral(createTimestampType(6), 0L)).isEqualTo(new TypeAndValue("TIMESTAMP", "1970-01-01 00:00:00.000000"));
    }

    /**
     * A connector reports its storage precision, which can be finer than the column declares.
     */
    @Test
    void testValueFinerThanDeclaredPrecisionIsDeclined()
    {
        assertThat(toLiteral(createTimeType(0), picosOfDay(1, 0, 0, 100_000_000_000L))).isEmpty();
        assertThat(toLiteral(createTimeType(3), picosOfDay(1, 0, 0, 100_000L))).isEmpty();
        assertThat(toLiteral(createTimestampType(0), 100_000L)).isEmpty();
        assertThat(toLiteral(createTimestampType(3), 1L)).isEmpty();
    }

    /**
     * Above precision 6 a TIMESTAMP carries a {@link LongTimestamp} rather than a {@code Long}, and
     * has no short-timestamp literal here. Iceberg cannot reach this — it caps the incremental
     * column at microseconds — but this class is the only engine-side renderer for the
     * {@code IncrementalColumnWatermark} SPI, so another connector can. It must be declined rather
     * than throw a ClassCastException mid-analysis.
     */
    @Test
    void testLongTimestampIsDeclined()
    {
        assertThat(toLiteral(createTimestampType(9), new LongTimestamp(0, 0))).isEmpty();
        assertThat(toLiteral(createTimestampType(12), new LongTimestamp(1_767_225_600_000_000L, 500_000))).isEmpty();
    }

    /**
     * Iceberg widens decimal precision in place, so a long decimal watermark can meet a short
     * decimal view column, and the other way round.
     */
    @Test
    void testWatermarkOfDifferentTypeIsDeclined()
    {
        assertThat(toLiteral(createDecimalType(10, 2), createDecimalType(20, 2), Int128.valueOf(100))).isEmpty();
        assertThat(toLiteral(createDecimalType(20, 2), createDecimalType(10, 2), 100L)).isEmpty();
        assertThat(toLiteral(createDecimalType(10, 2), createDecimalType(12, 2), 100L)).isEmpty();
        assertThat(toLiteral(createTimestampType(6), DATE, 1L)).isEmpty();
        assertThat(toLiteral(DATE, BIGINT, 1L)).isEmpty();
    }

    @Test
    void testWatermarkOfCompatibleTypeIsRendered()
    {
        assertThat(literalValue(INTEGER, BIGINT, 42L)).isEqualTo("42");
        assertThat(genericLiteral(createTimeType(3), createTimeType(6), picosOfDay(1, 0, 0, 100_000_000_000L))).isEqualTo(new TypeAndValue("TIME", "01:00:00.100"));
        assertThat(genericLiteral(createTimestampType(3), createTimestampType(6), 100_000L)).isEqualTo(new TypeAndValue("TIMESTAMP", "1970-01-01 00:00:00.100"));
    }

    @Test
    void testUnsupportedTypesAreDeclined()
    {
        assertThat(toLiteral(VARCHAR, utf8Slice("a"))).isEmpty();
        assertThat(toLiteral(BOOLEAN, true)).isEmpty();
        assertThat(toLiteral(TIMESTAMP_TZ_MICROS, 0L)).isEmpty();
    }

    private static Optional<Expression> toLiteral(Type type, Object nativeValue)
    {
        return toLiteral(type, type, nativeValue);
    }

    private static Optional<Expression> toLiteral(Type type, Type watermarkType, Object nativeValue)
    {
        return IncrementalColumnWatermarkLiteral.toLiteral(LOCATION, type, watermarkType, nativeValue);
    }

    private static String literalValue(Type type, Object nativeValue)
    {
        return literalValue(type, type, nativeValue);
    }

    private static String literalValue(Type type, Type watermarkType, Object nativeValue)
    {
        Optional<Expression> literal = toLiteral(type, watermarkType, nativeValue);
        assertThat(literal).isPresent();
        return ((LongLiteral) literal.get()).getValue();
    }

    private static TypeAndValue genericLiteral(Type type, Object nativeValue)
    {
        return genericLiteral(type, type, nativeValue);
    }

    private static TypeAndValue genericLiteral(Type type, Type watermarkType, Object nativeValue)
    {
        Optional<Expression> literal = toLiteral(type, watermarkType, nativeValue);
        assertThat(literal).isPresent();
        GenericLiteral genericLiteral = (GenericLiteral) literal.get();
        return new TypeAndValue(genericLiteral.getType(), genericLiteral.getValue());
    }

    private static long epochDay(String date)
    {
        return LocalDate.parse(date).toEpochDay();
    }

    private static long picosOfDay(int hour, int minute, int second, long picosOfSecond)
    {
        return ((hour * 3600L) + (minute * 60L) + second) * 1_000_000_000_000L + picosOfSecond;
    }

    private record TypeAndValue(String type, String value) {}
}
