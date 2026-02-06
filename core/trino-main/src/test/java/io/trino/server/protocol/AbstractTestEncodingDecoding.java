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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.Row;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Fixed12BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.client.IntervalDayTime.toMillis;
import static io.trino.client.IntervalYearMonth.toMonths;
import static io.trino.server.protocol.AbstractTestEncodingDecoding.TypedColumn.typed;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MICROS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_NANOS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_SECONDS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class AbstractTestEncodingDecoding
{
    protected abstract QueryDataDecoder createDecoder(List<Column> columns);

    protected abstract QueryDataEncoder createEncoder(List<OutputColumn> columns);

    QueryDataEncoder newEncoder(List<TypedColumn> types)
    {
        ImmutableList.Builder<OutputColumn> columns = ImmutableList.builderWithExpectedSize(types.size());
        for (int i = 0; i < types.size(); i++) {
            TypedColumn typedColumn = types.get(i);
            columns.add(new OutputColumn(i, typedColumn.name(), typedColumn.type()));
        }
        return createEncoder(columns.build());
    }

    QueryDataDecoder newDecoder(List<TypedColumn> types)
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builderWithExpectedSize(types.size());
        for (TypedColumn typedColumn : types) {
            columns.add(createColumn(typedColumn.name(), typedColumn.type(), true, true));
        }
        return createDecoder(columns.build());
    }

    @Test
    public void testBigintSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            BIGINT.writeLong(blockBuilder, 1L);
            BIGINT.writeLong(blockBuilder, 2L);
            BIGINT.writeLong(blockBuilder, 3L);
            BIGINT.writeLong(blockBuilder, 4L);
            blockBuilder.appendNull();
        };

        assertRoundTrip(BIGINT, builder, 1L, 2L, 3L, 4L, null);
    }

    @Test
    public void testIntegerSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            INTEGER.writeInt(blockBuilder, 1);
            INTEGER.writeInt(blockBuilder, 1337);
            INTEGER.writeInt(blockBuilder, -10);
            INTEGER.writeInt(blockBuilder, Integer.MAX_VALUE);
            INTEGER.writeInt(blockBuilder, Integer.MIN_VALUE);
            blockBuilder.appendNull();
        };

        assertRoundTrip(INTEGER, builder, 1, 1337, -10, Integer.MAX_VALUE, Integer.MIN_VALUE, null);
    }

    @Test
    public void testTinyintSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TINYINT.writeByte(blockBuilder, (byte) 1);
            TINYINT.writeByte(blockBuilder, (byte) -10);
            TINYINT.writeByte(blockBuilder, Byte.MAX_VALUE);
            TINYINT.writeByte(blockBuilder, Byte.MIN_VALUE);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TINYINT, builder, (byte) 1, (byte) -10, Byte.MAX_VALUE, Byte.MIN_VALUE, null);
    }

    @Test
    public void testSmallintSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            SMALLINT.writeShort(blockBuilder, (short) 1);
            SMALLINT.writeShort(blockBuilder, (short) -1337);
            SMALLINT.writeShort(blockBuilder, Short.MAX_VALUE);
            SMALLINT.writeShort(blockBuilder, Short.MIN_VALUE);
            blockBuilder.appendNull();
        };

        assertRoundTrip(SMALLINT, builder, (short) 1, (short) -1337, Short.MAX_VALUE, Short.MIN_VALUE, null);
    }

    @Test
    public void testDoubleSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            DOUBLE.writeDouble(blockBuilder, 1.337);
            DOUBLE.writeDouble(blockBuilder, Double.MIN_VALUE);
            DOUBLE.writeDouble(blockBuilder, Double.NaN);
            DOUBLE.writeDouble(blockBuilder, Double.MAX_VALUE);
            blockBuilder.appendNull();
        };

        assertRoundTrip(DOUBLE, builder, 1.337, Double.MIN_VALUE, Double.NaN, Double.MAX_VALUE, null);
    }

    @Test
    public void testRealSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            REAL.writeFloat(blockBuilder, 1.337f);
            REAL.writeFloat(blockBuilder, Float.MIN_VALUE);
            REAL.writeFloat(blockBuilder, Float.NaN);
            REAL.writeFloat(blockBuilder, Float.MAX_VALUE);
            blockBuilder.appendNull();
        };

        assertRoundTrip(REAL, builder, 1.337f, Float.MIN_VALUE, Float.NaN, Float.MAX_VALUE, null);
    }

    @Test
    public void testShortBigDecimalSerialization()
            throws IOException
    {
        DecimalType decimalType = DecimalType.createDecimalType(10, 5);

        Consumer<BlockBuilder> builder = blockBuilder -> {
            decimalType.writeLong(blockBuilder, encodeShortScaledValue(new BigDecimal("10.00000"), decimalType.getScale()));
            decimalType.writeLong(blockBuilder, encodeShortScaledValue(new BigDecimal("10.00010"), decimalType.getScale()));
            decimalType.writeLong(blockBuilder, encodeShortScaledValue(new BigDecimal("1.00010"), decimalType.getScale()));
            blockBuilder.appendNull();
        };

        assertRoundTrip(decimalType, builder, "10.00000", "10.00010", "1.00010", null);
    }

    @Test
    public void testLongBigDecimalSerialization()
            throws IOException
    {
        DecimalType decimalType = DecimalType.createDecimalType(38, 10);
        Consumer<BlockBuilder> builder = blockBuilder -> {
            decimalType.writeObject(blockBuilder, encodeScaledValue(new BigDecimal("1234567890123456789012.1234567890"), decimalType.getScale()));
            decimalType.writeObject(blockBuilder, encodeScaledValue(new BigDecimal("99999999999999999999.9999999999"), decimalType.getScale()));
            decimalType.writeObject(blockBuilder, encodeScaledValue(new BigDecimal("1000000000000000000000000000.1234000000"), decimalType.getScale()));
            blockBuilder.appendNull();
        };

        assertRoundTrip(decimalType, builder, "1234567890123456789012.1234567890", "99999999999999999999.9999999999", "1000000000000000000000000000.1234000000", null);
    }

    @Test
    public void testDateSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            DATE.writeLong(blockBuilder, LocalDate.EPOCH.toEpochDay());
            DATE.writeLong(blockBuilder, LocalDate.of(2025, 3, 1).toEpochDay());
            DATE.writeLong(blockBuilder, LocalDate.of(1986, 5, 30).toEpochDay());
            blockBuilder.appendNull();
        };

        assertRoundTrip(DATE, builder, "1970-01-01", "2025-03-01", "1986-05-30", null);
    }

    @Test
    public void testIntervalDayTimeSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            INTERVAL_DAY_TIME.writeLong(blockBuilder, toMillis(1, 2, 3, 4, 5));
            INTERVAL_DAY_TIME.writeLong(blockBuilder, toMillis(0, 1, 2, 3, 4));
            INTERVAL_DAY_TIME.writeLong(blockBuilder, toMillis(0, 0, 1, 2, 3));
            INTERVAL_DAY_TIME.writeLong(blockBuilder, toMillis(0, 0, 0, 1, 2));
            INTERVAL_DAY_TIME.writeLong(blockBuilder, toMillis(0, 0, 0, 0, 9));
            blockBuilder.appendNull();
        };

        assertRoundTrip(INTERVAL_DAY_TIME, builder, "1 02:03:04.005", "0 01:02:03.004", "0 00:01:02.003", "0 00:00:01.002", "0 00:00:00.009", null);
    }

    @Test
    public void testIntervalDayMonthSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            INTERVAL_YEAR_MONTH.writeInt(blockBuilder, toMonths(80, 1));
            INTERVAL_YEAR_MONTH.writeInt(blockBuilder, toMonths(1, 1));
            INTERVAL_YEAR_MONTH.writeInt(blockBuilder, toMonths(1, 13));
            INTERVAL_YEAR_MONTH.writeInt(blockBuilder, toMonths(99, 9));
            INTERVAL_YEAR_MONTH.writeInt(blockBuilder, toMonths(1, 0));
            blockBuilder.appendNull();
        };

        assertRoundTrip(INTERVAL_YEAR_MONTH, builder, "80-1", "1-1", "2-1", "99-9", "1-0", null);
    }

    @Test
    public void testVarcharSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            VARCHAR.writeSlice(blockBuilder, utf8Slice("ala ma kota 🐈"));
            VARCHAR.writeSlice(blockBuilder, utf8Slice("数据应用"));
            VARCHAR.writeSlice(blockBuilder, utf8Slice("\"quoted\""));
            VARCHAR.writeSlice(blockBuilder, utf8Slice("zażółć gęślą jaźń"));
            VARCHAR.writeSlice(blockBuilder, utf8Slice("\0\0\0")); // garbage in, garbage out
            VARCHAR.writeSlice(blockBuilder, utf8Slice("\r\t\n"));
            VARCHAR.writeSlice(blockBuilder, utf8Slice("\uD83E\uDD83"));
            blockBuilder.appendNull();
        };

        assertRoundTrip(VARCHAR, builder,
                "ala ma kota 🐈",
                "数据应用",
                "\"quoted\"",
                "zażółć gęślą jaźń",
                "\0\0\0",
                "\r\t\n",
                "\uD83E\uDD83",
                null);
    }

    @Test
    public void testVarbinarySerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            VARBINARY.writeSlice(blockBuilder, Slices.wrappedBuffer(new byte[] {0, 1, 2, 3, 4, 5, 6}));
            blockBuilder.appendNull();
        };

        assertThat(roundTrip(ImmutableList.of(typed("singleType", VARBINARY)), page(buildBlock(VARBINARY, 2, builder))))
                .usingRecursiveComparison()
                .isEqualTo(column(new byte[] {0, 1, 2, 3, 4, 5, 6}, null));

        assertThat(roundTrip(ImmutableList.of(typed("arrayType", new ArrayType(VARBINARY))), page(buildArrayBlock(VARBINARY, 2, builder))).getFirst().getFirst())
                .as("Expected values for array type: " + new ArrayType(VARBINARY))
                .usingRecursiveComparison()
                .isEqualTo(array(new byte[] {0, 1, 2, 3, 4, 5, 6}, null));
    }

    @Test
    public void testCharSerialization()
            throws IOException
    {
        CharType charType = CharType.createCharType(5);
        Consumer<BlockBuilder> builder = blockBuilder -> {
            charType.writeSlice(blockBuilder, utf8Slice("ala"));
            charType.writeSlice(blockBuilder, utf8Slice("ma"));
            charType.writeSlice(blockBuilder, utf8Slice("kota"));
            charType.writeSlice(blockBuilder, utf8Slice("🐈"));
            blockBuilder.appendNull();
        };

        assertRoundTrip(charType, builder,
                "ala  ",
                "ma   ",
                "kota ",
                "🐈    ",
                null);
    }

    @Test
    public void testBooleanSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            BOOLEAN.writeBoolean(blockBuilder, true);
            BOOLEAN.writeBoolean(blockBuilder, false);
            BOOLEAN.writeBoolean(blockBuilder, true);
            blockBuilder.appendNull();
        };

        assertRoundTrip(BOOLEAN, builder, true, false, true, null);
    }

    @Test
    public void testTime0Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_SECONDS.writeLong(blockBuilder, PICOSECONDS_PER_SECOND);
            TIME_SECONDS.writeLong(blockBuilder, 60 * PICOSECONDS_PER_SECOND);
            TIME_SECONDS.writeLong(blockBuilder, 99 * PICOSECONDS_PER_SECOND);
            TIME_SECONDS.writeLong(blockBuilder, 999 * PICOSECONDS_PER_SECOND);
            TIME_SECONDS.writeLong(blockBuilder, 86399 * PICOSECONDS_PER_SECOND);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_SECONDS, builder, "00:00:01", "00:01:00", "00:01:39", "00:16:39", "23:59:59", null);
    }

    @Test
    public void testTime0WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(NANOSECONDS_PER_SECOND, 60));
            TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(60 * NANOSECONDS_PER_SECOND, 30));
            TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(99 * NANOSECONDS_PER_SECOND, 45));
            TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(999 * NANOSECONDS_PER_SECOND, 180));
            TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND, 719));
            TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND, 14 * 60));
            TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND, -14 * 60));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_TZ_SECONDS, builder, "00:00:01+01:00", "00:01:00+00:30", "00:01:39+00:45", "00:16:39+03:00", "23:59:59+11:59", "23:59:59+14:00", "23:59:59-14:00", null);
    }

    @Test
    public void testTimestamp0WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIMESTAMP_TZ_SECONDS.writeLong(blockBuilder, packDateTimeWithZone(1741028237000L, UTC_KEY));
            TIMESTAMP_TZ_SECONDS.writeLong(blockBuilder, packDateTimeWithZone(0L, UTC_KEY));
            TIMESTAMP_TZ_SECONDS.writeLong(blockBuilder, packDateTimeWithZone(1741028237000L, getTimeZoneKey("America/Bahia_Banderas")));
            TIMESTAMP_TZ_SECONDS.writeLong(blockBuilder, packDateTimeWithZone(0L, getTimeZoneKey("America/Bahia_Banderas")));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_TZ_SECONDS, builder, "2025-03-03 18:57:17 UTC", "1970-01-01 00:00:00 UTC", "2025-03-03 12:57:17 America/Bahia_Banderas", "1969-12-31 17:00:00 America/Bahia_Banderas", null);
    }

    @Test
    public void testTimestamp3WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(1741028237000L, UTC_KEY));
            TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(1L, UTC_KEY));
            TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(1741028237010L, getTimeZoneKey("America/Bahia_Banderas")));
            TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(0L, getTimeZoneKey("America/Bahia_Banderas")));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_TZ_MILLIS, builder, "2025-03-03 18:57:17.000 UTC", "1970-01-01 00:00:00.001 UTC", "2025-03-03 12:57:17.010 America/Bahia_Banderas", "1969-12-31 17:00:00.000 America/Bahia_Banderas", null);
    }

    @Test
    public void testTimestamp6WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1741028237000L, PICOSECONDS_PER_MICROSECOND, UTC_KEY));
            TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1, 0, UTC_KEY));
            TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1741028237010L, 0, getTimeZoneKey("America/Bahia_Banderas")));
            TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0, getTimeZoneKey("America/Bahia_Banderas")));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_TZ_MICROS, builder, "2025-03-03 18:57:17.000001 UTC", "1970-01-01 00:00:00.001000 UTC", "2025-03-03 12:57:17.010000 America/Bahia_Banderas", "1969-12-31 17:00:00.000000 America/Bahia_Banderas", null);
    }

    @Test
    public void testTimestamp9WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIMESTAMP_TZ_NANOS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1741028237000L, PICOSECONDS_PER_MICROSECOND, UTC_KEY));
            TIMESTAMP_TZ_NANOS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1, 0, UTC_KEY));
            TIMESTAMP_TZ_NANOS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1741028237010L, PICOSECONDS_PER_NANOSECOND, getTimeZoneKey("America/Bahia_Banderas")));
            TIMESTAMP_TZ_NANOS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 66 * PICOSECONDS_PER_NANOSECOND, getTimeZoneKey("America/Bahia_Banderas")));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_TZ_NANOS, builder, "2025-03-03 18:57:17.000001000 UTC", "1970-01-01 00:00:00.001000000 UTC", "2025-03-03 12:57:17.010000001 America/Bahia_Banderas", "1969-12-31 17:00:00.000000066 America/Bahia_Banderas", null);
    }

    @Test
    public void testTime3Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_MILLIS.writeLong(blockBuilder, PICOSECONDS_PER_SECOND);
            TIME_MILLIS.writeLong(blockBuilder, 60 * PICOSECONDS_PER_SECOND);
            TIME_MILLIS.writeLong(blockBuilder, 99 * PICOSECONDS_PER_SECOND);
            TIME_MILLIS.writeLong(blockBuilder, 999 * PICOSECONDS_PER_SECOND);
            TIME_MILLIS.writeLong(blockBuilder, 86399 * PICOSECONDS_PER_SECOND);
            TIME_MILLIS.writeLong(blockBuilder, PICOSECONDS_PER_MILLISECOND);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_MILLIS, builder, "00:00:01.000", "00:01:00.000", "00:01:39.000", "00:16:39.000", "23:59:59.000", "00:00:00.001", null);
    }

    @Test
    public void testTime3WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(NANOSECONDS_PER_SECOND + NANOSECONDS_PER_MILLISECOND, 60));
            TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(60 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MILLISECOND, 30));
            TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(99 * NANOSECONDS_PER_SECOND + 100 * NANOSECONDS_PER_MILLISECOND, 45));
            TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(999 * NANOSECONDS_PER_SECOND + 999 * NANOSECONDS_PER_MILLISECOND, 180));
            TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MILLISECOND, 719));
            TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MILLISECOND, 14 * 60));
            TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MILLISECOND, -14 * 60));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_TZ_MILLIS, builder, "00:00:01.001+01:00", "00:01:00.010+00:30", "00:01:39.100+00:45", "00:16:39.999+03:00", "23:59:59.010+11:59", "23:59:59.010+14:00", "23:59:59.010-14:00", null);
    }

    @Test
    public void testTime6Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_MICROS.writeLong(blockBuilder, PICOSECONDS_PER_SECOND);
            TIME_MICROS.writeLong(blockBuilder, 60 * PICOSECONDS_PER_SECOND);
            TIME_MICROS.writeLong(blockBuilder, 99 * PICOSECONDS_PER_SECOND);
            TIME_MICROS.writeLong(blockBuilder, 999 * PICOSECONDS_PER_SECOND);
            TIME_MICROS.writeLong(blockBuilder, 86399 * PICOSECONDS_PER_SECOND);
            TIME_MICROS.writeLong(blockBuilder, PICOSECONDS_PER_MICROSECOND);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_MICROS, builder, "00:00:01.000000", "00:01:00.000000", "00:01:39.000000", "00:16:39.000000", "23:59:59.000000", "00:00:00.000001", null);
    }

    @Test
    public void testTime6WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(NANOSECONDS_PER_SECOND + NANOSECONDS_PER_MICROSECOND, 60));
            TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(60 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MICROSECOND, 30));
            TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(99 * NANOSECONDS_PER_SECOND + 100 * NANOSECONDS_PER_MICROSECOND, 45));
            TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(999 * NANOSECONDS_PER_SECOND + 999 * NANOSECONDS_PER_MICROSECOND, 180));
            TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MICROSECOND, 719));
            TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MICROSECOND, 14 * 60));
            TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10 * NANOSECONDS_PER_MICROSECOND, -14 * 60));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_TZ_MICROS, builder, "00:00:01.000001+01:00", "00:01:00.000010+00:30", "00:01:39.000100+00:45", "00:16:39.000999+03:00", "23:59:59.000010+11:59", "23:59:59.000010+14:00", "23:59:59.000010-14:00", null);
    }

    @Test
    public void testTime9Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_NANOS.writeLong(blockBuilder, PICOSECONDS_PER_SECOND);
            TIME_NANOS.writeLong(blockBuilder, 60 * PICOSECONDS_PER_SECOND);
            TIME_NANOS.writeLong(blockBuilder, 99 * PICOSECONDS_PER_SECOND);
            TIME_NANOS.writeLong(blockBuilder, 999 * PICOSECONDS_PER_SECOND);
            TIME_NANOS.writeLong(blockBuilder, 86399 * PICOSECONDS_PER_SECOND);
            TIME_NANOS.writeLong(blockBuilder, PICOSECONDS_PER_MICROSECOND);
            TIME_NANOS.writeLong(blockBuilder, PICOSECONDS_PER_NANOSECOND);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_NANOS, builder, "00:00:01.000000000", "00:01:00.000000000", "00:01:39.000000000", "00:16:39.000000000", "23:59:59.000000000", "00:00:00.000001000", "00:00:00.000000001", null);
    }

    @Test
    public void testTime9WithTzSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(NANOSECONDS_PER_SECOND + 1, 60));
            TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(60 * NANOSECONDS_PER_SECOND + 10, 30));
            TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(99 * NANOSECONDS_PER_SECOND + 100, 45));
            TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(999 * NANOSECONDS_PER_SECOND + 999, 180));
            TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10, 719));
            TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10, 14 * 60));
            TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(86399 * NANOSECONDS_PER_SECOND + 10, -14 * 60));
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIME_TZ_NANOS, builder, "00:00:01.000000001+01:00", "00:01:00.000000010+00:30", "00:01:39.000000100+00:45", "00:16:39.000000999+03:00", "23:59:59.000000010+11:59", "23:59:59.000000010+14:00", "23:59:59.000000010-14:00", null);
    }

    @Test
    public void testTimestamp0Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIMESTAMP_SECONDS.writeLong(blockBuilder, 1741028237000L * MICROSECONDS_PER_MILLISECOND);
            TIMESTAMP_SECONDS.writeLong(blockBuilder, 0L);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_SECONDS, builder, "2025-03-03 18:57:17", "1970-01-01 00:00:00", null);
    }

    @Test
    public void testTimestamp3Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIMESTAMP_MILLIS.writeLong(blockBuilder, 1741028237425L * MICROSECONDS_PER_MILLISECOND);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_MILLIS, builder, "2025-03-03 18:57:17.425", null);
    }

    @Test
    public void testTimestamp6Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            TIMESTAMP_MICROS.writeLong(blockBuilder, 1741028237425123L);
            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_MICROS, builder, "2025-03-03 18:57:17.425123", null);
    }

    @Test
    public void testTimestamp9Serialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            ((Fixed12BlockBuilder) blockBuilder).writeFixed12(1741028237425123L, 999 * PICOSECONDS_PER_NANOSECOND);
            ((Fixed12BlockBuilder) blockBuilder).writeFixed12(0, 0);
            ((Fixed12BlockBuilder) blockBuilder).writeFixed12(Long.MIN_VALUE / PICOSECONDS_PER_NANOSECOND, 0);
            ((Fixed12BlockBuilder) blockBuilder).writeFixed12(Long.MAX_VALUE / PICOSECONDS_PER_NANOSECOND, 0);

            blockBuilder.appendNull();
        };

        assertRoundTrip(TIMESTAMP_NANOS, builder, "2025-03-03 18:57:17.425123999", "1970-01-01 00:00:00.000000000", "1677-09-21 00:12:43.145225000", "2262-04-11 23:47:16.854775000", null);
    }

    @Test
    public void testUuidSerialization()
            throws IOException
    {
        UUID uuid = randomUUID();
        Consumer<BlockBuilder> builder = blockBuilder -> {
            UUID.writeSlice(blockBuilder, javaUuidToTrinoUuid(uuid));
            blockBuilder.appendNull();
        };

        assertRoundTrip(UUID, builder, uuid.toString(), null);
    }

    @Test
    public void testIpAddressSerialization()
            throws IOException
    {
        Consumer<BlockBuilder> builder = blockBuilder -> {
            IPADDRESS.writeSlice(blockBuilder, parseIpAddress("127.0.0.1"));
            IPADDRESS.writeSlice(blockBuilder, parseIpAddress("192.168.0.1"));
            IPADDRESS.writeSlice(blockBuilder, parseIpAddress("8.8.8.8"));
            IPADDRESS.writeSlice(blockBuilder, parseIpAddress("2001:db8:3333:4444:5555:6666:7777:8888"));
            blockBuilder.appendNull();
        };

        assertRoundTrip(IPADDRESS, builder, "127.0.0.1", "192.168.0.1", "8.8.8.8", "2001:db8:3333:4444:5555:6666:7777:8888", null);
    }

    @Test
    public void testMapSerialization()
            throws IOException
    {
        MapType mapType = new MapType(REAL, DOUBLE, new TypeOperators());
        List<TypedColumn> columns = ImmutableList.of(typed("col0", mapType));
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 7);

        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            REAL.writeFloat(keyBuilder, 0.0f);
            DOUBLE.writeDouble(valueBuilder, 0.0d);

            REAL.writeFloat(keyBuilder, 1.0f);
            DOUBLE.writeDouble(valueBuilder, 1.0d);

            REAL.writeFloat(keyBuilder, 2.0f);
            DOUBLE.writeDouble(valueBuilder, 2.0d);

            REAL.writeFloat(keyBuilder, 3.0f);
            DOUBLE.writeDouble(valueBuilder, 3.0d);

            REAL.writeFloat(keyBuilder, 4.0f);
            DOUBLE.writeDouble(valueBuilder, 4.0d);

            REAL.writeFloat(keyBuilder, 5.0f);
            valueBuilder.appendNull();
        });

        blockBuilder.appendNull();

        Page page = page(blockBuilder.build());

        List<List<Object>> values = roundTrip(columns, page);
        assertThat(values.getFirst())
                .containsExactly(map(
                        entry(0.0f, 0.0d),
                        entry(1.0f, 1.0d),
                        entry(2.0f, 2.0d),
                        entry(3.0f, 3.0d),
                        entry(4.0f, 4.0d),
                        entry(5.0f, null)));

        assertThat(values.getLast()).isEqualTo(nullRow());
    }

    @Test
    public void testMapOfVarbinaryKeysSerialization()
            throws IOException
    {
        MapType mapType = new MapType(VARBINARY, DOUBLE, new TypeOperators());
        List<TypedColumn> columns = ImmutableList.of(typed("col0", mapType));
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 6);

        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARBINARY.writeSlice(keyBuilder, utf8Slice("value"));
            DOUBLE.writeDouble(valueBuilder, 0.0d);

            VARBINARY.writeSlice(keyBuilder, utf8Slice("value2"));
            valueBuilder.appendNull();
        });

        Page page = page(blockBuilder.build());
        List<Object> values = roundTrip(columns, page).getFirst();
        assertThat(values.getFirst()).isInstanceOf(Map.class);
        Map<Object, Object> valuesMap = (Map<Object, Object>) values.getFirst();

        assertThat(valuesMap.keySet().stream()
                .map(bytes -> new String((byte[]) bytes, UTF_8))
                .collect(toImmutableSet()))
                .containsExactlyInAnyOrder("value", "value2");
    }

    @Test
    public void testRowSerialization()
            throws IOException
    {
        RowType rowType = RowType.rowType(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR),
                RowType.field("c", BOOLEAN));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", rowType));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 2);

        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 1);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("ala"));
            BOOLEAN.writeBoolean(builders.get(2), true);
        });

        blockBuilder.buildEntry(builders -> {
            builders.get(0).appendNull();
            builders.get(1).appendNull();
            builders.get(2).appendNull();
        });

        blockBuilder.appendNull();

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page))
                .containsExactly(
                        List.of(Row.builderWithExpectedSize(3)
                                .addField("a", 1L)
                                .addField("b", "ala")
                                .addField("c", true)
                                .build()),
                        List.of(Row.builderWithExpectedSize(3)
                                .addField("a", null)
                                .addField("b", null)
                                .addField("c", null)
                                .build()),
                        nullRow());
    }

    protected List<List<Object>> roundTrip(List<TypedColumn> columns, Page page)
            throws IOException
    {
        QueryDataEncoder encoder = newEncoder(columns);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        encoder.encodeTo(output, List.of(page));
        return ImmutableList.copyOf(decodeValues(columns, output.toByteArray()));
    }

    protected void assertRoundTrip(Type type, Consumer<BlockBuilder> builder, Object... expectedValues)
            throws IOException
    {
        assertThat(roundTrip(ImmutableList.of(typed("singleType", type)), page(buildBlock(type, expectedValues.length, builder))))
                .as("Expected values for type: " + type)
                .isEqualTo(column(expectedValues));

        assertThat(roundTrip(ImmutableList.of(typed("arrayType", new ArrayType(type))), page(buildArrayBlock(type, expectedValues.length, builder))).getFirst())
                .as("Expected values for array type: " + new ArrayType(type))
                .containsExactly(array(expectedValues));
    }

    protected List<List<Object>> decodeValues(List<TypedColumn> columns, byte[] data)
            throws IOException
    {
        QueryDataDecoder decoder = newDecoder(columns);
        return ImmutableList.copyOf(decoder.decode(new ByteArrayInputStream(data), null));
    }

    record TypedColumn(String name, Type type)
    {
        public TypedColumn
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
        }

        public static TypedColumn typed(String name, Type type)
        {
            return new TypedColumn(name, type);
        }
    }

    private static Page page(Block... blocks)
    {
        return new Page(blocks);
    }

    private static <T> List<List<T>> column(T... values)
    {
        return Arrays.stream(values)
                // Allow nulls in values
                .map(value -> {
                    List<T> list = new ArrayList<>();
                    list.add(value);
                    return list;
                })
                .collect(toList());
    }

    private static <T> List<T> array(T... values)
    {
        return Arrays.asList(values);
    }

    private static <K, V> Map<K, V> map(Entry<K, V>... entries)
    {
        Map<K, V> values = new HashMap<>();
        for (Entry<K, V> entry : entries) {
            values.put(entry.key(), entry.value());
        }
        return values;
    }

    record Entry<K, V>(K key, V value)
    {
        // Allow nulls
    }

    static <K, V> Entry<K, V> entry(K key, V value)
    {
        return new Entry<>(key, value);
    }

    private static Block buildBlock(Type type, int positions, Consumer<BlockBuilder> builderConsumer)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positions);
        builderConsumer.accept(blockBuilder);
        return blockBuilder.build();
    }

    private static Block buildArrayBlock(Type type, int positions, Consumer<BlockBuilder> builderConsumer)
    {
        ArrayType arrayType = new ArrayType(type);
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positions);
        blockBuilder.buildEntry(builderConsumer::accept);
        return blockBuilder.build();
    }

    private static Slice parseIpAddress(String value)
    {
        byte[] address;
        try {
            address = InetAddresses.forString(value).getAddress();
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot cast value to IPADDRESS: " + value);
        }

        byte[] bytes;
        if (address.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
        }
        else if (address.length == 16) {
            bytes = address;
        }
        else {
            throw new IllegalArgumentException("Invalid InetAddress length: " + address.length);
        }
        return wrappedBuffer(bytes);
    }

    private static <T> List<T> nullRow()
    {
        return new ArrayList<>(nCopies(1, null));
    }
}
