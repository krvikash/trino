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
package io.starburst.materialization.ir;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.starburst.materialization.ir.ConstantValue.BytesValue;
import io.starburst.materialization.ir.ConstantValue.DoubleValue;
import io.starburst.materialization.ir.ConstantValue.Int128Value;
import io.starburst.materialization.ir.ConstantValue.LongValue;
import io.starburst.materialization.ir.ConstantValue.NullValue;
import io.starburst.materialization.ir.ConstantValue.RealValue;
import io.starburst.materialization.ir.ConstantValue.TimeWithZoneValue;
import io.starburst.materialization.ir.ConstantValue.TimestampValue;
import io.starburst.materialization.ir.ConstantValue.TimestampWithZoneValue;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.math.LongMath.pow;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_UNSCALED_DECIMAL;
import static io.trino.spi.type.Decimals.MIN_UNSCALED_DECIMAL;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestConstantValues
{
    private static final JsonCodec<ConstantValue> CODEC = IrJson.codec(ConstantValue.class);

    private static final long MIN_EPOCH_MILLIS_WITH_ZONE = -(1L << 51);
    private static final long MAX_EPOCH_MILLIS_WITH_ZONE = (1L << 51) - 1;
    private static final TimeZoneKey NEW_YORK = TimeZoneKey.getTimeZoneKey("America/New_York");
    private static final TimeZoneKey PLUS_05_30 = TimeZoneKey.getTimeZoneKey("+05:30");
    private static final int NON_CANONICAL_REAL_NAN = 0x7F80_0001;

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("nativeValues")
    void testRoundTrip(Type type, Object nativeValue)
    {
        ConstantValue value = ConstantValues.encode(type, nativeValue).orElseThrow();
        ConstantValue read = CODEC.fromJson(CODEC.toJson(value));
        assertThat(read).isEqualTo(value);
        assertThat(ConstantValues.decode(type, read)).isEqualTo(nativeValue);
    }

    static Stream<Arguments> nativeValues()
    {
        Stream.Builder<Arguments> values = Stream.builder();
        values.add(Arguments.of(BOOLEAN, true));
        values.add(Arguments.of(BOOLEAN, false));
        addIntegerValues(values, TINYINT, Byte.MIN_VALUE, Byte.MAX_VALUE);
        addIntegerValues(values, SMALLINT, Short.MIN_VALUE, Short.MAX_VALUE);
        addIntegerValues(values, INTEGER, Integer.MIN_VALUE, Integer.MAX_VALUE);
        addIntegerValues(values, BIGINT, Long.MIN_VALUE, Long.MAX_VALUE);
        values.add(Arguments.of(DATE, 0L));
        values.add(Arguments.of(DATE, (long) Integer.MIN_VALUE));
        values.add(Arguments.of(DATE, (long) Integer.MAX_VALUE));
        for (float value : new float[] {0.0f, -0.0f, -1.5f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, Float.MAX_VALUE}) {
            values.add(Arguments.of(REAL, (long) floatToIntBits(value)));
        }
        for (double value : new double[] {0.0, -0.0, -1.5, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE}) {
            values.add(Arguments.of(DOUBLE, value));
        }
        values.add(Arguments.of(createDecimalType(18, 0), pow(10, 18) - 1));
        values.add(Arguments.of(createDecimalType(18, 0), -(pow(10, 18) - 1)));
        values.add(Arguments.of(createDecimalType(18, 0), 0L));
        values.add(Arguments.of(createDecimalType(18, 6), -123_456_789L));
        values.add(Arguments.of(createDecimalType(38, 10), MAX_UNSCALED_DECIMAL));
        values.add(Arguments.of(createDecimalType(38, 10), MIN_UNSCALED_DECIMAL));
        values.add(Arguments.of(createDecimalType(38, 10), Int128.ZERO));
        values.add(Arguments.of(createDecimalType(38, 10), Int128.valueOf("-12345678901234567890123456789")));
        values.add(Arguments.of(VARCHAR, utf8Slice("")));
        values.add(Arguments.of(VARCHAR, utf8Slice("𝄞")));
        values.add(Arguments.of(VARCHAR, wrappedBuffer((byte) 0xFF, (byte) 0xFE, (byte) 0x80)));
        values.add(Arguments.of(VARCHAR, utf8Slice("a\0b")));
        values.add(Arguments.of(VARCHAR, utf8Slice("x".repeat(10_000))));
        values.add(Arguments.of(createVarcharType(3), utf8Slice("abc")));
        values.add(Arguments.of(VARBINARY, wrappedBuffer()));
        values.add(Arguments.of(VARBINARY, wrappedBuffer(new byte[16])));
        values.add(Arguments.of(VARBINARY, allByteValues()));
        values.add(Arguments.of(UUID, javaUuidToTrinoUuid(java.util.UUID.fromString("12151fd2-7586-11e9-8f9e-2a86e4085a59"))));
        for (int precision : new int[] {0, 3, 6}) {
            long unit = pow(10, 6 - precision);
            values.add(Arguments.of(createTimestampType(precision), 0L));
            values.add(Arguments.of(createTimestampType(precision), -123 * unit));
            values.add(Arguments.of(createTimestampType(precision), Long.MIN_VALUE - Long.MIN_VALUE % unit));
            values.add(Arguments.of(createTimestampType(precision), Long.MAX_VALUE - Long.MAX_VALUE % unit));
        }
        values.add(Arguments.of(createTimestampType(9), new LongTimestamp(-1, 0)));
        values.add(Arguments.of(createTimestampType(9), new LongTimestamp(1, 999_000)));
        values.add(Arguments.of(createTimestampType(12), new LongTimestamp(0, 0)));
        values.add(Arguments.of(createTimestampType(12), new LongTimestamp(1, 999_999)));
        for (int precision : new int[] {0, 6, 12}) {
            values.add(Arguments.of(createTimeType(precision), 0L));
            values.add(Arguments.of(createTimeType(precision), PICOSECONDS_PER_DAY - pow(10, 12 - precision)));
        }
        return values.build();
    }

    /**
     * The payloads are built directly: {@link ConstantValues#encode} does not produce session-sensitive types.
     */
    @ParameterizedTest(name = "{0} {2}")
    @MethodSource("sessionSensitiveValues")
    void testSessionSensitiveRoundTrip(Type type, ConstantValue value, Object nativeValue)
    {
        ConstantValue read = CODEC.fromJson(CODEC.toJson(value));
        assertThat(read).isEqualTo(value);
        assertThat(ConstantValues.decode(type, read)).isEqualTo(nativeValue);
        assertThat(new Constant(type, read).value()).isEqualTo(value);
    }

    static Stream<Arguments> sessionSensitiveValues()
    {
        Stream.Builder<Arguments> values = Stream.builder();
        values.add(Arguments.of(createCharType(5), new BytesValue(utf8Slice("abc")), utf8Slice("abc")));
        for (TimeZoneKey zone : ImmutableList.of(UTC_KEY, NEW_YORK, PLUS_05_30)) {
            for (long epochMillis : new long[] {0, -1_000, MIN_EPOCH_MILLIS_WITH_ZONE, MAX_EPOCH_MILLIS_WITH_ZONE}) {
                values.add(Arguments.of(
                        createTimestampWithTimeZoneType(3),
                        new TimestampWithZoneValue(epochMillis, 0, zone.getId()),
                        packDateTimeWithZone(epochMillis, zone)));
                values.add(Arguments.of(
                        createTimestampWithTimeZoneType(9),
                        new TimestampWithZoneValue(epochMillis, 123_456_000, zone.getId()),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 123_456_000, zone)));
            }
        }
        for (int offsetMinutes : new int[] {0, 330, -480, -1024, 1023}) {
            values.add(Arguments.of(
                    createTimeWithTimeZoneType(9),
                    new TimeWithZoneValue(12_345_678_901_234_000L, offsetMinutes),
                    packTimeWithTimeZone(12_345_678_901_234L, offsetMinutes)));
            values.add(Arguments.of(
                    createTimeWithTimeZoneType(12),
                    new TimeWithZoneValue(12_345_678_901_234_567L, offsetMinutes),
                    new LongTimeWithTimeZone(12_345_678_901_234_567L, offsetMinutes)));
        }
        sessionSensitiveTypes().forEach(type -> values.add(Arguments.of(type, new NullValue(), null)));
        return values.build();
    }

    @ParameterizedTest(name = "{0} {2}")
    @MethodSource("sessionSensitiveValues")
    void testSessionSensitiveValueIsNotEncoded(Type type, ConstantValue ignored, Object nativeValue)
    {
        assertThat(ConstantValues.encode(type, nativeValue)).isEmpty();
    }

    static Stream<Type> sessionSensitiveTypes()
    {
        return Stream.of(
                createTimestampWithTimeZoneType(0),
                createTimestampWithTimeZoneType(3),
                createTimestampWithTimeZoneType(9),
                createTimestampWithTimeZoneType(12),
                createTimeWithTimeZoneType(0),
                createTimeWithTimeZoneType(9),
                createTimeWithTimeZoneType(12),
                createCharType(1),
                createCharType(5));
    }

    /**
     * Types adjacent to the session-sensitive ones in the codec's dispatch are encoded.
     */
    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("sessionIndependentNeighbors")
    void testNeighboringTypeIsEncoded(Type type, Object nativeValue)
    {
        assertThat(ConstantValues.encode(type, null)).contains(new NullValue());
        assertThat(ConstantValues.encode(type, nativeValue)).isPresent();
    }

    static Stream<Arguments> sessionIndependentNeighbors()
    {
        return Stream.of(
                Arguments.of(createTimestampType(3), 1_000L),
                Arguments.of(createTimestampType(9), new LongTimestamp(1, 0)),
                Arguments.of(createTimeType(3), 1_000_000_000L),
                Arguments.of(createTimeType(12), 1L),
                Arguments.of(DATE, 1L),
                Arguments.of(VARCHAR, utf8Slice("abc")),
                Arguments.of(createVarcharType(5), utf8Slice("abc")),
                Arguments.of(VARBINARY, wrappedBuffer((byte) 1)));
    }

    private static void addIntegerValues(Stream.Builder<Arguments> values, Type type, long min, long max)
    {
        for (long value : new long[] {min, max, 0, -1}) {
            values.add(Arguments.of(type, value));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("supportedTypes")
    void testNullRoundTrip(Type type)
    {
        ConstantValue value = ConstantValues.encode(type, null).orElseThrow();
        assertThat(value).isEqualTo(new NullValue());
        assertThat(CODEC.toJson(value)).isEqualTo("{\"kind\":\"null\"}");
        assertThat(ConstantValues.decode(type, value)).isNull();
    }

    static Stream<Type> supportedTypes()
    {
        return Stream.of(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                createDecimalType(18, 0),
                createDecimalType(38, 10),
                DATE,
                createTimeType(3),
                createTimestampType(3),
                createTimestampType(9),
                VARCHAR,
                createVarcharType(3),
                VARBINARY,
                UUID);
    }

    @Test
    void testDoubleIsStoredAsCanonicalBits()
    {
        double nonCanonicalNaN = longBitsToDouble(0x7FF0_0000_0000_0001L);
        assertThat(ConstantValues.encode(DOUBLE, nonCanonicalNaN)).contains(new DoubleValue(doubleToLongBits(Double.NaN)));
        assertThat(ConstantValues.encode(DOUBLE, -0.0)).contains(new DoubleValue(Long.MIN_VALUE));
    }

    @Test
    void testTimestampWithTimeZoneIsDecodedByZoneId()
    {
        assertThat(ConstantValues.decode(createTimestampWithTimeZoneType(3), new TimestampWithZoneValue(1_000, 0, "America/New_York")))
                .isEqualTo(packDateTimeWithZone(1_000, NEW_YORK));
        Object decoded = ConstantValues.decode(createTimestampWithTimeZoneType(9), new TimestampWithZoneValue(1_000, 5_000, "+05:30"));
        assertThat(decoded).isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_000, 5_000, PLUS_05_30));
    }

    @Test
    void testShortTimeWithTimeZoneIsDecodedFromPicoseconds()
    {
        assertThat(ConstantValues.decode(createTimeWithTimeZoneType(9), new TimeWithZoneValue(7_000, -480)))
                .isEqualTo(packTimeWithTimeZone(7, -480));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedValues")
    void testUnsupportedValueIsNotEncoded(Type type, Object nativeValue)
    {
        assertThat(ConstantValues.encode(type, nativeValue)).isEmpty();
    }

    static Stream<Arguments> unsupportedValues()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        return Stream.of(
                Arguments.of(arrayType, new LongArrayBlock(1, Optional.empty(), new long[] {1})),
                Arguments.of(arrayType, null),
                Arguments.of(new MapType(VARCHAR, BIGINT, new TypeOperators()), null),
                Arguments.of(RowType.anonymousRow(BIGINT), null),
                Arguments.of(HYPER_LOG_LOG, wrappedBuffer((byte) 1)),
                Arguments.of(HYPER_LOG_LOG, null),
                Arguments.of(BIGINT, "not a long"),
                Arguments.of(REAL, (long) NON_CANONICAL_REAL_NAN),
                Arguments.of(UUID, wrappedBuffer(new byte[15])));
    }

    @ParameterizedTest(name = "{0}: {2}")
    @MethodSource("undecodablePayloads")
    void testUndecodablePayloadIsRejected(Type type, ConstantValue payload, String expectedMessage)
    {
        assertThatThrownBy(() -> ConstantValues.decode(type, payload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage)
                .hasNoCause();
    }

    static Stream<Arguments> undecodablePayloads()
    {
        return Stream.of(
                rejected(new ArrayType(BIGINT), new NullValue(), "type is not supported"),
                rejected(new ArrayType(BIGINT), new LongValue(1), "type is not supported"),
                rejected(DOUBLE, new LongValue(1), "expected kind double"),
                rejected(createDecimalType(38, 10), new LongValue(1), "expected kind int128"),
                rejected(DOUBLE, new DoubleValue(0x7FF0_0000_0000_0001L), "bits are not canonical"),
                rejected(REAL, new RealValue(NON_CANONICAL_REAL_NAN), "bits are not canonical"),
                rejected(UUID, new BytesValue(wrappedBuffer(new byte[15])), "expected 16 bytes"),
                rejected(UUID, new BytesValue(wrappedBuffer(new byte[17])), "expected 16 bytes"),
                rejected(createDecimalType(18, 0), new LongValue(pow(10, 18)), "value overflows the precision"),
                rejected(createDecimalType(38, 10), int128(Int128.valueOf("1" + "0".repeat(38))), "value overflows the precision"),
                rejected(INTEGER, new LongValue(1L << 31), "value is out of range"),
                rejected(TINYINT, new LongValue(128), "value is out of range"),
                rejected(SMALLINT, new LongValue(Short.MAX_VALUE + 1L), "value is out of range"),
                rejected(DATE, new LongValue(Integer.MIN_VALUE - 1L), "value is out of range"),
                rejected(createCharType(5), new BytesValue(utf8Slice("abcdef")), "value exceeds the length"),
                rejected(createCharType(5), new BytesValue(utf8Slice("ab ")), "value has a trailing space"),
                rejected(createVarcharType(3), new BytesValue(utf8Slice("abcd")), "value exceeds the length"),
                rejected(createTimeType(12), new LongValue(-1), "picoseconds of day is out of range"),
                rejected(createTimeType(12), new LongValue(PICOSECONDS_PER_DAY), "picoseconds of day is out of range"),
                rejected(createTimeType(3), new LongValue(1), "picoseconds exceed the precision"),
                rejected(createTimestampType(3), new LongValue(1), "epochMicros exceeds the precision"),
                rejected(createTimestampType(9), new TimestampValue(0, 1), "picosOfMicro exceeds the precision"),
                rejected(createTimestampWithTimeZoneType(0), new TimestampWithZoneValue(1, 0, "UTC"), "epochMillis exceeds the precision"),
                rejected(createTimestampWithTimeZoneType(3), new TimestampWithZoneValue(0, 1, "UTC"), "picosOfMilli exceeds the precision"),
                rejected(createTimestampWithTimeZoneType(9), new TimestampWithZoneValue(0, 1, "UTC"), "picosOfMilli exceeds the precision"),
                rejected(createTimeWithTimeZoneType(12), new TimeWithZoneValue(PICOSECONDS_PER_DAY, 0), "picoseconds of day is out of range"),
                rejected(createTimeWithTimeZoneType(12), new TimeWithZoneValue(0, 1024), "offsetMinutes is out of range"),
                rejected(createTimeWithTimeZoneType(9), new TimeWithZoneValue(0, -1025), "offsetMinutes is out of range"),
                rejected(createTimeWithTimeZoneType(6), new TimeWithZoneValue(1, 0), "picoseconds exceed the precision"));
    }

    /**
     * Failures raised inside trino-spi surface without their message, which can quote the value.
     */
    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("payloadsTrinoSpiRejects")
    void testPayloadRejectedByTrinoSpiIsReportedWithoutValue(Type type, ConstantValue payload)
    {
        assertThatThrownBy(() -> ConstantValues.decode(type, payload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Constant: cannot decode timestampWithZone constant of type %s".formatted(type.getDisplayName()))
                .hasNoCause();
    }

    static Stream<Arguments> payloadsTrinoSpiRejects()
    {
        Stream.Builder<Arguments> payloads = Stream.builder();
        for (Type type : ImmutableList.of(createTimestampWithTimeZoneType(3), createTimestampWithTimeZoneType(9))) {
            payloads.add(Arguments.of(type, new TimestampWithZoneValue(MIN_EPOCH_MILLIS_WITH_ZONE - 1, 0, "UTC")));
            payloads.add(Arguments.of(type, new TimestampWithZoneValue(MAX_EPOCH_MILLIS_WITH_ZONE + 1, 0, "UTC")));
            payloads.add(Arguments.of(type, new TimestampWithZoneValue(0, 0, "Mars/Olympus_Mons")));
        }
        return payloads.build();
    }

    private static Arguments rejected(Type type, ConstantValue payload, String reason)
    {
        return Arguments.of(type, payload, "Constant: cannot decode %s constant of type %s: %s".formatted(payload.kind(), type.getDisplayName(), reason));
    }

    private static Int128Value int128(Int128 value)
    {
        return new Int128Value(value.getHigh(), value.getLow());
    }

    private static Slice allByteValues()
    {
        byte[] bytes = new byte[256];
        IntStream.range(0, 256).forEach(i -> bytes[i] = (byte) i);
        return wrappedBuffer(bytes);
    }
}
