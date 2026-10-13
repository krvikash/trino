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

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.CHAR;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.StandardTypes.IPADDRESS;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the native representation {@link ConstantValues} assumes for every supported base type: its Java holder and,
 * where the holder carries a unit, scale, packing or byte format, the value a known native value stands for. A
 * failure here means the engine changed a type's native representation: bump that base name's encoding version (its
 * {@code ctype:} key) and teach the codec the new form, never just update the pin.
 * <p>
 * The trino-main types are pinned in trino-main's {@code TestConstantEncodingPins}.
 */
class TestConstantEncodingPins
{
    static final Set<String> SESSION_SENSITIVE_BASE_NAMES = ImmutableSet.of(TIMESTAMP_WITH_TIME_ZONE, TIME_WITH_TIME_ZONE, CHAR);
    static final Set<String> TRINO_MAIN_BASE_NAMES = ImmutableSet.of(JSON, IPADDRESS, "unknown", INTERVAL_YEAR_TO_MONTH, INTERVAL_DAY_TO_SECOND);

    @ParameterizedTest(name = "{0}")
    @MethodSource("pins")
    void testNativeRepresentation(Type type, Class<?> javaType)
    {
        assertThat(type.getJavaType()).isEqualTo(javaType);
        assertThat(ConstantValues.encode(type, null).isPresent()).isEqualTo(!SESSION_SENSITIVE_BASE_NAMES.contains(type.getBaseName()));
    }

    static Stream<Arguments> pins()
    {
        return Stream.of(
                Arguments.of(BOOLEAN, boolean.class),
                Arguments.of(TINYINT, long.class),
                Arguments.of(SMALLINT, long.class),
                Arguments.of(INTEGER, long.class),
                Arguments.of(BIGINT, long.class),
                Arguments.of(REAL, long.class),
                Arguments.of(DOUBLE, double.class),
                Arguments.of(createDecimalType(Decimals.MAX_SHORT_PRECISION, 0), long.class),
                Arguments.of(createDecimalType(Decimals.MAX_SHORT_PRECISION + 1, 0), Int128.class),
                Arguments.of(DATE, long.class),
                Arguments.of(createTimeType(0), long.class),
                Arguments.of(createTimeType(TimeType.MAX_PRECISION), long.class),
                Arguments.of(createTimeWithTimeZoneType(TimeWithTimeZoneType.MAX_SHORT_PRECISION), long.class),
                Arguments.of(createTimeWithTimeZoneType(TimeWithTimeZoneType.MAX_SHORT_PRECISION + 1), LongTimeWithTimeZone.class),
                Arguments.of(createTimestampType(TimestampType.MAX_SHORT_PRECISION), long.class),
                Arguments.of(createTimestampType(TimestampType.MAX_SHORT_PRECISION + 1), LongTimestamp.class),
                Arguments.of(createTimestampWithTimeZoneType(TimestampWithTimeZoneType.MAX_SHORT_PRECISION), long.class),
                Arguments.of(createTimestampWithTimeZoneType(TimestampWithTimeZoneType.MAX_SHORT_PRECISION + 1), LongTimestampWithTimeZone.class),
                Arguments.of(VARCHAR, Slice.class),
                Arguments.of(createCharType(5), Slice.class),
                Arguments.of(VARBINARY, Slice.class),
                Arguments.of(UUID, Slice.class));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("meanings")
    void testNativeValueMeaning(Type type, Object nativeValue, String rendered)
    {
        assertThat(type.getObjectValue(writeNativeValue(type, nativeValue), 0)).hasToString(rendered);
    }

    static Stream<Arguments> meanings()
    {
        return Stream.of(
                Arguments.of(REAL, (long) floatToIntBits(1.5f), "1.5"),
                Arguments.of(createDecimalType(10, 2), 12_345L, "123.45"),
                Arguments.of(createDecimalType(38, 2), Int128.valueOf(12_345), "123.45"),
                Arguments.of(DATE, 1L, "1970-01-02"),
                Arguments.of(createTimeType(3), 1_000_000_000_000L, "00:00:01.000"),
                Arguments.of(createTimeType(12), 1L, "00:00:00.000000000001"),
                Arguments.of(createTimeWithTimeZoneType(9), packTimeWithTimeZone(1_000_000_000L, -480), "00:00:01.000000000-08:00"),
                Arguments.of(createTimeWithTimeZoneType(12), new LongTimeWithTimeZone(1L, -480), "00:00:00.000000000001-08:00"),
                Arguments.of(createTimestampType(3), 1_000_000L, "1970-01-01 00:00:01.000"),
                Arguments.of(createTimestampType(12), new LongTimestamp(1_000_000L, 1), "1970-01-01 00:00:01.000000000001"),
                Arguments.of(createTimestampWithTimeZoneType(3), packDateTimeWithZone(1_000L, UTC_KEY), "1970-01-01 00:00:01.000 UTC"),
                Arguments.of(createTimestampWithTimeZoneType(12), LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_000L, 1, UTC_KEY), "1970-01-01 00:00:01.000000000001 UTC"));
    }

    /**
     * The byte format of each {@code Slice} holder: what the bytes of a native value stand for.
     */
    @ParameterizedTest(name = "{0} {2}")
    @MethodSource("sliceFormats")
    void testSliceByteFormat(Type type, Slice bytes, String rendered)
    {
        assertThat(type.getObjectValue(writeNativeValue(type, bytes), 0)).hasToString(rendered);
    }

    static Stream<Arguments> sliceFormats()
    {
        return Stream.of(
                // UTF-8
                Arguments.of(VARCHAR, wrappedBuffer((byte) 0xC3, (byte) 0xA9), "é"),
                Arguments.of(VARCHAR, wrappedBuffer((byte) 0xF0, (byte) 0x9D, (byte) 0x84, (byte) 0x9E), "𝄞"),
                // the bytes themselves, rendered as base64
                Arguments.of(VARBINARY, wrappedBuffer((byte) 0x01, (byte) 0x02, (byte) 0xFF), "AQL/"),
                // big-endian most significant bits first
                Arguments.of(
                        UUID,
                        wrappedBuffer(new byte[] {0x12, 0x15, 0x1F, (byte) 0xD2, 0x75, (byte) 0x86, 0x11, (byte) 0xE9, (byte) 0x8F, (byte) 0x9E, 0x2A, (byte) 0x86, (byte) 0xE4, 0x08, 0x5A, 0x59}),
                        "12151fd2-7586-11e9-8f9e-2a86e4085a59"));
    }

    @Test
    void testPrecisionBoundaries()
    {
        assertThat(Decimals.MAX_SHORT_PRECISION).isEqualTo(18);
        assertThat(TimeType.MAX_PRECISION).isEqualTo(12);
        assertThat(TimeWithTimeZoneType.MAX_SHORT_PRECISION).isEqualTo(9);
        assertThat(TimestampType.MAX_SHORT_PRECISION).isEqualTo(6);
        assertThat(TimestampWithTimeZoneType.MAX_SHORT_PRECISION).isEqualTo(3);
    }

    /**
     * Adding a base type to the set {@link ConstantValues#encode} never produces is a deliberate act: update the set
     * here together with the constant and the README.
     */
    @Test
    void testSessionSensitiveBaseTypes()
    {
        assertThat(ConstantValues.SESSION_SENSITIVE_BASE_TYPES).isEqualTo(SESSION_SENSITIVE_BASE_NAMES);
        assertThat(ConstantValues.encodingVersions().keySet()).containsAll(SESSION_SENSITIVE_BASE_NAMES);
    }

    @Test
    void testEveryEncodedBaseTypeIsPinned()
    {
        Set<String> pinned = ImmutableSet.<String>builder()
                .addAll(pins().map(arguments -> ((Type) arguments.get()[0]).getBaseName()).collect(toImmutableSet()))
                .addAll(TRINO_MAIN_BASE_NAMES)
                .build();
        assertThat(ConstantValues.encodingVersions().keySet()).isEqualTo(pinned);
        assertThat(IrVersions.SUPPORTED.keySet())
                .filteredOn(name -> name.startsWith(IrVersions.CONSTANT_TYPE_PREFIX))
                .containsExactlyInAnyOrderElementsOf(pinned.stream().map(baseName -> IrVersions.CONSTANT_TYPE_PREFIX + baseName).collect(toImmutableSet()));
    }
}
