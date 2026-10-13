/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import io.airlift.slice.Slice;
import io.starburst.materialization.ir.ConstantValues;
import io.trino.spi.type.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the native representation {@link ConstantValues} assumes for the supported base types that live in trino-main.
 * The trino-spi types are pinned in the materialization metastore API module's {@code TestConstantEncodingPins}.
 */
class TestConstantEncodingPins
{
    @ParameterizedTest(name = "{1}")
    @MethodSource("pins")
    void testNativeRepresentation(Type type, String baseName, Class<?> javaType)
    {
        assertThat(type.getBaseName()).isEqualTo(baseName);
        assertThat(type.getJavaType()).isEqualTo(javaType);
        assertThat(ConstantValues.encodingVersions()).containsKey(baseName);
        assertThat(ConstantValues.encode(type, null)).isPresent();
    }

    private static Stream<Arguments> pins()
    {
        return Stream.of(
                Arguments.of(JSON, "json", Slice.class),
                Arguments.of(IPADDRESS, "ipaddress", Slice.class),
                Arguments.of(UNKNOWN, "unknown", boolean.class),
                Arguments.of(INTERVAL_YEAR_MONTH, "interval year to month", long.class),
                Arguments.of(INTERVAL_DAY_TIME, "interval day to second", long.class));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("meanings")
    void testNativeValueMeaning(Type type, Object nativeValue, String rendered)
    {
        assertThat(type.getObjectValue(writeNativeValue(type, nativeValue), 0)).hasToString(rendered);
    }

    private static Stream<Arguments> meanings()
    {
        return Stream.of(
                Arguments.of(INTERVAL_YEAR_MONTH, 14L, "1-2"),
                Arguments.of(INTERVAL_DAY_TIME, 90_061_001L, "1 01:01:01.001"));
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

    private static Stream<Arguments> sliceFormats()
    {
        return Stream.of(
                // UTF-8 JSON text
                Arguments.of(JSON, utf8Slice("{\"a\":[1,2]}"), "{\"a\":[1,2]}"),
                Arguments.of(JSON, wrappedBuffer(new byte[] {0x22, (byte) 0xC3, (byte) 0xA9, 0x22}), "\"é\""),
                // 16 bytes in network order, an IPv4 address as its IPv4-mapped IPv6 form
                Arguments.of(IPADDRESS, wrappedBuffer(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xFF, (byte) 0xFF, (byte) 192, (byte) 168, 0, 1}), "192.168.0.1"),
                Arguments.of(IPADDRESS, wrappedBuffer(new byte[] {0x20, 0x01, 0x0D, (byte) 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}), "2001:db8::1"));
    }
}
