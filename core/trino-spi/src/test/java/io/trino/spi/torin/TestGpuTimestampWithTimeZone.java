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
package io.trino.spi.torin;

import ai.rapids.cudf.DType;
import io.trino.spi.TrinoException;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.torin.GpuTimestampWithTimeZone.hasUtcZone;
import static io.trino.spi.torin.GpuTimestampWithTimeZone.instantDType;
import static io.trino.spi.torin.GpuTimestampWithTimeZone.instantTicks;
import static io.trino.spi.torin.GpuTimestampWithTimeZone.packedEpochMillis;
import static io.trino.spi.torin.GpuTimestampWithTimeZone.picosOfMilli;
import static io.trino.spi.torin.GpuTimestampWithTimeZone.withUtcZone;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Covers the parts of the GPU `timestamp with time zone` representation that are plain functions,
/// so that they are exercised without a device.
final class TestGpuTimestampWithTimeZone
{
    @Test
    void testInstantDType()
    {
        assertThat(instantDType(0)).contains(DType.TIMESTAMP_SECONDS);
        assertThat(instantDType(1)).contains(DType.TIMESTAMP_MILLISECONDS);
        assertThat(instantDType(3)).contains(DType.TIMESTAMP_MILLISECONDS);
        assertThat(instantDType(4)).contains(DType.TIMESTAMP_MICROSECONDS);
        assertThat(instantDType(6)).contains(DType.TIMESTAMP_MICROSECONDS);
        assertThat(instantDType(7)).contains(DType.TIMESTAMP_NANOSECONDS);
        assertThat(instantDType(9)).contains(DType.TIMESTAMP_NANOSECONDS);
        for (int precision = 10; precision <= TimestampWithTimeZoneType.MAX_PRECISION; precision++) {
            assertThat(instantDType(precision)).as("precision %s", precision).isEmpty();
        }
    }

    @Test
    void testInstantRoundTrips()
    {
        // The negative ones are before the epoch, where truncation and flooring differ
        List<Long> epochMillisValues = List.of(0L, 1L, 1730611800000L, -1L, -1730611800000L);
        for (long epochMillis : epochMillisValues) {
            assertInstantRoundTrips(DType.TIMESTAMP_SECONDS, (epochMillis / 1000) * 1000, 0);
            assertInstantRoundTrips(DType.TIMESTAMP_MILLISECONDS, epochMillis, 0);
            assertInstantRoundTrips(DType.TIMESTAMP_MICROSECONDS, epochMillis, 0);
            assertInstantRoundTrips(DType.TIMESTAMP_MICROSECONDS, epochMillis, 999_000_000);
            assertInstantRoundTrips(DType.TIMESTAMP_NANOSECONDS, epochMillis, 999_999_000);
        }
    }

    private static void assertInstantRoundTrips(DType instantType, long epochMillis, int expectedPicosOfMilli)
    {
        long packed = packDateTimeWithZone(epochMillis, UTC_KEY);
        long ticks = instantTicks(TIMESTAMP_TZ_NANOS, instantType, packed, expectedPicosOfMilli);
        assertThat(packedEpochMillis(instantType, ticks))
                .as("%s at %s.%s", instantType, epochMillis, expectedPicosOfMilli)
                .isEqualTo(packed);
        assertThat(picosOfMilli(instantType, ticks))
                .as("%s at %s.%s", instantType, epochMillis, expectedPicosOfMilli)
                .isEqualTo(expectedPicosOfMilli);
    }

    /// The representation carries the instant alone, so a value in any other zone would come back
    /// rendered in UTC and is refused instead.
    @Test
    void testZoneOtherThanUtcIsRejected()
    {
        List<TimeZoneKey> zoneKeys = List.of(
                getTimeZoneKeyForOffset(5 * 60 + 30),
                getTimeZoneKeyForOffset(-(8 * 60)),
                getTimeZoneKey("America/New_York"),
                getTimeZoneKey("Europe/Warsaw"));
        for (TimeZoneKey zoneKey : zoneKeys) {
            long packed = packDateTimeWithZone(1730611800000L, zoneKey);
            assertThatThrownBy(() -> instantTicks(TIMESTAMP_TZ_MILLIS, DType.TIMESTAMP_MILLISECONDS, packed, 0))
                    .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                    .hasMessage("timestamp(3) with time zone value not supported by GPU: time zone must be UTC, was " + zoneKey.getId());
        }
    }

    /// A fixed offset of zero is `UTC_KEY` itself, so it is the same value rather than a second
    /// spelling of UTC that would have to be rejected.
    @Test
    void testZeroOffsetZoneIsUtc()
    {
        assertThat(getTimeZoneKeyForOffset(0)).isEqualTo(UTC_KEY);
        assertThat(instantTicks(TIMESTAMP_TZ_MILLIS, DType.TIMESTAMP_MILLISECONDS, packDateTimeWithZone(17L, getTimeZoneKeyForOffset(0)), 0))
                .isEqualTo(17L);
    }

    @Test
    void testHasUtcZone()
    {
        assertThat(hasUtcZone(packDateTimeWithZone(17L, UTC_KEY))).isTrue();
        assertThat(hasUtcZone(packDateTimeWithZone(17L, getTimeZoneKey("Europe/Warsaw")))).isFalse();
        assertThat(hasUtcZone(LongTimestampWithTimeZone.fromEpochMillisAndFraction(17L, 500_000, UTC_KEY))).isTrue();
        assertThat(hasUtcZone(LongTimestampWithTimeZone.fromEpochMillisAndFraction(17L, 500_000, getTimeZoneKey("Europe/Warsaw")))).isFalse();
    }

    @Test
    void testWithUtcZone()
    {
        assertThat(withUtcZone(packDateTimeWithZone(17L, getTimeZoneKey("Europe/Warsaw")))).isEqualTo(packDateTimeWithZone(17L, UTC_KEY));
        assertThat(withUtcZone(packDateTimeWithZone(17L, UTC_KEY))).isEqualTo(packDateTimeWithZone(17L, UTC_KEY));
        assertThat(withUtcZone(LongTimestampWithTimeZone.fromEpochMillisAndFraction(17L, 500_000, getTimeZoneKey("Europe/Warsaw"))))
                .isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(17L, 500_000, UTC_KEY));
    }

    /// The nanosecond carrier is the one that holds less than the value does, so it has to say so
    /// with a range error rather than fail as an arithmetic overflow.
    @Test
    void testInstantOutsideNanosecondRangeIsRejected()
    {
        long packed = packDateTimeWithZone(1L << 45, UTC_KEY);
        assertThatThrownBy(() -> instantTicks(TIMESTAMP_TZ_NANOS, DType.TIMESTAMP_NANOSECONDS, packed, 0))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode()))
                .hasMessage("Timestamp value out of range support for GPU for type timestamp(9) with time zone: 3084-12-12T12:41:28.832");
    }

    /// A negative instant with a fraction below the millisecond, so the reported value has to borrow
    /// its fraction from the preceding second.
    @Test
    void testNegativeInstantOutsideNanosecondRangeIsRejected()
    {
        LongTimestampWithTimeZone timestamp = LongTimestampWithTimeZone.fromEpochMillisAndFraction(-(1L << 45) - 1, 123_456_789, UTC_KEY);
        assertThatThrownBy(() -> instantTicks(TIMESTAMP_TZ_NANOS, DType.TIMESTAMP_NANOSECONDS, timestamp))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode()))
                .hasMessage("Timestamp value out of range support for GPU for type timestamp(9) with time zone: 0855-01-19T11:18:31.167123456");
    }

    /// The microsecond carrier holds more than the value does: `packDateTimeWithZone` keeps the zone
    /// in the low 12 bits, so every instant is already capped at 2^51 milliseconds, well inside what
    /// a microsecond count reaches.
    @Test
    void testInstantAtTheEdgeOfTheMicrosecondRangeIsAccepted()
    {
        for (long epochMillis : List.of((1L << 51) - 1, -(1L << 51))) {
            long packed = packDateTimeWithZone(epochMillis, UTC_KEY);
            assertThat(instantTicks(TIMESTAMP_TZ_MICROS, DType.TIMESTAMP_MICROSECONDS, packed, 0))
                    .isEqualTo(epochMillis * 1000);
        }
    }
}
