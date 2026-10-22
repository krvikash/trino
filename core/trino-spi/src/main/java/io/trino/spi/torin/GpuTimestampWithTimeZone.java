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
import io.trino.spi.type.Type;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.addExact;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;

/// The cuDF representation of a Trino `timestamp(p) with time zone`, and the only place that knows
/// its shape. It is the UTC instant alone, as `TIMESTAMP_*` at the resolution `timestamp(p)` uses.
///
/// The zone is not part of it. A Trino value carries one, so this represents only the values whose
/// zone is UTC, and [#instantTicks] rejects every other value rather than dropping the zone. Trino
/// compares, orders, groups and hashes a `timestamp with time zone` by instant alone, so over the
/// values it does represent the cuDF column behaves exactly as the Trino type does, which is what
/// lets the type be used anywhere a GPU operator accepts one.
///
/// Parquet is the reason the restriction costs little: the Delta, Iceberg and Hive readers all
/// produce UTC, so a scanned column is representable throughout.
public final class GpuTimestampWithTimeZone
{
    private GpuTimestampWithTimeZone() {}

    /// Resolution of the column, mirroring the carrier of `timestamp(p)` of the same precision.
    /// Empty when cuDF has no resolution fine enough, which is precision 10 and finer.
    ///
    /// Only the nanosecond carrier, precision 7 and finer, is narrower than the value it holds: it
    /// spans roughly 1677 to 2262, and [#instantTicks] rejects an instant outside it. The
    /// microsecond carrier is wider than the value, because `packDateTimeWithZone` keeps the zone in
    /// the low 12 bits and so already caps every instant at 2^51 milliseconds.
    public static Optional<DType> instantDType(int precision)
    {
        if (precision == 0) {
            return Optional.of(DType.TIMESTAMP_SECONDS);
        }
        if (precision <= 3) {
            return Optional.of(DType.TIMESTAMP_MILLISECONDS);
        }
        if (precision <= 6) {
            return Optional.of(DType.TIMESTAMP_MICROSECONDS);
        }
        if (precision <= 9) {
            return Optional.of(DType.TIMESTAMP_NANOSECONDS);
        }
        return Optional.empty();
    }

    /// Instant of a value in the ticks of `instantType`, which has to be the carrier
    /// [#instantDType] gives for `type`. A value of the declared precision is a whole number of
    /// ticks, so this is exact. `type` names the value in the errors raised for a zone that is not
    /// UTC and for an instant a nanosecond carrier cannot hold.
    public static long instantTicks(Type type, DType instantType, long packedEpochMillis, int picosOfMilli)
    {
        TimeZoneKey zoneKey = unpackZoneKey(packedEpochMillis);
        if (!zoneKey.equals(UTC_KEY)) {
            throw zoneNotSupported(type, zoneKey);
        }
        long millisUtc = unpackMillisUtc(packedEpochMillis);
        return switch (instantType.getTypeId()) {
            case TIMESTAMP_SECONDS -> millisUtc / MILLISECONDS_PER_SECOND;
            case TIMESTAMP_MILLISECONDS -> millisUtc;
            // The packed value caps millisUtc at 2^51, so a microsecond count of it stays in the long
            case TIMESTAMP_MICROSECONDS -> addExact(multiplyExact(millisUtc, MICROSECONDS_PER_MILLISECOND), picosOfMilli / PICOSECONDS_PER_MICROSECOND);
            case TIMESTAMP_NANOSECONDS -> nanosecondTicks(type, millisUtc, picosOfMilli);
            default -> throw new IllegalArgumentException("Unexpected instant type: " + instantType);
        };
    }

    /// [#instantTicks] of a value of the long representation.
    public static long instantTicks(Type type, DType instantType, LongTimestampWithTimeZone timestamp)
    {
        return instantTicks(
                type,
                instantType,
                packDateTimeWithZone(timestamp.getEpochMillis(), timestamp.getTimeZoneKey()),
                timestamp.getPicosOfMilli());
    }

    /// Whether a Trino native value of the type is in the one zone the representation holds. Lets a
    /// caller that still has a choice, such as expression compilation, leave the value to the CPU
    /// instead of reaching [#instantTicks] and failing.
    public static boolean hasUtcZone(Object trinoNativeValue)
    {
        return switch (trinoNativeValue) {
            case Long packedEpochMillis -> unpackZoneKey(packedEpochMillis).equals(UTC_KEY);
            case LongTimestampWithTimeZone timestamp -> timestamp.getTimeZoneKey() == UTC_KEY.getKey();
            default -> throw new IllegalArgumentException("Unexpected value type: " + trinoNativeValue.getClass().getName());
        };
    }

    /// The same instant as a Trino native value of the type, in UTC. Trino compares a
    /// `timestamp with time zone` by instant alone, so a value that is only compared against, as in
    /// an IN-list or a dynamic filter, can be moved to UTC and so into the representation, where a
    /// value that is returned cannot.
    public static Object withUtcZone(Object trinoNativeValue)
    {
        return switch (trinoNativeValue) {
            case Long packedEpochMillis -> packDateTimeWithZone(unpackMillisUtc(packedEpochMillis), UTC_KEY);
            case LongTimestampWithTimeZone timestamp -> LongTimestampWithTimeZone.fromEpochMillisAndFraction(timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), UTC_KEY);
            default -> throw new IllegalArgumentException("Unexpected value type: " + trinoNativeValue.getClass().getName());
        };
    }

    /// A zone the representation drops rather than carries would turn into a different rendering of
    /// the same instant, so a value that has one fails here instead.
    private static TrinoException zoneNotSupported(Type type, TimeZoneKey zoneKey)
    {
        return new TrinoException(
                NOT_SUPPORTED,
                "%s value not supported by GPU: time zone must be UTC, was %s".formatted(type.getDisplayName(), zoneKey.getId()));
    }

    private static long nanosecondTicks(Type type, long millisUtc, int picosOfMilli)
    {
        try {
            return addExact(multiplyExact(millisUtc, NANOSECONDS_PER_MILLISECOND), picosOfMilli / PICOSECONDS_PER_NANOSECOND);
        }
        catch (ArithmeticException _) {
            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(
                    floorDiv(millisUtc, MILLISECONDS_PER_SECOND),
                    floorMod(millisUtc, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + picosOfMilli / PICOSECONDS_PER_NANOSECOND,
                    ZoneOffset.UTC);
            throw new TrinoException(
                    NUMERIC_VALUE_OUT_OF_RANGE,
                    "Timestamp value out of range support for GPU for type %s: %s".formatted(type.getDisplayName(), localDateTime));
        }
    }

    /// Trino's packed epoch milliseconds, in UTC, the inverse of [#instantTicks] together with
    /// [#picosOfMilli]. Every tick it reads came either from a column this class describes or from a
    /// GPU expression producing the type, both of which stay inside the range the packed value
    /// holds, so the exact arithmetic here guards an invariant rather than a value a query can
    /// reach.
    public static long packedEpochMillis(DType instantType, long instantTicks)
    {
        long millisUtc = switch (instantType.getTypeId()) {
            case TIMESTAMP_SECONDS -> multiplyExact(instantTicks, MILLISECONDS_PER_SECOND);
            case TIMESTAMP_MILLISECONDS -> instantTicks;
            case TIMESTAMP_MICROSECONDS -> floorDiv(instantTicks, MICROSECONDS_PER_MILLISECOND);
            case TIMESTAMP_NANOSECONDS -> floorDiv(instantTicks, NANOSECONDS_PER_MILLISECOND);
            default -> throw new IllegalArgumentException("Unexpected instant type: " + instantType);
        };
        return packDateTimeWithZone(millisUtc, UTC_KEY);
    }

    /// Sub-millisecond part of an instant, zero for a resolution that has none.
    public static int picosOfMilli(DType instantType, long instantTicks)
    {
        return switch (instantType.getTypeId()) {
            case TIMESTAMP_SECONDS, TIMESTAMP_MILLISECONDS -> 0;
            case TIMESTAMP_MICROSECONDS -> toIntExact(floorMod(instantTicks, MICROSECONDS_PER_MILLISECOND) * (long) PICOSECONDS_PER_MICROSECOND);
            case TIMESTAMP_NANOSECONDS -> toIntExact(floorMod(instantTicks, NANOSECONDS_PER_MILLISECOND) * (long) PICOSECONDS_PER_NANOSECOND);
            default -> throw new IllegalArgumentException("Unexpected instant type: " + instantType);
        };
    }
}
