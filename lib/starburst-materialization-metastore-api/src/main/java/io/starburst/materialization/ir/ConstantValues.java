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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.starburst.materialization.ir.ConstantValue.BooleanValue;
import io.starburst.materialization.ir.ConstantValue.BytesValue;
import io.starburst.materialization.ir.ConstantValue.DoubleValue;
import io.starburst.materialization.ir.ConstantValue.Int128Value;
import io.starburst.materialization.ir.ConstantValue.LongValue;
import io.starburst.materialization.ir.ConstantValue.NullValue;
import io.starburst.materialization.ir.ConstantValue.RealValue;
import io.starburst.materialization.ir.ConstantValue.TimeWithZoneValue;
import io.starburst.materialization.ir.ConstantValue.TimestampValue;
import io.starburst.materialization.ir.ConstantValue.TimestampWithZoneValue;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.starburst.materialization.ir.IrMessages.describe;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.CHAR;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.DECIMAL;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.StandardTypes.IPADDRESS;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.StandardTypes.REAL;
import static io.trino.spi.type.StandardTypes.SMALLINT;
import static io.trino.spi.type.StandardTypes.TIME;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.StandardTypes.UUID;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

/**
 * Converts between a constant's native value, as the engine holds it for a {@link Type}, and its durable
 * {@link ConstantValue} payload.
 * <p>
 * A type is supported iff its base name <em>and</em> its {@code Type#getJavaType()} holder match an entry below;
 * a holder alone never admits a type, and anything unmatched is unsupported. Payloads mirror the native
 * representation, except where it embeds a bit packing or a build-local ordinal table ({@code TimeZoneKey}): there
 * the semantic fields are stored instead.
 * <p>
 * {@link #encode} never produces a constant of a session-sensitive base type ({@code timestamp with time zone},
 * {@code time with time zone}, {@code char}): the optimizer folds a coerced literal into such a constant under the
 * planning session's time zone or {@code legacy_varchar_to_char_coercion} setting, so the stored value depends on the
 * session that planned it. {@link #decode} and the payload kinds support these types.
 * <p>
 * Every supported base name carries the version of this module's belief about its native representation, stamped
 * as a {@code ctype:} key by every non-null {@link Constant} of that type. When a type's native representation
 * changes, bump its entry in {@code TYPE_ENCODING_VERSIONS}, update its pin in {@code TestConstantEncodingPins},
 * and update the affected golden fixtures.
 */
public final class ConstantValues
{
    // UnknownType, the type of a NULL literal, lives in trino-main
    private static final String UNKNOWN = "unknown";

    // Base types whose constants are folded under a session setting, so their values are not session-independent
    static final Set<String> SESSION_SENSITIVE_BASE_TYPES = ImmutableSet.of(TIMESTAMP_WITH_TIME_ZONE, TIME_WITH_TIME_ZONE, CHAR);

    private static final Map<String, Integer> TYPE_ENCODING_VERSIONS = ImmutableMap.<String, Integer>builder()
            .put(BOOLEAN, 1)
            .put(TINYINT, 1)
            .put(SMALLINT, 1)
            .put(INTEGER, 1)
            .put(BIGINT, 1)
            .put(REAL, 1)
            .put(DOUBLE, 1)
            .put(DECIMAL, 1)
            .put(DATE, 1)
            .put(TIME, 1)
            .put(TIME_WITH_TIME_ZONE, 1)
            .put(TIMESTAMP, 1)
            .put(TIMESTAMP_WITH_TIME_ZONE, 1)
            .put(INTERVAL_YEAR_TO_MONTH, 1)
            .put(INTERVAL_DAY_TO_SECOND, 1)
            .put(VARCHAR, 1)
            .put(CHAR, 1)
            .put(VARBINARY, 1)
            .put(JSON, 1)
            .put(UUID, 1)
            .put(IPADDRESS, 1)
            .put(UNKNOWN, 1)
            .buildOrThrow();

    private ConstantValues() {}

    /**
     * Returns the payload for {@code nativeValue}, or empty if {@code type} is not supported, is of a session-sensitive
     * base type (null values included), or the value is not a valid native value of it. Empty is the only way
     * unsupported is reported: nothing here throws for it.
     */
    public static Optional<ConstantValue> encode(Type type, @Nullable Object nativeValue)
    {
        requireNonNull(type, "type is null");
        if (SESSION_SENSITIVE_BASE_TYPES.contains(type.getBaseName())) {
            return Optional.empty();
        }
        if (nativeValue == null) {
            if (supportsNull(type)) {
                return Optional.of(new NullValue());
            }
            return Optional.empty();
        }
        Optional<Kind> kind = kindOf(type);
        if (kind.isEmpty() || !Primitives.wrap(type.getJavaType()).isInstance(nativeValue)) {
            return Optional.empty();
        }
        ConstantValue value;
        try {
            value = encode(kind.get(), nativeValue);
            if (!nativeValue.equals(decode(type, value))) {
                return Optional.empty();
            }
        }
        catch (RuntimeException _) {
            // a native value the codec cannot represent, e.g. a LongTimestampWithTimeZone with an invalid zone key
            return Optional.empty();
        }
        return Optional.of(value);
    }

    /**
     * Returns the native value {@code value} holds for {@code type}, {@code null} for SQL NULL.
     *
     * @throws IllegalArgumentException if the payload cannot be decoded as a value of {@code type}. The message
     *         names the type and the kind, never the value.
     */
    @Nullable
    public static Object decode(Type type, ConstantValue value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        if (value instanceof NullValue) {
            check(supportsNull(type), type, value, "type is not supported");
            return null;
        }
        Kind kind = kindOf(type).orElseThrow(() -> failure(type, value, "type is not supported"));
        check(kind.kind().equals(value.kind()), type, value, "expected kind " + kind.kind());
        Object decoded;
        try {
            decoded = decodeValue(type, value);
        }
        catch (ConstantDecodingException e) {
            throw e;
        }
        catch (RuntimeException _) {
            // A trino-spi factory message can quote the value, so it is neither kept nor chained
            throw new ConstantDecodingException("Constant: cannot decode %s constant of type %s".formatted(value.kind(), describe(type)));
        }
        check(Primitives.wrap(type.getJavaType()).isInstance(decoded), type, value, "decoded value does not match the type's Java type");
        return decoded;
    }

    /**
     * The version of this module's belief about the native representation of {@code type}'s base type.
     */
    public static int encodingVersion(Type type)
    {
        Integer version = TYPE_ENCODING_VERSIONS.get(type.getBaseName());
        checkArgument(version != null, "Constant type %s is not supported", type.getBaseName());
        return version;
    }

    /**
     * Every base type name a {@link Constant} may hold, with its {@link #encodingVersion(Type)}. This includes the
     * session-sensitive base types, which {@link #encode} never produces.
     */
    public static Map<String, Integer> encodingVersions()
    {
        return TYPE_ENCODING_VERSIONS;
    }

    private static boolean supportsNull(Type type)
    {
        if (type.getBaseName().equals(UNKNOWN)) {
            return type.getJavaType() == boolean.class;
        }
        return kindOf(type).isPresent();
    }

    private static Optional<Kind> kindOf(Type type)
    {
        Class<?> javaType = type.getJavaType();
        return switch (type.getBaseName()) {
            case BOOLEAN -> holder(javaType, boolean.class, Kind.BOOLEAN);
            case TINYINT, SMALLINT, INTEGER, BIGINT, DATE, INTERVAL_YEAR_TO_MONTH, INTERVAL_DAY_TO_SECOND -> holder(javaType, long.class, Kind.LONG);
            case REAL -> holder(javaType, long.class, Kind.REAL);
            case DOUBLE -> holder(javaType, double.class, Kind.DOUBLE);
            case DECIMAL -> {
                if (!(type instanceof DecimalType decimalType)) {
                    yield Optional.empty();
                }
                if (decimalType.isShort()) {
                    yield holder(javaType, long.class, Kind.LONG);
                }
                yield holder(javaType, Int128.class, Kind.INT128);
            }
            case TIME -> {
                if (!(type instanceof TimeType)) {
                    yield Optional.empty();
                }
                yield holder(javaType, long.class, Kind.LONG);
            }
            case TIMESTAMP -> {
                if (!(type instanceof TimestampType timestampType)) {
                    yield Optional.empty();
                }
                if (timestampType.isShort()) {
                    yield holder(javaType, long.class, Kind.LONG);
                }
                yield holder(javaType, LongTimestamp.class, Kind.TIMESTAMP);
            }
            case TIMESTAMP_WITH_TIME_ZONE -> {
                if (!(type instanceof TimestampWithTimeZoneType timestampType)) {
                    yield Optional.empty();
                }
                if (timestampType.isShort()) {
                    yield holder(javaType, long.class, Kind.TIMESTAMP_WITH_ZONE);
                }
                yield holder(javaType, LongTimestampWithTimeZone.class, Kind.TIMESTAMP_WITH_ZONE);
            }
            case TIME_WITH_TIME_ZONE -> {
                if (!(type instanceof TimeWithTimeZoneType timeType)) {
                    yield Optional.empty();
                }
                if (timeType.isShort()) {
                    yield holder(javaType, long.class, Kind.TIME_WITH_ZONE);
                }
                yield holder(javaType, LongTimeWithTimeZone.class, Kind.TIME_WITH_ZONE);
            }
            case VARCHAR -> {
                if (!(type instanceof VarcharType)) {
                    yield Optional.empty();
                }
                yield holder(javaType, Slice.class, Kind.BYTES);
            }
            case CHAR -> {
                if (!(type instanceof CharType)) {
                    yield Optional.empty();
                }
                yield holder(javaType, Slice.class, Kind.BYTES);
            }
            case VARBINARY, JSON, UUID, IPADDRESS -> holder(javaType, Slice.class, Kind.BYTES);
            default -> Optional.empty();
        };
    }

    private static Optional<Kind> holder(Class<?> javaType, Class<?> expected, Kind kind)
    {
        if (javaType != expected) {
            return Optional.empty();
        }
        return Optional.of(kind);
    }

    private static ConstantValue encode(Kind kind, Object nativeValue)
    {
        return switch (kind) {
            case BOOLEAN -> new BooleanValue((Boolean) nativeValue);
            case LONG -> new LongValue((Long) nativeValue);
            case REAL -> new RealValue((int) (long) (Long) nativeValue);
            case DOUBLE -> new DoubleValue(doubleToLongBits((Double) nativeValue));
            case BYTES -> new BytesValue((Slice) nativeValue);
            case INT128 -> {
                Int128 value = (Int128) nativeValue;
                yield new Int128Value(value.getHigh(), value.getLow());
            }
            case TIMESTAMP -> {
                LongTimestamp value = (LongTimestamp) nativeValue;
                yield new TimestampValue(value.getEpochMicros(), value.getPicosOfMicro());
            }
            case TIMESTAMP_WITH_ZONE -> {
                if (nativeValue instanceof LongTimestampWithTimeZone value) {
                    yield new TimestampWithZoneValue(value.getEpochMillis(), value.getPicosOfMilli(), TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey()).getId());
                }
                long packed = (Long) nativeValue;
                yield new TimestampWithZoneValue(unpackMillisUtc(packed), 0, unpackZoneKey(packed).getId());
            }
            case TIME_WITH_ZONE -> {
                if (nativeValue instanceof LongTimeWithTimeZone value) {
                    yield new TimeWithZoneValue(value.getPicoseconds(), value.getOffsetMinutes());
                }
                long packed = (Long) nativeValue;
                yield new TimeWithZoneValue(unpackTimeNanos(packed) * PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(packed));
            }
        };
    }

    private static Object decodeValue(Type type, ConstantValue value)
    {
        return switch (value) {
            case NullValue _ -> throw new IllegalStateException("null is decoded before dispatch");
            case BooleanValue booleanValue -> booleanValue.value();
            case LongValue longValue -> decodeLong(type, longValue);
            case RealValue realValue -> {
                check(floatToIntBits(intBitsToFloat(realValue.bits())) == realValue.bits(), type, value, "bits are not canonical");
                // sign-extended, as RealType requires
                yield (long) realValue.bits();
            }
            case DoubleValue doubleValue -> {
                check(doubleToLongBits(longBitsToDouble(doubleValue.bits())) == doubleValue.bits(), type, value, "bits are not canonical");
                yield longBitsToDouble(doubleValue.bits());
            }
            case BytesValue bytesValue -> decodeBytes(type, bytesValue);
            case Int128Value int128Value -> {
                Int128 decoded = Int128.valueOf(int128Value.high(), int128Value.low());
                check(!overflows(decoded, ((DecimalType) type).getPrecision()), type, value, "value overflows the precision");
                yield decoded;
            }
            case TimestampValue timestampValue -> {
                int precision = ((TimestampType) type).getPrecision();
                int picosOfMicro = timestampValue.picosOfMicro();
                check(picosOfMicro >= 0 && picosOfMicro < PICOSECONDS_PER_MICROSECOND, type, value, "picosOfMicro is out of range");
                check(picosOfMicro % powerOfTen(12 - precision) == 0, type, value, "picosOfMicro exceeds the precision");
                yield new LongTimestamp(timestampValue.epochMicros(), picosOfMicro);
            }
            case TimestampWithZoneValue timestampValue -> decodeTimestampWithZone((TimestampWithTimeZoneType) type, timestampValue);
            case TimeWithZoneValue timeValue -> decodeTimeWithZone((TimeWithTimeZoneType) type, timeValue);
        };
    }

    private static long decodeLong(Type type, LongValue value)
    {
        long decoded = value.value();
        switch (type) {
            case DecimalType decimalType -> check(!overflows(decoded, decimalType.getPrecision()), type, value, "value overflows the precision");
            case TimeType timeType -> {
                check(decoded >= 0 && decoded < PICOSECONDS_PER_DAY, type, value, "picoseconds of day is out of range");
                check(decoded % powerOfTen(12 - timeType.getPrecision()) == 0, type, value, "picoseconds exceed the precision");
            }
            case TimestampType timestampType -> check(decoded % powerOfTen(6 - timestampType.getPrecision()) == 0, type, value, "epochMicros exceeds the precision");
            default -> {
                switch (type.getBaseName()) {
                    case TINYINT -> check(decoded == (byte) decoded, type, value, "value is out of range");
                    case SMALLINT -> check(decoded == (short) decoded, type, value, "value is out of range");
                    case INTEGER, DATE, INTERVAL_YEAR_TO_MONTH -> check(decoded == (int) decoded, type, value, "value is out of range");
                    case BIGINT, INTERVAL_DAY_TO_SECOND -> {}
                    default -> throw failure(type, value, "type is not supported");
                }
            }
        }
        return decoded;
    }

    private static Slice decodeBytes(Type type, BytesValue value)
    {
        Slice bytes = value.bytes();
        switch (type) {
            case CharType charType -> {
                check(countCodePoints(bytes) <= charType.getLength(), type, value, "value exceeds the length");
                check(bytes.length() == 0 || bytes.getByte(bytes.length() - 1) != ' ', type, value, "value has a trailing space");
            }
            case VarcharType varcharType -> check(varcharType.isUnbounded() || countCodePoints(bytes) <= varcharType.getBoundedLength(), type, value, "value exceeds the length");
            default -> {
                switch (type.getBaseName()) {
                    case UUID, IPADDRESS -> check(bytes.length() == 16, type, value, "expected 16 bytes");
                    case VARBINARY, JSON -> {}
                    default -> throw failure(type, value, "type is not supported");
                }
            }
        }
        return bytes;
    }

    private static Object decodeTimestampWithZone(TimestampWithTimeZoneType type, TimestampWithZoneValue value)
    {
        int precision = type.getPrecision();
        TimeZoneKey zone = TimeZoneKey.getTimeZoneKey(value.zoneId());
        if (type.isShort()) {
            check(value.picosOfMilli() == 0, type, value, "picosOfMilli exceeds the precision");
            check(value.epochMillis() % powerOfTen(3 - precision) == 0, type, value, "epochMillis exceeds the precision");
            // packDateTimeWithZone enforces the representable epochMillis range
            return packDateTimeWithZone(value.epochMillis(), zone);
        }
        check(value.picosOfMilli() >= 0 && value.picosOfMilli() < PICOSECONDS_PER_MILLISECOND, type, value, "picosOfMilli is out of range");
        check(value.picosOfMilli() % powerOfTen(12 - precision) == 0, type, value, "picosOfMilli exceeds the precision");
        // the constructor enforces the representable epochMillis range
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(value.epochMillis(), value.picosOfMilli(), zone);
    }

    private static Object decodeTimeWithZone(TimeWithTimeZoneType type, TimeWithZoneValue value)
    {
        long picoseconds = value.picoseconds();
        int offsetMinutes = value.offsetMinutes();
        check(picoseconds >= 0 && picoseconds < PICOSECONDS_PER_DAY, type, value, "picoseconds of day is out of range");
        check(picoseconds % powerOfTen(12 - type.getPrecision()) == 0, type, value, "picoseconds exceed the precision");
        // the 11-bit two's-complement range the packed form holds without aliasing
        check(offsetMinutes >= -1024 && offsetMinutes <= 1023, type, value, "offsetMinutes is out of range");
        if (type.isShort()) {
            return packTimeWithTimeZone(picoseconds / PICOSECONDS_PER_NANOSECOND, offsetMinutes);
        }
        return new LongTimeWithTimeZone(picoseconds, offsetMinutes);
    }

    private static long powerOfTen(int exponent)
    {
        return LongMath.pow(10, exponent);
    }

    private static void check(boolean condition, Type type, ConstantValue value, String reason)
    {
        if (!condition) {
            throw failure(type, value, reason);
        }
    }

    private static ConstantDecodingException failure(Type type, ConstantValue value, String reason)
    {
        return new ConstantDecodingException("Constant: cannot decode %s constant of type %s: %s".formatted(value.kind(), describe(type), reason));
    }

    private enum Kind
    {
        BOOLEAN(BooleanValue.KIND),
        LONG(LongValue.KIND),
        REAL(RealValue.KIND),
        DOUBLE(DoubleValue.KIND),
        BYTES(BytesValue.KIND),
        INT128(Int128Value.KIND),
        TIMESTAMP(TimestampValue.KIND),
        TIMESTAMP_WITH_ZONE(TimestampWithZoneValue.KIND),
        TIME_WITH_ZONE(TimeWithZoneValue.KIND);

        private final String kind;

        Kind(String kind)
        {
            this.kind = kind;
        }

        public String kind()
        {
            return kind;
        }
    }

    /**
     * A payload that cannot be decoded. Its message names the type and the kind, never the value.
     */
    private static final class ConstantDecodingException
            extends IllegalArgumentException
    {
        ConstantDecodingException(String message)
        {
            super(message);
        }
    }
}
