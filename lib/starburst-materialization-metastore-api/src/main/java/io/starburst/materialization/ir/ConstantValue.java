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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Base64;

import static java.util.Objects.requireNonNull;

/**
 * The persisted payload of a {@link Constant}: a native value in a durable form of its own, never the engine's
 * {@code Block} wire format. The {@code kind} discriminator is explicit so a reader can assert it agrees with the
 * constant's type instead of deriving it. {@link ConstantValues} converts between native values and payloads.
 * <p>
 * Each kind is stamped as a {@code const:} key; bump a kind's {@code VERSION} when its JSON shape changes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConstantValue.NullValue.class, name = ConstantValue.NullValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.BooleanValue.class, name = ConstantValue.BooleanValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.LongValue.class, name = ConstantValue.LongValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.RealValue.class, name = ConstantValue.RealValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.DoubleValue.class, name = ConstantValue.DoubleValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.BytesValue.class, name = ConstantValue.BytesValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.Int128Value.class, name = ConstantValue.Int128Value.KIND),
        @JsonSubTypes.Type(value = ConstantValue.TimestampValue.class, name = ConstantValue.TimestampValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.TimestampWithZoneValue.class, name = ConstantValue.TimestampWithZoneValue.KIND),
        @JsonSubTypes.Type(value = ConstantValue.TimeWithZoneValue.class, name = ConstantValue.TimeWithZoneValue.KIND),
})
public sealed interface ConstantValue
        permits ConstantValue.BooleanValue,
                ConstantValue.BytesValue,
                ConstantValue.DoubleValue,
                ConstantValue.Int128Value,
                ConstantValue.LongValue,
                ConstantValue.NullValue,
                ConstantValue.RealValue,
                ConstantValue.TimeWithZoneValue,
                ConstantValue.TimestampValue,
                ConstantValue.TimestampWithZoneValue
{
    /**
     * Stable wire discriminator: the JSON {@code kind} value. Never rename.
     */
    String kind();

    int version();

    /**
     * SQL NULL, of any supported type.
     */
    @JsonTypeName(NullValue.KIND)
    record NullValue()
            implements ConstantValue
    {
        static final String KIND = "null";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    @JsonTypeName(BooleanValue.KIND)
    record BooleanValue(boolean value)
            implements ConstantValue
    {
        static final String KIND = "boolean";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * A {@code long} native value whose meaning is the number itself: an integer, a day, a short decimal's unscaled
     * value, a time's picoseconds of day, an interval, or a short timestamp's epoch microseconds.
     */
    @JsonTypeName(LongValue.KIND)
    record LongValue(long value)
            implements ConstantValue
    {
        static final String KIND = "long";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * A {@code real}, as {@code Float.floatToIntBits}.
     */
    @JsonTypeName(RealValue.KIND)
    record RealValue(int bits)
            implements ConstantValue
    {
        static final String KIND = "real";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * A {@code double}, as {@code Double.doubleToLongBits}.
     */
    @JsonTypeName(DoubleValue.KIND)
    record DoubleValue(long bits)
            implements ConstantValue
    {
        static final String KIND = "double";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * The raw bytes of a {@code Slice} native value, base64 encoded.
     */
    @JsonTypeName(BytesValue.KIND)
    record BytesValue(
            @JsonProperty("base64")
            @JsonSerialize(converter = SliceToBase64.class)
            @JsonDeserialize(converter = Base64ToSlice.class)
            Slice bytes)
            implements ConstantValue
    {
        static final String KIND = "bytes";
        static final int VERSION = 1;

        public BytesValue
        {
            requireNonNull(bytes, "bytes is null");
        }

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * A long decimal's unscaled value.
     */
    @JsonTypeName(Int128Value.KIND)
    record Int128Value(long high, long low)
            implements ConstantValue
    {
        static final String KIND = "int128";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * A long {@code timestamp(p)}, {@code p > 6}.
     */
    @JsonTypeName(TimestampValue.KIND)
    record TimestampValue(long epochMicros, int picosOfMicro)
            implements ConstantValue
    {
        static final String KIND = "timestamp";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * A {@code timestamp(p) with time zone} of any precision. The zone is persisted by id, never by the build-local
     * {@code TimeZoneKey} ordinal both native forms carry.
     */
    @JsonTypeName(TimestampWithZoneValue.KIND)
    record TimestampWithZoneValue(long epochMillis, int picosOfMilli, String zoneId)
            implements ConstantValue
    {
        static final String KIND = "timestampWithZone";
        static final int VERSION = 1;

        public TimestampWithZoneValue
        {
            requireNonNull(zoneId, "zoneId is null");
        }

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    /**
     * A {@code time(p) with time zone} of any precision, unpacked into picoseconds of day and a signed offset.
     */
    @JsonTypeName(TimeWithZoneValue.KIND)
    record TimeWithZoneValue(long picoseconds, int offsetMinutes)
            implements ConstantValue
    {
        static final String KIND = "timeWithZone";
        static final int VERSION = 1;

        @Override
        public String kind()
        {
            return KIND;
        }

        @Override
        public int version()
        {
            return VERSION;
        }
    }

    final class SliceToBase64
            extends StdConverter<Slice, String>
    {
        @Override
        public String convert(Slice value)
        {
            return Base64.getEncoder().encodeToString(value.getBytes());
        }
    }

    final class Base64ToSlice
            extends StdConverter<String, Slice>
    {
        @Override
        public Slice convert(String value)
        {
            return Slices.wrappedBuffer(Base64.getDecoder().decode(value));
        }
    }
}
