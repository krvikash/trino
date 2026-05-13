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
package com.starburstdata.plugin.kdb;

import com.google.common.annotations.VisibleForTesting;
import com.kx.c;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarcharType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.createVarcharType;

/**
 * Because kdb+ is a typed, columnar database built for performance, every column is a primitive array.
 * There's no room for a nullable wrapper — so kdb+ designates one specific value per type
 * as "this slot is null." The connector has to recognize these magic values and convert
 * them to SQL {@code NULL} before handing data to Trino.
 * <pre>
 * ┌────────────────┬────────────────────────────┬───────────────────────────────────────────┐
 * │   kdb+ type    │       kdb+ null            │              Why that value               │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ h (short)      │ 0Nh = Short.MIN_VALUE      │ Most extreme value                        │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ i (int)        │ 0Ni = Integer.MIN_VALUE    │ Same idea                                 │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ j (long)       │ 0Nj = Long.MIN_VALUE       │ Same idea                                 │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ e (real/float) │ 0Ne = Float.NaN            │ IEEE 754 NaN                              │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ f (double)     │ 0n = Double.NaN            │ IEEE 754 NaN                              │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ c (char)       │ ' ' (space)                │ kdb+ convention for null char             │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ s (symbol)     │ ` (empty symbol)           │ kdb+ convention                           │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ g (GUID)       │ 0Ng = all-zero UUID        │ kdb+ convention                           │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ p (timestamp)  │ 0Np = Long.MIN_VALUE nanos │ Most extreme value                        │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ b (boolean)    │ (none)                     │ Booleans can't be null in kdb+            │
 * ├────────────────┼────────────────────────────┼───────────────────────────────────────────┤
 * │ x (byte)       │ (none)                     │ Same                                      │
 * └────────────────┴────────────────────────────┴───────────────────────────────────────────┘
 * </pre>
 */
public final class KdbTypeMapping
{
    private static final int UUID_STRING_LENGTH = 36;
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    @VisibleForTesting
    static final int KDB_EPOCH_DAYS_OFFSET = 10957;
    // kdb+ month values are offsets from Jan 2000. This prevents errors for dates before 2000
    private static final int KDB_EPOCH_MONTHS = 2000 * 12;
    private static final long KDB_EPOCH_MILLIS_OFFSET = KDB_EPOCH_DAYS_OFFSET * 24L * 60L * 60L * 1000L;
    private static final long KDB_EPOCH_NANOS_OFFSET = KDB_EPOCH_DAYS_OFFSET * 24L * 60L * 60L * 1_000_000_000L;

    private KdbTypeMapping() {}

    public static Optional<ColumnMapping> toColumnMapping(char kdbType)
    {
        ColumnMapping mapping = switch (kdbType) {
            case 'b' -> booleanMapping();
            case 'g' -> guidMapping();
            case 'x' -> byteMapping();
            case 'h' -> shortMapping();
            case 'i' -> intMapping();
            case 'j' -> longMapping();
            case 'e' -> realMapping();
            case 'f' -> doubleMapping();
            case 'c' -> charMapping();
            case 'C' -> charArrayMapping();
            case 's' -> symbolMapping();
            case 'p' -> timestampMapping();
            case 'm' -> monthMapping();
            case 'd' -> dateMapping();
            case 'z' -> datetimeMapping();
            case 'n' -> timespanMapping();
            case 'u' -> minuteMapping();
            case 'v' -> secondMapping();
            case 't' -> timeMapping();
            default -> null;
        };
        return Optional.ofNullable(mapping);
    }

    private static ColumnMapping booleanMapping()
    {
        return new ColumnMapping(BooleanType.BOOLEAN, Byte.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null;
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                return value;
            }
        });
    }

    private static ColumnMapping guidMapping()
    {
        return new ColumnMapping(createVarcharType(UUID_STRING_LENGTH), 16L, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || value.equals(new UUID(0, 0));
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof UUID) {
                    return utf8Slice(value.toString());
                }
                return utf8Slice(String.valueOf(value));
            }
        });
    }

    private static ColumnMapping byteMapping()
    {
        return new ColumnMapping(TinyintType.TINYINT, Byte.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null;
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Byte b) {
                    return b.longValue();
                }
                return ((Number) value).longValue();
            }
        });
    }

    private static ColumnMapping shortMapping()
    {
        return new ColumnMapping(SmallintType.SMALLINT, Short.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || value.equals(Short.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Short s) {
                    return s.longValue();
                }
                return ((Number) value).longValue();
            }
        });
    }

    private static ColumnMapping intMapping()
    {
        return new ColumnMapping(IntegerType.INTEGER, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || value.equals(Integer.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Integer i) {
                    return i.longValue();
                }
                return ((Number) value).longValue();
            }
        });
    }

    private static ColumnMapping longMapping()
    {
        return new ColumnMapping(BigintType.BIGINT, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || value.equals(Long.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Long l) {
                    return l;
                }
                return ((Number) value).longValue();
            }
        });
    }

    private static ColumnMapping realMapping()
    {
        return new ColumnMapping(RealType.REAL, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || (value instanceof Float f && f.isNaN());
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Float f) {
                    return Float.floatToIntBits(f);
                }
                return Float.floatToIntBits(((Number) value).floatValue());
            }
        });
    }

    private static ColumnMapping doubleMapping()
    {
        return new ColumnMapping(DoubleType.DOUBLE, Long.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || (value instanceof Double d && d.isNaN());
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Double d) {
                    return d;
                }
                return ((Number) value).doubleValue();
            }
        });
    }

    private static ColumnMapping charMapping()
    {
        return new ColumnMapping(VarcharType.VARCHAR, Byte.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || value.equals(' ');
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Character c) {
                    return utf8Slice(String.valueOf((char) c));
                }
                String s = value.toString();
                return utf8Slice(s.isEmpty() ? " " : s.substring(0, 1));
            }
        });
    }

    private static ColumnMapping charArrayMapping()
    {
        return new ColumnMapping(VarcharType.VARCHAR, Long.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null;
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof char[] array) {
                    return utf8Slice(String.valueOf(array));
                }
                return utf8Slice(String.valueOf(value));
            }
        });
    }

    private static ColumnMapping symbolMapping()
    {
        return new ColumnMapping(VarcharType.VARCHAR, 20L, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null || value.equals("") || value.equals("`");
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                return utf8Slice(String.valueOf(value));
            }
        });
    }

    private static ColumnMapping timestampMapping()
    {
        return new ColumnMapping(TimestampType.TIMESTAMP_MICROS, Long.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof Timestamp timestamp && timestamp.getTime() == Long.MIN_VALUE)
                        || (value instanceof Instant instant && instant.equals(Instant.MIN))
                        || (value instanceof Long l && l == Long.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Timestamp ts) {
                    long millis = ts.getTime();
                    long nanos = ts.getNanos() % 1_000_000;
                    return millis * 1000 + nanos / 1000;
                }
                if (value instanceof Instant instant) {
                    return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
                }
                if (value instanceof Long l) {
                    long kdbNanos = l;
                    long epochNanos = kdbNanos + KDB_EPOCH_NANOS_OFFSET;
                    return epochNanos / 1000;
                }
                return 0L;
            }
        });
    }

    private static ColumnMapping monthMapping()
    {
        return new ColumnMapping(VarcharType.VARCHAR, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof Integer i && i == Integer.MIN_VALUE)
                        || (value instanceof c.Month m && m.i == Integer.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof c.Month month) {
                    int v = month.i + KDB_EPOCH_MONTHS;
                    int year = v / 12;
                    int m = 1 + v % 12;
                    return utf8Slice(String.format("%04d.%02dm", year, m));
                }
                return utf8Slice(String.valueOf(value));
            }
        });
    }

    private static ColumnMapping dateMapping()
    {
        return new ColumnMapping(DateType.DATE, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof Date date && date.getTime() == Long.MIN_VALUE)
                        || (value instanceof Integer i && i == Integer.MIN_VALUE)
                        || (value instanceof LocalDate ld && ld.equals(LocalDate.MIN));
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Date date) {
                    return TimeUnit.MILLISECONDS.toDays(date.getTime());
                }
                if (value instanceof Integer i) {
                    int kdbDays = i;
                    return (long) kdbDays + KDB_EPOCH_DAYS_OFFSET;
                }
                if (value instanceof LocalDate date) {
                    return date.toEpochDay();
                }
                return 0L;
            }
        });
    }

    private static ColumnMapping datetimeMapping()
    {
        return new ColumnMapping(TimestampType.TIMESTAMP_MILLIS, Long.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof Timestamp timestamp && timestamp.getTime() == Long.MIN_VALUE)
                        || (value instanceof Double d && d.isNaN())
                        || (value instanceof LocalDateTime ldt && ldt.equals(LocalDateTime.MIN));
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Timestamp timestamp) {
                    return timestamp.getTime() * 1000;
                }
                if (value instanceof Double d) {
                    double kdbDays = d;
                    long millis = (long) (kdbDays * MILLIS_PER_DAY) + KDB_EPOCH_MILLIS_OFFSET;
                    return millis * 1000;
                }
                if (value instanceof LocalDateTime ldt) {
                    Instant instant = ldt.toInstant(ZoneOffset.UTC);
                    return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
                }
                return 0L;
            }
        });
    }

    private static ColumnMapping timespanMapping()
    {
        return new ColumnMapping(BigintType.BIGINT, Long.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof Long l && l == Long.MIN_VALUE)
                        || (value instanceof c.Timespan ts && ts.j == Long.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof c.Timespan ts) {
                    return ts.j;
                }
                if (value instanceof Long l) {
                    return l;
                }
                return ((Number) value).longValue();
            }
        });
    }

    private static ColumnMapping minuteMapping()
    {
        return new ColumnMapping(IntegerType.INTEGER, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof c.Minute m && m.i == Integer.MIN_VALUE)
                        || (value instanceof Integer i && i == Integer.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof c.Minute minute) {
                    return minute.i;
                }
                if (value instanceof Number number) {
                    return number.longValue();
                }
                return 0L;
            }
        });
    }

    private static ColumnMapping secondMapping()
    {
        return new ColumnMapping(IntegerType.INTEGER, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof c.Second s && s.i == Integer.MIN_VALUE)
                        || (value instanceof Integer i && i == Integer.MIN_VALUE);
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof c.Second second) {
                    return second.i;
                }
                if (value instanceof Number number) {
                    return number.longValue();
                }
                return 0L;
            }
        });
    }

    private static ColumnMapping timeMapping()
    {
        return new ColumnMapping(TimeType.TIME_MILLIS, Integer.BYTES, new ReadFunction()
        {
            @Override
            public boolean isNull(Object value)
            {
                return value == null
                        || (value instanceof Time time && time.getTime() == Long.MIN_VALUE)
                        || (value instanceof Integer i && i == Integer.MIN_VALUE)
                        || (value instanceof LocalTime lt && lt.equals(c.LOCAL_TIME_NULL));
            }

            @Override
            public Object toTrinoValue(Object value)
            {
                if (value instanceof Time time) {
                    return (time.getTime() % MILLIS_PER_DAY) * 1_000_000_000L;
                }
                if (value instanceof Integer i) {
                    int kdbMillis = i;
                    return kdbMillis * 1_000_000_000L;
                }
                if (value instanceof c.Timespan timespan) {
                    return timespan.j * 1000L;
                }
                if (value instanceof LocalTime lt) {
                    return lt.toNanoOfDay() * 1000L;
                }
                return 0L;
            }
        });
    }
}
