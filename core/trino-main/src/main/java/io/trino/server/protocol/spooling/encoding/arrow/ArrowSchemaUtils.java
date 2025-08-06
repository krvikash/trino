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
package io.trino.server.protocol.spooling.encoding.arrow;

import com.google.common.collect.ImmutableList;
import io.trino.server.protocol.OutputColumn;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.IpAddressType;
import io.trino.type.UnknownType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.PRECISION_MICROS;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.PRECISION_MILLIS;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.PRECISION_NANOS;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.PRECISION_SECONDS;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIMESTAMP_VECTOR_NAME;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIMEZONE_VECTOR_NAME;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIME_OFFSET_VECTOR_NAME;
import static io.trino.client.spooling.encoding.arrow.ArrowDateTimeUtils.TIME_VECTOR_NAME;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
import static org.apache.arrow.vector.types.IntervalUnit.DAY_TIME;
import static org.apache.arrow.vector.types.IntervalUnit.YEAR_MONTH;
import static org.apache.arrow.vector.types.pojo.FieldType.notNullable;
import static org.apache.arrow.vector.types.pojo.FieldType.nullable;

public final class ArrowSchemaUtils
{
    public static final ArrowType TINYINT_ARROW_TYPE = new ArrowType.Int(8, true);
    public static final ArrowType SMALLINT_ARROW_TYPE = new ArrowType.Int(16, true);
    public static final ArrowType INTEGER_ARROW_TYPE = new ArrowType.Int(32, true);
    public static final ArrowType BIGINT_ARROW_TYPE = new ArrowType.Int(64, true);
    public static final ArrowType REAL_ARROW_TYPE = new ArrowType.FloatingPoint(SINGLE);
    public static final ArrowType DOUBLE_ARROW_TYPE = new ArrowType.FloatingPoint(DOUBLE);
    public static final ArrowType DATE_ARROW_TYPE = new ArrowType.Date(DateUnit.DAY);
    public static final ArrowType INTERVAL_DAY_TIME_ARROW_TYPE = new ArrowType.Interval(DAY_TIME);
    public static final ArrowType INTERVAL_YEAR_MONTH_ARROW_TYPE = new ArrowType.Interval(YEAR_MONTH);
    public static final ArrowType MAP_ARROW_TYPE = new ArrowType.Map(false);
    public static final ArrowType TIME_SEC_ARROW_TYPE = new ArrowType.Time(TimeUnit.SECOND, 32);
    public static final ArrowType TIME_MILLI_ARROW_TYPE = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    public static final ArrowType TIME_MICRO_ARROW_TYPE = new ArrowType.Time(TimeUnit.MICROSECOND, 64);
    public static final ArrowType TIME_NANO_ARROW_TYPE = new ArrowType.Time(TimeUnit.NANOSECOND, 64);
    public static final ArrowType TIMESTAMP_SEC_ARROW_TYPE = new ArrowType.Timestamp(TimeUnit.SECOND, null);
    public static final ArrowType TIMESTAMP_MILLI_ARROW_TYPE = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    public static final ArrowType TIMESTAMP_MICRO_ARROW_TYPE = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    public static final ArrowType TIMESTAMP_NANO_ARROW_TYPE = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);

    private ArrowSchemaUtils() {}

    public static Schema toArrowSchema(List<OutputColumn> columns)
    {
        return new Schema(columns.stream()
                .map(column -> toArrowField(column.columnName(), column.type(), true))
                .collect(toImmutableList()));
    }

    public static Field toArrowField(String name, Type type, boolean nullable)
    {
        return switch (type) {
            case ArrayType arrayType -> new Field(name, new FieldType(nullable, toArrowType(type), null), List.of(
                    toArrowField("element", arrayType.getElementType(), nullable))); // Children

            case MapType mapType -> {
                Field child = new Field("entries", notNullable(ArrowType.Struct.INSTANCE), List.of(
                        toArrowField("key", mapType.getKeyType(), false), // Keys are not nullable
                        toArrowField("value", mapType.getValueType(), nullable))); // Values can be

                yield new Field(name, nullable(toArrowType(type)), List.of(child));
            }
            case RowType rowType -> {
                List<Field> children = rowType.getFields().stream()
                        .map(field -> toArrowField(field.getName().orElse(""), field.getType(), nullable))
                        .collect(toImmutableList());
                yield new Field(name, nullableField(toArrowType(type), nullable), children);
            }
            case TimeWithTimeZoneType timeZoneType -> {
                List<Field> child = List.of(
                        toArrowField(TIME_VECTOR_NAME, createTimeType(timeZoneType.getPrecision()), nullable),
                        toArrowField(TIME_OFFSET_VECTOR_NAME, SMALLINT, nullable));
                yield new Field(name, nullable(ArrowType.Struct.INSTANCE), child);
            }
            case TimestampWithTimeZoneType timestampWithTimeZoneType -> {
                List<Field> child = List.of(
                        toArrowField(TIMESTAMP_VECTOR_NAME, createTimestampType(timestampWithTimeZoneType.getPrecision()), nullable),
                        toArrowField(TIMEZONE_VECTOR_NAME, VARCHAR, nullable));
                yield new Field(name, nullable(ArrowType.Struct.INSTANCE), child);
            }
            default -> new Field(name, nullableField(toArrowType(type), nullable), null);
        };
    }

    private static ArrowType toArrowType(Type type)
    {
        return switch (type) {
            case BooleanType _ -> ArrowType.Bool.INSTANCE;
            case TinyintType _ -> TINYINT_ARROW_TYPE;
            case SmallintType _ -> SMALLINT_ARROW_TYPE;
            case IntegerType _ -> INTEGER_ARROW_TYPE;
            case BigintType _ -> BIGINT_ARROW_TYPE;
            case RealType _ -> REAL_ARROW_TYPE;
            case DoubleType _ -> DOUBLE_ARROW_TYPE;
            case VarcharType _, CharType _ -> ArrowType.Utf8.INSTANCE;
            case VarbinaryType _ -> ArrowType.Binary.INSTANCE;
            case DateType _ -> DATE_ARROW_TYPE;
            case TimeType time -> switch (time.getPrecision()) {
                case PRECISION_SECONDS -> TIME_SEC_ARROW_TYPE;
                case PRECISION_MILLIS -> TIME_MILLI_ARROW_TYPE;
                case PRECISION_MICROS -> TIME_MICRO_ARROW_TYPE;
                case PRECISION_NANOS -> TIME_NANO_ARROW_TYPE;
                default -> throw unsupportedTypeException(time);
            };
            case TimestampType timestamp -> switch (timestamp.getPrecision()) {
                case PRECISION_SECONDS -> TIMESTAMP_SEC_ARROW_TYPE;
                case PRECISION_MILLIS -> TIMESTAMP_MILLI_ARROW_TYPE;
                case PRECISION_MICROS -> TIMESTAMP_MICRO_ARROW_TYPE;
                case PRECISION_NANOS -> TIMESTAMP_NANO_ARROW_TYPE;
                default -> throw unsupportedTypeException(timestamp);
            };
            case TimeWithTimeZoneType _, TimestampWithTimeZoneType _ -> ArrowType.Struct.INSTANCE;
            case DecimalType decimal -> new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), 128); // Trino decimals are 64 or 128 bits
            case UuidType _, IpAddressType _ -> new ArrowType.FixedSizeBinary(16);
            case HyperLogLogType _ -> ArrowType.Binary.INSTANCE;
            case ArrayType _ -> ArrowType.List.INSTANCE;
            case MapType _ -> MAP_ARROW_TYPE;
            case RowType _ -> ArrowType.Struct.INSTANCE;
            case IntervalDayTimeType _ -> INTERVAL_DAY_TIME_ARROW_TYPE;
            case IntervalYearMonthType _ -> INTERVAL_YEAR_MONTH_ARROW_TYPE;
            case UnknownType _ -> ArrowType.Null.INSTANCE;
            default -> throw unsupportedTypeException(type);
        };
    }

    private static FieldType nullableField(ArrowType type, boolean nullable)
    {
        if (nullable) {
            return nullable(type);
        }
        return notNullable(type);
    }

    public static List<OutputColumn> unsupported(List<OutputColumn> columns)
    {
        ImmutableList.Builder<OutputColumn> builder = ImmutableList.builder();
        for (OutputColumn column : columns) {
            try {
                toArrowField(column.columnName(), column.type(), false);
            }
            catch (UnsupportedOperationException e) {
                builder.add(column);
            }
        }
        return builder.build();
    }

    public static UnsupportedOperationException unsupportedTypeException(Type type)
    {
        return new UnsupportedOperationException("Unsupported type: " + type);
    }
}
