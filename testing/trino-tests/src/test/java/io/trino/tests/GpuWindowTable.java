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
package io.trino.tests;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

/// Tables of window input, one per data shape. A shape is a query whose keys and argument are integer ranks:
///
/// - {@code row_id}: unique
/// - {@code partition_rank}, {@code order_rank}, {@code argument_rank}: ranks, NULL for NULL
/// - {@code second_partition_rank}, {@code second_order_rank}: ranks of the second keys, which subdivide partitions
///   and peer groups
/// - {@code start_offset}, {@code end_offset}: frame offsets
///
/// The table holds each key rank as a value of every tested GPU type, and the argument rank as a value of every
/// argument type, mapped by {@link #value(Type, String)}, or for keys of {@link #EDGE_VALUES}, by
/// {@link #edgeValue(Type, String)}. Queries read it through a view per order key type, see
/// {@link #withOrderKey(Type)}, with columns:
///
/// - {@code row_id}, {@code start_offset}, {@code end_offset}
/// - {@code partition_key}, {@code second_partition_key}, {@code second_order_key}: of other key types, see
///   {@link #partitionKeyType(Type)}, {@link #secondPartitionKeyType(Type)} and {@link #secondOrderKeyType(Type)}
/// - {@code order_key}: of the order key type
/// - {@code argument_<type>}: of each argument type, such as {@code argument_decimal_38_6}
enum GpuWindowTable
{
    // Partitions of several sizes, with peer groups of growing length, a NULL partition key, a trailing NULL
    // peer group, scattered NULL arguments of both signs, and per-row frame offsets
    MIXED(
            """
            SELECT
                partition_number * 1000000 + row_index AS row_id,
                IF(partition_number = 4, NULL, partition_number) AS partition_rank,
                IF(row_index >= partition_size - 2, NULL, CAST(floor(sqrt(row_index)) AS bigint)) AS order_rank,
                IF(row_index % 3 = 1, NULL, IF(row_index % 2 = 0, -1, 1) * (row_index * 7919 % 100003)) AS argument_rank,
                IF(row_index % 4 = 3, NULL, row_index % 2) AS second_partition_rank,
                IF(row_index % 5 = 4, NULL, row_index % 2) AS second_order_rank,
                row_index % 3 AS start_offset,
                (row_index + 1) % 3 AS end_offset
            FROM UNNEST(ARRAY[1, 2, 3, 5, 8, 13]) WITH ORDINALITY partitions(partition_size, partition_number)
            CROSS JOIN UNNEST(sequence(0, partition_size - 1)) partition_rows(row_index)
            """),

    // Peer groups: all rows, groups of 10, of growing length, all NULL, alternating NULL, a trailing NULL group,
    // and a large partition; NULL arguments in the whole first partition, in whole peer groups of 10, and scattered
    TIES(
            """
            SELECT
                partition_number * 1000000 + row_index AS row_id,
                partition_number AS partition_rank,
                CASE partition_number
                    WHEN 1 THEN 0
                    WHEN 2 THEN row_index / 10
                    WHEN 3 THEN CAST(floor(sqrt(row_index)) AS bigint)
                    WHEN 4 THEN NULL
                    WHEN 5 THEN IF(row_index % 2 = 0, NULL, 0)
                    WHEN 6 THEN IF(row_index >= 40, NULL, row_index / 10)
                    ELSE row_index / 10
                END AS order_rank,
                IF(partition_number = 1 OR (row_index / 10) % 3 = 1 OR row_index % 7 = 0, NULL, row_index) AS argument_rank,
                IF(row_index % 13 = 0, NULL, row_index % 2) AS second_partition_rank,
                IF(row_index % 5 = 0, NULL, row_index % 3) AS second_order_rank,
                1 AS start_offset,
                1 AS end_offset
            FROM UNNEST(ARRAY[20, 40, 30, 10, 20, 50, 5000]) WITH ORDINALITY partitions(partition_size, partition_number)
            CROSS JOIN UNNEST(sequence(0, partition_size - 1)) partition_rows(row_index)
            """),

    // Frame offsets reach past both ends of the partition
    SINGLE_ROW(
            """
            SELECT
                BIGINT '0' AS row_id,
                BIGINT '0' AS partition_rank,
                BIGINT '0' AS order_rank,
                BIGINT '1' AS argument_rank,
                BIGINT '0' AS second_partition_rank,
                BIGINT '0' AS second_order_rank,
                BIGINT '1' AS start_offset,
                BIGINT '1' AS end_offset
            """),

    // Every key and argument is NULL
    ALL_NULLS(
            """
            SELECT
                row_index AS row_id,
                CAST(NULL AS bigint) AS partition_rank,
                CAST(NULL AS bigint) AS order_rank,
                CAST(NULL AS bigint) AS argument_rank,
                CAST(NULL AS bigint) AS second_partition_rank,
                CAST(NULL AS bigint) AS second_order_rank,
                row_index % 3 AS start_offset,
                (row_index + 1) % 3 AS end_offset
            FROM UNNEST(sequence(0, 19)) partition_rows(row_index)
            """),

    // Keys are indexes of edge values, such as NaN, infinities and both zeros, see edgeValue. Order keys:
    // every value, every value twice with NULLs, only the last value, the last value with NULL and others, infinities
    // with NaN, and both zeros with NaN. Partition keys: the last value in two partitions, NULL, both zeros, and others.
    EDGE_VALUES(
            """
            SELECT
                partition_number * 1000000 + row_index AS row_id,
                partition_rank,
                order_rank,
                IF(row_index % 3 = 1, NULL, row_index) AS argument_rank,
                0 AS second_partition_rank,
                0 AS second_order_rank,
                1 AS start_offset,
                1 AS end_offset
            FROM UNNEST(
                    ARRAY[-1, -1, NULL, 5, 6, 0, 11, 1, 2],
                    ARRAY[
                        sequence(0, 12),
                        concat(CAST(ARRAY[NULL, NULL] AS array(bigint)), transform(sequence(0, 25), index -> index / 2)),
                        ARRAY[-1, -1],
                        ARRAY[-1, NULL],
                        ARRAY[-1, NULL, 9],
                        ARRAY[-1, 7, 2, 9],
                        ARRAY[11, 0, -1],
                        ARRAY[5, 6, -1],
                        ARRAY[NULL]])
                WITH ORDINALITY partitions(partition_rank, order_ranks, partition_number)
            CROSS JOIN UNNEST(order_ranks) WITH ORDINALITY partition_rows(order_rank, row_index)
            """,
            GpuWindowTable::edgeValue),

    EMPTY(
            """
            SELECT
                BIGINT '0' AS row_id,
                BIGINT '0' AS partition_rank,
                BIGINT '0' AS order_rank,
                BIGINT '0' AS argument_rank,
                BIGINT '0' AS second_partition_rank,
                BIGINT '0' AS second_order_rank,
                BIGINT '0' AS start_offset,
                BIGINT '0' AS end_offset
            WHERE false
            """);

    private static final List<Type> ARGUMENT_TYPES = List.of(
            // Above precision 28 the GPU sums decimals differently
            createDecimalType(28, 2),
            createDecimalType(38, 6));
    // 2020-01-01 00:00:00 UTC
    private static final long BASE_EPOCH_NANOS = 1_577_836_800_000_000_000L;
    // Strings are ranks padded to this width
    private static final int STRING_WIDTH = 4;
    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);
    private static final LocalDateTime EARLIEST_TIMESTAMP = LocalDateTime.of(1700, 1, 1, 0, 0);
    private static final LocalDateTime LATEST_TIMESTAMP = LocalDateTime.of(2250, 1, 1, 0, 0);

    private final String shape;
    private final BiFunction<Type, String, String> keyMapping;

    GpuWindowTable(@Language("SQL") String shape)
    {
        this(shape, GpuWindowTable::value);
    }

    GpuWindowTable(@Language("SQL") String shape, BiFunction<Type, String, String> keyMapping)
    {
        this.shape = shape;
        this.keyMapping = keyMapping;
    }

    String tableName()
    {
        return name().toLowerCase(ENGLISH);
    }

    /// The view of the table with {@code order_key} of the type, such as {@code ties_timestamp_6_with_time_zone}
    String withOrderKey(Type orderKeyType)
    {
        return name(tableName(), orderKeyType);
    }

    /// The view of the table with {@code partition_key} of the type
    String withPartitionKey(Type partitionKeyType)
    {
        return withOrderKey(keyType(partitionKeyType, -1));
    }

    static Type partitionKeyType(Type orderKeyType)
    {
        return keyType(orderKeyType, 1);
    }

    static Type secondPartitionKeyType(Type orderKeyType)
    {
        return keyType(orderKeyType, 2);
    }

    static Type secondOrderKeyType(Type orderKeyType)
    {
        return keyType(orderKeyType, -2);
    }

    void create(QueryRunner queryRunner)
    {
        StringBuilder columns = new StringBuilder("row_id, start_offset, end_offset");
        for (Type type : TESTED_GPU_TYPES) {
            columns.append(format(",\n%s AS %s", keyMapping.apply(type, "partition_rank"), name("partition_key", type)));
            columns.append(format(",\n%s AS %s", keyMapping.apply(type, "order_rank"), name("order_key", type)));
            columns.append(format(",\n%s AS %s", keyMapping.apply(type, "second_partition_rank"), name("second_partition_key", type)));
            columns.append(format(",\n%s AS %s", keyMapping.apply(type, "second_order_rank"), name("second_order_key", type)));
        }
        for (Type type : ARGUMENT_TYPES) {
            columns.append(format(",\n%s AS %s", value(type, "argument_rank"), name("argument", type)));
        }
        // Rows are stored in a pseudo-random order, so that input is neither sorted nor partitioned. Without LIMIT,
        // ORDER BY in CREATE TABLE AS has no effect.
        queryRunner.execute(format(
                """
                CREATE TABLE %s AS
                SELECT
                %s
                FROM (
                %s)
                ORDER BY xxhash64(to_big_endian_64(row_id))
                LIMIT 1000000000
                """,
                tableName(),
                columns.toString().indent(4),
                shape.indent(4)));

        for (Type orderKeyType : TESTED_GPU_TYPES) {
            queryRunner.execute(format(
                    """
                    CREATE VIEW %s AS
                    SELECT row_id, %s AS partition_key, %s AS order_key, %s AS second_partition_key, %s AS second_order_key, %s, start_offset, end_offset
                    FROM %s
                    """,
                    withOrderKey(orderKeyType),
                    name("partition_key", partitionKeyType(orderKeyType)),
                    name("order_key", orderKeyType),
                    name("second_partition_key", secondPartitionKeyType(orderKeyType)),
                    name("second_order_key", secondOrderKeyType(orderKeyType)),
                    ARGUMENT_TYPES.stream().map(type -> name("argument", type)).collect(joining(", ")),
                    tableName()));
        }
    }

    private static Type keyType(Type type, int offset)
    {
        return TESTED_GPU_TYPES.get(floorMod(TESTED_GPU_TYPES.indexOf(type) + offset, TESTED_GPU_TYPES.size()));
    }

    private static String name(String prefix, Type type)
    {
        return prefix + "_" + type.getDisplayName().toLowerCase(ENGLISH)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("_$", "");
    }

    private static String value(Type type, String rank)
    {
        return switch (type) {
            case BooleanType _ -> format("(%s) >= 1", rank);
            case TinyintType _ -> format("CAST(least(%s, 255) - 128 AS tinyint)", rank);
            case SmallintType _, IntegerType _, BigintType _, RealType _, DoubleType _, DecimalType _ -> format("CAST(%s AS %s)", rank, type);
            case DateType _ -> format("date_add('day', %s, DATE '2020-01-01')", rank);
            // Step by the smallest unit of the precision, so that the fractional part is exercised
            case TimestampType timestampType -> format("CAST(%s AS %s)", epochNanos(rank, timestampType.getPrecision()), type);
            case TimestampWithTimeZoneType timestampType -> format("CAST(%s AS %s)", epochNanos(rank, timestampType.getPrecision()), type);
            case CharType _, VarcharType _ -> format("CAST(lpad(CAST(%s AS varchar), %s, '0') AS %s)", rank, STRING_WIDTH, type);
            case VarbinaryType _ -> format("to_utf8(lpad(CAST(%s AS varchar), %s, '0'))", rank, STRING_WIDTH);
            default -> throw new IllegalArgumentException("No mapping for " + type);
        };
    }

    /// Extreme and special values of the type as SQL literals, in the order of {@code ORDER BY ... ASC}.
    /// Values may be equal in that order, such as {@code -0.0} and {@code 0.0}, but never decreasing.
    /// See {@link #edgeValue(Type, String)}.
    private static List<String> edgeValues(Type type)
    {
        return switch (type) {
            case BooleanType _ -> List.of("false", "true");
            case TinyintType _ -> integers(type, Byte.MIN_VALUE, Byte.MAX_VALUE);
            case SmallintType _ -> integers(type, Short.MIN_VALUE, Short.MAX_VALUE);
            case IntegerType _ -> integers(type, Integer.MIN_VALUE, Integer.MAX_VALUE);
            case BigintType _ -> integers(type, Long.MIN_VALUE, Long.MAX_VALUE);
            case RealType _ -> Stream.of(
                            Float.NEGATIVE_INFINITY,
                            -Float.MAX_VALUE,
                            -1.0f,
                            -Float.MIN_NORMAL,
                            -Float.MIN_VALUE,
                            -0.0f,
                            0.0f,
                            Float.MIN_VALUE,
                            Float.MIN_NORMAL,
                            1.0f,
                            Float.MAX_VALUE,
                            Float.POSITIVE_INFINITY,
                            Float.NaN)
                    .map(value -> format("REAL '%s'", value))
                    .collect(toImmutableList());
            case DoubleType _ -> Stream.of(
                            Double.NEGATIVE_INFINITY,
                            -Double.MAX_VALUE,
                            -1.0,
                            -Double.MIN_NORMAL,
                            -Double.MIN_VALUE,
                            -0.0,
                            0.0,
                            Double.MIN_VALUE,
                            Double.MIN_NORMAL,
                            1.0,
                            Double.MAX_VALUE,
                            Double.POSITIVE_INFINITY,
                            Double.NaN)
                    .map(value -> format("DOUBLE '%s'", value))
                    .collect(toImmutableList());
            case DecimalType decimalType -> decimals(decimalType);
            case DateType _ -> Stream.of("1700-01-01", "1969-12-31", "1970-01-01", "2000-01-01", "2250-01-01")
                    .map(date -> format("DATE '%s'", date))
                    .collect(toImmutableList());
            case TimestampType timestampType -> timestamps(timestampType.getPrecision()).stream()
                    .map(timestamp -> format("CAST(TIMESTAMP '%s' AS %s)", timestamp, type))
                    .collect(toImmutableList());
            case TimestampWithTimeZoneType timestampType -> timestamps(timestampType.getPrecision()).stream()
                    .map(timestamp -> format("CAST(TIMESTAMP '%s UTC' AS %s)", timestamp, type))
                    .collect(toImmutableList());
            // A CHAR value is padded with spaces, which sort after the control character
            case CharType _ -> strings(type, List.of("chr(1)", "''", "'a' || chr(1)", "'a'", "'ab'", "'b'", "'\u017C'", "'\uD83D\uDE00'"));
            case VarcharType _ -> strings(type, List.of("''", "chr(1)", "'a'", "'a' || chr(1)", "'ab'", "'b'", "'\u017C'", "'\uD83D\uDE00'"));
            case VarbinaryType _ -> Stream.of("", "00", "0000", "7F", "80", "FF", "FFFF")
                    .map(hex -> format("from_hex('%s')", hex))
                    .collect(toImmutableList());
            default -> throw new IllegalArgumentException("No edge values for " + type);
        };
    }

    /// Maps an index (a BIGINT SQL expression) to one of {@link #edgeValues(Type)}. A non-negative index wraps around,
    /// so that types with fewer edge values repeat them, and a negative one counts from the end, so that {@code -1} is
    /// the last value, such as NaN. NULL maps to NULL.
    private static String edgeValue(Type type, String index)
    {
        List<String> values = edgeValues(type);
        return format("element_at(ARRAY[%1$s], IF((%2$s) < 0, (%2$s), (%2$s) %% %3$s + 1))", String.join(", ", values), index, values.size());
    }

    private static String epochNanos(String rank, int precision)
    {
        String step = "1" + repeat("0", 9 - precision);
        // from_unixtime_nanos returns the session time zone. The GPU accepts only UTC values, and
        // a cast to timestamp in a zone with DST would not be monotonic.
        return format("at_timezone(from_unixtime_nanos(%s + (%s) * %s), 'UTC')", BASE_EPOCH_NANOS, rank, step);
    }

    private static List<String> integers(Type type, long min, long max)
    {
        return Stream.of(min, -1L, 0L, 1L, max)
                // a generic literal, as the minimum of BIGINT is not a valid numeric literal
                .map(value -> format("%s '%s'", type, value))
                .collect(toImmutableList());
    }

    private static List<String> decimals(DecimalType type)
    {
        BigInteger max = BigInteger.TEN.pow(type.getPrecision()).subtract(BigInteger.ONE);
        return Stream.of(max.negate(), max.negate().add(BigInteger.ONE), BigInteger.ONE.negate(), BigInteger.ZERO, BigInteger.ONE, max.subtract(BigInteger.ONE), max)
                .map(unscaled -> format("CAST(DECIMAL '%s' AS %s)", new BigDecimal(unscaled, type.getScale()).toPlainString(), type))
                .collect(toImmutableList());
    }

    /// The earliest and the latest timestamps, and the ones next to the epoch, one unit of the precision apart
    private static List<String> timestamps(int precision)
    {
        long unitNanos = BigInteger.TEN.pow(9 - precision).longValueExact();
        return Stream.of(
                        EARLIEST_TIMESTAMP,
                        EPOCH.minusNanos(unitNanos),
                        EPOCH,
                        EPOCH.plusNanos(unitNanos),
                        LATEST_TIMESTAMP.minusNanos(unitNanos))
                .map(timestamp -> timestampLiteral(timestamp, precision))
                .collect(toImmutableList());
    }

    private static String timestampLiteral(LocalDateTime timestamp, int precision)
    {
        String seconds = format("%s %02d:%02d:%02d", timestamp.format(ISO_LOCAL_DATE), timestamp.getHour(), timestamp.getMinute(), timestamp.getSecond());
        if (precision == 0) {
            return seconds;
        }
        String nanos = format("%09d", timestamp.getNano());
        return seconds + "." + nanos.substring(0, precision);
    }

    private static List<String> strings(Type type, List<String> literals)
    {
        return literals.stream()
                .map(literal -> format("CAST(%s AS %s)", literal, type))
                .collect(toImmutableList());
    }
}
