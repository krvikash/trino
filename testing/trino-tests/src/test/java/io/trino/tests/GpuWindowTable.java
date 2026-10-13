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

import java.util.List;

import static com.google.common.base.Strings.repeat;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

/// Tables of window input, one per data shape. A shape is a query whose keys and argument are integer ranks:
///
/// - {@code row_id}: unique
/// - {@code partition_rank}, {@code order_rank}, {@code argument_rank}: ranks, NULL for NULL
/// - {@code start_offset}, {@code end_offset}: frame offsets
///
/// The table holds each key rank as a value of every tested GPU type, and the argument rank as a value of every
/// argument type, mapped by {@link #value(Type, String)}. Queries read it through a view per order key type, see
/// {@link #withOrderKey(Type)}, with columns:
///
/// - {@code row_id}, {@code start_offset}, {@code end_offset}
/// - {@code partition_key}: of the key type next to the order key type
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
                row_index % 3 AS start_offset,
                (row_index + 1) % 3 AS end_offset
            FROM UNNEST(sequence(0, 19)) partition_rows(row_index)
            """),

    EMPTY(
            """
            SELECT
                BIGINT '0' AS row_id,
                BIGINT '0' AS partition_rank,
                BIGINT '0' AS order_rank,
                BIGINT '0' AS argument_rank,
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

    private final String shape;

    GpuWindowTable(@Language("SQL") String shape)
    {
        this.shape = shape;
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
        return withOrderKey(TESTED_GPU_TYPES.get((TESTED_GPU_TYPES.indexOf(partitionKeyType) + TESTED_GPU_TYPES.size() - 1) % TESTED_GPU_TYPES.size()));
    }

    void create(QueryRunner queryRunner)
    {
        StringBuilder columns = new StringBuilder("row_id, start_offset, end_offset");
        for (Type type : TESTED_GPU_TYPES) {
            columns.append(format(",\n%s AS %s", value(type, "partition_rank"), name("partition_key", type)));
            columns.append(format(",\n%s AS %s", value(type, "order_rank"), name("order_key", type)));
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
            Type partitionKeyType = TESTED_GPU_TYPES.get((TESTED_GPU_TYPES.indexOf(orderKeyType) + 1) % TESTED_GPU_TYPES.size());
            queryRunner.execute(format(
                    "CREATE VIEW %s AS SELECT row_id, %s AS partition_key, %s AS order_key, %s, start_offset, end_offset FROM %s",
                    withOrderKey(orderKeyType),
                    name("partition_key", partitionKeyType),
                    name("order_key", orderKeyType),
                    ARGUMENT_TYPES.stream().map(type -> name("argument", type)).collect(joining(", ")),
                    tableName()));
        }
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

    private static String epochNanos(String rank, int precision)
    {
        String step = "1" + repeat("0", 9 - precision);
        // from_unixtime_nanos returns the session time zone. The GPU accepts only UTC values, and
        // a cast to timestamp in a zone with DST would not be monotonic.
        return format("at_timezone(from_unixtime_nanos(%s + (%s) * %s), 'UTC')", BASE_EPOCH_NANOS, rank, step);
    }
}
