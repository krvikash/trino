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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Int128;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IncrementalColumn.findWithExactUpperBound;
import static io.trino.plugin.iceberg.IncrementalColumn.hasExactIcebergUpperBound;
import static io.trino.plugin.iceberg.IncrementalColumn.isSupportedIncrementalColumnType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIncrementalColumn
{
    private static final Schema STORAGE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "int_col", Types.IntegerType.get()),
            Types.NestedField.optional(2, "bigint_col", Types.LongType.get()),
            Types.NestedField.optional(3, "date_col", Types.DateType.get()),
            Types.NestedField.optional(4, "short_decimal_col", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(5, "long_decimal_col", Types.DecimalType.of(38, 10)),
            Types.NestedField.optional(6, "time_col", Types.TimeType.get()),
            Types.NestedField.optional(7, "timestamp_col", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(8, "timestamp_nano_col", Types.TimestampNanoType.withoutZone()),
            Types.NestedField.optional(9, "timestamp_tz_col", Types.TimestampType.withZone()),
            Types.NestedField.optional(10, "varchar_col", Types.StringType.get()),
            Types.NestedField.optional(11, "varbinary_col", Types.BinaryType.get()),
            Types.NestedField.optional(12, "fixed_col", Types.FixedType.ofLength(4)),
            Types.NestedField.optional(13, "uuid_col", Types.UUIDType.get()),
            Types.NestedField.optional(14, "double_col", Types.DoubleType.get()),
            Types.NestedField.optional(15, "real_col", Types.FloatType.get()),
            Types.NestedField.optional(16, "boolean_col", Types.BooleanType.get()));

    @Test
    public void testTypesWithExactIcebergUpperBound()
    {
        assertThat(hasExactIcebergUpperBound(INTEGER)).isTrue();
        assertThat(hasExactIcebergUpperBound(BIGINT)).isTrue();
        assertThat(hasExactIcebergUpperBound(DATE)).isTrue();
        assertThat(hasExactIcebergUpperBound(createDecimalType(10, 2))).isTrue();
        assertThat(hasExactIcebergUpperBound(createDecimalType(38, 10))).isTrue();
        assertThat(hasExactIcebergUpperBound(createTimeType(0))).isTrue();
        assertThat(hasExactIcebergUpperBound(createTimeType(3))).isTrue();
        assertThat(hasExactIcebergUpperBound(TIME_MICROS)).isTrue();
        assertThat(hasExactIcebergUpperBound(createTimestampType(0))).isTrue();
        assertThat(hasExactIcebergUpperBound(createTimestampType(3))).isTrue();
        assertThat(hasExactIcebergUpperBound(TIMESTAMP_MICROS)).isTrue();

        assertThat(hasExactIcebergUpperBound(createTimeType(7))).isFalse();
        assertThat(hasExactIcebergUpperBound(createTimeType(9))).isFalse();
        assertThat(hasExactIcebergUpperBound(createTimestampType(7))).isFalse();
        assertThat(hasExactIcebergUpperBound(createTimestampType(9))).isFalse();
        assertThat(hasExactIcebergUpperBound(createTimestampType(12))).isFalse();
        assertThat(hasExactIcebergUpperBound(TIMESTAMP_TZ_MICROS)).isFalse();
        assertThat(hasExactIcebergUpperBound(VARCHAR)).isFalse();
        assertThat(hasExactIcebergUpperBound(createVarcharType(10))).isFalse();
        assertThat(hasExactIcebergUpperBound(VARBINARY)).isFalse();
        assertThat(hasExactIcebergUpperBound(UUID)).isFalse();
        assertThat(hasExactIcebergUpperBound(DOUBLE)).isFalse();
        assertThat(hasExactIcebergUpperBound(REAL)).isFalse();
        assertThat(hasExactIcebergUpperBound(BOOLEAN)).isFalse();
        assertThat(hasExactIcebergUpperBound(SMALLINT)).isFalse();
        assertThat(hasExactIcebergUpperBound(TINYINT)).isFalse();
    }

    @Test
    public void testSupportedIncrementalColumnTypes()
    {
        assertThat(isSupportedIncrementalColumnType(BIGINT)).isTrue();
        assertThat(isSupportedIncrementalColumnType(createTimestampType(3))).isTrue();
        assertThat(isSupportedIncrementalColumnType(TIMESTAMP_MICROS)).isTrue();
        assertThat(isSupportedIncrementalColumnType(TIME_MICROS)).isTrue();
        assertThat(isSupportedIncrementalColumnType(VARCHAR)).isTrue();
        assertThat(isSupportedIncrementalColumnType(createVarcharType(10))).isTrue();

        assertThat(isSupportedIncrementalColumnType(createTimestampType(9))).isFalse();
        assertThat(isSupportedIncrementalColumnType(createTimeType(9))).isFalse();
        assertThat(isSupportedIncrementalColumnType(TIMESTAMP_TZ_MICROS)).isFalse();
        assertThat(isSupportedIncrementalColumnType(VARBINARY)).isFalse();
        assertThat(isSupportedIncrementalColumnType(UUID)).isFalse();
        assertThat(isSupportedIncrementalColumnType(DOUBLE)).isFalse();
        assertThat(isSupportedIncrementalColumnType(BOOLEAN)).isFalse();
    }

    @Test
    public void testFindWithExactUpperBound()
    {
        assertThat(find("int_col").trinoType()).isEqualTo(INTEGER);
        assertThat(find("bigint_col").trinoType()).isEqualTo(BIGINT);
        assertThat(find("date_col").trinoType()).isEqualTo(DATE);
        assertThat(find("short_decimal_col").trinoType()).isEqualTo(createDecimalType(10, 2));
        assertThat(find("long_decimal_col").trinoType()).isEqualTo(createDecimalType(38, 10));
        assertThat(find("time_col").trinoType()).isEqualTo(TIME_MICROS);
        assertThat(find("timestamp_col").trinoType()).isEqualTo(TIMESTAMP_MICROS);

        IncrementalColumn timestampColumn = find("timestamp_col");
        assertThat(timestampColumn.name()).isEqualTo("timestamp_col");
        assertThat(timestampColumn.field().fieldId()).isEqualTo(7);
    }

    /**
     * Storage table columns are lower-cased, while {@code incremental_column} keeps the view column's spelling.
     */
    @Test
    public void testFindIgnoresColumnNameCase()
    {
        IncrementalColumn column = findWithExactUpperBound(STORAGE_SCHEMA, "BigInt_Col", TESTING_TYPE_MANAGER).orElseThrow();
        assertThat(column.name()).isEqualTo("BigInt_Col");
        assertThat(column.field().fieldId()).isEqualTo(2);
    }

    @Test
    public void testColumnWithoutExactIcebergUpperBoundIsNotFound()
    {
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "timestamp_nano_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "timestamp_tz_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "varchar_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "varbinary_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "fixed_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "uuid_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "double_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "real_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "boolean_col", TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(findWithExactUpperBound(STORAGE_SCHEMA, "missing_col", TESTING_TYPE_MANAGER)).isEmpty();
    }

    @Test
    public void testOrcTimestampUpperBoundIsNotExact()
    {
        IncrementalColumn timestampColumn = find("timestamp_col");
        assertThat(timestampColumn.hasExactUpperBoundIn(PARQUET)).isTrue();
        assertThat(timestampColumn.hasExactUpperBoundIn(AVRO)).isTrue();
        assertThat(timestampColumn.hasExactUpperBoundIn(ORC)).isFalse();

        for (String columnName : new String[] {"int_col", "bigint_col", "date_col", "short_decimal_col", "long_decimal_col", "time_col"}) {
            IncrementalColumn column = find(columnName);
            assertThat(column.hasExactUpperBoundIn(PARQUET)).as(columnName).isTrue();
            assertThat(column.hasExactUpperBoundIn(AVRO)).as(columnName).isTrue();
            assertThat(column.hasExactUpperBoundIn(ORC)).as(columnName).isTrue();
        }
    }

    @Test
    public void testMaxValueInTrinoRepresentation()
    {
        assertThat(maxValueIn(find("int_col"), 42)).isEqualTo(42L);
        assertThat(maxValueIn(find("bigint_col"), -200L)).isEqualTo(-200L);
        assertThat(maxValueIn(find("date_col"), 20_454)).isEqualTo(20_454L);
        assertThat(maxValueIn(find("short_decimal_col"), new BigDecimal("12345.67"))).isEqualTo(1_234_567L);
        assertThat(maxValueIn(find("long_decimal_col"), new BigDecimal("123456789012345678.0123456789")))
                .isEqualTo(Int128.valueOf(new BigInteger("1234567890123456780123456789")));
        assertThat(maxValueIn(find("time_col"), 3_723_000_001L)).isEqualTo(3_723_000_001_000_000L);
        assertThat(maxValueIn(find("timestamp_col"), 1_767_225_600_123_456L)).isEqualTo(1_767_225_600_123_456L);
    }

    @Test
    public void testMaxValueInFileWithoutUpperBound()
    {
        IncrementalColumn column = find("bigint_col");
        assertThat(column.maxValueIn(new Metrics(10L, null, null, null, null, null, null))).isEmpty();
        assertThat(column.maxValueIn(new Metrics(10L, null, null, null, null, ImmutableMap.of(), ImmutableMap.of()))).isEmpty();
        assertThat(column.maxValueIn(metricsWithUpperBound(find("int_col"), 42))).isEmpty();
    }

    @Test
    public void testIsNullInEveryRow()
    {
        IncrementalColumn column = find("bigint_col");
        assertThat(column.isNullInEveryRowOf(new Metrics(10L, null, null, ImmutableMap.of(2, 10L), null))).isTrue();
        assertThat(column.isNullInEveryRowOf(new Metrics(10L, null, null, ImmutableMap.of(2, 9L), null))).isFalse();
        assertThat(column.isNullInEveryRowOf(new Metrics(10L, null, null, ImmutableMap.of(1, 10L), null))).isFalse();
        assertThat(column.isNullInEveryRowOf(new Metrics(10L, null, null, null, null))).isFalse();
    }

    static IncrementalColumn find(String columnName)
    {
        return findWithExactUpperBound(STORAGE_SCHEMA, columnName, TESTING_TYPE_MANAGER).orElseThrow();
    }

    static Metrics metricsWithUpperBound(IncrementalColumn column, Object icebergValue)
    {
        Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(column.field().fieldId(), Conversions.toByteBuffer(column.field().type(), icebergValue));
        return new Metrics(10L, null, null, ImmutableMap.of(column.field().fieldId(), 0L), null, upperBounds, upperBounds);
    }

    private static Object maxValueIn(IncrementalColumn column, Object icebergValue)
    {
        return column.maxValueIn(metricsWithUpperBound(column, icebergValue)).orElseThrow();
    }
}
