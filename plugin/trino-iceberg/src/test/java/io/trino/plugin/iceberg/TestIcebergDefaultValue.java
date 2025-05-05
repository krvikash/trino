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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergDefaultValue
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "3")
                .addIcebergProperty("iceberg.max-format-version", "3")
                .build();
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBoolean(IcebergFileFormat format)
    {
        testDefaultValue(format, "BOOLEAN", "true", "true");
        testDefaultValue(format, "BOOLEAN", "false", "false");
        // Boolean NULL is disallowed at the engine level
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testInteger(IcebergFileFormat format)
    {
        testDefaultValue(format, "INTEGER", "-2147483648", "-2147483648");
        testDefaultValue(format, "INTEGER", "2147483647", "2147483647");
        testDefaultValue(format, "INTEGER", "NULL", "CAST(NULL AS INTEGER)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBigint(IcebergFileFormat format)
    {
        testDefaultValue(format, "BIGINT", "-9223372036854775808", "BIGINT '-9223372036854775808'");
        testDefaultValue(format, "BIGINT", "9223372036854775807", "BIGINT '9223372036854775807'");
        testDefaultValue(format, "BIGINT", "NULL", "CAST(NULL AS BIGINT)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testReal(IcebergFileFormat format)
    {
        testDefaultValue(format, "REAL", "REAL '3.14'", "REAL '3.14'");
        testDefaultValue(format, "REAL", "REAL '10.3e0'", "REAL '10.3e0'");
        testDefaultValue(format, "REAL", "123", "REAL '123'");
        testDefaultValue(format, "REAL", "NULL", "CAST(NULL AS REAL)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDouble(IcebergFileFormat format)
    {
        testDefaultValue(format, "DOUBLE", "DOUBLE '3.14'", "DOUBLE '3.14'");
        testDefaultValue(format, "DOUBLE", "DOUBLE '1.0E100'", "DOUBLE '1.0E100'");
        testDefaultValue(format, "DOUBLE", "DOUBLE '1.23456E12'", "DOUBLE '1.23456E12'");
        testDefaultValue(format, "DOUBLE", "123", "DOUBLE '123'");
        testDefaultValue(format, "DOUBLE", "NULL", "CAST(NULL AS DOUBLE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDecimal(IcebergFileFormat format)
    {
        testDefaultValue(format, "DECIMAL(3,0)", "DECIMAL '193'", "DECIMAL '193'");
        testDefaultValue(format, "DECIMAL(3,0)", "DECIMAL '-193'", "DECIMAL '-193'");
        testDefaultValue(format, "DECIMAL(3,1)", "DECIMAL '10.0'", "DECIMAL '10.0'");
        testDefaultValue(format, "DECIMAL(3,1)", "DECIMAL '-10.1'", "DECIMAL '-10.1'");
        testDefaultValue(format, "DECIMAL(30,5)", "DECIMAL '3141592653589793238462643.38327'", "DECIMAL '3141592653589793238462643.38327'");
        testDefaultValue(format, "DECIMAL(30,5)", "DECIMAL '-3141592653589793238462643.38327'", "DECIMAL '-3141592653589793238462643.38327'");
        testDefaultValue(format, "DECIMAL(38,0)", "DECIMAL '27182818284590452353602874713526624977'", "DECIMAL '27182818284590452353602874713526624977'");
        testDefaultValue(format, "DECIMAL(38,0)", "DECIMAL '-27182818284590452353602874713526624977'", "DECIMAL '-27182818284590452353602874713526624977'");
        testDefaultValue(format, "DECIMAL(3,0)", "NULL", "CAST(NULL AS DECIMAL(3,0))");
        testDefaultValue(format, "DECIMAL(38,0)", "NULL", "CAST(NULL AS DECIMAL(38,0))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDate(IcebergFileFormat format)
    {
        testDefaultValue(format, "DATE", "DATE '0001-01-01'", "DATE '0001-01-01'");
        testDefaultValue(format, "DATE", "DATE '1969-12-31'", "DATE '1969-12-31'");
        testDefaultValue(format, "DATE", "DATE '1970-01-01'", "DATE '1970-01-01'");
        testDefaultValue(format, "DATE", "DATE '9999-12-31'", "DATE '9999-12-31'");
        testDefaultValue(format, "DATE", "NULL", "CAST(NULL AS DATE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTime(IcebergFileFormat format)
    {
        testDefaultValue(format, "TIME", "TIME '00:00:00'", "TIME '00:00:00.000000'");
        testDefaultValue(format, "TIME", "TIME '00:00:00.1'", "TIME '00:00:00.100000'");
        testDefaultValue(format, "TIME", "TIME '00:00:00.12'", "TIME '00:00:00.120000'");
        testDefaultValue(format, "TIME", "TIME '00:00:00.123'", "TIME '00:00:00.123000'");
        testDefaultValue(format, "TIME", "TIME '00:00:00.1234'", "TIME '00:00:00.123400'");
        testDefaultValue(format, "TIME", "TIME '00:00:00.12345'", "TIME '00:00:00.123450'");
        testDefaultValue(format, "TIME", "TIME '00:00:00.123456'", "TIME '00:00:00.123456'");
        testDefaultValue(format, "TIME", "TIME '23:59:59.999999'", "TIME '23:59:59.999999'");
        testDefaultValue(format, "TIME", "NULL", "CAST(NULL AS TIME(6))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestamp(IcebergFileFormat format)
    {
        testDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56'", "TIMESTAMP '2025-01-23 12:34:56.000000'");
        testDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.1'", "TIMESTAMP '2025-01-23 12:34:56.100000'");
        testDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.12'", "TIMESTAMP '2025-01-23 12:34:56.120000'");
        testDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.123'", "TIMESTAMP '2025-01-23 12:34:56.123000'");
        testDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.1234'", "TIMESTAMP '2025-01-23 12:34:56.123400'");
        testDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.12345'", "TIMESTAMP '2025-01-23 12:34:56.123450'");
        testDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.123456'", "TIMESTAMP '2025-01-23 12:34:56.123456'");

        // short timestamp literal on long timestamp type
        testDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56'", "TIMESTAMP '2025-01-23 12:34:56.000000'");
        testDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.1'", "TIMESTAMP '2025-01-23 12:34:56.100000'");
        testDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.12'", "TIMESTAMP '2025-01-23 12:34:56.120000'");
        testDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.123'", "TIMESTAMP '2025-01-23 12:34:56.123000'");
        testDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.1234'", "TIMESTAMP '2025-01-23 12:34:56.123400'");
        testDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.12345'", "TIMESTAMP '2025-01-23 12:34:56.123450'");

        testDefaultValue(format, "TIMESTAMP", "NULL", "CAST(NULL AS TIMESTAMP(6))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampNanos(IcebergFileFormat format)
    {
        // TODO Iceberg does not support timestamp nanos as default values
        assertQueryFails(
                "CREATE TABLE test_default_value_timestamp_ns (id int, data TIMESTAMP(9) DEFAULT TIMESTAMP '2025-01-23 12:34:56.123456789') WITH (format='" + format + "')",
                "Timestamp nanos is not supported as default values");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWithTimeZone(IcebergFileFormat format)
    {
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '0000-01-01 00:00:00 UTC'", "TIMESTAMP '0000-01-01 00:00:00.000000 UTC'");
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'", "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'");

        // short timestamptz literal on long timestamptz type
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.000000 UTC'");
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.100000 UTC'");
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.120000 UTC'");
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123000 UTC'");
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1234 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123400 UTC'");
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12345 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123450 UTC'");
        testDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123456 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123456 UTC'");

        testDefaultValue(format, "TIMESTAMP WITH TIME ZONE", "NULL", "CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWithTimeZoneNanos(IcebergFileFormat format)
    {
        // TODO Iceberg does not support timestamp nanos as default values
        assertQueryFails(
                "CREATE TABLE test_default_value_timestamptz_ns (id int, data TIMESTAMP(9) DEFAULT TIMESTAMP '2025-01-23 12:34:56.123456789') WITH (format='" + format + "')",
                "Timestamp nanos is not supported as default values");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarchar(IcebergFileFormat format)
    {
        testDefaultValue(format, "VARCHAR", "'test varchar'", "VARCHAR 'test varchar'");
        testDefaultValue(format, "VARCHAR", "''", "VARCHAR ''");
        testDefaultValue(format, "VARCHAR", "'攻殻機動隊'", "VARCHAR '攻殻機動隊'");
        testDefaultValue(format, "VARCHAR", "'😂'", "VARCHAR '😂'");
        testDefaultValue(format, "VARCHAR", "'a''singlequote'", "VARCHAR 'a''singlequote'");

        testDefaultValue(format, "VARCHAR", "NULL", "CAST(NULL AS VARCHAR)");
        testDefaultValue(format, "VARCHAR(255)", "NULL", "CAST(NULL AS VARCHAR)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testUuid(IcebergFileFormat format)
    {
        testDefaultValue(format, "UUID", "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'");
        testDefaultValue(format, "UUID", "NULL", "CAST(NULL AS UUID)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarbinary(IcebergFileFormat format)
    {
        testDefaultValue(format, "VARBINARY", "X'65683F'", "X'65683F'");
        testDefaultValue(format, "VARBINARY", "NULL", "CAST(NULL AS VARBINARY)");
    }

    @Test
    void testUnsupportedDefaultColumnValue()
    {
        assertQueryFails(
                "CREATE TABLE test_unsupported_default_column_value(x int DEFAULT 1) WITH (format_version=2)",
                "Default values are not supported for format version < 3");

        try (TestTable table = newTrinoTable("test_unsupported_default_column_value", "(x int)  WITH (format_version=2)")) {
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN y int DEFAULT 1", "Default values are not supported for format version < 3");
        }
    }

    private void testDefaultValue(IcebergFileFormat format, @Language("SQL") String type, @Language("SQL") String defaultValue, @Language("SQL") String expectedValue)
    {
        try (TestTable table = newTrinoTable("test_default_value", "(id int, data %s DEFAULT %s) WITH (format='%s')".formatted(type, defaultValue, format))) {
            assertUpdate("INSERT INTO " + table.getName() + "(id) VALUES 1", 1);

            assertThat(query("SELECT data FROM " + table.getName()))
                    .as("%s type expected %s", type, defaultValue)
                    .matches("VALUES " + expectedValue);
        }
    }
}
