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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.expressions.Literal;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
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
                .build();
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBooleanWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "BOOLEAN", "true", "true");
        testWriteDefaultValue(format, "BOOLEAN", "false", "false");
        // Boolean NULL is disallowed at the engine level
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testIntegerWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "INTEGER", "-2147483648", "-2147483648");
        testWriteDefaultValue(format, "INTEGER", "2147483647", "2147483647");
        testWriteDefaultValue(format, "INTEGER", "NULL", "CAST(NULL AS INTEGER)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBigintWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "BIGINT", "-9223372036854775808", "BIGINT '-9223372036854775808'");
        testWriteDefaultValue(format, "BIGINT", "9223372036854775807", "BIGINT '9223372036854775807'");
        testWriteDefaultValue(format, "BIGINT", "NULL", "CAST(NULL AS BIGINT)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testRealWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "REAL", "REAL '3.14'", "REAL '3.14'");
        testWriteDefaultValue(format, "REAL", "REAL '10.3e0'", "REAL '10.3e0'");
        testWriteDefaultValue(format, "REAL", "123", "REAL '123'");
        testWriteDefaultValue(format, "REAL", "NULL", "CAST(NULL AS REAL)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDoubleWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "DOUBLE", "DOUBLE '3.14'", "DOUBLE '3.14'");
        testWriteDefaultValue(format, "DOUBLE", "DOUBLE '1.0E100'", "DOUBLE '1.0E100'");
        testWriteDefaultValue(format, "DOUBLE", "DOUBLE '1.23456E12'", "DOUBLE '1.23456E12'");
        testWriteDefaultValue(format, "DOUBLE", "123", "DOUBLE '123'");
        testWriteDefaultValue(format, "DOUBLE", "NULL", "CAST(NULL AS DOUBLE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDecimalWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "DECIMAL(3,0)", "DECIMAL '193'", "DECIMAL '193'");
        testWriteDefaultValue(format, "DECIMAL(3,0)", "DECIMAL '-193'", "DECIMAL '-193'");
        testWriteDefaultValue(format, "DECIMAL(3,1)", "DECIMAL '10.0'", "DECIMAL '10.0'");
        testWriteDefaultValue(format, "DECIMAL(3,1)", "DECIMAL '-10.1'", "DECIMAL '-10.1'");
        testWriteDefaultValue(format, "DECIMAL(30,5)", "DECIMAL '3141592653589793238462643.38327'", "DECIMAL '3141592653589793238462643.38327'");
        testWriteDefaultValue(format, "DECIMAL(30,5)", "DECIMAL '-3141592653589793238462643.38327'", "DECIMAL '-3141592653589793238462643.38327'");
        testWriteDefaultValue(format, "DECIMAL(38,0)", "DECIMAL '27182818284590452353602874713526624977'", "DECIMAL '27182818284590452353602874713526624977'");
        testWriteDefaultValue(format, "DECIMAL(38,0)", "DECIMAL '-27182818284590452353602874713526624977'", "DECIMAL '-27182818284590452353602874713526624977'");
        testWriteDefaultValue(format, "DECIMAL(3,0)", "NULL", "CAST(NULL AS DECIMAL(3,0))");
        testWriteDefaultValue(format, "DECIMAL(38,0)", "NULL", "CAST(NULL AS DECIMAL(38,0))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDateWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "DATE", "DATE '0001-01-01'", "DATE '0001-01-01'");
        testWriteDefaultValue(format, "DATE", "DATE '1969-12-31'", "DATE '1969-12-31'");
        testWriteDefaultValue(format, "DATE", "DATE '1970-01-01'", "DATE '1970-01-01'");
        testWriteDefaultValue(format, "DATE", "DATE '9999-12-31'", "DATE '9999-12-31'");
        testWriteDefaultValue(format, "DATE", "NULL", "CAST(NULL AS DATE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimeWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00'", "TIME '00:00:00.000000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.1'", "TIME '00:00:00.100000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.12'", "TIME '00:00:00.120000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.123'", "TIME '00:00:00.123000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.1234'", "TIME '00:00:00.123400'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.12345'", "TIME '00:00:00.123450'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.123456'", "TIME '00:00:00.123456'");
        testWriteDefaultValue(format, "TIME", "TIME '23:59:59.999999'", "TIME '23:59:59.999999'");
        testWriteDefaultValue(format, "TIME", "NULL", "CAST(NULL AS TIME(6))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56'", "TIMESTAMP '2025-01-23 12:34:56.000000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.1'", "TIMESTAMP '2025-01-23 12:34:56.100000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.12'", "TIMESTAMP '2025-01-23 12:34:56.120000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.123'", "TIMESTAMP '2025-01-23 12:34:56.123000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.1234'", "TIMESTAMP '2025-01-23 12:34:56.123400'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.12345'", "TIMESTAMP '2025-01-23 12:34:56.123450'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.123456'", "TIMESTAMP '2025-01-23 12:34:56.123456'");

        // short timestamp literal on long timestamp type
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56'", "TIMESTAMP '2025-01-23 12:34:56.000000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.1'", "TIMESTAMP '2025-01-23 12:34:56.100000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.12'", "TIMESTAMP '2025-01-23 12:34:56.120000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.123'", "TIMESTAMP '2025-01-23 12:34:56.123000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.1234'", "TIMESTAMP '2025-01-23 12:34:56.123400'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.12345'", "TIMESTAMP '2025-01-23 12:34:56.123450'");

        testWriteDefaultValue(format, "TIMESTAMP", "NULL", "CAST(NULL AS TIMESTAMP(6))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampNanosWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56'", "TIMESTAMP '2025-01-23 12:34:56.000000000'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.1'", "TIMESTAMP '2025-01-23 12:34:56.100000000'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.12'", "TIMESTAMP '2025-01-23 12:34:56.120000000'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.123'", "TIMESTAMP '2025-01-23 12:34:56.123000000'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.1234'", "TIMESTAMP '2025-01-23 12:34:56.123400000'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.12345'", "TIMESTAMP '2025-01-23 12:34:56.123450000'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.123456'", "TIMESTAMP '2025-01-23 12:34:56.123456000'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.1234567'", "TIMESTAMP '2025-01-23 12:34:56.123456700'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.12345678'", "TIMESTAMP '2025-01-23 12:34:56.123456780'");
        testWriteDefaultValue(format, "TIMESTAMP(9)", "TIMESTAMP '2025-01-23 12:34:56.123456789'", "TIMESTAMP '2025-01-23 12:34:56.123456789'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWithTimeZoneWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '0000-01-01 00:00:00 UTC'", "TIMESTAMP '0000-01-01 00:00:00.000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'", "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'");

        // short timestamptz literal on long timestamptz type
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.100000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.120000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1234 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123400 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12345 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123450 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123456 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123456 UTC'");

        testWriteDefaultValue(format, "TIMESTAMP WITH TIME ZONE", "NULL", "CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWithTimeZoneNanosWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.000000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.100000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.120000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1234 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123400000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12345 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123450000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123456 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123456000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1234567 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123456700 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12345678 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123456780 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123456789 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123456789 UTC'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVariantTypeFailsWriteDefault(IcebergFileFormat format)
    {
        assertQueryFails(
                "CREATE TABLE test_default_value_variant (id int, variant JSON DEFAULT JSON '{\"id\":3}') WITH (format='" + format + "')",
                "Variant is not supported as default values");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarcharWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "VARCHAR", "'test varchar'", "VARCHAR 'test varchar'");
        testWriteDefaultValue(format, "VARCHAR", "''", "VARCHAR ''");
        testWriteDefaultValue(format, "VARCHAR", "'攻殻機動隊'", "VARCHAR '攻殻機動隊'");
        testWriteDefaultValue(format, "VARCHAR", "'😂'", "VARCHAR '😂'");
        testWriteDefaultValue(format, "VARCHAR", "'a''singlequote'", "VARCHAR 'a''singlequote'");

        testWriteDefaultValue(format, "VARCHAR", "NULL", "CAST(NULL AS VARCHAR)");
        testWriteDefaultValue(format, "VARCHAR(255)", "NULL", "CAST(NULL AS VARCHAR)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testUuidWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "UUID", "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'");
        testWriteDefaultValue(format, "UUID", "NULL", "CAST(NULL AS UUID)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarbinaryWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "VARBINARY", "X'65683F'", "X'65683F'");
        testWriteDefaultValue(format, "VARBINARY", "NULL", "CAST(NULL AS VARBINARY)");
    }

    @Test
    void testUnsupportedDefaultColumnValueWriteDefault()
    {
        assertQueryFails(
                "CREATE TABLE test_unsupported_default_column_value(x int DEFAULT 1) WITH (format_version=2)",
                "Default values are not supported for format version < 3");

        try (TestTable table = newTrinoTable("test_unsupported_default_column_value", "(x int)  WITH (format_version=2)")) {
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN y int DEFAULT 1", "Default values are not supported for format version < 3");
            assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN x SET DEFAULT 123", "Default values are not supported for format version < 3");

            loadTable(table.getName()).updateSchema()
                    .updateColumnDefault("x", Literal.of(123))
                    .commit();
            assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN x DROP DEFAULT", "Default values are not supported for format version < 3");
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(
                tableName,
                getHiveMetastore(getQueryRunner()),
                getFileSystemFactory(getQueryRunner()),
                "iceberg",
                "tpch");
    }

    private void testWriteDefaultValue(IcebergFileFormat format, @Language("SQL") String type, @Language("SQL") String defaultValue, @Language("SQL") String expectedValue)
    {
        try (TestTable table = newTrinoTable("test_default_value", "(id int, data %s DEFAULT %s) WITH (format='%s')".formatted(type, defaultValue, format))) {
            assertUpdate("INSERT INTO " + table.getName() + "(id) VALUES 1", 1);

            assertThat(query("SELECT data FROM " + table.getName()))
                    .as("%s type expected %s", type, defaultValue)
                    .matches("VALUES " + expectedValue);
        }
    }
}
