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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.UuidType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAndTrinoInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.clickhouse.ClickHouseClient.CLICKHOUSE_MAX_SUPPORTED_TIMESTAMP_PRECISION;
import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseClickHouseTypeMapping
        extends AbstractTestQueryFramework
{
    public static final ZoneId UTC = ZoneId.of("UTC");
    public static final ZoneId JVM_ZONE = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    public static final ZoneId VILNIUS = ZoneId.of("Europe/Vilnius");
    // minutes offset change since 1932-04-01, no DST
    public static final ZoneId KATHMANDU = ZoneId.of("Asia/Kathmandu");
    private static final Function<ZoneId, String> DATETIME_TYPE_FACTORY = "DateTime('%s')"::formatted;
    private static final BiFunction<Integer, ZoneId, String> DATETIME64_TYPE_FACTORY = "DateTime64(%d, '%s')"::formatted;

    // https://clickhouse.com/docs/sql-reference/data-types/datetime
    private static final String MIN_SUPPORTED_DATETIME_VALUE = "1970-01-01 00:00:00";
    private static final String MAX_SUPPORTED_DATETIME_VALUE = "2106-02-07 06:28:15";
    // https://clickhouse.com/docs/sql-reference/data-types/datetime64
    private static final String MIN_SUPPORTED_DATETIME64_VALUE = "1900-01-01 00:00:00";
    private static final String MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_8 = "2299-12-31 23:59:59.99999999";
    private static final String MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_9 = "2262-04-11 23:47:16.854775807";

    protected TestingClickHouseServer clickhouseServer;

    @BeforeAll
    public void setUp()
    {
        checkState(JVM_ZONE.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1932, 4, 1);
        checkIsGap(JVM_ZONE, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(VILNIUS, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(VILNIUS, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        LocalDate timeGapInKathmandu = LocalDate.of(1986, 1, 1);
        checkIsGap(KATHMANDU, timeGapInKathmandu.atStartOfDay());
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    @Test
    public void testTrinoBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testBool()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bool", "true", BOOLEAN, "true")
                .addRoundTrip("bool", "false", BOOLEAN, "false")
                .addRoundTrip("Nullable(bool)", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in ClickHouse and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"))

                .addRoundTrip("Nullable(tinyint)", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tinyint"));

        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-129", TINYINT, "TINYINT '127'")
                .addRoundTrip("tinyint", "128", TINYINT, "TINYINT '-128'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_tinyint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in ClickHouse and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"))

                .addRoundTrip("Nullable(smallint)", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_smallint"));

        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32769", SMALLINT, "SMALLINT '32767'")
                .addRoundTrip("smallint", "32768", SMALLINT, "SMALLINT '-32768'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in ClickHouse and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"))

                .addRoundTrip("Nullable(integer)", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_int"));

        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483649", INTEGER, "INTEGER '2147483647'")
                .addRoundTrip("integer", "2147483648", INTEGER, "INTEGER '-2147483648'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_integer"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in ClickHouse and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"))

                .addRoundTrip("Nullable(bigint)", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_bigint"));

        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775809", BIGINT, "BIGINT '9223372036854775807'")
                .addRoundTrip("bigint", "9223372036854775808", BIGINT, "BIGINT '-9223372036854775808'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_bigint"));
    }

    @Test
    public void testUint8()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt8", "0", SMALLINT, "SMALLINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt8", "255", SMALLINT, "SMALLINT '255'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt8)", "NULL", SMALLINT, "CAST(null AS SMALLINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint8"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt8", "0", SMALLINT, "SMALLINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt8", "255", SMALLINT, "SMALLINT '255'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt8)", "NULL", SMALLINT, "CAST(null AS SMALLINT)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint8"));
    }

    @Test
    public void testUnsupportedUint8()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt8", "-1", SMALLINT, "SMALLINT '255'")
                .addRoundTrip("UInt8", "256", SMALLINT, "SMALLINT '0'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint8"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint8", "(value UInt8) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (-1)", table.getName()),
                    "Value must be between 0 and 255 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (256)", table.getName()),
                    "Value must be between 0 and 255 in ClickHouse: 256");
        }
    }

    @Test
    public void testUint16()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt16", "0", INTEGER, "0") // min value in ClickHouse
                .addRoundTrip("UInt16", "65535", INTEGER, "65535") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt16)", "NULL", INTEGER, "CAST(null AS INTEGER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint16"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt16", "0", INTEGER, "0") // min value in ClickHouse
                .addRoundTrip("UInt16", "65535", INTEGER, "65535") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt16)", "NULL", INTEGER, "CAST(null AS INTEGER)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint16"));
    }

    @Test
    public void testUnsupportedUint16()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt16", "-1", INTEGER, "65535")
                .addRoundTrip("UInt16", "65536", INTEGER, "0")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint16"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint16", "(value UInt16) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (-1)", table.getName()),
                    "Value must be between 0 and 65535 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (65536)", table.getName()),
                    "Value must be between 0 and 65535 in ClickHouse: 65536");
        }
    }

    @Test
    public void testUint32()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt32", "0", BIGINT, "BIGINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt32", "4294967295", BIGINT, "BIGINT '4294967295'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt32)", "NULL", BIGINT, "CAST(null AS BIGINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint32"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt32", "BIGINT '0'", BIGINT, "BIGINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt32", "BIGINT '4294967295'", BIGINT, "BIGINT '4294967295'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt32)", "NULL", BIGINT, "CAST(null AS BIGINT)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint32"));
    }

    @Test
    public void testUnsupportedUint32()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt32", "-1", BIGINT, "BIGINT '4294967295'")
                .addRoundTrip("UInt32", "4294967296", BIGINT, "BIGINT '0'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint32"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint32", "(value UInt32) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('-1' AS BIGINT))", table.getName()),
                    "Value must be between 0 and 4294967295 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('4294967296' AS BIGINT))", table.getName()),
                    "Value must be between 0 and 4294967295 in ClickHouse: 4294967296");
        }
    }

    @Test
    public void testUint64()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt64", "0", createDecimalType(20), "CAST('0' AS decimal(20, 0))") // min value in ClickHouse
                .addRoundTrip("UInt64", "18446744073709551615", createDecimalType(20), "CAST('18446744073709551615' AS decimal(20, 0))") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt64)", "NULL", createDecimalType(20), "CAST(null AS decimal(20, 0))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint64"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt64", "CAST('0' AS decimal(20, 0))", createDecimalType(20), "CAST('0' AS decimal(20, 0))") // min value in ClickHouse
                .addRoundTrip("UInt64", "CAST('18446744073709551615' AS decimal(20, 0))", createDecimalType(20), "CAST('18446744073709551615' AS decimal(20, 0))") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt64)", "NULL", createDecimalType(20), "CAST(null AS decimal(20, 0))")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint64"));
    }

    @Test
    public void testUnsupportedUint64()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt64", "-1", createDecimalType(20), "CAST('18446744073709551615' AS decimal(20, 0))")
                .addRoundTrip("UInt64", "18446744073709551616", createDecimalType(20), "CAST('0' AS decimal(20, 0))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint64"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint64", "(value UInt64) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('-1' AS decimal(20, 0)))", table.getName()),
                    "Value must be between 0 and 18446744073709551615 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('18446744073709551616' AS decimal(20, 0)))", table.getName()),
                    "Value must be between 0 and 18446744073709551615 in ClickHouse: 18446744073709551616");
        }
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS REAL)")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("real", "nan", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-inf", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+inf", REAL, "CAST(+infinity() AS REAL)")
                .addRoundTrip("Nullable(real)", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "3.1415926835", DOUBLE, "DOUBLE '3.1415926835'")
                .addRoundTrip("double", "1.79769E308", DOUBLE, "DOUBLE '1.79769E308'")

                // https://github.com/ClickHouse/ClickHouse/issues/60146
                // .addRoundTrip("double", "2.225E-307", DOUBLE, "DOUBLE '2.225E-307'")

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_double"))

                .addRoundTrip("double", "nan()", DOUBLE, "CAST(nan() AS DOUBLE)")
                .addRoundTrip("double", "-infinity()", DOUBLE, "CAST(-infinity() AS DOUBLE)")
                .addRoundTrip("double", "+infinity()", DOUBLE, "CAST(+infinity() AS DOUBLE)")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")

                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("double", "nan", DOUBLE, "CAST(nan() AS DOUBLE)")
                .addRoundTrip("double", "-inf", DOUBLE, "CAST(-infinity() AS DOUBLE)")
                .addRoundTrip("double", "+inf", DOUBLE, "CAST(+infinity() AS DOUBLE)")
                .addRoundTrip("Nullable(double)", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.trino_test_nullable_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal"))

                .addRoundTrip("decimal(3, 1)", "NULL", createDecimalType(3, 1), "CAST(NULL AS decimal(3,1))")
                .addRoundTrip("decimal(30, 5)", "NULL", createDecimalType(30, 5), "CAST(NULL AS decimal(30,5))")

                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));

        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(decimal(3, 1))", "NULL", createDecimalType(3, 1), "CAST(NULL AS decimal(3,1))")
                .addRoundTrip("Nullable(decimal(30, 5))", "NULL", createDecimalType(30, 5), "CAST(NULL AS decimal(30,5))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_nullable_decimal"));
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
    {
        // Test that DECIMAL types with precision > 38 map to NUMBER type
        // ClickHouse uses Decimal256 for precision from 39 to 76 digits
        // Scale range: [0 : P] where P is the precision

        // Test precision 39 (minimum for Decimal256, just above Trino's MAX_PRECISION of 38)
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(39, 0)", "123456789012345678901234567890123456789", NUMBER, "NUMBER '123456789012345678901234567890123456789'")
                .addRoundTrip("decimal(39, 0)", "-123456789012345678901234567890123456789", NUMBER, "NUMBER '-123456789012345678901234567890123456789'")
                .addRoundTrip("Nullable(decimal(39, 0))", "NULL", NUMBER, "CAST(NULL AS NUMBER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal_exceeding_precision_max_p39"));

        // Test precision 40, scale 5
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(40, 5)", "12345678901234567890123456789012345.12345", NUMBER, "NUMBER '12345678901234567890123456789012345.12345'")
                .addRoundTrip("decimal(40, 5)", "-12345678901234567890123456789012345.12345", NUMBER, "NUMBER '-12345678901234567890123456789012345.12345'")
                .addRoundTrip("decimal(40, 5)", "123.45", NUMBER, "NUMBER '123.45'")
                .addRoundTrip("decimal(40, 5)", "-123.45", NUMBER, "NUMBER '-123.45'")
                .addRoundTrip("Nullable(decimal(40, 5))", "NULL", NUMBER, "CAST(NULL AS NUMBER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal_exceeding_precision_max_p40"));

        // Test precision 50, scale 10
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(50, 10)", "1234567890123456789012345678901234567890.1234567890", NUMBER, "NUMBER '1234567890123456789012345678901234567890.1234567890'")
                .addRoundTrip("decimal(50, 10)", "-1234567890123456789012345678901234567890.1234567890", NUMBER, "NUMBER '-1234567890123456789012345678901234567890.1234567890'")
                .addRoundTrip("Nullable(decimal(50, 10))", "NULL", NUMBER, "CAST(NULL AS NUMBER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal_exceeding_precision_max_p50"));

        // Test precision 60, scale 20
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(60, 20)", "1234567890123456789012345678901234567890.12345678901234567890", NUMBER, "NUMBER '1234567890123456789012345678901234567890.12345678901234567890'")
                .addRoundTrip("decimal(60, 20)", "-1234567890123456789012345678901234567890.12345678901234567890", NUMBER, "NUMBER '-1234567890123456789012345678901234567890.12345678901234567890'")
                .addRoundTrip("Nullable(decimal(60, 20))", "NULL", NUMBER, "CAST(NULL AS NUMBER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal_exceeding_precision_max_p60"));

        // Test precision 76 (ClickHouse Decimal256's max), scale 30
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(76, 30)", "1234567890123456789012345678901234567890123456.123456789012345678901234567890", NUMBER, "NUMBER '1234567890123456789012345678901234567890123456.123456789012345678901234567890'")
                .addRoundTrip("decimal(76, 30)", "-1234567890123456789012345678901234567890123456.123456789012345678901234567890", NUMBER, "NUMBER '-1234567890123456789012345678901234567890123456.123456789012345678901234567890'")
                .addRoundTrip("Nullable(decimal(76, 30))", "NULL", NUMBER, "CAST(NULL AS NUMBER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal_exceeding_precision_max_p76"));

        // Test precision 76 (ClickHouse Decimal256's max), scale 76 (ClickHouse Decimal256's max)
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(76, 76)", "0.0123456789012345678901234567890123456789012345689012345678901234567890123456", NUMBER, "NUMBER '0.0123456789012345678901234567890123456789012345689012345678901234567890123456'")
                .addRoundTrip("decimal(76, 76)", "-0.0123456789012345678901234567890123456789012345689012345678901234567890123456", NUMBER, "NUMBER '-0.0123456789012345678901234567890123456789012345689012345678901234567890123456'")
                .addRoundTrip("Nullable(decimal(76, 76))", "NULL", NUMBER, "CAST(NULL AS NUMBER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal_exceeding_precision_max_p76_s76"));
    }

    @Test
    public void testClickHouseDecimalUnsupportedPrecision()
    {
        assertThatThrownBy(() -> clickhouseServer.execute("CREATE TABLE verify_negative_scale_not_supported(a decimal(77, 0)) ENGINE=Log"))
                .hasStackTraceContaining("Wrong precision");
    }

    @Test
    public void testClickHouseDecimalNegativeScale()
    {
        assertThatThrownBy(() -> clickhouseServer.execute("CREATE TABLE verify_negative_scale_not_supported(a decimal(5, -1)) ENGINE=Log"))
                .hasStackTraceContaining("Negative scales and scales larger than precision are not supported");
    }

    @Test
    public void testClickHouseDecimalScaleExceedingPrecision()
    {
        assertThatThrownBy(() -> clickhouseServer.execute("CREATE TABLE verify_negative_scale_not_supported(a decimal(76, 77)) ENGINE=Log"))
                .hasStackTraceContaining("Negative scales and scales larger than precision are not supported");
    }

    @Test
    public void testClickHouseChar()
    {
        // ClickHouse char is String, which is arbitrary bytes
        textAsBinaryRoundTripTest("char(255)")
                // plain
                .addRoundTrip("char(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("char(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // nullable
                .addRoundTrip("Nullable(char(10))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(char(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("Nullable(char(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("Nullable(char(255))", "''", VARBINARY, "X''")
                // low-cardinality
                .addRoundTrip("LowCardinality(char(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(char(255))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(char(5))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(char(32))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(char(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(char(77))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(Nullable(char(1)))", "'😂'", VARBINARY, "to_utf8('😂')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_char"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("char(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(char(10))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(char(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("Nullable(char(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(char(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(char(255))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(char(5))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(char(32))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(char(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(char(77))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(char(1)))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_char"));
    }

    @Test
    public void testClickHouseFixedString()
    {
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("FixedString(10)", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("FixedString(10)", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("FixedString(10)", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello\0\0\0\0\0')")
                .addRoundTrip("FixedString(10)", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                // nullable
                .addRoundTrip("Nullable(FixedString(10))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(FixedString(10))", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("Nullable(FixedString(10))", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("Nullable(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                // low-cardinality
                .addRoundTrip("LowCardinality(FixedString(10))", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("LowCardinality(FixedString(10))", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("LowCardinality(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_fixed_string"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("FixedString(10)", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("FixedString(10)", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("FixedString(10)", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("FixedString(10)", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                // nullable
                .addRoundTrip("Nullable(FixedString(10))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(FixedString(10))", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("LowCardinality(FixedString(10))", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("LowCardinality(FixedString(10))", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("LowCardinality(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_fixed_string"));
    }

    @Test
    public void testTrinoChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("char(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("char(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_char"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("char(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_char"));
    }

    @Test
    public void testClickHouseVarchar()
    {
        // ClickHouse varchar is String, which is arbitrary bytes
        textAsBinaryRoundTripTest("varchar(255)")
                // plain
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varchar(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("varchar(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // nullable
                .addRoundTrip("Nullable(varchar(30))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(varchar(30))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("Nullable(varchar(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("Nullable(varchar(255))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("Nullable(varchar(5))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(varchar(32))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(varchar(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("Nullable(varchar(77))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality
                .addRoundTrip("LowCardinality(varchar(30))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(varchar(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(varchar(255))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(varchar(5))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(varchar(32))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(varchar(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(varchar(77))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(Nullable(varchar(10)))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(Nullable(varchar(255)))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(Nullable(varchar(5)))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(varchar(32)))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(varchar(1)))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(Nullable(varchar(77)))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("varchar(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(varchar(30))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(varchar(30))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("Nullable(varchar(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("Nullable(varchar(255))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("Nullable(varchar(5))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(varchar(32))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(varchar(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("Nullable(varchar(77))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(varchar(30))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(255))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(5))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(32))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(77))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(10)))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(255)))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(5)))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(32)))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(1)))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(77)))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    private static SqlDataTypeTest textAsBinaryRoundTripTest(String inputType)
    {
        String nullInputType = format("Nullable(%s)", inputType);
        String lowCardInputType = format("LowCardinality(%s)", inputType);
        String nullLowCardInputType = format("LowCardinality(Nullable(%s))", inputType);

        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "''", VARBINARY, "X''")
                .addRoundTrip(inputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(inputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(inputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(inputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(inputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'")
                .addRoundTrip(nullInputType, "''", VARBINARY, "X''")
                .addRoundTrip(nullInputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(nullInputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(nullInputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(nullInputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(nullInputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'")
                .addRoundTrip(lowCardInputType, "''", VARBINARY, "X''")
                .addRoundTrip(lowCardInputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(lowCardInputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(lowCardInputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(lowCardInputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(lowCardInputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'")
                .addRoundTrip(nullLowCardInputType, "''", VARBINARY, "X''")
                .addRoundTrip(nullLowCardInputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(nullLowCardInputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(nullLowCardInputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(nullLowCardInputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(nullLowCardInputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'");
    }

    @Test
    public void testClickHouseString()
    {
        // TODO add more test cases
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("String", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("String", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("String", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("String", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("String", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("String", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("String", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // nullable
                .addRoundTrip("Nullable(String)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(String)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("Nullable(String)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("Nullable(String)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(String)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("Nullable(String)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality
                .addRoundTrip("LowCardinality(String)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(String)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(String)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(String)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(String)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(String))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("String", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("String", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("String", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("String", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("String", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("String", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("String", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(String)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(String)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("Nullable(String)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("Nullable(String)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(String)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("Nullable(String)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(String)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(String))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testTrinoVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(30)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varchar(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("varchar(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(30)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("varchar(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_varchar"));
    }

    @Test
    public void testTrinoVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"));
    }

    @Test
    public void testDate()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();
            SqlDataTypeTest.create()
                    // trino's date map to clickhouse's date32
                    .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                    .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                    .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                    .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));

            // Null
            SqlDataTypeTest.create()
                    .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
            SqlDataTypeTest.create()
                    .addRoundTrip("Nullable(date)", "NULL", DATE, "CAST(NULL AS DATE)")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"));
        }
    }

    @Test
    public void testDate32()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();
            SqlDataTypeTest.create()
                    .addRoundTrip("Nullable(date32)", "NULL", DATE, "CAST(NULL AS DATE)")
                    .addRoundTrip("date32", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                    .addRoundTrip("date32", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("date32", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("date32", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                    .addRoundTrip("date32", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                    .addRoundTrip("date32", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date32"));
        }
    }

    @Test
    public void testClickHouseDateMinMaxValues()
    {
        testClickHouseDateMinMaxValues("1970-01-01");
        testClickHouseDateMinMaxValues("2149-06-06");
    }

    private void testClickHouseDateMinMaxValues(String date)
    {
        SqlDataTypeTest dateTests = SqlDataTypeTest.create()
                .addRoundTrip("date", format("DATE '%s'", date), DATE, format("DATE '%s'", date));
        SqlDataTypeTest tupleDateTests = SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value Date)", format("('%s')", date), rowType(field("value", DATE)), format("cast(row(DATE '%s') as row(value date))", date));

        for (ZoneId timeZoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId.getId()))
                    .build();
            dateTests
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
            tupleDateTests
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_date_minmax"));
        }
    }

    @Test
    public void testClickHouseDate32MinMaxValues()
    {
        testClickHouseDate32MinMaxValues("1970-01-01");
        testClickHouseDate32MinMaxValues("2149-06-06");
    }

    private void testClickHouseDate32MinMaxValues(String date)
    {
        SqlDataTypeTest clickHouseCreateTests = SqlDataTypeTest.create()
                .addRoundTrip("date32", format("DATE '%s'", date), DATE, format("DATE '%s'", date));
        SqlDataTypeTest trinoCreateTests = SqlDataTypeTest.create()
                .addRoundTrip("date", format("DATE '%s'", date), DATE, format("DATE '%s'", date));
        SqlDataTypeTest tupleDate32Tests = SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value Date32)", format("('%s')", date), rowType(field("value", DATE)), format("cast(row(DATE '%s') as row(value date))", date));

        for (ZoneId timeZoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId.getId()))
                    .build();
            trinoCreateTests
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date32"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date32"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date32"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date32"));
            clickHouseCreateTests
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date32"));
            tupleDate32Tests
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_date32_minmax"));
        }
    }

    @Test
    public void testUnsupportedDate()
    {
        testUnsupportedDate("1969-12-31");
        testUnsupportedDate("2149-06-07");
    }

    private void testUnsupportedDate(String unsupportedDate)
    {
        String minSupportedDate = "1970-01-01";
        String maxSupportedDate = "2149-06-06";

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_date", "(dt date) ENGINE=Log")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES ('%s')", table.getName(), unsupportedDate));
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '%s')", table.getName(), unsupportedDate),
                    format("Date must be between %s and %s in ClickHouse: %s", minSupportedDate, maxSupportedDate, unsupportedDate));
            assertQuery(format("SELECT dt <> DATE '%s' FROM %s", unsupportedDate, table.getName()), "SELECT true"); // Inserting an unsupported date in ClickHouse will turn it into another date
        }
    }

    @Test
    public void testUnsupportedDate32()
    {
        testUnsupportedDate32("1899-12-31");
        testUnsupportedDate32("2300-01-01");
    }

    private void testUnsupportedDate32(String unsupportedDate)
    {
        String minSupportedDate = "1900-01-01";
        String maxSupportedDate = "2299-12-31";

        try (TestTable table = newTrinoTable("test_unsupported_date32", "(dt date)")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '%s')", table.getName(), unsupportedDate),
                    format("Date must be between %s and %s in ClickHouse: %s", minSupportedDate, maxSupportedDate, unsupportedDate));
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_date", "(dt date32) ENGINE=Log")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES ('%s')", table.getName(), unsupportedDate));
            assertQuery(format("SELECT dt <> DATE '%s' FROM %s", unsupportedDate, table.getName()), "SELECT true"); // Inserting an unsupported date in ClickHouse will turn it into another date
        }
    }

    @Test
    public void testTimestamp()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            SqlDataTypeTest.create()
                    .addRoundTrip("timestamp(0)", "timestamp '1986-01-01 00:13:07'", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'") // time gap in Kathmandu
                    .addRoundTrip("timestamp(0)", "timestamp '2018-03-25 03:17:17'", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'") // time gap in Vilnius
                    .addRoundTrip("timestamp(0)", "timestamp '2018-10-28 01:33:17'", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'") // time doubled in JVM zone
                    .addRoundTrip("timestamp(0)", "timestamp '2018-10-28 03:33:33'", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'") // time double in Vilnius
                    .addRoundTrip("timestamp(1)", "timestamp '2024-01-01 12:34:56.1'", createTimestampType(1), "TIMESTAMP '2024-01-01 12:34:56.1'")
                    .addRoundTrip("timestamp(2)", "timestamp '2024-01-01 12:34:56.12'", createTimestampType(2), "TIMESTAMP '2024-01-01 12:34:56.12'")
                    .addRoundTrip("timestamp(3)", "timestamp '2024-01-01 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2024-01-01 12:34:56.123'")
                    .addRoundTrip("timestamp(4)", "timestamp '2024-01-01 12:34:56.1234'", createTimestampType(4), "TIMESTAMP '2024-01-01 12:34:56.1234'")
                    .addRoundTrip("timestamp(5)", "timestamp '2024-01-01 12:34:56.12345'", createTimestampType(5), "TIMESTAMP '2024-01-01 12:34:56.12345'")
                    .addRoundTrip("timestamp(6)", "timestamp '2024-01-01 12:34:56.123456'", createTimestampType(6), "TIMESTAMP '2024-01-01 12:34:56.123456'")
                    .addRoundTrip("timestamp(7)", "timestamp '2024-01-01 12:34:56.1234567'", createTimestampType(7), "TIMESTAMP '2024-01-01 12:34:56.1234567'")
                    .addRoundTrip("timestamp(8)", "timestamp '2024-01-01 12:34:56.12345678'", createTimestampType(8), "TIMESTAMP '2024-01-01 12:34:56.12345678'")
                    .addRoundTrip("timestamp(9)", "timestamp '2024-01-01 12:34:56.123456789'", createTimestampType(9), "TIMESTAMP '2024-01-01 12:34:56.123456789'")
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));

            timestampTest("timestamp")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_timestamp"));
            timestampTest("datetime")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_datetime"));
        }
    }

    private SqlDataTypeTest timestampTest(String inputType)
    {
        return unsupportedTimestampBecomeUnexpectedValueTest(inputType)
                .addRoundTrip(inputType, "'1986-01-01 00:13:07'", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'") // time gap in Kathmandu
                .addRoundTrip(inputType, "'2018-03-25 03:17:17'", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'") // time gap in Vilnius
                .addRoundTrip(inputType, "'2018-10-28 01:33:17'", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'") // time doubled in JVM zone
                .addRoundTrip(inputType, "'2018-10-28 03:33:33'", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'") // time double in Vilnius
                .addRoundTrip(format("Nullable(%s)", inputType), "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))");
    }

    protected SqlDataTypeTest unsupportedTimestampBecomeUnexpectedValueTest(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "'1969-12-31 23:59:59'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'");
    }

    @Test
    public void testClickHouseDateTimeMinMaxValues()
    {
        testClickHouseDateTimeMinMaxValues(MIN_SUPPORTED_DATETIME_VALUE); // min value in ClickHouse
        testClickHouseDateTimeMinMaxValues(MAX_SUPPORTED_DATETIME_VALUE); // max value in ClickHouse
    }

    private void testClickHouseDateTimeMinMaxValues(String timestamp)
    {
        SqlDataTypeTest dateTests1 = SqlDataTypeTest.create()
                .addRoundTrip("timestamp(0)", format("timestamp '%s'", timestamp), createTimestampType(0), format("TIMESTAMP '%s'", timestamp));
        SqlDataTypeTest dateTests2 = SqlDataTypeTest.create()
                .addRoundTrip("timestamp", format("'%s'", timestamp), createTimestampType(0), format("TIMESTAMP '%s'", timestamp));
        SqlDataTypeTest dateTests3 = SqlDataTypeTest.create()
                .addRoundTrip("datetime", format("'%s'", timestamp), createTimestampType(0), format("TIMESTAMP '%s'", timestamp));
        SqlDataTypeTest tupleDateTimeTests = SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value DateTime)", format("('%s')", timestamp), rowType(field("value", createTimestampType(0))), format("cast(row(TIMESTAMP '%s') as row(value timestamp(0)))", timestamp));

        for (ZoneId timeZoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId.getId()))
                    .build();
            dateTests1
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
            dateTests2.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_timestamp"));
            dateTests3.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_datetime"));
            tupleDateTimeTests.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime_minmax"));
        }
    }

    @Test
    void testClickHouseDateTime64MinMaxValues()
    {
        assertClickHouseDateTime64MinMaxValues("1900-01-01 00:00:00.1", 1); // min value in ClickHouse
        assertClickHouseDateTime64MinMaxValues(MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_8, 8); // max value with 8 precision in ClickHouse
        assertClickHouseDateTime64MinMaxValues(MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_9, 9); // max value with 9 precision in ClickHouse
    }

    private void assertClickHouseDateTime64MinMaxValues(String timestamp, int precision)
    {
        SqlDataTypeTest trinoCreateTests = SqlDataTypeTest.create()
                .addRoundTrip("timestamp(%s)".formatted(precision), format("timestamp '%s'", timestamp), createTimestampType(precision), format("TIMESTAMP '%s'", timestamp));
        SqlDataTypeTest clickHouseCreateTests = SqlDataTypeTest.create()
                // In ClickHouse, timestamp(p) with p > 0 is mapped to DateTime64, If p = 0 (or no precision is specified), it is mapped to DateTime.
                .addRoundTrip("timestamp(%s)".formatted(precision), format("'%s'", timestamp), createTimestampType(precision), format("TIMESTAMP '%s'", timestamp))
                .addRoundTrip("datetime64(%s)".formatted(precision), format("'%s'", timestamp), createTimestampType(precision), format("TIMESTAMP '%s'", timestamp));
        SqlDataTypeTest tupleDateTime64Tests = SqlDataTypeTest.create()
                .addRoundTrip(format("Tuple(value DateTime64(%d))", precision),
                        format("('%s')", timestamp),
                        rowType(field("value", createTimestampType(precision))),
                        format("cast(row(TIMESTAMP '%s') as row(value timestamp(%d)))", timestamp, precision));

        for (ZoneId timeZoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId.getId()))
                    .build();
            trinoCreateTests
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
            clickHouseCreateTests.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_timestamp"));
            tupleDateTime64Tests.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime64_minmax"));
        }
    }

    @Test
    public void testUnsupportedTimestamp()
    {
        testUnsupportedTimestamp("1969-12-31 23:59:59"); // MIN_SUPPORTED_DATETIME_VALUE - 1 second
        testUnsupportedTimestamp("2106-02-07 06:28:16"); // MAX_SUPPORTED_DATETIME_VALUE + 1 second
    }

    public void testUnsupportedTimestamp(String unsupportedTimestamp)
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_timestamp", "(dt datetime) ENGINE=Log")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES ('%s')", table.getName(), unsupportedTimestamp));
            assertQuery(format("SELECT dt <> TIMESTAMP '%s' FROM %s", unsupportedTimestamp, table.getName()), "SELECT true"); // Inserting an unsupported datetime in ClickHouse will turn it into another datetime
        }
    }

    @Test
    void testUnsupportedDateTime64()
    {
        assertUnsupportedDateTime64("1899-12-31 23:59:59", 0, MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_8); // MIN_SUPPORTED_DATETIME64_VALUE - 1 second
        assertUnsupportedDateTime64("2300-01-01 00:00:00", 8, MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_8); // MAX_SUPPORTED_DATETIME64_VALUE_TILL_PRECISION_8 + 1 second
        assertUnsupportedDateTime64("2262-04-11 23:47:17", 9, MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_9); // MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_9 + 1 second
    }

    private void assertUnsupportedDateTime64(String unsupportedTimestamp, int precision, String maxSupportedTimestamp)
    {
        try (TestTable table = newTrinoTable("test_unsupported_timestamp_datetime64", "(dt timestamp(%d))".formatted(precision))) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (TIMESTAMP '%s')", table.getName(), unsupportedTimestamp),
                    format("Timestamp must be between %s and %s in ClickHouse: %s", MIN_SUPPORTED_DATETIME64_VALUE, maxSupportedTimestamp, unsupportedTimestamp));
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_timestamp_datetime64", "(dt datetime64(%s)) ENGINE=Log".formatted(precision))) {
            if (unsupportedTimestamp.equals("2262-04-11 23:47:17")) {
                assertThatThrownBy(() -> onRemoteDatabase().execute(format("INSERT INTO %s VALUES ('%s')", table.getName(), unsupportedTimestamp)))
                        .hasStackTraceContaining("Decimal math overflow: While executing ValuesBlockInputFormat. (DECIMAL_OVERFLOW)");
            }
            else {
                onRemoteDatabase().execute(format("INSERT INTO %s VALUES ('%s')", table.getName(), unsupportedTimestamp));
                assertQuery(format("SELECT dt <> TIMESTAMP '%s' FROM %s", unsupportedTimestamp, table.getName()), "SELECT true"); // Inserting an unsupported datetime in ClickHouse will turn it into another datetime
            }
        }
    }

    @Test
    void testUnsupportedDateTimeWithTimeZone()
    {
        for (ZoneId zoneId : timezones()) {
            String inputType = DATETIME_TYPE_FACTORY.apply(zoneId);
            testUnsupportedDateTimeWithTimeZone(inputType, "1969-12-31 23:59:59 UTC", "1969-12-31 23:59:59"); // MIN_SUPPORTED_DATETIME_VALUE - 1 second
            testUnsupportedDateTimeWithTimeZone(inputType, "2106-02-07 06:28:16 UTC", "2106-02-07 06:28:16"); // MAX_SUPPORTED_DATETIME_VALUE + 1 second
            testUnsupportedDateTimeWithTimeZone(inputType, "1970-01-01 00:00:00 Asia/Kathmandu", "1969-12-31 18:30:00");
            testUnsupportedDateTimeWithTimeZone(inputType, "1970-01-01 00:13:42 Asia/Kathmandu", "1969-12-31 18:43:42");
        }
    }

    private void testUnsupportedDateTimeWithTimeZone(String inputType, String unsupportedTimestampWithTz, String unsupportedTimestampUtc)
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_timestamp_with_tz", "(dt %s) ENGINE=Log".formatted(inputType))) {
            assertQueryFails(
                    "INSERT INTO %s VALUES (TIMESTAMP '%s')".formatted(table.getName(), unsupportedTimestampWithTz),
                    "Timestamp must be between %s and %s in ClickHouse: %s".formatted(MIN_SUPPORTED_DATETIME_VALUE, MAX_SUPPORTED_DATETIME_VALUE, unsupportedTimestampUtc));
        }
    }

    @Test
    void testDateTime64WithTimeZoneUnsupportedRanges()
    {
        for (ZoneId zoneId : timezones()) {
            String inputType = DATETIME64_TYPE_FACTORY.apply(8, zoneId);
            assertDateTime64WithTimeZoneUnsupported(inputType, 8, "1899-12-31 23:59:59 UTC", "1899-12-31 23:59:59"); // min - 1 second
            assertDateTime64WithTimeZoneUnsupported(inputType, 8, "2300-01-01 00:00:00 UTC", "2300-01-01 00:00:00"); // max with precision 8 + 1 second
            assertDateTime64WithTimeZoneUnsupported(inputType, 8, "1900-01-01 00:00:00 Asia/Kathmandu", "1899-12-31 18:18:44"); // +5:41:16 offset before 1920
            assertDateTime64WithTimeZoneUnsupported(inputType, 8, "1900-01-01 00:13:42 Asia/Kathmandu", "1899-12-31 18:32:26"); // +5:41:16 offset before 1920

            inputType = DATETIME64_TYPE_FACTORY.apply(9, zoneId);
            assertDateTime64WithTimeZoneUnsupported(inputType, 9, "2262-04-11 23:47:17 UTC", "2262-04-11 23:47:17"); // max with precision 9 + 1 second
        }
    }

    private void assertDateTime64WithTimeZoneUnsupported(String inputType, int precision, String unsupportedTimestampWithTz, String unsupportedTimestampUtc)
    {
        String maxSupportedTimestamp;
        if (precision == CLICKHOUSE_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
            maxSupportedTimestamp = MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_9;
        }
        else {
            maxSupportedTimestamp = MAX_SUPPORTED_DATETIME64_VALUE_PRECISION_8;
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_timestamp_with_tz", "(dt %s) ENGINE=Log".formatted(inputType))) {
            assertQueryFails(
                    "INSERT INTO %s VALUES (TIMESTAMP '%s')".formatted(table.getName(), unsupportedTimestampWithTz),
                    "Timestamp must be between %s and %s in ClickHouse: %s".formatted(MIN_SUPPORTED_DATETIME64_VALUE, maxSupportedTimestamp, unsupportedTimestampUtc));
        }
    }

    @Test
    public void testClickHouseDateTimeWithTimeZone()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            SqlDataTypeTest clickhouseCreateAndTrinoInsertTests = SqlDataTypeTest.create();
            addRoundTripTestsForClickHouseCreateAndTrinoInserts(clickhouseCreateAndTrinoInsertTests, DATETIME_TYPE_FACTORY, 0);
            clickhouseCreateAndTrinoInsertTests.execute(getQueryRunner(), session, clickhouseCreateAndTrinoInsert("tpch.test_timestamp_with_time_zone"));

            SqlDataTypeTest clickhouseCreateAndInsertTests = dateTimeWithTimeZoneTest(DATETIME_TYPE_FACTORY);
            clickhouseCreateAndInsertTests.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.datetime_tz"));
        }
    }

    private static void addRoundTripTestsForClickHouseCreateAndTrinoInserts(SqlDataTypeTest tests, Function<ZoneId, String> inputTypeFactory, int precision)
    {
        TimestampWithTimeZoneType expectedType = createTimestampWithTimeZoneType(precision);
        String nanos = precision == 0 ? "" : "." + "123456789".substring(0, precision);
        tests
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "timestamp '2024-01-01 12:34:56%s'".formatted(nanos), expectedType, "TIMESTAMP '2024-01-01 05:19:56%s +05:45'".formatted(nanos))
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "timestamp '2024-01-01 12:34:56%s %s'".formatted(nanos, KATHMANDU.getId()), expectedType, "TIMESTAMP '2024-01-01 12:34:56%s +05:45'".formatted(nanos))
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "timestamp '2024-01-01 12:34:56%s +00:00'".formatted(nanos), expectedType, "TIMESTAMP '2024-01-01 18:19:56%s +05:45'".formatted(nanos))
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "timestamp '2024-01-01 12:34:56%s -01:00'".formatted(nanos), expectedType, "TIMESTAMP '2024-01-01 19:19:56%s +05:45'".formatted(nanos));
    }

    private SqlDataTypeTest dateTimeWithTimeZoneTest(Function<ZoneId, String> inputTypeFactory)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create()
                .addRoundTrip(format("Nullable(%s)", inputTypeFactory.apply(UTC)), "NULL", TIMESTAMP_TZ_SECONDS, "CAST(NULL AS TIMESTAMP(0) WITH TIME ZONE)")

                // Since ClickHouse datetime(timezone) does not support values before epoch, we do not test this here.

                // epoch
                .addRoundTrip(inputTypeFactory.apply(UTC), "0", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 00:00:00 Z'")
                .addRoundTrip(inputTypeFactory.apply(UTC), "'1970-01-01 00:00:00'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 00:00:00 Z'")
                // DateTime supports the range [1970-01-01 00:00:00, 2106-02-07 06:28:15]
                // Values outside this range gets stored incorrectly in ClickHouse.
                // For example, 1970-01-01 00:00:00 in Asia/Kathmandu could be stored as 1970-01-01 05:30:00
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "'1970-01-01 00:00:00'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 05:30:00 +05:30'")

                // after epoch
                .addRoundTrip(inputTypeFactory.apply(UTC), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 Z'")
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 +05:45'")
                .addRoundTrip(inputTypeFactory.apply(ZoneId.of("GMT")), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 Z'")
                .addRoundTrip(inputTypeFactory.apply(ZoneId.of("UTC+00:00")), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 Z'")

                // time doubled in JVM zone
                .addRoundTrip(inputTypeFactory.apply(UTC), "'2018-10-28 01:33:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 01:33:17 Z'")
                .addRoundTrip(inputTypeFactory.apply(JVM_ZONE), "'2018-10-28 01:33:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 01:33:17 -05:00'")
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "'2018-10-28 01:33:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 01:33:17 +05:45'")

                // time doubled in Vilnius
                .addRoundTrip(inputTypeFactory.apply(UTC), "'2018-10-28 03:33:33'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 03:33:33 Z'")
                .addRoundTrip(inputTypeFactory.apply(VILNIUS), "'2018-10-28 03:33:33'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 03:33:33 +03:00'")
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "'2018-10-28 03:33:33'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 03:33:33 +05:45'")

                // time gap in JVM zone
                .addRoundTrip(inputTypeFactory.apply(UTC), "'1970-01-01 00:13:42'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 00:13:42 Z'")
                .addRoundTrip(inputTypeFactory.apply(UTC), "'2018-04-01 02:13:55'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-04-01 02:13:55 Z'")
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "'2018-04-01 02:13:55'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-04-01 02:13:55 +05:45'")

                // time gap in Vilnius
                .addRoundTrip(inputTypeFactory.apply(KATHMANDU), "'2018-03-25 03:17:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-03-25 03:17:17 +05:45'")

                // time gap in Kathmandu
                .addRoundTrip(inputTypeFactory.apply(VILNIUS), "'1986-01-01 00:13:07'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1986-01-01 00:13:07 +03:00'");

        return tests;
    }

    @Test
    void testClickHouseDateTime64WithTimeZone()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            SqlDataTypeTest clickhouseCreateAndTrinoInsertTests = SqlDataTypeTest.create();
            IntStream.rangeClosed(0, CLICKHOUSE_MAX_SUPPORTED_TIMESTAMP_PRECISION).forEach(precision ->
                    addRoundTripTestsForClickHouseCreateAndTrinoInserts(clickhouseCreateAndTrinoInsertTests, zoneId -> DATETIME64_TYPE_FACTORY.apply(precision, zoneId), precision));
            clickhouseCreateAndTrinoInsertTests.execute(getQueryRunner(), session, clickhouseCreateAndTrinoInsert("tpch.test_timestamp_with_time_zone"));

            SqlDataTypeTest clickhouseCreateAndInsertTests = SqlDataTypeTest.create();
            IntStream.rangeClosed(0, CLICKHOUSE_MAX_SUPPORTED_TIMESTAMP_PRECISION).forEach(precision ->
                    addRoundTripDateTime64WithTimeZoneTest(clickhouseCreateAndInsertTests, precision));
            clickhouseCreateAndInsertTests.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.datetime_tz"));
        }
    }

    private void addRoundTripDateTime64WithTimeZoneTest(SqlDataTypeTest tests, int precision)
    {
        String inputTypeUtc = DATETIME64_TYPE_FACTORY.apply(precision, UTC);
        String inputTypeKathmandu = DATETIME64_TYPE_FACTORY.apply(precision, KATHMANDU);
        String inputTypeJvmZone = DATETIME64_TYPE_FACTORY.apply(precision, JVM_ZONE);
        String inputTypeVilnius = DATETIME64_TYPE_FACTORY.apply(precision, VILNIUS);

        TimestampWithTimeZoneType expectedType = createTimestampWithTimeZoneType(precision);
        String nanos = precision == 0 ? "" : "." + "123456789".substring(0, precision);

        tests
                .addRoundTrip("Nullable(%s)".formatted(inputTypeUtc), "NULL", expectedType, "CAST(NULL AS TIMESTAMP(%d) WITH TIME ZONE)".formatted(precision))
                .addRoundTrip("Nullable(%s)".formatted(inputTypeKathmandu), "NULL", expectedType, "CAST(NULL AS TIMESTAMP(%d) WITH TIME ZONE)".formatted(precision))

                // before epoch
                .addRoundTrip(inputTypeUtc, "'1958-01-01 13:18:03%s'".formatted(nanos), expectedType, "TIMESTAMP '1958-01-01 13:18:03%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeKathmandu, "'1958-01-01 13:18:03%s'".formatted(nanos), expectedType, "TIMESTAMP '1958-01-01 13:18:03%s +05:30'".formatted(nanos))

                // epoch
                .addRoundTrip(inputTypeUtc, "0%s".formatted(nanos), expectedType, "TIMESTAMP '1970-01-01 00:00:00%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeUtc, "'1970-01-01 00:00:00%s'".formatted(nanos), expectedType, "TIMESTAMP '1970-01-01 00:00:00%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeKathmandu, "'1970-01-01 00:00:00%s'".formatted(nanos), expectedType, "TIMESTAMP '1970-01-01 00:00:00%s +05:30'".formatted(nanos))

                // after epoch
                .addRoundTrip(inputTypeUtc, "'2019-03-18 10:01:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2019-03-18 10:01:17%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeKathmandu, "'2019-03-18 10:01:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2019-03-18 10:01:17%s +05:45'".formatted(nanos))
                .addRoundTrip(DATETIME64_TYPE_FACTORY.apply(precision, ZoneId.of("GMT")), "'2019-03-18 10:01:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2019-03-18 10:01:17%s Z'".formatted(nanos))
                .addRoundTrip(DATETIME64_TYPE_FACTORY.apply(precision, ZoneId.of("UTC+00:00")), "'2019-03-18 10:01:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2019-03-18 10:01:17%s Z'".formatted(nanos))

                // time doubled in JVM zone
                .addRoundTrip(inputTypeUtc, "'2018-10-28 01:33:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-10-28 01:33:17%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeJvmZone, "'2018-10-28 01:33:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-10-28 01:33:17%s -05:00'".formatted(nanos))
                .addRoundTrip(inputTypeKathmandu, "'2018-10-28 01:33:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-10-28 01:33:17%s +05:45'".formatted(nanos))

                // time doubled in Vilnius
                .addRoundTrip(inputTypeUtc, "'2018-10-28 03:33:33%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-10-28 03:33:33%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeVilnius, "'2018-10-28 03:33:33%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-10-28 03:33:33%s +03:00'".formatted(nanos))
                .addRoundTrip(inputTypeKathmandu, "'2018-10-28 03:33:33%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-10-28 03:33:33%s +05:45'".formatted(nanos))

                // time gap in JVM zone
                .addRoundTrip(inputTypeUtc, "'1970-01-01 00:13:42%s'".formatted(nanos), expectedType, "TIMESTAMP '1970-01-01 00:13:42%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeKathmandu, "'1970-01-01 00:13:42%s'".formatted(nanos), expectedType, "TIMESTAMP '1970-01-01 00:13:42%s +05:30'".formatted(nanos))
                .addRoundTrip(inputTypeUtc, "'2018-04-01 02:13:55%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-04-01 02:13:55%s Z'".formatted(nanos))
                .addRoundTrip(inputTypeKathmandu, "'2018-04-01 02:13:55%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-04-01 02:13:55%s +05:45'".formatted(nanos))

                // time gap in Vilnius
                .addRoundTrip(inputTypeKathmandu, "'2018-03-25 03:17:17%s'".formatted(nanos), expectedType, "TIMESTAMP '2018-03-25 03:17:17%s +05:45'".formatted(nanos))

                // time gap in Kathmandu
                .addRoundTrip(inputTypeVilnius, "'1986-01-01 00:13:07%s'".formatted(nanos), expectedType, "TIMESTAMP '1986-01-01 00:13:07%s +03:00'".formatted(nanos));
    }

    @Test
    void testDateTime64WithTruncatedPrecision()
    {
        try (TestTable table = newTrinoTable("test_datetime64_truncated_precision", "(dt timestamp(3))")) {
            onRemoteDatabase().execute("INSERT INTO tpch.%s VALUES ('2024-01-01 12:34:56.978765')".formatted(table.getName()));
            computeActual("INSERT INTO %s VALUES (TIMESTAMP '2025-01-01 12:34:56.978765')".formatted(table.getName()));
            // Timestamps inserted from Trino are rounded to match the target precision
            assertQuery("SELECT dt FROM %s".formatted(table.getName()), "VALUES (TIMESTAMP '2024-01-01 12:34:56.978'), (TIMESTAMP '2025-01-01 12:34:56.979')");
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_datetime64_truncated_precision", "(dt datetime64(3)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO %s VALUES ('2024-01-01 12:34:56.978765')".formatted(table.getName()));
            computeActual("INSERT INTO %s VALUES (TIMESTAMP '2025-01-01 12:34:56.978765')".formatted(table.getName()));
            // Timestamps inserted from Trino are rounded to match the target precision
            assertQuery("SELECT dt FROM %s".formatted(table.getName()), "VALUES (TIMESTAMP '2024-01-01 12:34:56.978'), (TIMESTAMP '2025-01-01 12:34:56.979')");
        }
    }

    @Test
    void testDateTime64WithTimeZoneWithTruncatedPrecision()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            SqlDataTypeTest.create()
                    .addRoundTrip(DATETIME64_TYPE_FACTORY.apply(3, UTC), "'2024-01-01 12:34:56.978765'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2024-01-01 12:34:56.978 Z'")
                    .addRoundTrip(DATETIME64_TYPE_FACTORY.apply(3, KATHMANDU), "'2024-01-01 12:34:56.978765'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2024-01-01 12:34:56.978 +05:45'")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_datetime64_truncated_precision"));

            // Timestamps inserted from Trino are rounded to match the target precision
            SqlDataTypeTest.create()
                    .addRoundTrip(DATETIME64_TYPE_FACTORY.apply(3, KATHMANDU), "TIMESTAMP '2024-01-01 12:34:56.978765'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2024-01-01 05:19:56.979 +05:45'")
                    .execute(getQueryRunner(), session, clickhouseCreateAndTrinoInsert("tpch.test_datetime64_truncated_precision"));
        }
    }

    private List<ZoneId> timezones()
    {
        return ImmutableList.of(
                UTC,
                JVM_ZONE,
                // using two non-JVM zones so that we don't need to worry what ClickHouse system zone is
                VILNIUS,
                KATHMANDU,
                TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    @Test
    public void testEnum()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Enum('hello' = 1, 'world' = 2)", "'hello'", createUnboundedVarcharType(), "VARCHAR 'hello'")
                .addRoundTrip("Enum('hello' = 1, 'world' = 2)", "'world'", createUnboundedVarcharType(), "VARCHAR 'world'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_enum"));
    }

    @Test
    public void testUuid()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(UUID)", "NULL", UuidType.UUID, "CAST(NULL AS UUID)")
                .addRoundTrip("Nullable(UUID)", "'114514ea-0601-1981-1142-e9b55b0abd6d'", UuidType.UUID, "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("default.ck_test_uuid"));

        SqlDataTypeTest.create()
                .addRoundTrip("CAST(NULL AS UUID)", "cast(NULL as UUID)")
                .addRoundTrip("UUID '114514ea-0601-1981-1142-e9b55b0abd6d'", "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), trinoCreateAsSelect("default.ck_test_uuid"))
                .execute(getQueryRunner(), trinoCreateAndInsert("default.ck_test_uuid"));
    }

    @Test
    public void testIp()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("IPv4", "'0.0.0.0'", IPADDRESS, "IPADDRESS '0.0.0.0'")
                .addRoundTrip("IPv4", "'116.253.40.133'", IPADDRESS, "IPADDRESS '116.253.40.133'")
                .addRoundTrip("IPv4", "'255.255.255.255'", IPADDRESS, "IPADDRESS '255.255.255.255'")
                .addRoundTrip("IPv6", "'::'", IPADDRESS, "IPADDRESS '::'")
                .addRoundTrip("IPv6", "'2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "IPADDRESS '2001:44c8:129:2632:33:0:252:2'")
                .addRoundTrip("IPv6", "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'")
                .addRoundTrip("Nullable(IPv4)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .addRoundTrip("Nullable(IPv6)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_ip"));

        SqlDataTypeTest.create()
                .addRoundTrip("IPv4", "IPADDRESS '0.0.0.0'", IPADDRESS, "IPADDRESS '0.0.0.0'")
                .addRoundTrip("IPv4", "IPADDRESS '116.253.40.133'", IPADDRESS, "IPADDRESS '116.253.40.133'")
                .addRoundTrip("IPv4", "IPADDRESS '255.255.255.255'", IPADDRESS, "IPADDRESS '255.255.255.255'")
                .addRoundTrip("IPv6", "IPADDRESS '::'", IPADDRESS, "IPADDRESS '::'")
                .addRoundTrip("IPv6", "IPADDRESS '2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "IPADDRESS '2001:44c8:129:2632:33:0:252:2'")
                .addRoundTrip("IPv6", "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'")
                .addRoundTrip("Nullable(IPv4)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .addRoundTrip("Nullable(IPv6)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_ip"));
    }

    @Test
    public void testTuple()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value Bool)", "(true)", rowType(field("value", BOOLEAN)), "cast(row(true) as row(value boolean))")
                .addRoundTrip("Tuple(value Int8)", "(-128)", rowType(field("value", TINYINT)), "cast(row(-128) as row(value tinyint))")
                .addRoundTrip("Tuple(value Int16)", "(-32768)", rowType(field("value", SMALLINT)), "cast(row(-32768) as row(value smallint))")
                .addRoundTrip("Tuple(value Int32)", "(30)", rowType(field("value", INTEGER)), "cast(row(30) as row(value integer))")
                .addRoundTrip("Tuple(value Int64)", "(9223372036854775807)", rowType(field("value", BIGINT)), "cast(row(9223372036854775807) as row(value bigint))")
                .addRoundTrip("Tuple(value UInt8)", "(255)", rowType(field("value", SMALLINT)), "cast(row(SMALLINT '255') as row(value smallint))")
                .addRoundTrip("Tuple(value UInt16)", "(65535)", rowType(field("value", INTEGER)), "cast(row(65535) as row(value integer))")
                .addRoundTrip("Tuple(value UInt32)", "(4294967295)", rowType(field("value", BIGINT)), "cast(row(BIGINT '4294967295') as row(value bigint))")
                .addRoundTrip("Tuple(value UInt64)", "(18446744073709551615)", rowType(field("value", createDecimalType(20))), "cast(row(CAST('18446744073709551615' AS decimal(20, 0))) as row(value decimal(20, 0)))")
                .addRoundTrip("Tuple(value Float32)", "(3.14)", rowType(field("value", REAL)), "cast(row(REAL '3.14') as row(value real))")
                .addRoundTrip("Tuple(value real)", "(12.5)", rowType(field("value", REAL)), "cast(row(REAL '12.5') as row(value real))")
                .addRoundTrip("Tuple(value real)", "(nan)", rowType(field("value", REAL)), "cast(row(CAST(nan() AS REAL)) as row(value real))")
                .addRoundTrip("Tuple(value real)", "(-inf)", rowType(field("value", REAL)), "cast(row(CAST(-infinity() AS REAL)) as row(value real))")
                .addRoundTrip("Tuple(value real)", "(+inf)", rowType(field("value", REAL)), "cast(row(CAST(infinity() AS REAL)) as row(value real))")
                .addRoundTrip("Tuple(value Float64)", "(2.718)", rowType(field("value", DOUBLE)), "cast(row(DOUBLE '2.718') as row(value double))")
                .addRoundTrip("Tuple(value double)", "(3.1415926835)", rowType(field("value", DOUBLE)), "cast(row(DOUBLE '3.1415926835') as row(value double))")
                .addRoundTrip("Tuple(value double)", "(1.79769E308)", rowType(field("value", DOUBLE)), "cast(row(DOUBLE '1.79769E308') as row(value double))")
                // https://github.com/ClickHouse/ClickHouse/issues/60146
                // .addRoundTrip("Tuple(value double)", "(2.225E-307)", rowType(field("value", DOUBLE)), "cast(row(DOUBLE '2.225E-307') as row(value double))")
                .addRoundTrip("Tuple(value double)", "(nan)", rowType(field("value", DOUBLE)), "cast(row(CAST(nan() AS DOUBLE)) as row(value double))")
                .addRoundTrip("Tuple(value double)", "(-inf)", rowType(field("value", DOUBLE)), "cast(row(CAST(-infinity() AS DOUBLE)) as row(value double))")
                .addRoundTrip("Tuple(value double)", "(+inf)", rowType(field("value", DOUBLE)), "cast(row(CAST(infinity() AS DOUBLE)) as row(value double))")
                .addRoundTrip("Tuple(value Decimal(10, 2))", "(123.45)", rowType(field("value", createDecimalType(10, 2))), "cast(row(CAST('123.45' AS decimal(10, 2))) as row(value decimal(10, 2)))")
                .addRoundTrip("Tuple(value FixedString(8))", "('Alice')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Alice\0\0\0')) as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('Alice')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Alice')) as row(value varbinary))")
                .addRoundTrip("Tuple(value LowCardinality(FixedString(8)))", "('Alice')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Alice\0\0\0')) as row(value varbinary))")
                .addRoundTrip("Tuple(value LowCardinality(String))", "('Alice')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Alice')) as row(value varbinary))")
                .addRoundTrip("Tuple(value Date)", "('2024-01-15')", rowType(field("value", DATE)), "cast(row(DATE '2024-01-15') as row(value date))")
                .addRoundTrip("Tuple(value Date32)", "('2024-01-15')", rowType(field("value", DATE)), "cast(row(DATE '2024-01-15') as row(value date))")
                .addRoundTrip("Tuple(value DateTime)", "('2024-01-15 12:30:45')", rowType(field("value", createTimestampType(0))), "cast(row(TIMESTAMP '2024-01-15 12:30:45') as row(value timestamp(0)))")
                .addRoundTrip("Tuple(value DateTime32)", "('2024-01-15 12:30:45')", rowType(field("value", createTimestampType(0))), "cast(row(TIMESTAMP '2024-01-15 12:30:45') as row(value timestamp(0)))")
                .addRoundTrip("Tuple(value DateTime64(3))", "('2024-01-15 12:30:45.123')", rowType(field("value", createTimestampType(3))), "cast(row(TIMESTAMP '2024-01-15 12:30:45.123') as row(value timestamp(3)))")
                .addRoundTrip("Tuple(value DateTime('UTC'))", "('2024-01-15 12:30:45')", rowType(field("value", TIMESTAMP_TZ_SECONDS)), "cast(row(TIMESTAMP '2024-01-15 12:30:45 UTC') as row(value timestamp(0) with time zone))")
                .addRoundTrip("Tuple(value DateTime64(3, 'UTC'))", "('2024-01-15 12:30:45.123')", rowType(field("value", createTimestampWithTimeZoneType(3))), "cast(row(TIMESTAMP '2024-01-15 12:30:45.123 UTC') as row(value timestamp(3) with time zone))")
                .addRoundTrip("Tuple(value Enum8('active' = 1, 'inactive' = 2))", "('active')", rowType(field("value", createUnboundedVarcharType())), "cast(row(VARCHAR 'active') as row(value varchar))")
                .addRoundTrip("Tuple(value Enum16('low' = 1, 'high' = 2))", "('low')", rowType(field("value", createUnboundedVarcharType())), "cast(row(VARCHAR 'low') as row(value varchar))")
                .addRoundTrip("Tuple(value UUID)", "('114514ea-0601-1981-1142-e9b55b0abd6d')", rowType(field("value", UuidType.UUID)), "cast(row(UUID '114514ea-0601-1981-1142-e9b55b0abd6d') as row(value uuid))")
                .addRoundTrip("Tuple(value IPv4)", "('192.168.1.1')", rowType(field("value", IPADDRESS)), "cast(row(IPADDRESS '192.168.1.1') as row(value ipaddress))")
                .addRoundTrip("Tuple(value IPv6)", "('2001:db8::1')", rowType(field("value", IPADDRESS)), "cast(row(IPADDRESS '2001:db8::1') as row(value ipaddress))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tuple"));

        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value FixedString(8))", "('Alice')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Alice\0\0\0') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('Alice')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Alice') as row(value varchar))")
                .addRoundTrip("Tuple(value LowCardinality(FixedString(8)))", "('Alice')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Alice\0\0\0') as row(value varchar))")
                .addRoundTrip("Tuple(value LowCardinality(String))", "('Alice')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Alice') as row(value varchar))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple"));
    }

    @Test
    public void testTupleTrinoVarbinary()
    {
        // ClickHouse JDBC TupleDeserializer converts String/FixedString element bytes to Java Strings
        // (with U+FFFD replacement for invalid UTF-8) before returning the Tuple object. Non-UTF-8
        // bytes are therefore unrecoverable without SQL-level rewriting (e.g. tupleElement projections),
        // so only valid-UTF-8 content is tested here.
        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value String)", "('')", rowType(field("value", VARBINARY)), "cast(row(X'') as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('\\x68\\x65\\x6C\\x6C\\x6F')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('hello')) as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Piękna łąka w 東京都')) as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Bag full of 💰')) as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('\\x00\\x00\\x00\\x00\\x00\\x00')", rowType(field("value", VARBINARY)), "cast(row(X'000000000000') as row(value varbinary))")
                .addRoundTrip("Tuple(value FixedString(10))", "('c12345678b')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('c12345678b')) as row(value varbinary))")
                .addRoundTrip("Tuple(value FixedString(10))", "('c123')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('c123\0\0\0\0\0\0')) as row(value varbinary))")
                .addRoundTrip("Tuple(value FixedString(10))", "('\\x00\\x00\\x00\\x00\\x00\\x00')", rowType(field("value", VARBINARY)), "cast(row(X'00000000000000000000') as row(value varbinary))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tuple_varbinary"));
    }

    @Test
    public void testTupleTrinoVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value String)", "('Piękna łąka w 東京都')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Piękna łąka w 東京都') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('text_a')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'text_a') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('text_b')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'text_b') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('攻殻機動隊')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '攻殻機動隊') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('😂')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '😂') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('Ну, погоди!')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Ну, погоди!') as row(value varchar))")
                .addRoundTrip("Tuple(value FixedString(8))", "('Alice')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Alice\0\0\0') as row(value varchar))")
                .addRoundTrip("Tuple(value FixedString(10))", "('c123')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'c123\0\0\0\0\0\0') as row(value varchar))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_varchar"));
    }

    @Test
    public void testTupleClickHouseString()
    {
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("Tuple(value String)", "('Piękna łąka w 東京都')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Piękna łąka w 東京都')) as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('text_a')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('text_a')) as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('攻殻機動隊')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('攻殻機動隊')) as row(value varbinary))")
                .addRoundTrip("Tuple(value String)", "('😂')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('😂')) as row(value varbinary))")
                // low-cardinality
                .addRoundTrip("Tuple(value LowCardinality(String))", "('text_a')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('text_a')) as row(value varbinary))")
                .addRoundTrip("Tuple(value LowCardinality(String))", "('攻殻機動隊')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('攻殻機動隊')) as row(value varbinary))")
                .addRoundTrip("Tuple(value LowCardinality(String))", "('😂')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('😂')) as row(value varbinary))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tuple_string"));

        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("Tuple(value String)", "('Piękna łąka w 東京都')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Piękna łąka w 東京都') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('text_a')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'text_a') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('攻殻機動隊')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '攻殻機動隊') as row(value varchar))")
                .addRoundTrip("Tuple(value String)", "('😂')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '😂') as row(value varchar))")
                // low-cardinality
                .addRoundTrip("Tuple(value LowCardinality(String))", "('text_a')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'text_a') as row(value varchar))")
                .addRoundTrip("Tuple(value LowCardinality(String))", "('攻殻機動隊')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '攻殻機動隊') as row(value varchar))")
                .addRoundTrip("Tuple(value LowCardinality(String))", "('😂')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '😂') as row(value varchar))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_string"));
    }

    @Test
    public void testTupleVarbinaryNonUtf8()
    {
        // The ClickHouse JDBC TupleDeserializer converts String/FixedString bytes to Java Strings,
        // replacing invalid UTF-8 sequences with U+FFFD (Unicode replacement character, EF BF BD)
        // before this code sees the values. These tests verify the substituted bytes are returned.

        // stray continuation byte \x80 → U+FFFD (efbfbd)
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_non_utf8",
                "(col Tuple(value String)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (('\\x80'))");
            assertThat(query("SELECT col.value FROM clickhouse." + table.getName()))
                    .matches("VALUES (X'efbfbd')");
        }
        // always-invalid byte \xFF → U+FFFD (efbfbd)
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_non_utf8",
                "(col Tuple(value String)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (('\\xFF'))");
            assertThat(query("SELECT col.value FROM clickhouse." + table.getName()))
                    .matches("VALUES (X'efbfbd')");
        }
        // overlong null encoding \xC0\x80 → two U+FFFD, one per byte (efbfbdefbfbd)
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_non_utf8",
                "(col Tuple(value String)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (('\\xC0\\x80'))");
            assertThat(query("SELECT col.value FROM clickhouse." + table.getName()))
                    .matches("VALUES (X'efbfbdefbfbd')");
        }
        // mixed valid+invalid: \xF9\x36\x7A\xA7 → efbfbd 36 7a efbfbd
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_non_utf8",
                "(col Tuple(value String)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (('\\xF9\\x36\\x7A\\xA7'))");
            assertThat(query("SELECT col.value FROM clickhouse." + table.getName()))
                    .matches("VALUES (X'efbfbd367aefbfbd')");
        }
        // FixedString(4) with non-UTF-8 bytes \xF9\x36\x7A\xA7 → efbfbd 36 7a efbfbd
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_non_utf8",
                "(col Tuple(value FixedString(4))) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (('\\xF9\\x36\\x7A\\xA7'))");
            assertThat(query("SELECT col.value FROM clickhouse." + table.getName()))
                    .matches("VALUES (X'efbfbd367aefbfbd')");
        }
        // MergeTree with partition key
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_non_utf8",
                "(id Int32, col Tuple(value String)) ENGINE=MergeTree PARTITION BY id ORDER BY id")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (1, ('\\xF9\\x36\\x7A\\xA7'))");
            assertThat(query("SELECT col.value FROM clickhouse." + table.getName()))
                    .matches("VALUES (X'efbfbd367aefbfbd')");
        }
    }

    @Test
    public void testTupleClickHouseChar()
    {
        // ClickHouse char is String, which is arbitrary bytes (no fixed-width padding)
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("Tuple(value char(10))", "('text_a')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('text_a')) as row(value varbinary))")
                .addRoundTrip("Tuple(value char(255))", "('text_b')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('text_b')) as row(value varbinary))")
                .addRoundTrip("Tuple(value char(5))", "('攻殻機動隊')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('攻殻機動隊')) as row(value varbinary))")
                .addRoundTrip("Tuple(value char(1))", "('😂')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('😂')) as row(value varbinary))")
                .addRoundTrip("Tuple(value char(77))", "('Ну, погоди!')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('Ну, погоди!')) as row(value varbinary))")
                // low-cardinality
                .addRoundTrip("Tuple(value LowCardinality(char(10)))", "('text_a')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('text_a')) as row(value varbinary))")
                .addRoundTrip("Tuple(value LowCardinality(char(5)))", "('攻殻機動隊')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('攻殻機動隊')) as row(value varbinary))")
                .addRoundTrip("Tuple(value LowCardinality(char(1)))", "('😂')", rowType(field("value", VARBINARY)), "cast(row(to_utf8('😂')) as row(value varbinary))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tuple_char"));

        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("Tuple(value char(10))", "('text_a')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'text_a') as row(value varchar))")
                .addRoundTrip("Tuple(value char(255))", "('text_b')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'text_b') as row(value varchar))")
                .addRoundTrip("Tuple(value char(5))", "('攻殻機動隊')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '攻殻機動隊') as row(value varchar))")
                .addRoundTrip("Tuple(value char(1))", "('😂')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '😂') as row(value varchar))")
                .addRoundTrip("Tuple(value char(77))", "('Ну, погоди!')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'Ну, погоди!') as row(value varchar))")
                // low-cardinality
                .addRoundTrip("Tuple(value LowCardinality(char(10)))", "('text_a')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR 'text_a') as row(value varchar))")
                .addRoundTrip("Tuple(value LowCardinality(char(5)))", "('攻殻機動隊')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '攻殻機動隊') as row(value varchar))")
                .addRoundTrip("Tuple(value LowCardinality(char(1)))", "('😂')", rowType(field("value", VARCHAR)), "cast(row(VARCHAR '😂') as row(value varchar))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_char"));
    }

    @Test
    public void testTupleTrinoVarbinaryWithPartitionKey()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_varbinary_part",
                "(col Tuple(str String, num Int32, flt Float64)) ENGINE=MergeTree PARTITION BY tupleElement(col, 'num') ORDER BY tupleElement(col, 'num')")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES " +
                    "(('hello', 1, 3.14)), " +
                    "(('\\xE4\\xB8\\x9C\\xE4\\xBA\\xAC\\xE9\\x83\\xBD', 2, 0.0))");
            assertThat(query("SELECT col.str, col.num, col.flt FROM clickhouse." + table.getName() + " ORDER BY col.num"))
                    .matches("VALUES (X'68656c6c6f', 1, 3.14E0), (X'e4b89ce4baace983bd', 2, 0.0E0)");
        }
        // whole Tuple as partition key
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_varbinary_part",
                "(col Tuple(num Int32, flt Float64)) ENGINE=MergeTree PARTITION BY col ORDER BY col")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES " +
                    "((1, 3.14)), " +
                    "((2, 0.0))");
            assertThat(query("SELECT col.num, col.flt FROM clickhouse." + table.getName() + " ORDER BY col.num"))
                    .matches("VALUES (1, 3.14E0), (2, 0.0E0)");
        }
    }

    @Test
    public void testTupleMultiColumn()
    {
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(bool_val Bool, int_val Int32, bigint_val Int64, float_val Float32, double_val Float64, string_val String, date_val Date)",
                        "(true, 42, 1000000, 3.14, 2.718, 'test', '2024-01-15')",
                        rowType(
                                field("bool_val", BOOLEAN),
                                field("int_val", INTEGER),
                                field("bigint_val", BIGINT),
                                field("float_val", REAL),
                                field("double_val", DOUBLE),
                                field("string_val", VARBINARY),
                                field("date_val", DATE)),
                        "cast(ROW(true, 42, 1000000, REAL '3.14', DOUBLE '2.718', to_utf8(VARCHAR 'test'), DATE '2024-01-15') as row(bool_val boolean, int_val integer, bigint_val bigint, float_val real, double_val double, string_val varbinary, date_val date))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tuple_multi"));

        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(bool_val Bool, int_val Int32, bigint_val Int64, float_val Float32, double_val Float64, string_val String, date_val Date)",
                        "(true, 42, 1000000, 3.14, 2.718, 'test', '2024-01-15')",
                        rowType(
                                field("bool_val", BOOLEAN),
                                field("int_val", INTEGER),
                                field("bigint_val", BIGINT),
                                field("float_val", REAL),
                                field("double_val", DOUBLE),
                                field("string_val", VARCHAR),
                                field("date_val", DATE)),
                        "cast(ROW(true, 42, 1000000, REAL '3.14', DOUBLE '2.718', VARCHAR 'test', DATE '2024-01-15') as row(bool_val boolean, int_val integer, bigint_val bigint, float_val real, double_val double, string_val varchar, date_val date))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_multi"));
    }

    @Test
    public void testTupleWithDateTypes()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            SqlDataTypeTest.create()
                    .addRoundTrip("Tuple(value Date)", "('1970-02-03')", rowType(field("value", DATE)), "cast(row(DATE '1970-02-03') as row(value date))")
                    .addRoundTrip("Tuple(value Date)", "('2017-07-01')", rowType(field("value", DATE)), "cast(row(DATE '2017-07-01') as row(value date))") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("Tuple(value Date)", "('2017-01-01')", rowType(field("value", DATE)), "cast(row(DATE '2017-01-01') as row(value date))") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("Tuple(value Date)", "('1970-01-01')", rowType(field("value", DATE)), "cast(row(DATE '1970-01-01') as row(value date))")
                    .addRoundTrip("Tuple(value Date)", "('1983-04-01')", rowType(field("value", DATE)), "cast(row(DATE '1983-04-01') as row(value date))")
                    .addRoundTrip("Tuple(value Date)", "('1983-10-01')", rowType(field("value", DATE)), "cast(row(DATE '1983-10-01') as row(value date))")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_date"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Tuple(value Date32)", "('1970-02-03')", rowType(field("value", DATE)), "cast(row(DATE '1970-02-03') as row(value date))")
                    .addRoundTrip("Tuple(value Date32)", "('2017-07-01')", rowType(field("value", DATE)), "cast(row(DATE '2017-07-01') as row(value date))") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("Tuple(value Date32)", "('2017-01-01')", rowType(field("value", DATE)), "cast(row(DATE '2017-01-01') as row(value date))") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("Tuple(value Date32)", "('1970-01-01')", rowType(field("value", DATE)), "cast(row(DATE '1970-01-01') as row(value date))")
                    .addRoundTrip("Tuple(value Date32)", "('1983-04-01')", rowType(field("value", DATE)), "cast(row(DATE '1983-04-01') as row(value date))")
                    .addRoundTrip("Tuple(value Date32)", "('1983-10-01')", rowType(field("value", DATE)), "cast(row(DATE '1983-10-01') as row(value date))")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_date32"));
        }
    }

    @Test
    public void testTupleWithTimestampTypes()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            tupleTimestampTest("timestamp")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_timestamp"));
            tupleTimestampTest("datetime")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime"));
            tupleTimestampTest("DateTime64(0)")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime64_0"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Tuple(value DateTime64(0))", "('2024-01-01 12:34:56')", rowType(field("value", createTimestampType(0))), "cast(row(TIMESTAMP '2024-01-01 12:34:56') as row(value timestamp(0)))")
                    .addRoundTrip("Tuple(value DateTime64(1))", "('2024-01-01 12:34:56.1')", rowType(field("value", createTimestampType(1))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1') as row(value timestamp(1)))")
                    .addRoundTrip("Tuple(value DateTime64(2))", "('2024-01-01 12:34:56.12')", rowType(field("value", createTimestampType(2))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12') as row(value timestamp(2)))")
                    .addRoundTrip("Tuple(value DateTime64(3))", "('2024-01-01 12:34:56.123')", rowType(field("value", createTimestampType(3))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123') as row(value timestamp(3)))")
                    .addRoundTrip("Tuple(value DateTime64(4))", "('2024-01-01 12:34:56.1234')", rowType(field("value", createTimestampType(4))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1234') as row(value timestamp(4)))")
                    .addRoundTrip("Tuple(value DateTime64(5))", "('2024-01-01 12:34:56.12345')", rowType(field("value", createTimestampType(5))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12345') as row(value timestamp(5)))")
                    .addRoundTrip("Tuple(value DateTime64(6))", "('2024-01-01 12:34:56.123456')", rowType(field("value", createTimestampType(6))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123456') as row(value timestamp(6)))")
                    .addRoundTrip("Tuple(value DateTime64(7))", "('2024-01-01 12:34:56.1234567')", rowType(field("value", createTimestampType(7))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1234567') as row(value timestamp(7)))")
                    .addRoundTrip("Tuple(value DateTime64(8))", "('2024-01-01 12:34:56.12345678')", rowType(field("value", createTimestampType(8))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12345678') as row(value timestamp(8)))")
                    .addRoundTrip("Tuple(value DateTime64(9))", "('2024-01-01 12:34:56.123456789')", rowType(field("value", createTimestampType(9))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123456789') as row(value timestamp(9)))")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime64"));
        }
    }

    private SqlDataTypeTest tupleTimestampTest(String inputType)
    {
        String tupleType = format("Tuple(value %s)", inputType);
        RowType expectedType = rowType(field("value", createTimestampType(0)));
        return SqlDataTypeTest.create()
                .addRoundTrip(tupleType, "('1986-01-01 00:13:07')", expectedType, "cast(row(TIMESTAMP '1986-01-01 00:13:07') as row(value timestamp(0)))") // time gap in Kathmandu
                .addRoundTrip(tupleType, "('2018-03-25 03:17:17')", expectedType, "cast(row(TIMESTAMP '2018-03-25 03:17:17') as row(value timestamp(0)))") // time gap in Vilnius
                .addRoundTrip(tupleType, "('2018-10-28 01:33:17')", expectedType, "cast(row(TIMESTAMP '2018-10-28 01:33:17') as row(value timestamp(0)))") // time doubled in JVM zone
                .addRoundTrip(tupleType, "('2018-10-28 03:33:33')", expectedType, "cast(row(TIMESTAMP '2018-10-28 03:33:33') as row(value timestamp(0)))"); // time doubled in Vilnius
    }

    @Test
    public void testTupleWithTimestampWithTimeZoneTypes()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            tupleTimestampWithTimeZoneTest("DateTime('UTC')", "UTC")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime_tz"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Tuple(value DateTime64(0, 'UTC'))", "('2024-01-01 12:34:56')", rowType(field("value", createTimestampWithTimeZoneType(0))), "cast(row(TIMESTAMP '2024-01-01 12:34:56 UTC') as row(value timestamp(0) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(1, 'UTC'))", "('2024-01-01 12:34:56.1')", rowType(field("value", createTimestampWithTimeZoneType(1))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1 UTC') as row(value timestamp(1) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(2, 'UTC'))", "('2024-01-01 12:34:56.12')", rowType(field("value", createTimestampWithTimeZoneType(2))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12 UTC') as row(value timestamp(2) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(3, 'UTC'))", "('2024-01-01 12:34:56.123')", rowType(field("value", createTimestampWithTimeZoneType(3))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123 UTC') as row(value timestamp(3) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(4, 'UTC'))", "('2024-01-01 12:34:56.1234')", rowType(field("value", createTimestampWithTimeZoneType(4))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1234 UTC') as row(value timestamp(4) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(5, 'UTC'))", "('2024-01-01 12:34:56.12345')", rowType(field("value", createTimestampWithTimeZoneType(5))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12345 UTC') as row(value timestamp(5) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(6, 'UTC'))", "('2024-01-01 12:34:56.123456')", rowType(field("value", createTimestampWithTimeZoneType(6))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123456 UTC') as row(value timestamp(6) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(7, 'UTC'))", "('2024-01-01 12:34:56.1234567')", rowType(field("value", createTimestampWithTimeZoneType(7))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1234567 UTC') as row(value timestamp(7) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(8, 'UTC'))", "('2024-01-01 12:34:56.12345678')", rowType(field("value", createTimestampWithTimeZoneType(8))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12345678 UTC') as row(value timestamp(8) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(9, 'UTC'))", "('2024-01-01 12:34:56.123456789')", rowType(field("value", createTimestampWithTimeZoneType(9))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123456789 UTC') as row(value timestamp(9) with time zone))")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime64_tz"));
        }
    }

    @Test
    public void testTupleWithTimestampNamedTimezone()
    {
        // Asia/Kolkata has had a constant offset of +05:30 since 1945 with no DST.
        // The ClickHouse JDBC driver returns OffsetDateTime with fixed offset (+05:30) for named
        // timezone columns, both at the top level and inside Tuple elements.
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            tupleTimestampWithTimeZoneTest("DateTime('Asia/Kolkata')", "+05:30")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime_named_tz"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Tuple(value DateTime64(0, 'Asia/Kolkata'))", "('2024-01-01 12:34:56')", rowType(field("value", createTimestampWithTimeZoneType(0))), "cast(row(TIMESTAMP '2024-01-01 12:34:56 +05:30') as row(value timestamp(0) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(1, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.1')", rowType(field("value", createTimestampWithTimeZoneType(1))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1 +05:30') as row(value timestamp(1) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(2, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.12')", rowType(field("value", createTimestampWithTimeZoneType(2))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12 +05:30') as row(value timestamp(2) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(3, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.123')", rowType(field("value", createTimestampWithTimeZoneType(3))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123 +05:30') as row(value timestamp(3) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(4, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.1234')", rowType(field("value", createTimestampWithTimeZoneType(4))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1234 +05:30') as row(value timestamp(4) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(5, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.12345')", rowType(field("value", createTimestampWithTimeZoneType(5))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12345 +05:30') as row(value timestamp(5) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(6, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.123456')", rowType(field("value", createTimestampWithTimeZoneType(6))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123456 +05:30') as row(value timestamp(6) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(7, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.1234567')", rowType(field("value", createTimestampWithTimeZoneType(7))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.1234567 +05:30') as row(value timestamp(7) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(8, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.12345678')", rowType(field("value", createTimestampWithTimeZoneType(8))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.12345678 +05:30') as row(value timestamp(8) with time zone))")
                    .addRoundTrip("Tuple(value DateTime64(9, 'Asia/Kolkata'))", "('2024-01-01 12:34:56.123456789')", rowType(field("value", createTimestampWithTimeZoneType(9))), "cast(row(TIMESTAMP '2024-01-01 12:34:56.123456789 +05:30') as row(value timestamp(9) with time zone))")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_tuple_datetime64_named_tz"));
        }
    }

    private SqlDataTypeTest tupleTimestampWithTimeZoneTest(String inputType, String expectedZoneId)
    {
        String tupleType = format("Tuple(value %s)", inputType);
        RowType expectedType = rowType(field("value", TIMESTAMP_TZ_SECONDS));
        return SqlDataTypeTest.create()
                .addRoundTrip(tupleType, "('1986-01-01 00:13:07')", expectedType, "cast(row(TIMESTAMP '1986-01-01 00:13:07 %s') as row(value timestamp(0) with time zone))".formatted(expectedZoneId)) // time gap in Kathmandu
                .addRoundTrip(tupleType, "('2018-03-25 03:17:17')", expectedType, "cast(row(TIMESTAMP '2018-03-25 03:17:17 %s') as row(value timestamp(0) with time zone))".formatted(expectedZoneId)) // time gap in Vilnius
                .addRoundTrip(tupleType, "('2018-10-28 01:33:17')", expectedType, "cast(row(TIMESTAMP '2018-10-28 01:33:17 %s') as row(value timestamp(0) with time zone))".formatted(expectedZoneId)) // time doubled in JVM zone
                .addRoundTrip(tupleType, "('2018-10-28 03:33:33')", expectedType, "cast(row(TIMESTAMP '2018-10-28 03:33:33 %s') as row(value timestamp(0) with time zone))".formatted(expectedZoneId)); // time doubled in Vilnius
    }

    @Test
    public void testTupleWithDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(id Int32, name String, balance Decimal(10, 2))",
                        "(1, 'Alice', toDecimal64(10000.50, 2))",
                        rowType(
                                field("id", INTEGER),
                                field("name", createUnboundedVarcharType()),
                                field("balance", createDecimalType(10, 2))),
                        "CAST(ROW(1, VARCHAR 'Alice', DECIMAL '10000.50') AS ROW(id integer, name varchar, balance decimal(10,2)))")
                .addRoundTrip(
                        "Tuple(id Int32, name String, balance Decimal(10, 2))",
                        "(2, 'Bob', toDecimal64(0.00, 2))",
                        rowType(
                                field("id", INTEGER),
                                field("name", createUnboundedVarcharType()),
                                field("balance", createDecimalType(10, 2))),
                        "CAST(ROW(2, VARCHAR 'Bob', DECIMAL '0.00') AS ROW(id integer, name varchar, balance decimal(10,2)))")
                // value with trailing zeros: JDBC driver may return BigDecimal with lower scale than column scale
                .addRoundTrip(
                        "Tuple(id Int32, name String, balance Decimal(10, 2))",
                        "(3, 'Carol', toDecimal64(1.00, 2))",
                        rowType(
                                field("id", INTEGER),
                                field("name", createUnboundedVarcharType()),
                                field("balance", createDecimalType(10, 2))),
                        "CAST(ROW(3, VARCHAR 'Carol', DECIMAL '1.00') AS ROW(id integer, name varchar, balance decimal(10,2)))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_decimal"));

        // long decimal (p > 18): Decimal(19,2) maps to Decimal128 internally; JDBC driver returns BigDecimal
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(value Decimal(19, 2))",
                        "(12345678901234567.89)",
                        rowType(field("value", createDecimalType(19, 2))),
                        "CAST(ROW(DECIMAL '12345678901234567.89') AS ROW(value decimal(19, 2)))")
                // trailing zeros with long decimal: JDBC driver may return BigDecimal with lower scale
                .addRoundTrip(
                        "Tuple(value Decimal(19, 2))",
                        "(1.00)",
                        rowType(field("value", createDecimalType(19, 2))),
                        "CAST(ROW(DECIMAL '1.00') AS ROW(value decimal(19, 2)))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_long_decimal"));

        // Decimal128(6) = Decimal(38, 6): JDBC driver returns BigDecimal
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(value Decimal128(6))",
                        "(12345678901234567890123456789012.123456)",
                        rowType(field("value", createDecimalType(38, 6))),
                        "CAST(ROW(DECIMAL '12345678901234567890123456789012.123456') AS ROW(value decimal(38, 6)))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_decimal128"));
    }

    @Test
    public void testNestedTuple()
    {
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(id Int32, person Tuple(name String, age Int32))",
                        "(1, ('Alice', 30))",
                        rowType(
                                field("id", INTEGER),
                                field("person", rowType(field("name", createUnboundedVarcharType()), field("age", INTEGER)))),
                        "CAST(ROW(1, CAST(ROW(VARCHAR 'Alice', 30) AS ROW(name varchar, age integer))) AS ROW(id integer, person ROW(name varchar, age integer)))")
                .addRoundTrip(
                        "Tuple(id Int32, person Tuple(name String, age Int32))",
                        "(2, ('Bob', 25))",
                        rowType(
                                field("id", INTEGER),
                                field("person", rowType(field("name", createUnboundedVarcharType()), field("age", INTEGER)))),
                        "CAST(ROW(2, CAST(ROW(VARCHAR 'Bob', 25) AS ROW(name varchar, age integer))) AS ROW(id integer, person ROW(name varchar, age integer)))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_nested_tuple"));
    }

    @Test
    public void testNestedTupleWithMixedTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(order_id Int32, customer Tuple(name String, vip Bool), amount Float64, order_date Date)",
                        "(1001, ('Edward', true), 249.99, '2024-02-14')",
                        rowType(
                                field("order_id", INTEGER),
                                field("customer", rowType(
                                        field("name", createUnboundedVarcharType()),
                                        field("vip", BOOLEAN))),
                                field("amount", DOUBLE),
                                field("order_date", DATE)),
                        "CAST(ROW(1001, CAST(ROW(VARCHAR 'Edward', true) AS ROW(name varchar, vip boolean)), DOUBLE '249.99', DATE '2024-02-14') AS ROW(order_id integer, customer ROW(name varchar, vip boolean), amount double, order_date date))")
                .addRoundTrip(
                        "Tuple(order_id Int32, customer Tuple(name String, vip Bool), amount Float64, order_date Date)",
                        "(1002, ('Fiona', false), 99.50, '2024-02-15')",
                        rowType(
                                field("order_id", INTEGER),
                                field("customer", rowType(
                                        field("name", createUnboundedVarcharType()),
                                        field("vip", BOOLEAN))),
                                field("amount", DOUBLE),
                                field("order_date", DATE)),
                        "CAST(ROW(1002, CAST(ROW(VARCHAR 'Fiona', false) AS ROW(name varchar, vip boolean)), DOUBLE '99.50', DATE '2024-02-15') AS ROW(order_id integer, customer ROW(name varchar, vip boolean), amount double, order_date date))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_nested_tuple_mixed_types"));
    }

    @Test
    public void testTupleWithNullableElements()
    {
        // Tuple(Nullable(T)) — the element can be null; the tuple itself is not nullable.
        // Non-null inputs verified via SqlDataTypeTest (= comparison works when ROW fields are non-null).

        // non-null values only

        // Single nullable element
        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(value Nullable(Int32))", "(42)", rowType(field("value", INTEGER)), "cast(row(42) as row(value integer))")
                .addRoundTrip("Tuple(value Nullable(Int64))", "(9223372036854775807)", rowType(field("value", BIGINT)), "cast(row(9223372036854775807) as row(value bigint))")
                .addRoundTrip("Tuple(value Nullable(Float64))", "(2.718)", rowType(field("value", DOUBLE)), "cast(row(DOUBLE '2.718') as row(value double))")
                .addRoundTrip("Tuple(value Nullable(Date))", "('2024-01-15')", rowType(field("value", DATE)), "cast(row(DATE '2024-01-15') as row(value date))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tuple_nullable"));
        // Mixed nullable / non-nullable elements
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(id Int32, name Nullable(String), score Nullable(Float64))",
                        "(1, 'Alice', 9.5)",
                        rowType(field("id", INTEGER), field("name", VARCHAR), field("score", DOUBLE)),
                        "CAST(ROW(1, VARCHAR 'Alice', DOUBLE '9.5') AS ROW(id integer, name varchar, score double))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_nullable_mixed"));
        // Nested tuple with nullable elements
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(id Int32, info Tuple(name Nullable(String), age Nullable(Int32)))",
                        "(1, ('Alice', 30))",
                        rowType(field("id", INTEGER), field("info", rowType(field("name", VARCHAR), field("age", INTEGER)))),
                        "CAST(ROW(1, CAST(ROW(VARCHAR 'Alice', 30) AS ROW(name varchar, age integer))) AS ROW(id integer, info ROW(name varchar, age integer)))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_tuple_nullable_nested"));

        // null values only

        // Single nullable element
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_nullable_null",
                "(c1 Tuple(value Nullable(Int32)), c2 Tuple(value Nullable(Int64)), c3 Tuple(value Nullable(Float64)), c4 Tuple(value Nullable(Date))) ENGINE=Log",
                List.of("(null), (null), (null), (null)"))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(ROW(CAST(NULL AS integer)) AS ROW(value integer))," +
                            " CAST(ROW(CAST(NULL AS bigint)) AS ROW(value bigint))," +
                            " CAST(ROW(CAST(NULL AS double)) AS ROW(value double))," +
                            " CAST(ROW(CAST(NULL AS date)) AS ROW(value date))");
            assertQuery("SELECT c1.value, c2.value, c3.value, c4.value FROM " + table.getName(),
                    "VALUES (CAST(NULL AS integer), CAST(NULL AS bigint), CAST(NULL AS double), CAST(NULL AS date))");
            assertQuery(
                    "SELECT c1.value, c2.value, c3.value, c4.value FROM " + table.getName()
                            + " WHERE c1.value IS NULL AND c2.value IS NULL AND c3.value IS NULL AND c4.value IS NULL",
                    "VALUES (CAST(NULL AS integer), CAST(NULL AS bigint), CAST(NULL AS double), CAST(NULL AS date))");
        }

        // Mixed nullable / non-nullable elements
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_nullable_mixed_null",
                "(c1 Tuple(id Int32, name Nullable(String), score Nullable(Float64))) ENGINE=Log",
                List.of("(2, null, null)"))) {
            assertThat(query(mapStringAsVarcharSession(), "SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(ROW(2, CAST(NULL AS varchar), CAST(NULL AS double)) AS ROW(id integer, name varchar, score double))");
            assertQuery(mapStringAsVarcharSession(),
                    "SELECT c1.id, c1.name, c1.score FROM " + table.getName(),
                    "VALUES (2, CAST(NULL AS varchar), CAST(NULL AS double))");
            assertQuery(mapStringAsVarcharSession(),
                    "SELECT c1.id, c1.name, c1.score FROM " + table.getName()
                            + " WHERE c1.name IS NULL AND c1.score IS NULL",
                    "VALUES (2, CAST(NULL AS varchar), CAST(NULL AS double))");
        }

        // Nested tuple with nullable elements
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_nullable_nested_null",
                "(c1 Tuple(id Int32, info Tuple(name Nullable(String), age Nullable(Int32)))) ENGINE=Log",
                List.of("(2, (null, null))"))) {
            assertThat(query(mapStringAsVarcharSession(), "SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(ROW(2, CAST(ROW(CAST(NULL AS varchar), CAST(NULL AS integer)) AS ROW(name varchar, age integer))) AS ROW(id integer, info ROW(name varchar, age integer)))");
            assertQuery(mapStringAsVarcharSession(),
                    "SELECT c1.id, c1.info.name, c1.info.age FROM " + table.getName(),
                    "VALUES (2, CAST(NULL AS varchar), CAST(NULL AS integer))");
            assertQuery(mapStringAsVarcharSession(),
                    "SELECT c1.id, c1.info.name, c1.info.age FROM " + table.getName()
                            + " WHERE c1.info.name IS NULL AND c1.info.age IS NULL",
                    "VALUES (2, CAST(NULL AS varchar), CAST(NULL AS integer))");
        }
    }

    @Test
    public void testNullableTupleExperimental()
    {
        // Nullable(Tuple(...)) is an experimental ClickHouse feature requiring SET allow_experimental_nullable_tuple_type = 1.
        // The entire tuple can be NULL; its elements are NOT nullable.
        // This test is skipped for ClickHouse versions that do not support the setting.
        String tableName = "tpch.test_nullable_tuple_exp_" + randomNameSuffix();
        createTableWithExperimentalNullableTupleType(tableName, "(id Int32, data Nullable(Tuple(name String, score Int64)))");
        try {
            clickhouseServer.execute("INSERT INTO " + tableName + " VALUES (1, ('hello', 42)), (2, NULL)");

            assertThat(query(
                    mapStringAsVarcharSession(), "SELECT data FROM " + tableName + " ORDER BY id"))
                    .matches("SELECT CAST(ROW(VARCHAR 'hello', BIGINT '42') AS ROW(name varchar, score bigint)) " +
                            "UNION ALL SELECT CAST(NULL AS ROW(name varchar, score bigint))");

            assertQuery(
                    mapStringAsVarcharSession(),
                    "SELECT id FROM " + tableName + " WHERE data IS NOT NULL",
                    "VALUES 1");
            assertQuery(
                    mapStringAsVarcharSession(),
                    "SELECT id FROM " + tableName + " WHERE data IS NULL",
                    "VALUES 2");

            assertQuery(
                    mapStringAsVarcharSession(),
                    "SELECT data.name, data.score FROM " + tableName + " WHERE id = 1",
                    "VALUES ('hello', 42)");
            assertQuery(
                    mapStringAsVarcharSession(),
                    "SELECT data.name, data.score FROM " + tableName + " WHERE id = 2",
                    "VALUES (CAST(NULL AS varchar), CAST(NULL AS bigint))");

            assertQuery(
                    mapStringAsVarcharSession(),
                    "SELECT id FROM " + tableName + " WHERE data.name IS NOT NULL",
                    "VALUES 1");
            assertQuery(
                    mapStringAsVarcharSession(),
                    "SELECT id FROM " + tableName + " WHERE data.score IS NULL",
                    "VALUES 2");
        }
        finally {
            clickhouseServer.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testTupleWithNullableNestedTupleElement()
    {
        // Tuple(Nullable(Tuple(...))) — an outer tuple whose element is a nullable inner tuple.
        // Requires allow_experimental_nullable_tuple_type = 1.
        String tableName = "tpch.test_tuple_nullable_nested_" + randomNameSuffix();
        createTableWithExperimentalNullableTupleType(tableName, "(id Int32, data Tuple(nested Nullable(Tuple(value String))))");
        try {
            clickhouseServer.execute("INSERT INTO " + tableName + " VALUES (1, (('hello'))), (2, (NULL))");

            assertThat(query(mapStringAsVarcharSession(), "SELECT data FROM " + tableName + " ORDER BY id"))
                    .matches("SELECT CAST(ROW(CAST(ROW(VARCHAR 'hello') AS ROW(value varchar))) AS ROW(nested ROW(value varchar))) " +
                            "UNION ALL SELECT CAST(ROW(CAST(ROW(CAST(NULL AS varchar)) AS ROW(value varchar))) AS ROW(nested ROW(value varchar)))");

            assertQuery(
                    mapStringAsVarcharSession(),
                    "SELECT data.nested.value FROM " + tableName + " ORDER BY id",
                    "VALUES 'hello', NULL");
        }
        finally {
            clickhouseServer.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void createTableWithExperimentalNullableTupleType(String tableName, String definition)
    {
        try {
            clickhouseServer.executeWithSettings(
                    ImmutableMap.of("allow_experimental_nullable_tuple_type", "1"),
                    ImmutableList.of("CREATE TABLE %s %s ENGINE=Log".formatted(tableName, definition)));
        }
        catch (RuntimeException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            String message = cause.getMessage();
            if (message == null || (!message.contains("allow_experimental_nullable_tuple_type") && !message.contains("Unknown setting"))) {
                throw e;
            }
            assumeTrue(false, "allow_experimental_nullable_tuple_type not supported in this ClickHouse version: " + message);
        }
    }

    @Test
    public void testUnnamedTuple()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(Int32, String)", "(42, 'hello')", rowType(field(INTEGER), field(VARBINARY)), "cast(ROW(42, to_utf8(VARCHAR 'hello')) as row(integer, varbinary))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unnamed_tuple"));

        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(Int32, String)", "(42, 'hello')", rowType(field(INTEGER), field(VARCHAR)), "cast(ROW(42, VARCHAR 'hello') as row(integer, varchar))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_unnamed_tuple"));
    }

    @Test
    public void testTupleWithUnsupportedElement()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_unsupported",
                "(id Int32," +
                        " map_string_col Tuple(name String, attrs Map(String, String))," +
                        " map_int_col Tuple(name String, scores Map(String, Int32))," +
                        " point_col Tuple(name String, location Point)," +
                        " ring_col Tuple(name String, ring Ring)) ENGINE=Log")) {
            assertQueryFails("SELECT map_string_col FROM " + testTable.getName(), ".*Column 'map_string_col' cannot be resolved.*");
            assertQueryFails("SELECT map_int_col FROM " + testTable.getName(), ".*Column 'map_int_col' cannot be resolved.*");
            assertQueryFails("SELECT point_col FROM " + testTable.getName(), ".*Column 'point_col' cannot be resolved.*");
            assertQueryFails("SELECT ring_col FROM " + testTable.getName(), ".*Column 'ring_col' cannot be resolved.*");
        }
    }

    @Test
    public void testTupleWithUnsupportedElementConvertToVarchar()
    {
        Session convertToVarchar = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("Tuple(name String, attrs Map(String, String))", "('Alice', {'key': 'val'})", VARCHAR, "varchar '[Alice, {key=val}]'")
                .addRoundTrip("Tuple(name String, scores Map(String, Int32))", "('Alice', {'score': 100})", VARCHAR, "varchar '[Alice, {score=100}]'")
                .addRoundTrip("Tuple(name String, location Point)", "('Alice', (10.0, 20.0))", VARCHAR, "varchar '[Alice, [10.0, 20.0]]'")
                .addRoundTrip("Tuple(name String, ring Ring)", "('Alice', [(1.0, 2.0), (3.0, 4.0)])", VARCHAR, "varchar '[Alice, [[1.0, 2.0], [3.0, 4.0]]]'")
                // Nested Tuple whose sub-elements are unsupported: the outer Tuple should also fall back to varchar
                .addRoundTrip("Tuple(id Int32, sub Tuple(name String, attrs Map(String, String)))", "(1, ('Alice', {'key': 'val'}))", VARCHAR, "varchar '[1, [Alice, {key=val}]]'")
                .execute(getQueryRunner(), convertToVarchar, clickhouseCreateAndInsert("tpch.test_tuple_unsupported_varchar"));
    }

    @Test
    public void testTupleWithElementTypeForcedToVarchar()
            throws Exception
    {
        // jdbc-types-mapped-to-varchar forces an element type to VARCHAR, but JDBC returns the raw
        // Java object (e.g. Integer) inside a Tuple. The whole Tuple column is mapped to VARCHAR.
        try (QueryRunner queryRunner = ClickHouseQueryRunner.builder(clickhouseServer)
                .addConnectorProperty("jdbc-types-mapped-to-varchar", "Int32")
                .build()) {
            try (TestTable table = new TestTable(
                    clickhouseServer::execute,
                    "tpch.test_tuple_forced_varchar",
                    "(col Tuple(value Int32)) ENGINE=Log")) {
                clickhouseServer.execute("INSERT INTO " + table.getName() + " VALUES ((42))");
                assertThat(queryRunner.execute(getSession(), "SELECT col FROM clickhouse." + table.getName()).getOnlyValue())
                        .isEqualTo("[42]");
            }
        }
    }

    @Test
    public void testUnsupportedPoint()
    {
        Session convertToVarchar = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("Point", "(10, 10)", VARCHAR, "varchar '(10.0,10.0)'")
                .execute(getQueryRunner(), convertToVarchar, clickhouseCreateAndInsert("tpch.point"));
    }

    @Test
    public void testArray()
    {
        // Boolean
        SqlDataTypeTest.create()
                .addRoundTrip("Array(Bool)", "[true, false]", new ArrayType(BOOLEAN), "ARRAY[true, false]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_boolean"));

        // Numeric types
        SqlDataTypeTest.create()
                .addRoundTrip("Array(Int8)", "[-128, 5, 127]", new ArrayType(TINYINT), "ARRAY[TINYINT '-128', TINYINT '5', TINYINT '127']")
                .addRoundTrip("Array(Int16)", "[-32768, 32456, 32767]", new ArrayType(SMALLINT), "ARRAY[SMALLINT '-32768', SMALLINT '32456', SMALLINT '32767']")
                .addRoundTrip("Array(Int32)", "[1, 2, 1234567890]", new ArrayType(INTEGER), "ARRAY[1, 2, 1234567890]")
                .addRoundTrip("Array(Int64)", "[123456789012]", new ArrayType(BIGINT), "ARRAY[123456789012]")
                .addRoundTrip("Array(UInt8)", "[0, 255]", new ArrayType(SMALLINT), "ARRAY[SMALLINT '0', SMALLINT '255']")
                .addRoundTrip("Array(UInt16)", "[0, 65535]", new ArrayType(INTEGER), "ARRAY[0, 65535]")
                .addRoundTrip("Array(UInt32)", "[0, 4294967295]", new ArrayType(BIGINT), "ARRAY[BIGINT '0', BIGINT '4294967295']")
                .addRoundTrip("Array(UInt64)", "[0, 18446744073709551615]", new ArrayType(createDecimalType(20)), "ARRAY[CAST('0' AS decimal(20, 0)), CAST('18446744073709551615' AS decimal(20, 0))]")
                .addRoundTrip("Array(Float32)", "[3.14]", new ArrayType(REAL), "ARRAY[REAL '3.14']")
                .addRoundTrip("Array(Float32)", "[nan]", new ArrayType(REAL), "ARRAY[CAST(nan() AS REAL)]")
                .addRoundTrip("Array(Float32)", "[-inf]", new ArrayType(REAL), "ARRAY[CAST(-infinity() AS REAL)]")
                .addRoundTrip("Array(Float32)", "[+inf]", new ArrayType(REAL), "ARRAY[CAST(infinity() AS REAL)]")
                .addRoundTrip("Array(Float64)", "[2.718]", new ArrayType(DOUBLE), "ARRAY[DOUBLE '2.718']")
                .addRoundTrip("Array(Float64)", "[nan]", new ArrayType(DOUBLE), "ARRAY[CAST(nan() AS DOUBLE)]")
                .addRoundTrip("Array(Float64)", "[-inf]", new ArrayType(DOUBLE), "ARRAY[CAST(-infinity() AS DOUBLE)]")
                .addRoundTrip("Array(Float64)", "[+inf]", new ArrayType(DOUBLE), "ARRAY[CAST(infinity() AS DOUBLE)]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_numeric"));

        // Date
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Date)",
                        "['1970-01-01', '2017-07-01']",
                        new ArrayType(DATE),
                        "ARRAY[DATE '1970-01-01', DATE '2017-07-01']")
                .addRoundTrip(
                        "Array(Date32)",
                        "['1952-04-03', '2017-07-01']",
                        new ArrayType(DATE),
                        "ARRAY[DATE '1952-04-03', DATE '2017-07-01']")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_date"));

        // Timestamp
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(DateTime)",
                        "['2024-01-15 12:30:45']",
                        new ArrayType(createTimestampType(0)),
                        "ARRAY[TIMESTAMP '2024-01-15 12:30:45']")
                .addRoundTrip(
                        "Array(DateTime64(3))",
                        "['2024-01-15 12:30:45.123']",
                        new ArrayType(createTimestampType(3)),
                        "ARRAY[TIMESTAMP '2024-01-15 12:30:45.123']")
                .addRoundTrip(
                        "Array(DateTime('UTC'))",
                        "['2024-01-15 12:30:45']",
                        new ArrayType(TIMESTAMP_TZ_SECONDS),
                        "ARRAY[TIMESTAMP '2024-01-15 12:30:45 UTC']")
                .addRoundTrip(
                        "Array(DateTime64(3, 'UTC'))",
                        "['2024-01-15 12:30:45.123']",
                        new ArrayType(createTimestampWithTimeZoneType(3)),
                        "ARRAY[TIMESTAMP '2024-01-15 12:30:45.123 UTC']")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_timestamp"));

        // Decimal
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Decimal(3, 1))",
                        "[10.0, 10.1, -10.1]",
                        new ArrayType(createDecimalType(3, 1)),
                        "ARRAY[CAST('10.0' AS decimal(3, 1)), CAST('10.1' AS decimal(3, 1)), CAST('-10.1' AS decimal(3, 1))]")
                .addRoundTrip(
                        "Array(Decimal(24, 2))",
                        "[2, 2.3, 123456789.3]",
                        new ArrayType(createDecimalType(24, 2)),
                        "ARRAY[CAST('2.00' AS decimal(24, 2)), CAST('2.30' AS decimal(24, 2)), CAST('123456789.30' AS decimal(24, 2))]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_decimal"));

        // String as varbinary (default)
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(String)",
                        "['hello', 'world']",
                        new ArrayType(VARBINARY),
                        "ARRAY[to_utf8('hello'), to_utf8('world')]")
                .addRoundTrip(
                        "Array(FixedString(8))",
                        "['Alice']",
                        new ArrayType(VARBINARY),
                        "ARRAY[to_utf8('Alice\0\0\0')]")
                .addRoundTrip(
                        "Array(LowCardinality(String))",
                        "['hello', 'world']",
                        new ArrayType(VARBINARY),
                        "ARRAY[to_utf8('hello'), to_utf8('world')]")
                .addRoundTrip(
                        "Array(LowCardinality(FixedString(8)))",
                        "['Alice']",
                        new ArrayType(VARBINARY),
                        "ARRAY[to_utf8('Alice\0\0\0')]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_string"));

        // String as varchar
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(String)",
                        "['hello', 'world']",
                        new ArrayType(VARCHAR),
                        "CAST(ARRAY['hello', 'world'] AS array(varchar))")
                .addRoundTrip(
                        "Array(FixedString(8))",
                        "['Alice']",
                        new ArrayType(VARCHAR),
                        "CAST(ARRAY[VARCHAR 'Alice\0\0\0'] AS array(varchar))")
                .addRoundTrip(
                        "Array(LowCardinality(String))",
                        "['hello', 'world']",
                        new ArrayType(VARCHAR),
                        "CAST(ARRAY['hello', 'world'] AS array(varchar))")
                .addRoundTrip(
                        "Array(LowCardinality(FixedString(8)))",
                        "['Alice']",
                        new ArrayType(VARCHAR),
                        "CAST(ARRAY[VARCHAR 'Alice\0\0\0'] AS array(varchar))")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_array_string_varchar"));

        // Enum
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Enum8('active' = 1, 'inactive' = 2))",
                        "['active', 'inactive']",
                        new ArrayType(createUnboundedVarcharType()),
                        "ARRAY[VARCHAR 'active', VARCHAR 'inactive']")
                .addRoundTrip(
                        "Array(Enum16('low' = 1, 'high' = 2))",
                        "['low', 'high']",
                        new ArrayType(createUnboundedVarcharType()),
                        "ARRAY[VARCHAR 'low', VARCHAR 'high']")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_enum"));

        // UUID
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(UUID)",
                        "['114514ea-0601-1981-1142-e9b55b0abd6d']",
                        new ArrayType(UuidType.UUID),
                        "ARRAY[UUID '114514ea-0601-1981-1142-e9b55b0abd6d']")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_uuid"));

        // IP address
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(IPv4)",
                        "['192.168.1.1', '10.0.0.1']",
                        new ArrayType(IPADDRESS),
                        "ARRAY[IPADDRESS '192.168.1.1', IPADDRESS '10.0.0.1']")
                .addRoundTrip(
                        "Array(IPv6)",
                        "['2001:db8::1', '::1']",
                        new ArrayType(IPADDRESS),
                        "ARRAY[IPADDRESS '2001:db8::1', IPADDRESS '::1']")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_ip"));

        // Nested arrays
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Array(Int32))",
                        "[[1, 2], [3, 4]]",
                        new ArrayType(new ArrayType(INTEGER)),
                        "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_nested"));
    }

    @Test
    public void testArrayWithTupleElement()
    {
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Tuple(a Int32, b String))",
                        "[(1, 'hello'), (2, 'world')]",
                        new ArrayType(rowType(field("a", INTEGER), field("b", VARBINARY))),
                        "ARRAY[CAST(ROW(1, to_utf8('hello')) AS row(a integer, b varbinary)), CAST(ROW(2, to_utf8('world')) AS row(a integer, b varbinary))]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_of_tuple"));
    }

    @Test
    public void testArrayTrinoVarbinary()
    {
        // Array(String)/Array(FixedString) elements are returned as raw byte[] by the JDBC driver,
        // unlike Tuple elements which go through TupleDeserializer and lose non-UTF-8 bytes.
        // Original bytes are preserved even when they are not valid UTF-8.
        SqlDataTypeTest.create()
                .addRoundTrip("Array(String)", "['']", new ArrayType(VARBINARY), "ARRAY[X'']")
                .addRoundTrip("Array(String)", "['\\x68\\x65\\x6C\\x6C\\x6F']", new ArrayType(VARBINARY), "ARRAY[to_utf8('hello')]")
                .addRoundTrip("Array(String)", "['Piękna łąka w 東京都']", new ArrayType(VARBINARY), "ARRAY[to_utf8('Piękna łąka w 東京都')]")
                .addRoundTrip("Array(String)", "['Bag full of 💰']", new ArrayType(VARBINARY), "ARRAY[to_utf8('Bag full of 💰')]")
                .addRoundTrip("Array(String)", "['\\x00\\x00\\x00\\x00\\x00\\x00']", new ArrayType(VARBINARY), "ARRAY[X'000000000000']")
                // non-UTF-8 bytes are preserved as-is (unlike Tuple where they become U+FFFD)
                .addRoundTrip("Array(String)", "['\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00']", new ArrayType(VARBINARY), "ARRAY[X'0001020304050607080df9367aa7000000']")
                .addRoundTrip("Array(FixedString(10))", "['c12345678b']", new ArrayType(VARBINARY), "ARRAY[to_utf8('c12345678b')]")
                .addRoundTrip("Array(FixedString(10))", "['c123']", new ArrayType(VARBINARY), "ARRAY[to_utf8('c123\0\0\0\0\0\0')]")
                .addRoundTrip("Array(FixedString(10))", "['\\x00\\x00\\x00\\x00\\x00\\x00']", new ArrayType(VARBINARY), "ARRAY[X'00000000000000000000']")
                // non-UTF-8 bytes in FixedString elements are also preserved as-is
                .addRoundTrip("Array(FixedString(17))", "['\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00']", new ArrayType(VARBINARY), "ARRAY[X'0001020304050607080df9367aa7000000']")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_varbinary"));
    }

    @Test
    public void testArrayTrinoVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Array(String)", "['Piękna łąka w 東京都']", new ArrayType(VARCHAR), "ARRAY[VARCHAR 'Piękna łąka w 東京都']")
                .addRoundTrip("Array(String)", "['text_a']", new ArrayType(VARCHAR), "ARRAY[VARCHAR 'text_a']")
                .addRoundTrip("Array(String)", "['攻殻機動隊']", new ArrayType(VARCHAR), "ARRAY[VARCHAR '攻殻機動隊']")
                .addRoundTrip("Array(String)", "['😂']", new ArrayType(VARCHAR), "ARRAY[VARCHAR '😂']")
                .addRoundTrip("Array(String)", "['Ну, погоди!']", new ArrayType(VARCHAR), "ARRAY[VARCHAR 'Ну, погоди!']")
                .addRoundTrip("Array(FixedString(8))", "['Alice']", new ArrayType(VARCHAR), "ARRAY[VARCHAR 'Alice\0\0\0']")
                .addRoundTrip("Array(FixedString(10))", "['c123']", new ArrayType(VARCHAR), "ARRAY[VARCHAR 'c123\0\0\0\0\0\0']")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_array_varchar"));
    }

    @Test
    public void testArrayWithDateTypes()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            SqlDataTypeTest.create()
                    .addRoundTrip("Array(Date)", "['1970-02-03']", new ArrayType(DATE), "ARRAY[DATE '1970-02-03']")
                    .addRoundTrip("Array(Date)", "['2017-07-01']", new ArrayType(DATE), "ARRAY[DATE '2017-07-01']") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("Array(Date)", "['2017-01-01']", new ArrayType(DATE), "ARRAY[DATE '2017-01-01']") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("Array(Date)", "['1970-01-01']", new ArrayType(DATE), "ARRAY[DATE '1970-01-01']")
                    .addRoundTrip("Array(Date)", "['1983-04-01']", new ArrayType(DATE), "ARRAY[DATE '1983-04-01']")
                    .addRoundTrip("Array(Date)", "['1983-10-01']", new ArrayType(DATE), "ARRAY[DATE '1983-10-01']")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_date_dst"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Array(Date32)", "['1970-02-03']", new ArrayType(DATE), "ARRAY[DATE '1970-02-03']")
                    .addRoundTrip("Array(Date32)", "['2017-07-01']", new ArrayType(DATE), "ARRAY[DATE '2017-07-01']") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("Array(Date32)", "['2017-01-01']", new ArrayType(DATE), "ARRAY[DATE '2017-01-01']") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("Array(Date32)", "['1970-01-01']", new ArrayType(DATE), "ARRAY[DATE '1970-01-01']")
                    .addRoundTrip("Array(Date32)", "['1983-04-01']", new ArrayType(DATE), "ARRAY[DATE '1983-04-01']")
                    .addRoundTrip("Array(Date32)", "['1983-10-01']", new ArrayType(DATE), "ARRAY[DATE '1983-10-01']")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_date32_dst"));
        }
    }

    @Test
    public void testArrayWithTimestampTypes()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            arrayTimestampTest("timestamp")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_timestamp_dst"));
            arrayTimestampTest("datetime")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_datetime_dst"));
            arrayTimestampTest("DateTime64(0)")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_datetime64_0_dst"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Array(DateTime64(0))", "['2024-01-01 12:34:56']", new ArrayType(createTimestampType(0)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56']")
                    .addRoundTrip("Array(DateTime64(1))", "['2024-01-01 12:34:56.1']", new ArrayType(createTimestampType(1)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.1']")
                    .addRoundTrip("Array(DateTime64(2))", "['2024-01-01 12:34:56.12']", new ArrayType(createTimestampType(2)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.12']")
                    .addRoundTrip("Array(DateTime64(3))", "['2024-01-01 12:34:56.123']", new ArrayType(createTimestampType(3)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.123']")
                    .addRoundTrip("Array(DateTime64(4))", "['2024-01-01 12:34:56.1234']", new ArrayType(createTimestampType(4)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.1234']")
                    .addRoundTrip("Array(DateTime64(5))", "['2024-01-01 12:34:56.12345']", new ArrayType(createTimestampType(5)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.12345']")
                    .addRoundTrip("Array(DateTime64(6))", "['2024-01-01 12:34:56.123456']", new ArrayType(createTimestampType(6)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.123456']")
                    .addRoundTrip("Array(DateTime64(7))", "['2024-01-01 12:34:56.1234567']", new ArrayType(createTimestampType(7)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.1234567']")
                    .addRoundTrip("Array(DateTime64(8))", "['2024-01-01 12:34:56.12345678']", new ArrayType(createTimestampType(8)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.12345678']")
                    .addRoundTrip("Array(DateTime64(9))", "['2024-01-01 12:34:56.123456789']", new ArrayType(createTimestampType(9)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.123456789']")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_datetime64_precisions"));
        }
    }

    private SqlDataTypeTest arrayTimestampTest(String inputType)
    {
        String arrayType = format("Array(%s)", inputType);
        ArrayType expectedType = new ArrayType(createTimestampType(0));
        return SqlDataTypeTest.create()
                .addRoundTrip(arrayType, "['1986-01-01 00:13:07']", expectedType, "ARRAY[TIMESTAMP '1986-01-01 00:13:07']") // time gap in Kathmandu
                .addRoundTrip(arrayType, "['2018-03-25 03:17:17']", expectedType, "ARRAY[TIMESTAMP '2018-03-25 03:17:17']") // time gap in Vilnius
                .addRoundTrip(arrayType, "['2018-10-28 01:33:17']", expectedType, "ARRAY[TIMESTAMP '2018-10-28 01:33:17']") // time doubled in JVM zone
                .addRoundTrip(arrayType, "['2018-10-28 03:33:33']", expectedType, "ARRAY[TIMESTAMP '2018-10-28 03:33:33']"); // time doubled in Vilnius
    }

    @Test
    public void testArrayWithTimestampWithTimeZoneTypes()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            arrayTimestampWithTimeZoneTest("DateTime('UTC')", "UTC")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_datetime_tz"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Array(DateTime64(0, 'UTC'))", "['2024-01-01 12:34:56']", new ArrayType(createTimestampWithTimeZoneType(0)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56 UTC']")
                    .addRoundTrip("Array(DateTime64(1, 'UTC'))", "['2024-01-01 12:34:56.1']", new ArrayType(createTimestampWithTimeZoneType(1)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.1 UTC']")
                    .addRoundTrip("Array(DateTime64(2, 'UTC'))", "['2024-01-01 12:34:56.12']", new ArrayType(createTimestampWithTimeZoneType(2)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.12 UTC']")
                    .addRoundTrip("Array(DateTime64(3, 'UTC'))", "['2024-01-01 12:34:56.123']", new ArrayType(createTimestampWithTimeZoneType(3)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.123 UTC']")
                    .addRoundTrip("Array(DateTime64(4, 'UTC'))", "['2024-01-01 12:34:56.1234']", new ArrayType(createTimestampWithTimeZoneType(4)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.1234 UTC']")
                    .addRoundTrip("Array(DateTime64(5, 'UTC'))", "['2024-01-01 12:34:56.12345']", new ArrayType(createTimestampWithTimeZoneType(5)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.12345 UTC']")
                    .addRoundTrip("Array(DateTime64(6, 'UTC'))", "['2024-01-01 12:34:56.123456']", new ArrayType(createTimestampWithTimeZoneType(6)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.123456 UTC']")
                    .addRoundTrip("Array(DateTime64(7, 'UTC'))", "['2024-01-01 12:34:56.1234567']", new ArrayType(createTimestampWithTimeZoneType(7)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.1234567 UTC']")
                    .addRoundTrip("Array(DateTime64(8, 'UTC'))", "['2024-01-01 12:34:56.12345678']", new ArrayType(createTimestampWithTimeZoneType(8)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.12345678 UTC']")
                    .addRoundTrip("Array(DateTime64(9, 'UTC'))", "['2024-01-01 12:34:56.123456789']", new ArrayType(createTimestampWithTimeZoneType(9)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.123456789 UTC']")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_datetime64_tz"));
        }
    }

    @Test
    public void testArrayWithTimestampNamedTimezone()
    {
        // Asia/Kolkata has had a constant offset of +05:30 since 1945 with no DST.
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            arrayTimestampWithTimeZoneTest("DateTime('Asia/Kolkata')", "+05:30")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_datetime_named_tz"));

            SqlDataTypeTest.create()
                    .addRoundTrip("Array(DateTime64(0, 'Asia/Kolkata'))", "['2024-01-01 12:34:56']", new ArrayType(createTimestampWithTimeZoneType(0)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56 +05:30']")
                    .addRoundTrip("Array(DateTime64(3, 'Asia/Kolkata'))", "['2024-01-01 12:34:56.123']", new ArrayType(createTimestampWithTimeZoneType(3)), "ARRAY[TIMESTAMP '2024-01-01 12:34:56.123 +05:30']")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_array_datetime64_named_tz"));
        }
    }

    private SqlDataTypeTest arrayTimestampWithTimeZoneTest(String inputType, String expectedZoneId)
    {
        String arrayType = format("Array(%s)", inputType);
        ArrayType expectedType = new ArrayType(TIMESTAMP_TZ_SECONDS);
        return SqlDataTypeTest.create()
                .addRoundTrip(arrayType, "['1986-01-01 00:13:07']", expectedType, "ARRAY[TIMESTAMP '1986-01-01 00:13:07 %s']".formatted(expectedZoneId)) // time gap in Kathmandu
                .addRoundTrip(arrayType, "['2018-03-25 03:17:17']", expectedType, "ARRAY[TIMESTAMP '2018-03-25 03:17:17 %s']".formatted(expectedZoneId)) // time gap in Vilnius
                .addRoundTrip(arrayType, "['2018-10-28 01:33:17']", expectedType, "ARRAY[TIMESTAMP '2018-10-28 01:33:17 %s']".formatted(expectedZoneId)) // time doubled in JVM zone
                .addRoundTrip(arrayType, "['2018-10-28 03:33:33']", expectedType, "ARRAY[TIMESTAMP '2018-10-28 03:33:33 %s']".formatted(expectedZoneId)); // time doubled in Vilnius
    }

    @Test
    public void testArrayWithDecimal()
    {
        // value with trailing zeros: JDBC driver may return BigDecimal with lower scale than column scale
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Decimal(10, 2))",
                        "[123.45, 0.00, 1.00]",
                        new ArrayType(createDecimalType(10, 2)),
                        "ARRAY[CAST('123.45' AS decimal(10, 2)), CAST('0.00' AS decimal(10, 2)), CAST('1.00' AS decimal(10, 2))]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_decimal_short"));

        // long decimal (p > 18)
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Decimal(19, 2))",
                        "[12345678901234567.89, 1.00]",
                        new ArrayType(createDecimalType(19, 2)),
                        "ARRAY[CAST('12345678901234567.89' AS decimal(19, 2)), CAST('1.00' AS decimal(19, 2))]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_decimal_long"));

        // Decimal128(6) = Decimal(38, 6)
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Array(Decimal128(6))",
                        "[12345678901234567890123456789012.123456]",
                        new ArrayType(createDecimalType(38, 6)),
                        "ARRAY[CAST('12345678901234567890123456789012.123456' AS decimal(38, 6))]")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_array_decimal128"));
    }

    @Test
    public void testArrayWithNullableElements()
    {
        // Arrays with mixed null and non-null values — cannot use SqlDataTypeTest because
        // verifyPredicate generates "WHERE col = ARRAY[..., NULL, ...]" which ClickHouse rejects
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_array_nullable",
                "(c1 Array(Nullable(Int32)), c2 Array(Nullable(Float64)), c3 Array(Nullable(Date))) ENGINE=Log",
                List.of("[42, NULL, -1], [2.718, NULL], ['2024-01-15', NULL]"))) {
            assertThat(query("SELECT c1, c2, c3 FROM " + table.getName()))
                    .matches("SELECT ARRAY[42, NULL, -1]," +
                            " ARRAY[DOUBLE '2.718', NULL]," +
                            " ARRAY[DATE '2024-01-15', NULL]");
        }

        // All-null arrays
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_array_all_null",
                "(c1 Array(Nullable(Int32)), c2 Array(Nullable(Float64)), c3 Array(Nullable(Date))) ENGINE=Log",
                List.of("[null, null], [null], [null, null, null]"))) {
            assertThat(query("SELECT c1, c2, c3 FROM " + table.getName()))
                    .matches("SELECT ARRAY[CAST(NULL AS integer), CAST(NULL AS integer)]," +
                            " ARRAY[CAST(NULL AS double)]," +
                            " ARRAY[CAST(NULL AS date), CAST(NULL AS date), CAST(NULL AS date)]");
        }
    }

    @Test
    public void testArrayWithUnsupportedElement()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_array_unsupported",
                "(id Int32," +
                        " map_string_col Array(Map(String, String))," +
                        " map_int_col Array(Map(String, Int32))," +
                        " point_col Array(Point)," +
                        " ring_col Array(Ring)) ENGINE=Log")) {
            assertQueryFails("SELECT map_string_col FROM " + testTable.getName(), ".*Column 'map_string_col' cannot be resolved.*");
            assertQueryFails("SELECT map_int_col FROM " + testTable.getName(), ".*Column 'map_int_col' cannot be resolved.*");
            assertQueryFails("SELECT point_col FROM " + testTable.getName(), ".*Column 'point_col' cannot be resolved.*");
            assertQueryFails("SELECT ring_col FROM " + testTable.getName(), ".*Column 'ring_col' cannot be resolved.*");
        }
    }

    @Test
    public void testArrayWithUnsupportedElementConvertToVarchar()
    {
        Session convertToVarchar = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("Array(Map(String, String))", "[map('key', 'val')]", VARCHAR, "varchar '[{key=val}]'")
                .addRoundTrip("Array(Map(String, Int32))", "[map('score', 100)]", VARCHAR, "varchar '[{score=100}]'")
                .execute(getQueryRunner(), convertToVarchar, clickhouseCreateAndInsert("tpch.test_array_unsupported_varchar"));
    }

    @Test
    public void testArrayWithElementTypeForcedToVarchar()
            throws Exception
    {
        // jdbc-types-mapped-to-varchar forces an element type to VARCHAR, but JDBC returns the raw
        // Java object (e.g. Integer) inside an Array. The whole Array column is mapped to VARCHAR.
        try (QueryRunner queryRunner = ClickHouseQueryRunner.builder(clickhouseServer)
                .addConnectorProperty("jdbc-types-mapped-to-varchar", "Int32")
                .build()) {
            try (TestTable table = new TestTable(
                    clickhouseServer::execute,
                    "tpch.test_array_forced_varchar",
                    "(col Array(Int32)) ENGINE=Log")) {
                clickhouseServer.execute("INSERT INTO " + table.getName() + " VALUES ([1, 2, 3])");
                assertThat(queryRunner.execute(getSession(), "SELECT col FROM clickhouse." + table.getName()).getOnlyValue())
                        .isEqualTo("[1,2,3]");
            }
        }
    }

    @Test
    public void testTupleWithArrayField()
    {
        SqlDataTypeTest.create()
                .addRoundTrip(
                        "Tuple(a Int32, b Array(Int32))",
                        "(42, [1, 2, 3])",
                        rowType(field("a", INTEGER), field("b", new ArrayType(INTEGER))),
                        "CAST(ROW(42, ARRAY[1, 2, 3]) AS row(a integer, b array(integer)))")
                .addRoundTrip(
                        "Tuple(a Array(String), b Int32)",
                        "(['hello', 'world'], 7)",
                        rowType(field("a", new ArrayType(VARBINARY)), field("b", INTEGER)),
                        "CAST(ROW(ARRAY[to_utf8('hello'), to_utf8('world')], 7) AS row(a array(varbinary), b integer))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tuple_with_array"));
    }

    @Test
    public void testNestedColumn()
    {
        // ClickHouse Nested type is flattened by the server into separate Array columns named
        // "col.field". The JDBC driver exposes them as individual Array(T) columns, so the
        // connector reads them as regular array columns without special handling.
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_nested",
                "(id Int32, tags Nested(name String, score Int32)) ENGINE=Log",
                List.of("1, ['Alice', 'Bob'], [95, 80]", "2, ['Charlie'], [70]"))) {
            // ClickHouse flattens Nested into: tags.name Array(String), tags.score Array(Int32)
            assertThat(query("SELECT * FROM " + table.getName() + " ORDER BY id"))
                    .matches("VALUES " +
                            "(1, ARRAY[to_utf8('Alice'), to_utf8('Bob')], ARRAY[95, 80]), " +
                            "(2, ARRAY[to_utf8('Charlie')], ARRAY[70])");
            assertThat(query("SELECT \"tags.name\", \"tags.score\" FROM " + table.getName() + " ORDER BY id"))
                    .matches("VALUES " +
                            "(ARRAY[to_utf8('Alice'), to_utf8('Bob')], ARRAY[95, 80]), " +
                            "(ARRAY[to_utf8('Charlie')], ARRAY[70])");

            assertThat(query("SELECT id FROM " + table.getName() + " WHERE cardinality(\"tags.name\") > 1"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE contains(\"tags.score\", 70)"))
                    .matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE \"tags.name\"[1] = to_utf8('Alice')"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE \"tags.score\"[1] = 95"))
                    .matches("VALUES 1");
        }
    }

    protected static Session mapStringAsVarcharSession()
    {
        return testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema(TPCH_SCHEMA)
                .setCatalogSessionProperty("clickhouse", "map_string_as_varchar", "true")
                .build();
    }

    protected DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    protected DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    protected DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    protected DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    protected DataSetup clickhouseCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new ClickHouseSqlExecutor(onRemoteDatabase()), tableNamePrefix);
    }

    protected DataSetup clickhouseCreateAndTrinoInsert(String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(new ClickHouseSqlExecutor(onRemoteDatabase()), new TrinoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    protected SqlExecutor onRemoteDatabase()
    {
        return clickhouseServer::execute;
    }
}
