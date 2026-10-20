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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/// Query-level coverage for the scalar expressions the GPU compiles, over data read from Parquet.
final class TestGpuExpressionsQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner runner = IcebergQueryRunner.builder()
                .configureGpuDistributedExecution()
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .build();
        try {
            // Iceberg has no tinyint, smallint, char or bounded varchar, so tests of those types use Hive
            runner.installPlugin(new TestingHivePlugin(runner.getCoordinator().getBaseDataDir().resolve("hive_data")));
            runner.createCatalog("hive", "hive", ImmutableMap.of(
                    "hive.storage-format", "PARQUET",
                    "hive.parquet.time-zone", "UTC"));
            runner.execute("CREATE SCHEMA hive.default");
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, runner);
            throw e;
        }
        return runner;
    }

    @Test
    void testArithmetic()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_arithmetic AS
                SELECT
                    CAST(a_tinyint AS tinyint) a_tinyint,
                    CAST(a_smallint AS smallint) a_smallint,
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_real AS real) a_real,
                    CAST(a_double AS double) a_double
                FROM (VALUES
                    (1, 10, 100, 1000, 1.5, 2.5),
                    (-3, -20, -7, -123456789, -0.25, -1e10),
                    (5, 30, 12345, 98765, 3.0, 0.1),
                    (NULL, NULL, NULL, NULL, NULL, NULL))
                    t(a_tinyint, a_smallint, an_integer, a_bigint, a_real, a_double)
                """,
                4);

        assertThat(query("SELECT a_tinyint + a_tinyint FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_tinyint * a_tinyint FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_smallint - a_smallint FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_smallint * a_smallint FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer * an_integer FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_bigint * a_bigint FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_bigint / an_integer FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer % a_tinyint FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_real + a_real FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_double / a_real FROM hive.default.test_arithmetic")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_arithmetic");
    }

    @Test
    void testDecimalArithmetic()
    {
        assertUpdate(
                """
                CREATE TABLE test_decimal_arithmetic AS
                SELECT
                    CAST(a_short_decimal AS decimal(8, 2)) a_short_decimal,
                    CAST(a_long_decimal AS decimal(30, 5)) a_long_decimal
                FROM (VALUES
                    (12.34, 1234567890.12345),
                    (-0.01, -99999.99999),
                    (999.99, 0.00001),
                    (NULL, NULL))
                    t(a_short_decimal, a_long_decimal)
                """,
                4);

        assertThat(query("SELECT a_short_decimal + a_short_decimal FROM test_decimal_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal - 1.5 FROM test_decimal_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal * a_short_decimal FROM test_decimal_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal / a_short_decimal FROM test_decimal_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_long_decimal + a_long_decimal FROM test_decimal_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_long_decimal - a_short_decimal FROM test_decimal_arithmetic")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_long_decimal / a_short_decimal FROM test_decimal_arithmetic")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_decimal_arithmetic");
    }

    @Test
    void testDecimalDivideRoundToScale()
    {
        assertUpdate(
                """
                CREATE TABLE test_decimal_divide_round_to_scale AS
                SELECT
                    CAST(a_short_decimal AS decimal(8, 2)) a_short_decimal,
                    CAST(a_long_decimal AS decimal(30, 5)) a_long_decimal
                FROM (VALUES
                    (12.34, 1234567890.12345),
                    (-0.01, -99999.99999),
                    (999.99, 0.00001),
                    (NULL, NULL))
                    t(a_short_decimal, a_long_decimal)
                """,
                4);

        // Next to a sum of the same decimal, avg is planned as that sum divided by a count in a projection
        assertThat(query("SELECT sum(a_short_decimal), avg(a_short_decimal) FROM test_decimal_divide_round_to_scale")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT sum(a_long_decimal), avg(a_long_decimal) FROM test_decimal_divide_round_to_scale")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_decimal_divide_round_to_scale");
    }

    @Test
    void testMod()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_mod AS
                SELECT
                    CAST(a_tinyint AS tinyint) a_tinyint,
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_real AS real) a_real,
                    CAST(a_double AS double) a_double
                FROM (VALUES
                    (1, 100, 1000, 1.5, 2.5),
                    (-3, -7, -123456789, -0.25, -1e10),
                    (5, 12345, 98765, 3.0, 0.1),
                    (NULL, NULL, NULL, NULL, NULL))
                    t(a_tinyint, an_integer, a_bigint, a_real, a_double)
                """,
                4);

        assertThat(query("SELECT mod(an_integer, a_tinyint) FROM hive.default.test_mod")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT mod(a_bigint, an_integer) FROM hive.default.test_mod")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT mod(a_double, a_real) FROM hive.default.test_mod")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_mod");
    }

    @Test
    void testConstant()
    {
        assertUpdate(
                """
                CREATE TABLE test_constant AS
                SELECT
                    CAST(an_integer AS integer) an_integer
                FROM (VALUES
                    100,
                    -7,
                    12345,
                    NULL)
                    t(an_integer)
                """,
                4);

        assertThat(query("SELECT an_integer, 42, 'constant', true, 1.5e0 FROM test_constant")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_constant");
    }

    @Test
    void testCasts()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_casts AS
                SELECT
                    CAST(a_tinyint AS tinyint) a_tinyint,
                    CAST(a_smallint AS smallint) a_smallint,
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_real AS real) a_real,
                    CAST(a_double AS double) a_double,
                    CAST(a_short_decimal AS decimal(8, 2)) a_short_decimal,
                    CAST(a_char AS char(5)) a_char,
                    CAST(a_date AS date) a_date
                FROM (VALUES
                    (1, 10, 100, 1000, 1.5, 2.5, 12.34, 'ab', DATE '2024-02-29'),
                    (-3, -20, -7, -123456789, -0.25, -1e10, -0.01, 'abcde', DATE '1969-12-31'),
                    (5, 30, 12345, 98765, 3.0, 0.1, 999.99, 'x', DATE '1900-01-01'),
                    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))
                    t(a_tinyint, a_smallint, an_integer, a_bigint, a_real, a_double, a_short_decimal, a_char, a_date)
                """,
                4);

        assertThat(query("SELECT CAST(a_tinyint AS bigint) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_smallint AS tinyint) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_bigint AS integer) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(an_integer AS decimal(12, 2)) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(an_integer AS varchar) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_real AS integer) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_double AS bigint) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_short_decimal AS double) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_char AS char(10)) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_date AS varchar) FROM hive.default.test_casts")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_casts");
    }

    @Test
    void testNarrowingDecimalCast()
    {
        assertUpdate(
                """
                CREATE TABLE test_narrowing_decimal_cast AS
                SELECT
                    CAST(a_long_decimal AS decimal(30, 5)) a_long_decimal
                FROM (VALUES
                    1234567890.12345,
                    -99999.99999,
                    0.00001,
                    NULL)
                    t(a_long_decimal)
                """,
                4);

        assertThat(query("SELECT CAST(a_long_decimal AS decimal(20, 5)) FROM test_narrowing_decimal_cast")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_long_decimal AS decimal(15, 5)) FROM test_narrowing_decimal_cast")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_narrowing_decimal_cast");
    }

    @Test
    void testComparison()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_comparison AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_real AS real) a_real,
                    CAST(a_double AS double) a_double,
                    CAST(a_short_decimal AS decimal(8, 2)) a_short_decimal,
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_char AS char(5)) a_char,
                    CAST(a_date AS date) a_date,
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    (100, 1000, 1.5, 2.5, 12.34, 'hello', 'ab', DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:30.123'),
                    (-7, -123456789, -0.25, -1e10, -0.01, ' padded  ', 'abcde', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999'),
                    (12345, 98765, 3.0, 0.1, 999.99, 'Żółw ąę', 'x', DATE '1900-01-01', TIMESTAMP '2000-01-01 00:00:00.000'),
                    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))
                    t(an_integer, a_bigint, a_real, a_double, a_short_decimal, a_varchar, a_char, a_date, a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE test_comparison AS
                SELECT
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123456',
                    TIMESTAMP '1969-12-31 23:59:59.999999',
                    TIMESTAMP '2000-01-01 00:00:00.000000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT an_integer < a_bigint FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_double <> a_real FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal >= 0 FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar = 'hello' FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_char = 'ab' FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_date <= DATE '2000-01-01' FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_timestamp > TIMESTAMP '2000-01-01 00:00:00' FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer IS DISTINCT FROM 100 FROM hive.default.test_comparison")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_timestamp > TIMESTAMP '2000-01-01 00:00:00' FROM test_comparison")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_comparison");
        assertUpdate("DROP TABLE test_comparison");
    }

    @Test
    void testBetween()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_between AS
                SELECT
                    CAST(a_tinyint AS tinyint) a_tinyint,
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_date AS date) a_date
                FROM (VALUES
                    (1, 100, 1000, 'hello', DATE '2024-02-29'),
                    (-3, -7, -123456789, ' padded  ', DATE '1969-12-31'),
                    (5, 12345, 98765, 'Żółw ąę', DATE '1900-01-01'),
                    (NULL, NULL, NULL, NULL, NULL))
                    t(a_tinyint, an_integer, a_bigint, a_varchar, a_date)
                """,
                4);

        assertThat(query("SELECT an_integer BETWEEN 0 AND 1000 FROM hive.default.test_between")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar BETWEEN 'a' AND 'm' FROM hive.default.test_between")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_date BETWEEN DATE '1950-01-01' AND DATE '2030-01-01' FROM hive.default.test_between")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer BETWEEN a_tinyint AND a_bigint FROM hive.default.test_between")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_between");
    }

    @Test
    void testLet()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_let AS
                SELECT
                    CAST(a_tinyint AS tinyint) a_tinyint,
                    CAST(an_integer AS integer) an_integer
                FROM (VALUES
                    (1, 100),
                    (-3, -7),
                    (5, 12345),
                    (NULL, NULL))
                    t(a_tinyint, an_integer)
                """,
                4);

        // BETWEEN SYMMETRIC uses each operand twice, so the non-trivial ones are bound once
        assertThat(query("SELECT an_integer + 1 BETWEEN SYMMETRIC 500 AND a_tinyint FROM hive.default.test_let")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_let");
    }

    @Test
    void testIn()
    {
        assertUpdate(
                """
                CREATE TABLE test_in AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_date AS date) a_date
                FROM (VALUES
                    (100, 'hello', DATE '2024-02-29'),
                    (-7, ' padded  ', DATE '1969-12-31'),
                    (12345, 'Żółw ąę', DATE '1900-01-01'),
                    (NULL, NULL, NULL))
                    t(an_integer, a_varchar, a_date)
                """,
                4);

        assertThat(query("SELECT an_integer IN (100, 12345) FROM test_in")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer IN (1, NULL) FROM test_in")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar IN ('hello', 'XYZ') FROM test_in")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_date IN (DATE '2024-02-29', DATE '1900-01-01') FROM test_in")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_in");
    }

    @Test
    void testIsNull()
    {
        assertUpdate(
                """
                CREATE TABLE test_is_null AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_boolean AS boolean) a_boolean,
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    (100, true, 'hello'),
                    (-7, false, ' padded  '),
                    (12345, true, 'Żółw ąę'),
                    (NULL, NULL, NULL))
                    t(an_integer, a_boolean, a_varchar)
                """,
                4);

        assertThat(query("SELECT a_varchar IS NULL FROM test_is_null")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer IS NOT NULL FROM test_is_null")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean IS UNKNOWN FROM test_is_null")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_is_null");
    }

    @Test
    void testLogical()
    {
        assertUpdate(
                """
                CREATE TABLE test_logical AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_boolean AS boolean) a_boolean,
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    (100, true, 'hello'),
                    (-7, false, ' padded  '),
                    (12345, true, 'Żółw ąę'),
                    (NULL, NULL, NULL))
                    t(an_integer, a_boolean, a_varchar)
                """,
                4);

        assertThat(query("SELECT a_boolean AND an_integer > 0 FROM test_logical")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean OR a_varchar = 'hello' FROM test_logical")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT NOT a_boolean FROM test_logical")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean IS TRUE FROM test_logical")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean IS NOT FALSE FROM test_logical")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_logical");
    }

    @Test
    void testCase()
    {
        assertUpdate(
                """
                CREATE TABLE test_case AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_double AS double) a_double,
                    CAST(a_boolean AS boolean) a_boolean
                FROM (VALUES
                    (100, 1000, 2.5, true),
                    (-7, -123456789, -1e10, false),
                    (12345, 98765, 0.1, true),
                    (NULL, NULL, NULL, NULL))
                    t(an_integer, a_bigint, a_double, a_boolean)
                """,
                4);

        assertThat(query("SELECT CASE WHEN an_integer > 0 THEN 'positive' WHEN an_integer < 0 THEN 'negative' ELSE 'zero' END FROM test_case")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CASE WHEN a_boolean THEN a_double END FROM test_case")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CASE WHEN a_bigint > 0 THEN a_bigint / an_integer ELSE 0 END FROM test_case")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_case");
    }

    @Test
    void testIf()
    {
        assertUpdate(
                """
                CREATE TABLE test_if AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_boolean AS boolean) a_boolean,
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_date AS date) a_date
                FROM (VALUES
                    (100, true, 'hello', DATE '2024-02-29'),
                    (-7, false, ' padded  ', DATE '1969-12-31'),
                    (12345, true, 'Żółw ąę', DATE '1900-01-01'),
                    (NULL, NULL, NULL, NULL))
                    t(an_integer, a_boolean, a_varchar, a_date)
                """,
                4);

        assertThat(query("SELECT IF(a_boolean, an_integer, an_integer * 2) FROM test_if")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT IF(an_integer > 0, a_varchar, 'none') FROM test_if")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT IF(a_boolean, a_date) FROM test_if")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_if");
    }

    @Test
    void testCoalesce()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_coalesce AS
                SELECT
                    CAST(a_tinyint AS tinyint) a_tinyint,
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_real AS real) a_real,
                    CAST(a_double AS double) a_double,
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    (1, 100, 1.5, 2.5, 'hello'),
                    (-3, -7, -0.25, -1e10, ' padded  '),
                    (5, 12345, 3.0, 0.1, 'Żółw ąę'),
                    (NULL, NULL, NULL, NULL, NULL))
                    t(a_tinyint, an_integer, a_real, a_double, a_varchar)
                """,
                4);

        assertThat(query("SELECT coalesce(an_integer, a_tinyint) FROM hive.default.test_coalesce")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT coalesce(a_varchar, 'default') FROM hive.default.test_coalesce")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT coalesce(a_double, a_real, 0) FROM hive.default.test_coalesce")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_coalesce");
    }

    @Test
    void testNullIf()
    {
        assertUpdate(
                """
                CREATE TABLE test_null_if AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    (100, 1000, 'hello'),
                    (-7, -123456789, ' padded  '),
                    (12345, 98765, 'Żółw ąę'),
                    (NULL, NULL, NULL))
                    t(an_integer, a_bigint, a_varchar)
                """,
                4);

        assertThat(query("SELECT nullif(an_integer, 100) FROM test_null_if")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT nullif(a_varchar, 'hello') FROM test_null_if")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT nullif(a_bigint, an_integer) FROM test_null_if")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_null_if");
    }

    @Test
    void testLeast()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_least AS
                SELECT
                    CAST(a_tinyint AS tinyint) a_tinyint,
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_date AS date) a_date
                FROM (VALUES
                    (1, 100, 1000, 'hello', DATE '2024-02-29'),
                    (-3, -7, -123456789, ' padded  ', DATE '1969-12-31'),
                    (5, 12345, 98765, 'Żółw ąę', DATE '1900-01-01'),
                    (NULL, NULL, NULL, NULL, NULL))
                    t(a_tinyint, an_integer, a_bigint, a_varchar, a_date)
                """,
                4);

        assertThat(query("SELECT least(an_integer, a_tinyint) FROM hive.default.test_least")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT least(a_bigint, an_integer, 0) FROM hive.default.test_least")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT least(a_varchar, 'm') FROM hive.default.test_least")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT least(a_date, DATE '2000-01-01') FROM hive.default.test_least")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_least");
    }

    @Test
    void testExtract()
    {
        assertUpdate(
                """
                CREATE TABLE test_extract AS
                SELECT
                    CAST(a_date AS date) a_date,
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    (DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:30.123456'),
                    (DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999'),
                    (DATE '1900-01-01', TIMESTAMP '2000-01-01 00:00:00.000000'),
                    (NULL, NULL))
                    t(a_date, a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_extract AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT year(a_date) FROM test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT EXTRACT(YEAR FROM a_timestamp) FROM test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT day(a_date) FROM test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT hour(a_timestamp) FROM test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT minute(a_timestamp) FROM test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT second(a_timestamp) FROM test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT EXTRACT(YEAR FROM a_timestamp) FROM hive.default.test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT hour(a_timestamp) FROM hive.default.test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT minute(a_timestamp) FROM hive.default.test_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT second(a_timestamp) FROM hive.default.test_extract")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_extract");
        assertUpdate("DROP TABLE hive.default.test_extract");
    }

    @Test
    void testLastDayOfMonth()
    {
        assertUpdate(
                """
                CREATE TABLE test_last_day_of_month AS
                SELECT
                    CAST(a_date AS date) a_date,
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    (DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:30.123456'),
                    (DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999'),
                    (DATE '1900-01-01', TIMESTAMP '2000-01-01 00:00:00.000000'),
                    (NULL, NULL))
                    t(a_date, a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_last_day_of_month AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT last_day_of_month(a_date) FROM test_last_day_of_month")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT last_day_of_month(a_timestamp) FROM test_last_day_of_month")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT last_day_of_month(a_timestamp) FROM hive.default.test_last_day_of_month")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_last_day_of_month");
        assertUpdate("DROP TABLE hive.default.test_last_day_of_month");
    }

    @Test
    void testDateTrunc()
    {
        assertUpdate(
                """
                CREATE TABLE test_date_trunc AS
                SELECT
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123456',
                    TIMESTAMP '1969-12-31 23:59:59.999999',
                    TIMESTAMP '2000-01-01 00:00:00.000000',
                    NULL)
                    t(a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_date_trunc AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT date_trunc('second', a_timestamp) FROM test_date_trunc")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_trunc('hour', a_timestamp) FROM test_date_trunc")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_trunc('day', a_timestamp) FROM test_date_trunc")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_trunc('second', a_timestamp) FROM hive.default.test_date_trunc")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_trunc('hour', a_timestamp) FROM hive.default.test_date_trunc")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_trunc('day', a_timestamp) FROM hive.default.test_date_trunc")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_date_trunc");
        assertUpdate("DROP TABLE hive.default.test_date_trunc");
    }

    @Test
    void testDateAdd()
    {
        assertUpdate(
                """
                CREATE TABLE test_date_add AS
                SELECT
                    CAST(a_date AS date) a_date,
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    (DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:30.123456'),
                    (DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999'),
                    (DATE '1900-01-01', TIMESTAMP '2000-01-01 00:00:00.000000'),
                    (NULL, NULL))
                    t(a_date, a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_date_add AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT date_add('day', 7, a_date) FROM test_date_add")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('month', 1, a_date) FROM test_date_add")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('hour', -3, a_timestamp) FROM test_date_add")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('year', 1, a_timestamp) FROM test_date_add")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('hour', -3, a_timestamp) FROM hive.default.test_date_add")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('year', 1, a_timestamp) FROM hive.default.test_date_add")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_date_add");
        assertUpdate("DROP TABLE hive.default.test_date_add");
    }

    @Test
    void testDateDiff()
    {
        assertUpdate(
                """
                CREATE TABLE test_date_diff AS
                SELECT
                    CAST(a_date AS date) a_date,
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    (DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:30.123456'),
                    (DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999'),
                    (DATE '1900-01-01', TIMESTAMP '2000-01-01 00:00:00.000000'),
                    (NULL, NULL))
                    t(a_date, a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_date_diff AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT date_diff('day', a_date, DATE '2000-01-01') FROM test_date_diff")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('week', DATE '2000-01-01', a_date) FROM test_date_diff")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('hour', a_timestamp, TIMESTAMP '2000-01-01 00:00:00.000') FROM test_date_diff")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('millisecond', TIMESTAMP '2000-01-01 00:00:00.000', a_timestamp) FROM test_date_diff")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('hour', a_timestamp, TIMESTAMP '2000-01-01 00:00:00.000') FROM hive.default.test_date_diff")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('millisecond', TIMESTAMP '2000-01-01 00:00:00.000', a_timestamp) FROM hive.default.test_date_diff")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_date_diff");
        assertUpdate("DROP TABLE hive.default.test_date_diff");
    }

    @Test
    void testFormatDateTime()
    {
        assertUpdate(
                """
                CREATE TABLE test_format_date_time AS
                SELECT
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123456',
                    TIMESTAMP '1969-12-31 23:59:59.999999',
                    TIMESTAMP '2000-01-01 00:00:00.000000',
                    NULL)
                    t(a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_format_date_time AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT format_datetime(a_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') FROM test_format_date_time")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT format_datetime(a_timestamp, 'yyyy/MM/dd') FROM test_format_date_time")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT format_datetime(a_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') FROM hive.default.test_format_date_time")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT format_datetime(a_timestamp, 'yyyy/MM/dd') FROM hive.default.test_format_date_time")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_format_date_time");
        assertUpdate("DROP TABLE hive.default.test_format_date_time");
    }

    @Test
    void testDateFormat()
    {
        assertUpdate(
                """
                CREATE TABLE test_date_format AS
                SELECT
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123456',
                    TIMESTAMP '1969-12-31 23:59:59.999999',
                    TIMESTAMP '2000-01-01 00:00:00.000000',
                    NULL)
                    t(a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_date_format AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT date_format(a_timestamp, '%Y-%m-%d %H:%i:%s.%f') FROM test_date_format")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_format(a_timestamp, '%y/%j %T') FROM test_date_format")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_format(a_timestamp, '%Y-%m-%d %H:%i:%s.%f') FROM hive.default.test_date_format")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_format(a_timestamp, '%y/%j %T') FROM hive.default.test_date_format")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_date_format");
        assertUpdate("DROP TABLE hive.default.test_date_format");
    }

    @Test
    void testFromUnixTime()
    {
        assertUpdate(
                """
                CREATE TABLE test_from_unix_time AS
                SELECT
                    CAST(a_bigint AS bigint) a_bigint,
                    CAST(a_double AS double) a_double
                FROM (VALUES
                    (1000, 2.5),
                    (-123456789, -1e10),
                    (98765, 0.1),
                    (NULL, NULL))
                    t(a_bigint, a_double)
                """,
                4);

        assertThat(query(utcSession(), "SELECT from_unixtime(a_double) FROM test_from_unix_time")).executesWithGpu(ProjectNode.class);
        assertThat(query(utcSession(), "SELECT from_unixtime(a_bigint) FROM test_from_unix_time")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_from_unix_time");
    }

    @Test
    void testFromUnixTimeOutsideUtcSession()
    {
        assertUpdate(
                """
                CREATE TABLE test_from_unix_time_outside_utc_session AS
                SELECT
                    CAST(a_double AS double) a_double
                FROM (VALUES
                    2.5,
                    -1e10,
                    0.1,
                    NULL)
                    t(a_double)
                """,
                4);

        assertThat(query("SELECT from_unixtime(a_double) FROM test_from_unix_time_outside_utc_session")).executesWithoutGpu();

        assertUpdate("DROP TABLE test_from_unix_time_outside_utc_session");
    }

    @Test
    void testToUnixTime()
    {
        assertUpdate(
                """
                CREATE TABLE test_to_unix_time AS
                SELECT
                    CAST(a_timestamp_tz AS timestamp(6) with time zone) a_timestamp_tz
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123456 UTC',
                    TIMESTAMP '1969-12-31 23:59:59.999999 UTC',
                    TIMESTAMP '2000-01-01 00:00:00.000000 UTC',
                    NULL)
                    t(a_timestamp_tz)
                """,
                4);

        assertThat(query("SELECT to_unixtime(a_timestamp_tz) FROM test_to_unix_time")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_to_unix_time");
    }

    @Test
    void testToUnixTimeOnTimestamp()
    {
        assertUpdate(
                """
                CREATE TABLE test_to_unix_time_on_timestamp AS
                SELECT
                    CAST(a_timestamp AS timestamp(6)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123456',
                    TIMESTAMP '1969-12-31 23:59:59.999999',
                    TIMESTAMP '2000-01-01 00:00:00.000000',
                    NULL)
                    t(a_timestamp)
                """,
                4);
        assertUpdate(
                """
                CREATE TABLE hive.default.test_to_unix_time_on_timestamp AS
                SELECT
                    CAST(a_timestamp AS timestamp(3)) a_timestamp
                FROM (VALUES
                    TIMESTAMP '2024-02-29 13:45:30.123',
                    TIMESTAMP '1969-12-31 23:59:59.999',
                    TIMESTAMP '2000-01-01 00:00:00.000',
                    NULL)
                    t(a_timestamp)
                """,
                4);

        assertThat(query("SELECT to_unixtime(a_timestamp) FROM test_to_unix_time_on_timestamp")).executesWithoutGpu();
        assertThat(query("SELECT to_unixtime(a_timestamp) FROM hive.default.test_to_unix_time_on_timestamp")).executesWithoutGpu();

        assertUpdate("DROP TABLE test_to_unix_time_on_timestamp");
        assertUpdate("DROP TABLE hive.default.test_to_unix_time_on_timestamp");
    }

    @Test
    void testLength()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_length AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_varchar_10 AS varchar(10)) a_varchar_10
                FROM (VALUES
                    ('hello', 'abc'),
                    (' padded  ', 'XYZ'),
                    ('Żółw ąę', 'a%b_c'),
                    (NULL, NULL))
                    t(a_varchar, a_varchar_10)
                """,
                4);

        assertThat(query("SELECT length(a_varchar) FROM hive.default.test_length")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT length(a_varchar_10) FROM hive.default.test_length")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_length");
    }

    @Test
    void testLower()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_lower AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_varchar_10 AS varchar(10)) a_varchar_10,
                    CAST(a_char AS char(5)) a_char
                FROM (VALUES
                    ('hello', 'abc', 'ab'),
                    (' padded  ', 'XYZ', 'abcde'),
                    ('Żółw ąę', 'a%b_c', 'x'),
                    (NULL, NULL, NULL))
                    t(a_varchar, a_varchar_10, a_char)
                """,
                4);

        assertThat(query("SELECT lower(a_varchar) FROM hive.default.test_lower")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT lower(a_varchar_10) FROM hive.default.test_lower")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT lower(a_char) FROM hive.default.test_lower")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_lower");
    }

    @Test
    void testUpper()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_upper AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_char AS char(5)) a_char
                FROM (VALUES
                    ('hello', 'ab'),
                    (' padded  ', 'abcde'),
                    ('Żółw ąę', 'x'),
                    (NULL, NULL))
                    t(a_varchar, a_char)
                """,
                4);

        assertThat(query("SELECT upper(a_varchar) FROM hive.default.test_upper")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT upper(a_char) FROM hive.default.test_upper")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_upper");
    }

    @Test
    void testLike()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_like AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_varchar_10 AS varchar(10)) a_varchar_10
                FROM (VALUES
                    ('hello', 'abc'),
                    (' padded  ', 'XYZ'),
                    ('Żółw ąę', 'a%b_c'),
                    (NULL, NULL))
                    t(a_varchar, a_varchar_10)
                """,
                4);

        assertThat(query("SELECT a_varchar LIKE 'h%' FROM hive.default.test_like")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar LIKE '%l_o%' FROM hive.default.test_like")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar_10 LIKE 'a!%b%' ESCAPE '!' FROM hive.default.test_like")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_like");
    }

    @Test
    void testStartsWith()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_starts_with AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_varchar_10 AS varchar(10)) a_varchar_10
                FROM (VALUES
                    ('hello', 'abc'),
                    (' padded  ', 'XYZ'),
                    ('Żółw ąę', 'a%b_c'),
                    (NULL, NULL))
                    t(a_varchar, a_varchar_10)
                """,
                4);

        assertThat(query("SELECT starts_with(a_varchar, 'he') FROM hive.default.test_starts_with")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT starts_with(a_varchar_10, '') FROM hive.default.test_starts_with")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_starts_with");
    }

    @Test
    void testSubstring()
    {
        assertUpdate(
                """
                CREATE TABLE test_substring AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    'hello',
                    ' padded  ',
                    'Żółw ąę',
                    NULL)
                    t(a_varchar)
                """,
                4);

        assertThat(query("SELECT substring(a_varchar, 2) FROM test_substring")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT substring(a_varchar, 2, 3) FROM test_substring")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT substring(a_varchar, -3) FROM test_substring")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_substring");
    }

    @Test
    void testConcatWs()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_concat_ws AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_varchar_10 AS varchar(10)) a_varchar_10,
                    CAST(a_char AS char(5)) a_char
                FROM (VALUES
                    ('hello', 'abc', 'ab'),
                    (' padded  ', 'XYZ', 'abcde'),
                    ('Żółw ąę', 'a%b_c', 'x'),
                    (NULL, NULL, NULL))
                    t(a_varchar, a_varchar_10, a_char)
                """,
                4);

        assertThat(query("SELECT concat_ws(',', a_varchar, a_varchar_10) FROM hive.default.test_concat_ws")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT concat_ws('', a_varchar) FROM hive.default.test_concat_ws")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT concat_ws(' - ', a_varchar_10, CAST(a_char AS varchar), a_varchar) FROM hive.default.test_concat_ws")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_concat_ws");
    }

    @Test
    void testRegexpReplace()
    {
        assertUpdate(
                """
                CREATE TABLE test_regexp_replace AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    'hello',
                    ' padded  ',
                    'Żółw ąę',
                    NULL)
                    t(a_varchar)
                """,
                4);

        assertThat(query("SELECT regexp_replace(a_varchar, '[aeiou]', '*') FROM test_regexp_replace")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, 'l+') FROM test_regexp_replace")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, '(h)(e)', '$2$1') FROM test_regexp_replace")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_regexp_replace");
    }

    @Test
    void testRegexpReplaceRepeatOfRepeat()
    {
        assertUpdate(
                """
                CREATE TABLE test_regexp_replace_repeat_of_repeat AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    'hello',
                    ' padded  ',
                    'Żółw ąę',
                    NULL)
                    t(a_varchar)
                """,
                4);

        assertThat(query("SELECT regexp_replace(a_varchar, '(?:l+)?', '*') FROM test_regexp_replace_repeat_of_repeat")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, '(?:l+)+', '*') FROM test_regexp_replace_repeat_of_repeat")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, '(?:l+)*', '*') FROM test_regexp_replace_repeat_of_repeat")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_regexp_replace_repeat_of_repeat");
    }

    @Test
    void testRegexpReplaceRejectedByCudf()
    {
        assertUpdate(
                """
                CREATE TABLE test_regexp_replace_rejected_by_cudf AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    'hello',
                    ' padded  ',
                    'Żółw ąę',
                    NULL)
                    t(a_varchar)
                """,
                4);

        assertThat(query("SELECT regexp_replace(a_varchar, '(l*)+', '*') FROM test_regexp_replace_rejected_by_cudf")).executesWithoutGpu();
        assertThat(query("SELECT regexp_replace(a_varchar, '(^)?', '*') FROM test_regexp_replace_rejected_by_cudf")).executesWithoutGpu();

        assertUpdate("DROP TABLE test_regexp_replace_rejected_by_cudf");
    }

    @Test
    void testRegexpExtract()
    {
        assertUpdate(
                """
                CREATE TABLE test_regexp_extract AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    'hello',
                    ' padded  ',
                    'Żółw ąę',
                    NULL)
                    t(a_varchar)
                """,
                4);

        assertThat(query("SELECT regexp_extract(a_varchar, 'l+') FROM test_regexp_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_extract(a_varchar, '[a-z]+') FROM test_regexp_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_extract(a_varchar, '(h)(e)', 0) FROM test_regexp_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_extract(a_varchar, '(h)(e)', 2) FROM test_regexp_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_extract(a_varchar, '(?:h)(e)(l+)', 2) FROM test_regexp_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_extract(a_varchar, '((l)(o))', 3) FROM test_regexp_extract")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_extract(a_varchar, '(x)?(l)', 1) FROM test_regexp_extract")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_regexp_extract");
    }

    @Test
    void testRegexpExtractRejectedByCudf()
    {
        assertUpdate(
                """
                CREATE TABLE test_regexp_extract_rejected_by_cudf AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    'hello',
                    ' padded  ',
                    'Żółw ąę',
                    NULL)
                    t(a_varchar)
                """,
                4);

        assertThat(query("SELECT regexp_extract(a_varchar, '(l*)+', 1) FROM test_regexp_extract_rejected_by_cudf")).executesWithoutGpu();
        assertThat(query("SELECT regexp_extract(a_varchar, '(^)?', 1) FROM test_regexp_extract_rejected_by_cudf")).executesWithoutGpu();

        assertUpdate("DROP TABLE test_regexp_extract_rejected_by_cudf");
    }

    @Test
    void testRegexpExtractNonConstantGroup()
    {
        assertUpdate(
                """
                CREATE TABLE test_regexp_extract_non_constant_group AS
                SELECT
                    CAST(a_boolean AS boolean) a_boolean,
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    (true, 'hello'),
                    (false, ' padded  '),
                    (true, 'Żółw ąę'),
                    (NULL, NULL))
                    t(a_boolean, a_varchar)
                """,
                4);

        assertThat(query("SELECT regexp_extract(a_varchar, '(h)(e)', IF(a_boolean, 1, 2)) FROM test_regexp_extract_non_constant_group")).executesWithoutGpu();

        assertUpdate("DROP TABLE test_regexp_extract_non_constant_group");
    }

    @Test
    void testReplace()
    {
        assertUpdate(
                """
                CREATE TABLE test_replace AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar
                FROM (VALUES
                    'hello',
                    ' padded  ',
                    'Żółw ąę',
                    NULL)
                    t(a_varchar)
                """,
                4);

        assertThat(query("SELECT replace(a_varchar, 'l', 'L') FROM test_replace")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT replace(a_varchar, 'l') FROM test_replace")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_replace");
    }

    @Test
    void testTrim()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_trim AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_char AS char(5)) a_char
                FROM (VALUES
                    ('hello', 'ab'),
                    (' padded  ', 'abcde'),
                    ('Żółw ąę', 'x'),
                    (NULL, NULL))
                    t(a_varchar, a_char)
                """,
                4);

        assertThat(query("SELECT trim(a_varchar) FROM hive.default.test_trim")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT ltrim(a_varchar) FROM hive.default.test_trim")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT rtrim(a_varchar) FROM hive.default.test_trim")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT trim(a_char) FROM hive.default.test_trim")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT ltrim(a_varchar, ' h') FROM hive.default.test_trim")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT trim(BOTH 'a' FROM a_char) FROM hive.default.test_trim")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_trim");
    }

    @Test
    void testPad()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_pad AS
                SELECT
                    CAST(an_integer AS integer) an_integer,
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_varchar_10 AS varchar(10)) a_varchar_10
                FROM (VALUES
                    (100, 'hello', 'abc'),
                    (-7, ' padded  ', 'XYZ'),
                    (12345, 'Żółw ąę', 'a%b_c'),
                    (NULL, NULL, NULL))
                    t(an_integer, a_varchar, a_varchar_10)
                """,
                4);

        assertThat(query("SELECT lpad(a_varchar, 8, '*') FROM hive.default.test_pad")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT rpad(a_varchar_10, 2, '-') FROM hive.default.test_pad")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT lpad(CAST(an_integer AS varchar), 10, '0') FROM hive.default.test_pad")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_pad");
    }

    @Test
    void testToUtf8()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_to_utf8 AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_char AS char(5)) a_char
                FROM (VALUES
                    ('hello', 'ab'),
                    (' padded  ', 'abcde'),
                    ('Żółw ąę', 'x'),
                    (NULL, NULL))
                    t(a_varchar, a_char)
                """,
                4);

        assertThat(query("SELECT to_utf8(a_varchar) FROM hive.default.test_to_utf8")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT to_utf8(a_char) FROM hive.default.test_to_utf8")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_to_utf8");
    }

    @Test
    void testToHex()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_to_hex AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_char AS char(5)) a_char
                FROM (VALUES
                    ('hello', 'ab'),
                    (' padded  ', 'abcde'),
                    ('Żółw ąę', 'x'),
                    (NULL, NULL))
                    t(a_varchar, a_char)
                """,
                4);

        assertThat(query("SELECT to_hex(to_utf8(a_varchar)) FROM hive.default.test_to_hex")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT to_hex(to_utf8(a_char)) FROM hive.default.test_to_hex")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_to_hex");
    }

    @Test
    void testSha1()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_sha1 AS
                SELECT
                    CAST(a_varchar AS varchar) a_varchar,
                    CAST(a_varchar_10 AS varchar(10)) a_varchar_10
                FROM (VALUES
                    ('hello', 'abc'),
                    (' padded  ', 'XYZ'),
                    ('Żółw ąę', 'a%b_c'),
                    (NULL, NULL))
                    t(a_varchar, a_varchar_10)
                """,
                4);

        assertThat(query("SELECT sha1(to_utf8(a_varchar)) FROM hive.default.test_sha1")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT to_hex(sha1(to_utf8(a_varchar_10))) FROM hive.default.test_sha1")).executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_sha1");
    }

    @Test
    void testCharTrimming()
    {
        assertUpdate("CREATE TABLE hive.default.test_char_trimming(a_char) AS (VALUES CHAR 'abc', CHAR 'ab', CHAR 'a', CHAR '', NULL)", 5);

        assertThat(query("SELECT a_char FROM hive.default.test_char_trimming"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT CAST(a_char AS varchar) FROM hive.default.test_char_trimming"))
                .executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT length(a_char) FROM hive.default.test_char_trimming"))
                .executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT substring(a_char, 1, 3) FROM hive.default.test_char_trimming"))
                .executesWithoutGpu();
        assertThat(query("SELECT substring(a_char, 3, 1) FROM hive.default.test_char_trimming"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE hive.default.test_char_trimming");
    }

    @Test
    void testConcat()
    {
        assertUpdate(
                """
                CREATE TABLE hive.default.test_concat(a_char, b_char, a_varchar) AS (VALUES
                    (CHAR 'ab', CHAR 'cd', VARCHAR 'ef'),
                    (CHAR '', CHAR '', VARCHAR ''),
                    (CHAR 'x', NULL, NULL))
                """,
                3);

        // Trino defines only a two-argument char concat. How an expression reaches it is decided by the
        // analyzer, outside the expression compiler, so assert it here rather than over hand-built IR.
        assertThat(query("SELECT concat(a_char, b_char) FROM hive.default.test_concat"))
                .executesWithGpu(ProjectNode.class);
        // Each || is its own two-argument call, so a chain of them stays on the char overload
        assertThat(query("SELECT a_char || b_char || a_char FROM hive.default.test_concat"))
                .executesWithGpu(ProjectNode.class);
        // A three-argument call has no char overload to bind to: it resolves to the variadic varchar one,
        // over the char -> varchar coercions it adds
        assertThat(query("SELECT concat(a_char, b_char, a_char) FROM hive.default.test_concat"))
                .executesWithGpu(ProjectNode.class);

        assertThat(query("SELECT concat(a_varchar, a_varchar, a_varchar, a_varchar) FROM hive.default.test_concat"))
                .executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar || a_varchar FROM hive.default.test_concat"))
                .executesWithGpu(ProjectNode.class);

        // char and varchar arguments meet through a char -> varchar cast
        assertThat(query("SELECT concat(a_char, a_varchar) FROM hive.default.test_concat"))
                .executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE hive.default.test_concat");
    }

    /// `from_unixtime` returns the session time zone, and the GPU representation holds only UTC
    private Session utcSession()
    {
        return Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
    }
}
