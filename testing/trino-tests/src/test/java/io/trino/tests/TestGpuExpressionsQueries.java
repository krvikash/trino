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

import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static org.assertj.core.api.Assertions.assertThat;

/// Query-level coverage for the scalar expressions the GPU compiles, over data read from Parquet.
final class TestGpuExpressionsQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner runner = HiveQueryRunner.builder()
                .configureGpuDistributedExecution()
                .setSkipTimezoneSetup(true)
                .addHiveProperty("hive.storage-format", "PARQUET")
                .addHiveProperty("hive.parquet.time-zone", "UTC")
                .build();
        try {
            runner.execute(
                    """
                    CREATE TABLE expressions AS
                    SELECT
                        CAST(a_tinyint AS tinyint) a_tinyint,
                        CAST(a_smallint AS smallint) a_smallint,
                        CAST(an_integer AS integer) an_integer,
                        CAST(a_bigint AS bigint) a_bigint,
                        CAST(a_real AS real) a_real,
                        CAST(a_double AS double) a_double,
                        CAST(a_short_decimal AS decimal(8, 2)) a_short_decimal,
                        CAST(a_long_decimal AS decimal(30, 5)) a_long_decimal,
                        CAST(a_boolean AS boolean) a_boolean,
                        CAST(a_varchar AS varchar) a_varchar,
                        CAST(a_varchar10 AS varchar(10)) a_varchar10,
                        CAST(a_char AS char(5)) a_char,
                        CAST(a_date AS date) a_date,
                        CAST(a_timestamp AS timestamp(3)) a_timestamp
                    FROM (VALUES
                        (1, 10, 100, 1000, 1.5, 2.5, 12.34, 1234567890.12345, true, 'hello', 'abc', 'ab', DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:30.123'),
                        (-3, -20, -7, -123456789, -0.25, -1e10, -0.01, -99999.99999, false, ' padded  ', 'XYZ', 'abcde', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999'),
                        (5, 30, 12345, 98765, 3.0, 0.1, 999.99, 0.00001, true, 'Żółw ąę', 'a%b_c', 'x', DATE '1900-01-01', TIMESTAMP '2000-01-01 00:00:00.000'),
                        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))
                        t(a_tinyint, a_smallint, an_integer, a_bigint, a_real, a_double, a_short_decimal, a_long_decimal, a_boolean, a_varchar, a_varchar10, a_char, a_date, a_timestamp)
                    """);
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
        assertThat(query("SELECT a_tinyint + a_tinyint FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_tinyint * a_tinyint FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_smallint - a_smallint FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_smallint * a_smallint FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer * an_integer FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_bigint * a_bigint FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_bigint / an_integer FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer % a_tinyint FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_real + a_real FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_double / a_real FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testDecimalArithmetic()
    {
        assertThat(query("SELECT a_short_decimal + a_short_decimal FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal - 1.5 FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal * a_short_decimal FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal / a_short_decimal FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_long_decimal + a_long_decimal FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_long_decimal - a_short_decimal FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_long_decimal / a_short_decimal FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testDecimalDivideRoundToScale()
    {
        // Next to a sum of the same decimal, avg is planned as that sum divided by a count in a projection
        assertThat(query("SELECT sum(a_short_decimal), avg(a_short_decimal) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT sum(a_long_decimal), avg(a_long_decimal) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testMod()
    {
        assertThat(query("SELECT mod(an_integer, a_tinyint) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT mod(a_bigint, an_integer) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT mod(a_double, a_real) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testConstant()
    {
        assertThat(query("SELECT an_integer, 42, 'constant', true, 1.5e0 FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testCasts()
    {
        assertThat(query("SELECT CAST(a_tinyint AS bigint) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_smallint AS tinyint) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_bigint AS integer) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(an_integer AS decimal(12, 2)) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(an_integer AS varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_real AS integer) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_double AS bigint) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_short_decimal AS double) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_char AS char(10)) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_date AS varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testNarrowingDecimalCast()
    {
        assertThat(query("SELECT CAST(a_long_decimal AS decimal(20, 5)) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CAST(a_long_decimal AS decimal(15, 5)) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testComparison()
    {
        assertThat(query("SELECT an_integer < a_bigint FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_double <> a_real FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_short_decimal >= 0 FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar = 'hello' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_char = 'ab' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_date <= DATE '2000-01-01' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_timestamp > TIMESTAMP '2000-01-01 00:00:00' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer IS DISTINCT FROM 100 FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testBetween()
    {
        assertThat(query("SELECT an_integer BETWEEN 0 AND 1000 FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar BETWEEN 'a' AND 'm' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_date BETWEEN DATE '1950-01-01' AND DATE '2030-01-01' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer BETWEEN a_tinyint AND a_bigint FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testLet()
    {
        // BETWEEN SYMMETRIC uses each operand twice, so the non-trivial ones are bound once
        assertThat(query("SELECT an_integer + 1 BETWEEN SYMMETRIC 500 AND a_tinyint FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testIn()
    {
        assertThat(query("SELECT an_integer IN (100, 12345) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer IN (1, NULL) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar IN ('hello', 'XYZ') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_date IN (DATE '2024-02-29', DATE '1900-01-01') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testIsNull()
    {
        assertThat(query("SELECT a_varchar IS NULL FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT an_integer IS NOT NULL FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean IS UNKNOWN FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testLogical()
    {
        assertThat(query("SELECT a_boolean AND an_integer > 0 FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean OR a_varchar = 'hello' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT NOT a_boolean FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean IS TRUE FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_boolean IS NOT FALSE FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testCase()
    {
        assertThat(query("SELECT CASE WHEN an_integer > 0 THEN 'positive' WHEN an_integer < 0 THEN 'negative' ELSE 'zero' END FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CASE WHEN a_boolean THEN a_double END FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT CASE WHEN a_bigint > 0 THEN a_bigint / an_integer ELSE 0 END FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testIf()
    {
        assertThat(query("SELECT IF(a_boolean, an_integer, an_integer * 2) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT IF(an_integer > 0, a_varchar, 'none') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT IF(a_boolean, a_date) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testCoalesce()
    {
        assertThat(query("SELECT coalesce(an_integer, a_tinyint) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT coalesce(a_varchar, 'default') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT coalesce(a_double, a_real, 0) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testNullIf()
    {
        assertThat(query("SELECT nullif(an_integer, 100) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT nullif(a_varchar, 'hello') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT nullif(a_bigint, an_integer) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testLeast()
    {
        assertThat(query("SELECT least(an_integer, a_tinyint) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT least(a_bigint, an_integer, 0) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT least(a_varchar, 'm') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT least(a_date, DATE '2000-01-01') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testExtract()
    {
        assertThat(query("SELECT year(a_date) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT EXTRACT(YEAR FROM a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT day(a_date) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT hour(a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT minute(a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT second(a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testLastDayOfMonth()
    {
        assertThat(query("SELECT last_day_of_month(a_date) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT last_day_of_month(a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testDateTrunc()
    {
        assertThat(query("SELECT date_trunc('second', a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_trunc('hour', a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_trunc('day', a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testDateAdd()
    {
        assertThat(query("SELECT date_add('day', 7, a_date) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('month', 1, a_date) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('hour', -3, a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_add('year', 1, a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testDateDiff()
    {
        assertThat(query("SELECT date_diff('day', a_date, DATE '2000-01-01') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('week', DATE '2000-01-01', a_date) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('hour', a_timestamp, TIMESTAMP '2000-01-01 00:00:00.000') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT date_diff('millisecond', TIMESTAMP '2000-01-01 00:00:00.000', a_timestamp) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testFormatDateTime()
    {
        assertThat(query("SELECT format_datetime(a_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT format_datetime(a_timestamp, 'yyyy/MM/dd') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testLength()
    {
        assertThat(query("SELECT length(a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT length(a_varchar10) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testLower()
    {
        assertThat(query("SELECT lower(a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT lower(a_varchar10) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT lower(a_char) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testUpper()
    {
        assertThat(query("SELECT upper(a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT upper(a_char) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testLike()
    {
        assertThat(query("SELECT a_varchar LIKE 'h%' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar LIKE '%l_o%' FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar10 LIKE 'a!%b%' ESCAPE '!' FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testStartsWith()
    {
        assertThat(query("SELECT starts_with(a_varchar, 'he') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT starts_with(a_varchar10, '') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testSubstring()
    {
        assertThat(query("SELECT substring(a_varchar, 2) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT substring(a_varchar, 2, 3) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT substring(a_varchar, -3) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testConcatWs()
    {
        assertThat(query("SELECT concat_ws(',', a_varchar, a_varchar10) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT concat_ws('', a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT concat_ws(' - ', a_varchar10, CAST(a_char AS varchar), a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testRegexpReplace()
    {
        assertThat(query("SELECT regexp_replace(a_varchar, '[aeiou]', '*') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, 'l+') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, '(h)(e)', '$2$1') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testRegexpReplaceRepeatOfRepeat()
    {
        assertThat(query("SELECT regexp_replace(a_varchar, '(?:l+)?', '*') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, '(?:l+)+', '*') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT regexp_replace(a_varchar, '(?:l+)*', '*') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testRegexpReplaceRejectedByCudf()
    {
        assertThat(query("SELECT regexp_replace(a_varchar, '(l*)+', '*') FROM expressions")).executesWithoutGpu();
        assertThat(query("SELECT regexp_replace(a_varchar, '(^)?', '*') FROM expressions")).executesWithoutGpu();
    }

    @Test
    void testReplace()
    {
        assertThat(query("SELECT replace(a_varchar, 'l', 'L') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT replace(a_varchar, 'l') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testTrim()
    {
        assertThat(query("SELECT trim(a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT ltrim(a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT rtrim(a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT trim(a_char) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT ltrim(a_varchar, ' h') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT trim(BOTH 'a' FROM a_char) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testPad()
    {
        assertThat(query("SELECT lpad(a_varchar, 8, '*') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT rpad(a_varchar10, 2, '-') FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT lpad(CAST(an_integer AS varchar), 10, '0') FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testToUtf8()
    {
        assertThat(query("SELECT to_utf8(a_varchar) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT to_utf8(a_char) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testToHex()
    {
        assertThat(query("SELECT to_hex(to_utf8(a_varchar)) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT to_hex(to_utf8(a_char)) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testSha1()
    {
        assertThat(query("SELECT sha1(to_utf8(a_varchar)) FROM expressions")).executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT to_hex(sha1(to_utf8(a_varchar10))) FROM expressions")).executesWithGpu(ProjectNode.class);
    }

    @Test
    void testCharTrimming()
    {
        assertUpdate("CREATE TABLE test_char_trimming(a_char) AS (VALUES CHAR 'abc', CHAR 'ab', CHAR 'a', CHAR '', NULL)", 5);

        assertThat(query("SELECT a_char FROM test_char_trimming"))
                .executesWithGpu(TableScanNode.class);
        assertThat(query("SELECT CAST(a_char AS varchar) FROM test_char_trimming"))
                .executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT length(a_char) FROM test_char_trimming"))
                .executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT substring(a_char, 1, 3) FROM test_char_trimming"))
                .executesWithoutGpu();
        assertThat(query("SELECT substring(a_char, 3, 1) FROM test_char_trimming"))
                .executesWithoutGpu();

        assertUpdate("DROP TABLE test_char_trimming");
    }

    @Test
    void testConcat()
    {
        assertUpdate(
                """
                CREATE TABLE test_concat(a_char, b_char, a_varchar) AS (VALUES
                    (CHAR 'ab', CHAR 'cd', VARCHAR 'ef'),
                    (CHAR '', CHAR '', VARCHAR ''),
                    (CHAR 'x', NULL, NULL))
                """,
                3);

        // Trino defines only a two-argument char concat. How an expression reaches it is decided by the
        // analyzer, outside the expression compiler, so assert it here rather than over hand-built IR.
        assertThat(query("SELECT concat(a_char, b_char) FROM test_concat"))
                .executesWithGpu(ProjectNode.class);
        // Each || is its own two-argument call, so a chain of them stays on the char overload
        assertThat(query("SELECT a_char || b_char || a_char FROM test_concat"))
                .executesWithGpu(ProjectNode.class);
        // A three-argument call has no char overload to bind to: it resolves to the variadic varchar one,
        // over the char -> varchar coercions it adds
        assertThat(query("SELECT concat(a_char, b_char, a_char) FROM test_concat"))
                .executesWithGpu(ProjectNode.class);

        assertThat(query("SELECT concat(a_varchar, a_varchar, a_varchar, a_varchar) FROM test_concat"))
                .executesWithGpu(ProjectNode.class);
        assertThat(query("SELECT a_varchar || a_varchar FROM test_concat"))
                .executesWithGpu(ProjectNode.class);

        // char and varchar arguments meet through a char -> varchar cast
        assertThat(query("SELECT concat(a_char, a_varchar) FROM test_concat"))
                .executesWithGpu(ProjectNode.class);

        assertUpdate("DROP TABLE test_concat");
    }
}
