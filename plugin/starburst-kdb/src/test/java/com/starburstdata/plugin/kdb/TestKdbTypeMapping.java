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

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.junit.jupiter.api.Test;

import static java.time.ZoneOffset.UTC;

class TestKdbTypeMapping
        extends AbstractTestQueryFramework
{
    private KdbClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        KdbContainer server = closeAfterClass(new KdbContainer());
        client = server.client();
        return KdbQueryRunner.builder(server).build();
    }

    // -----------------------------------------------------------------------
    // Scalar types
    // -----------------------------------------------------------------------

    @Test
    void testBoolean()
    {
        // KDB+ 'b' — no kdb+ null
        SqlDataTypeTest.create()
                .addRoundTrip("1b", "true")
                .addRoundTrip("0b", "false")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_bool"));
    }

    @Test
    void testGuid()
    {
        // KDB+ 'g' — mapped to VARCHAR(36); kdb+ null is 0Ng (all-zero UUID)
        SqlDataTypeTest.create()
                .addRoundTrip("\"G\"$\"550e8400-e29b-41d4-a716-446655440000\"", "CAST('550e8400-e29b-41d4-a716-446655440000' AS VARCHAR(36))")
                .addRoundTrip("0Ng", "CAST(NULL AS VARCHAR(36))")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_guid"));
    }

    @Test
    void testTinyint()
    {
        // KDB+ 'x' (byte) — no kdb+ null
        SqlDataTypeTest.create()
                .addRoundTrip("0x2a", "TINYINT '42'")
                .addRoundTrip("0xff", "TINYINT '-1'")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_tinyint"));
    }

    @Test
    void testSmallint()
    {
        // KDB+ 'h' — kdb+ null is 0Nh (Short.MIN_VALUE)
        SqlDataTypeTest.create()
                .addRoundTrip("1000h", "SMALLINT '1000'")
                .addRoundTrip("0Nh", "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_smallint"));
    }

    @Test
    void testInteger()
    {
        // KDB+ 'i' — kdb+ null is 0Ni (Integer.MIN_VALUE)
        SqlDataTypeTest.create()
                .addRoundTrip("123456i", "INTEGER '123456'")
                .addRoundTrip("0Ni", "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_integer"));
    }

    @Test
    void testBigint()
    {
        // KDB+ 'j' — kdb+ null is 0Nj (Long.MIN_VALUE)
        SqlDataTypeTest.create()
                .addRoundTrip("9876543210j", "BIGINT '9876543210'")
                .addRoundTrip("0Nj", "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_bigint"));
    }

    @Test
    void testReal()
    {
        // KDB+ 'e' — kdb+ null is 0Ne (Float.NaN)
        SqlDataTypeTest.create()
                .addRoundTrip("1.5e", "REAL '1.5'")
                .addRoundTrip("0Ne", "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_real"));
    }

    @Test
    void testDouble()
    {
        // KDB+ 'f' — kdb+ null is 0n (Double.NaN)
        SqlDataTypeTest.create()
                .addRoundTrip("3.14", "DOUBLE '3.14'")
                .addRoundTrip("0n", "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_double"));
    }

    @Test
    void testChar()
    {
        // KDB+ 'c' — mapped to VARCHAR; kdb+ null is space character (' ')
        // Use first "A" to produce a char atom (type -10h) so that enlist creates a char
        // vector (type 10h) rather than a generic list.
        SqlDataTypeTest.create()
                .addRoundTrip("first \"A\"", "VARCHAR 'A'")
                .addRoundTrip("first \" \"", "CAST(NULL AS VARCHAR)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_char"));
    }

    @Test
    void testSymbol()
    {
        // KDB+ 's' — mapped to VARCHAR; kdb+ null is the empty symbol (``)
        SqlDataTypeTest.create()
                .addRoundTrip("`hello", "VARCHAR 'hello'")
                .addRoundTrip("`", "CAST(NULL AS VARCHAR)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_symbol"));
    }

    // -----------------------------------------------------------------------
    // Temporal types — timestamps are stored in UTC by KDB+ and interpreted
    // as local datetime by Trino, so we run these in UTC session.
    // -----------------------------------------------------------------------

    @Test
    void testTimestamp()
    {
        // KDB+ 'p' — mapped to TIMESTAMP(6); kdb+ null is 0Np (Long.MIN_VALUE nanos).
        // KDB+ timestamps are UTC-based; we run the assertion in UTC so the expected
        // literal matches the epoch value returned by the connector.
        Session utcSession = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(UTC.getId()))
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("2023.01.01D00:00:00.000001", "TIMESTAMP '2023-01-01 00:00:00.000001'")
                .addRoundTrip("0Np", "CAST(NULL AS TIMESTAMP(6))")
                .execute(getQueryRunner(), utcSession, kdbCreateAndInsert("test_ts"));
    }

    @Test
    void testMonth()
    {
        // KDB+ 'm' — mapped to VARCHAR; kdb+ null is 0Nm (Integer.MIN_VALUE months)
        SqlDataTypeTest.create()
                .addRoundTrip("2023.01m", "VARCHAR '2023.01m'")
                .addRoundTrip("0Nm", "CAST(NULL AS VARCHAR)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_month"));
    }

    @Test
    void testDate()
    {
        // KDB+ 'd' — mapped to DATE; kdb+ null is 0Nd (Integer.MIN_VALUE days)
        SqlDataTypeTest.create()
                .addRoundTrip("2023.01.01", "DATE '2023-01-01'")
                .addRoundTrip("0Nd", "CAST(NULL AS DATE)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_date"));
    }

    @Test
    void testDatetime()
    {
        // KDB+ 'z' — mapped to TIMESTAMP(3); kdb+ null is 0Nz (Double.NaN days).
        // KDB+ datetimes are UTC-based; run in UTC so the expected literal matches.
        Session utcSession = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(UTC.getId()))
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("2023.01.01T00:00:00.000", "TIMESTAMP '2023-01-01 00:00:00.000'")
                .addRoundTrip("0Nz", "CAST(NULL AS TIMESTAMP(3))")
                .execute(getQueryRunner(), utcSession, kdbCreateAndInsert("test_datetime"));
    }

    @Test
    void testTimespan()
    {
        // KDB+ 'n' — mapped to BIGINT (nanoseconds); kdb+ null is 0Nn (Long.MIN_VALUE)
        SqlDataTypeTest.create()
                .addRoundTrip("01:00:00.000000000", "BIGINT '3600000000000'")
                .addRoundTrip("0Nn", "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_timespan"));
    }

    @Test
    void testMinute()
    {
        // KDB+ 'u' — mapped to INTEGER (minutes since midnight); kdb+ null is 0Nu
        SqlDataTypeTest.create()
                .addRoundTrip("01:30", "INTEGER '90'")
                .addRoundTrip("0Nu", "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_minute"));
    }

    @Test
    void testSecond()
    {
        // KDB+ 'v' — mapped to INTEGER (seconds since midnight); kdb+ null is 0Nv
        SqlDataTypeTest.create()
                .addRoundTrip("01:01:01", "INTEGER '3661'")
                .addRoundTrip("0Nv", "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_second"));
    }

    @Test
    void testTime()
    {
        // KDB+ 't' — mapped to TIME(3); kdb+ null is 0Nt (Integer.MIN_VALUE millis)
        SqlDataTypeTest.create()
                .addRoundTrip("01:00:00.000", "TIME '01:00:00.000'")
                .addRoundTrip("0Nt", "CAST(NULL AS TIME(3))")
                .execute(getQueryRunner(), kdbCreateAndInsert("test_time"));
    }

    // Helper method to create a 1-row table column
    private DataSetup kdbCreateAndInsert(String tableNamePrefix)
    {
        return new KdbCreateAndInsertDataSetup(client, tableNamePrefix);
    }
}
