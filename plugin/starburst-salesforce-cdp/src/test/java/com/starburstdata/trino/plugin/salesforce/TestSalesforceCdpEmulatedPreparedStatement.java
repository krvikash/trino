/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import io.trino.plugin.jdbc.ForwardingConnection;
import org.assertj.core.api.ThrowableAssert;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSalesforceCdpEmulatedPreparedStatement
{
    @Test
    void testCreateSqlQueryBoolean()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setBoolean(1, true);
            assertPreparedStatement(statement, "select * from sample where item=true");
        }
    }

    @Test
    void testCreateSqlQueryInt()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setInt(1, 234434343);
            assertPreparedStatement(statement, "select * from sample where item=234434343");
        }
    }

    @Test
    void testCreateSqlQueryLong()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setLong(1, 234434343L);
            assertPreparedStatement(statement, "select * from sample where item=234434343");
        }
    }

    @Test
    void testCreateSqlQueryFloat()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setFloat(1, 234.34442f);
            assertPreparedStatement(statement, "select * from sample where item=234.34442");
        }
    }

    @Test
    void testCreateSqlQueryDouble()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setDouble(1, 234.34442);
            assertPreparedStatement(statement, "select * from sample where item=234.34442");
        }
    }

    @Test
    void testUnsupportedDouble()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            assertThatThrownBy(() -> statement.setDouble(1, Double.NaN)).hasMessageContaining("Salesforce does not support infinite numbers");
            assertThatThrownBy(() -> statement.setDouble(1, Double.POSITIVE_INFINITY)).hasMessageContaining("Salesforce does not support infinite numbers");
            assertThatThrownBy(() -> statement.setDouble(1, Double.NEGATIVE_INFINITY)).hasMessageContaining("Salesforce does not support infinite numbers");
        }
    }

    @Test
    void testCreateSqlQueryObject()
            throws SQLFeatureNotSupportedException
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setObject(1, LocalDate.of(2008, 8, 31));
            assertPreparedStatement(statement, "select * from sample where item=cast('2008-08-31' as date)");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setObject(1, ZonedDateTime.of(LocalDate.of(2022, 6, 15), LocalTime.of(15, 23, 34), ZoneId.of("UTC")));
            assertPreparedStatement(statement, "select * from sample where item=cast('2022-06-15 15:23:34.000 Z' as timestamp)");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setObject(1, ZonedDateTime.of(LocalDate.of(2022, 6, 15), LocalTime.of(15, 23, 34, 23455555), ZoneId.of("America/Los_Angeles")));
            assertPreparedStatement(statement, "select * from sample where item=cast('2022-06-15 22:23:34.023 Z' as timestamp)");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setObject(1, ZonedDateTime.of(LocalDate.of(-2022, 6, 15), LocalTime.of(15, 23, 34, 23455555), ZoneId.of("America/Los_Angeles")));
            assertPreparedStatement(statement, "select * from sample where item=cast('-2022-06-15 23:16:32.023 Z' as timestamp)");
        }
    }

    @Test
    void testCreateSqlQueryString()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "value");
            assertPreparedStatement(statement, "select * from sample where item='value'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "????");
            assertPreparedStatement(statement, "select * from sample where item='????'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "''234'sample'");
            assertPreparedStatement(statement, "select * from sample where item='''''234''sample'''");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "''");
            assertPreparedStatement(statement, "select * from sample where item=''''''");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "\\");
            assertPreparedStatement(statement, "select * from sample where item='\\'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "\\\\");
            assertPreparedStatement(statement, "select * from sample where item='\\\\'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item1=? and item2=?")) {
            statement.setString(1, "????");
            statement.setString(2, "????????????");
            assertPreparedStatement(statement, "select * from sample where item1='????' and item2='????????????'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "?.*//\\???");
            assertPreparedStatement(statement, "select * from sample where item='?.*//\\???'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "?\"\".*//\\???");
            assertPreparedStatement(statement, "select * from sample where item='?\"\".*//\\???'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("SELECT \"orderkey__c\" FROM \"orders__dll\" WHERE (\"orderkey__c\" < ? OR \"orderkey__c\" > ?)")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertPreparedStatement(statement, "SELECT \"orderkey__c\" FROM \"orders__dll\" WHERE (\"orderkey__c\" < 20.0 OR \"orderkey__c\" > 100.0)");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=?")) {
            statement.setString(1, "2 or 1=1");
            assertPreparedStatement(statement, "select * from sample where item='2 or 1=1'");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("SELECT \"or=?de??=rkey__c\" FROM \"orders__dll\" WHERE (\"order= ?name__c\" < ? OR \"order= ?id__c\" > ?)")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertPreparedStatement(statement, "SELECT \"or=?de??=rkey__c\" FROM \"orders__dll\" WHERE (\"order= ?name__c\" < 20.0 OR \"order= ?id__c\" > 100.0)");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("SELECT \"or=?de??=rk\\ey__c\" FROM \"orders__dll\" WHERE (\"order= ?na/me__c\" < ? OR \"or..-|,;+^&@#!der()[]{}~`= ?id__c\" > ?)")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertPreparedStatement(statement, "SELECT \"or=?de??=rk\\ey__c\" FROM \"orders__dll\" WHERE (\"order= ?na/me__c\" < 20.0 OR \"or..-|,;+^&@#!der()[]{}~`= ?id__c\" > 100.0)");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? and item2= ?")) {
            statement.setDouble(1, 20.0);
            assertThatThrownBy(statement::createSqlQuery).hasMessageContaining("Too few parameters");
        }
    }

    @Test
    void testCreateSqlQueryWithMultiLineComment()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("SELECT \"or=?de??=rk\\ey\" FROM \"orders\" WHERE (\"or/* column name multi line comment ?? der= ?na/me\" < ? OR \"or..// col name single line comment-|,;+^&@#!der()[]{}~`= ?id__c\" > ?)")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertPreparedStatement(statement, "SELECT \"or=?de??=rk\\ey\" FROM \"orders\" WHERE (\"or/* column name multi line comment ?? der= ?na/me\" < 20.0 OR \"or..// col name single line comment-|,;+^&@#!der()[]{}~`= ?id__c\" > 100.0)");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? / / incorrect = ? ending with comment end/* also multi ??line within single line ?? */start ?? yes")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            statement.setDouble(3, 300.0);
            statement.setDouble(4, 400.0);
            assertPreparedStatement(statement, "select * from sample where item=20.0 / / incorrect = 100.0 ending with comment endstart 300.0400.0 yes");
        }
        // Failures
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from /*some comment ?? in between */ sample where item=?")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertThatThrownBy(statement::createSqlQuery).hasMessageContaining("Too many parameters");
        }
    }

    @Test
    void testCreateSqlQueryWithSingleLineComment()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("SELECT \"or=?de??=rk\\ey\" FROM \"orders\" WHERE (\"or/* column name multi line comment ?? der= ?na/me\" < ? OR \"or..// col name single line comment-|,;+^&@#!der()[]{}~`= ?id__c\" > ?)")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertPreparedStatement(statement, "SELECT \"or=?de??=rk\\ey\" FROM \"orders\" WHERE (\"or/* column name multi line comment ?? der= ?na/me\" < 20.0 OR \"or..// col name single line comment-|,;+^&@#!der()[]{}~`= ?id__c\" > 100.0)");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("-- starting with single line comment select * from sample where item=?")) {
            assertPreparedStatement(statement, "");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? -- ending with comment /* also multi ??line within single line ?? */ ?? yes")) {
            statement.setDouble(1, 20.0);
            assertPreparedStatement(statement, "select * from sample where item=20.0 ");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? / * \"col?name\"incorrect multi ? line */ -- incorrect = ? ending with comment ?? yes")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertPreparedStatement(statement, "select * from sample where item=20.0 / * \"col?name\"incorrect multi 100.0 line */ ");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? /* \"col?name\"incorrect multi ? line * / -- incorrect = ? ending with comment ?? yes")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertThatThrownBy(statement::createSqlQuery).hasMessageContaining("Block comment did not end properly");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? /* incorrect //multi ? line */ in between -- incorrect = ? ending with comment ?? yes")) {
            statement.setDouble(1, 10.0);
            assertPreparedStatement(statement, "select * from sample where item=10.0  in between ");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where \"/*col_Na?me */ -- single line cmt\"=? -- ending with comment /* also multi ??line within single line ?? */ ?? yes")) {
            statement.setDouble(1, 20.0);
            assertPreparedStatement(statement, "select * from sample where \"/*col_Na?me */ -- single line cmt\"=20.0 ");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? -- ending with comment /* also multi ??line within single line ?? */ ?? yes")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertThatThrownBy(statement::createSqlQuery).hasMessageContaining("Too many parameters");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("-- starting with single /* multi line as well */ line comment select * from sample where item=?")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertThatThrownBy(statement::createSqlQuery).hasMessageContaining("Too many parameters");
        }
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select * from sample where item=? -- ending with comment /* also multi ??line within single line ?? */ ?? yes")) {
            statement.setDouble(1, 20.0);
            statement.setDouble(2, 100.0);
            assertThatThrownBy(statement::createSqlQuery).hasMessageContaining("Too many parameters");
        }
    }

    @Test
    void testCreateSqlQueryOrderByAndLimit()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select count(*) from sample where item=? order by col")) {
            statement.setString(1, "?\"\".*//\\???");
            assertPreparedStatement(statement, "select count(*) from sample where item='?\"\".*//\\???' order by col");
        }

        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select sum(item) from sample where item=? limit 56")) {
            statement.setString(1, "?\"\".*//\\???");
            assertPreparedStatement(statement, "select sum(item) from sample where item='?\"\".*//\\???' limit 56");
        }
    }

    @Test
    void testUnsupportedMethods()
    {
        try (SalesforceCdpEmulatedPreparedStatement statement = newPreparedStatement("select count(*) from sample where item=?")) {
            assertThatThrownBy(() -> statement.setObject(1, BigDecimal.ONE)).isExactlyInstanceOf(SQLFeatureNotSupportedException.class).hasMessageContaining("class java.math.BigDecimal class is not supported");
            assertThatUnsupportedOperation(() -> statement.setBigDecimal(1, BigDecimal.ONE));
            assertThatUnsupportedOperation(() -> statement.setTimestamp(1, Timestamp.from(Instant.now())));
            assertThatUnsupportedOperation(() -> statement.setDate(1, Date.valueOf("2024-01-01")));
            assertThatUnsupportedOperation(() -> statement.setByte(1, Byte.MIN_VALUE));
            assertThatUnsupportedOperation(() -> statement.setShort(1, Short.MIN_VALUE));
            assertThatUnsupportedOperation(() -> statement.setBytes(1, "value".getBytes(StandardCharsets.US_ASCII)));
            assertThatUnsupportedOperation(() -> statement.setTime(1, new Time(Long.MIN_VALUE)));
            assertThatUnsupportedOperation(() -> statement.setAsciiStream(1, new ByteArrayInputStream("value".getBytes(StandardCharsets.UTF_8)), 2));
            assertThatUnsupportedOperation(() -> statement.setUnicodeStream(1, new ByteArrayInputStream("value".getBytes(StandardCharsets.UTF_8)), 2));
            assertThatUnsupportedOperation(() -> statement.setBinaryStream(1, new ByteArrayInputStream("value".getBytes(StandardCharsets.UTF_8)), 2));
            assertThatUnsupportedOperation(() -> statement.setObject(1, BigDecimal.ONE, 2));
            assertThatUnsupportedOperation(statement::executeUpdate);
            assertThatUnsupportedOperation(statement::execute);
            assertThatUnsupportedOperation(statement::addBatch);
            assertThatUnsupportedOperation(() -> statement.setCharacterStream(1, Reader.of("value"), 1));
            assertThatUnsupportedOperation(() -> statement.setRef(1, null));
            assertThatUnsupportedOperation(() -> statement.setBlob(1, (Blob) null));
            assertThatUnsupportedOperation(() -> statement.setClob(1, (Clob) null));
            assertThatUnsupportedOperation(() -> statement.setArray(1, null));
            assertThatUnsupportedOperation(statement::getMetaData);
            assertThatUnsupportedOperation(() -> statement.setDate(1, null, null));
            assertThatUnsupportedOperation(() -> statement.setTime(1, null, null));
            assertThatUnsupportedOperation(() -> statement.setTimestamp(1, null, null));
            assertThatUnsupportedOperation(() -> statement.setNull(1, 1, null));
            assertThatUnsupportedOperation(() -> statement.setURL(1, null));
            assertThatUnsupportedOperation(statement::getParameterMetaData);
            assertThatUnsupportedOperation(() -> statement.setRowId(1, null));
            assertThatUnsupportedOperation(() -> statement.setNString(1, null));
            assertThatUnsupportedOperation(() -> statement.setNCharacterStream(1, null, 1));
            assertThatUnsupportedOperation(() -> statement.setNClob(1, (NClob) null));
            assertThatUnsupportedOperation(() -> statement.setClob(1, null, 1));
            assertThatUnsupportedOperation(() -> statement.setBlob(1, null, 1));
            assertThatUnsupportedOperation(() -> statement.setNClob(1, null, 1));
            assertThatUnsupportedOperation(() -> statement.setSQLXML(1, null));
            assertThatUnsupportedOperation(() -> statement.setObject(1, null, 1, 1));
            assertThatUnsupportedOperation(() -> statement.setAsciiStream(1, null, 2));
            assertThatUnsupportedOperation(() -> statement.setBinaryStream(1, null, 2));
            assertThatUnsupportedOperation(() -> statement.setCharacterStream(1, null, 2));
            assertThatUnsupportedOperation(() -> statement.setAsciiStream(1, null));
            assertThatUnsupportedOperation(() -> statement.setBinaryStream(1, new ByteArrayInputStream("value".getBytes(StandardCharsets.UTF_8))));
            assertThatUnsupportedOperation(() -> statement.setCharacterStream(1, Reader.of("value")));
            assertThatUnsupportedOperation(() -> statement.setNCharacterStream(1, Reader.of("value")));
            assertThatUnsupportedOperation(() -> statement.setClob(1, Reader.of("value")));
            assertThatUnsupportedOperation(() -> statement.setBlob(1, new ByteArrayInputStream("value".getBytes(StandardCharsets.UTF_8))));
            assertThatUnsupportedOperation(() -> statement.setNClob(1, Reader.of("value")));
            assertThatUnsupportedOperation(() -> statement.executeQuery("sql"));
            assertThatUnsupportedOperation(() -> statement.executeUpdate("sql"));
            assertThatUnsupportedOperation(statement::getMaxFieldSize);
            assertThatUnsupportedOperation(() -> statement.setMaxFieldSize(3));
            assertThatUnsupportedOperation(statement::getMaxRows);
            assertThatUnsupportedOperation(() -> statement.setMaxRows(3));
            assertThatUnsupportedOperation(() -> statement.setEscapeProcessing(false));
            assertThatUnsupportedOperation(statement::getQueryTimeout);
            assertThatUnsupportedOperation(() -> statement.setQueryTimeout(3));
            assertThatUnsupportedOperation(statement::cancel);
            assertThatUnsupportedOperation(statement::getWarnings);
            assertThatUnsupportedOperation(statement::clearWarnings);
            assertThatUnsupportedOperation(() -> statement.setCursorName("value"));
            assertThatUnsupportedOperation(() -> statement.execute("sql"));
            assertThatUnsupportedOperation(statement::getResultSet);
            assertThatUnsupportedOperation(statement::getUpdateCount);
            assertThatUnsupportedOperation(statement::getMoreResults);
            assertThatUnsupportedOperation(() -> statement.setFetchDirection(ResultSet.FETCH_FORWARD));
            assertThatUnsupportedOperation(statement::getFetchDirection);
            assertThatUnsupportedOperation(() -> statement.setFetchSize(1));
            assertThatUnsupportedOperation(statement::getFetchSize);
            assertThatUnsupportedOperation(statement::getResultSetConcurrency);
            assertThatUnsupportedOperation(statement::getResultSetType);
            assertThatUnsupportedOperation(() -> statement.addBatch("sql"));
            assertThatUnsupportedOperation(statement::clearBatch);
            assertThatUnsupportedOperation(statement::executeBatch);
            assertThatUnsupportedOperation(() -> statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            assertThatUnsupportedOperation(statement::getGeneratedKeys);
            assertThatUnsupportedOperation(() -> statement.executeUpdate("sql", Statement.RETURN_GENERATED_KEYS));
            assertThatUnsupportedOperation(() -> statement.executeUpdate("sql", new int[] {1}));
            assertThatUnsupportedOperation(() -> statement.executeUpdate("sql", new String[] {"col"}));
            assertThatUnsupportedOperation(() -> statement.execute("sql", Statement.RETURN_GENERATED_KEYS));
            assertThatUnsupportedOperation(() -> statement.execute("sql", new int[] {1}));
            assertThatUnsupportedOperation(() -> statement.execute("sql", new String[] {"col"}));
            assertThatUnsupportedOperation(statement::getResultSetHoldability);
            assertThatUnsupportedOperation(statement::isClosed);
            assertThatUnsupportedOperation(() -> statement.setPoolable(false));
            assertThatUnsupportedOperation(statement::isPoolable);
            assertThatUnsupportedOperation(statement::closeOnCompletion);
            assertThatUnsupportedOperation(statement::isCloseOnCompletion);
            assertThatUnsupportedOperation(() -> statement.unwrap(Date.class));
            assertThatUnsupportedOperation(() -> statement.isWrapperFor(Date.class));
        }
    }

    private static void assertThatUnsupportedOperation(ThrowableAssert.ThrowingCallable shouldRaiseThrowable)
    {
        assertThatThrownBy(shouldRaiseThrowable)
                .isExactlyInstanceOf(SQLFeatureNotSupportedException.class)
                .hasMessageContaining("Not supported");
    }

    private static SalesforceCdpEmulatedPreparedStatement newPreparedStatement(@Language("SQL") String sql)
    {
        // Used ForwardingConnection to simulate not null connection
        ForwardingConnection connection = new ForwardingConnection()
        {
            @Override
            protected Connection delegate()
                    throws SQLException
            {
                return null;
            }
        };
        return new SalesforceCdpEmulatedPreparedStatement(connection, sql);
    }

    private static void assertPreparedStatement(SalesforceCdpEmulatedPreparedStatement statement, @Language("SQL") String expected)
    {
        assertThat(statement.createSqlQuery()).isEqualTo(expected);
    }
}
