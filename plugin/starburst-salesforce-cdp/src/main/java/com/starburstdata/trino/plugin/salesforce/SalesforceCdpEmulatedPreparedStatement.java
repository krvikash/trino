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

import com.google.common.collect.PeekingIterator;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.lang.Double.isFinite;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

/**
 * <a href="https://github.com/forcedotcom/Salesforce-CDP-jdbc/blob/3bdaee1468c2e3b18ac84f350b01ff9f5e06305b/src/main/java/com/salesforce/cdp/queryservice/core/QueryServiceConnection.java#L145-L147">Salesforce CDP drivers</a>
 * does not support {@link java.sql.PreparedStatement}.
 *  {@link SalesforceCdpEmulatedPreparedStatement} emulates {@link java.sql.PreparedStatement} by implementing necessary functionality for the connector.
 *
 */
public class SalesforceCdpEmulatedPreparedStatement
        implements PreparedStatement
{
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS zzz");

    private final Connection connection;
    private final String sql;
    private final Map<Integer, String> parameters = new HashMap<>();

    public SalesforceCdpEmulatedPreparedStatement(Connection connection, String sql)
    {
        this.connection = requireNonNull(connection, "connection is null");
        this.sql = requireNonNull(sql, "sql is null");
    }

    private enum ParsingState
    {
        STATEMENT,
        LINE_COMMENT,
        BLOCK_COMMENT,
        VARCHAR_LITERAL,
        DELIMITED_IDENTIFIER,
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return connection;
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
        parameters.clear();
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        Statement statement = connection.createStatement();
        return statement.executeQuery(createSqlQuery());
    }

    String createSqlQuery()
    {
        List<String> list = new ArrayList<>(parameters.size());
        for (int i = 1; i <= parameters.size(); i++) {
            String literal = parameters.get(i);
            checkArgument(literal != null, "No value for parameter %s", i);
            list.add(literal);
        }
        return interpolate(list);
    }

    private String interpolate(List<String> sqlLiterals)
    {
        Iterator<String> literals = sqlLiterals.iterator();
        StringBuilder result = new StringBuilder();
        ParsingState state = ParsingState.STATEMENT;
        PeekingIterator<Integer> iterator = peekingIterator(sql.codePoints().iterator());
        while (iterator.hasNext()) {
            int next = iterator.next();
            switch (state) {
                case STATEMENT -> {
                    if (next == '-' && iterator.hasNext() && iterator.peek() == '-') {
                        state = ParsingState.LINE_COMMENT;
                    }
                    else if (next == '/' && iterator.hasNext() && iterator.peek() == '*') {
                        state = ParsingState.BLOCK_COMMENT;
                    }
                    else if (next == '\'') {
                        result.appendCodePoint(next);
                        state = ParsingState.VARCHAR_LITERAL;
                    }
                    else if (next == '"') {
                        result.appendCodePoint(next);
                        state = ParsingState.DELIMITED_IDENTIFIER;
                    }
                    else if (next == '?') {
                        checkState(literals.hasNext(), "Too few parameters");
                        result.append(literals.next());
                    }
                    else {
                        result.appendCodePoint(next);
                    }
                }
                case LINE_COMMENT -> {
                    if (next == '\n' || !iterator.hasNext()) {
                        state = ParsingState.STATEMENT;
                    }
                }
                case BLOCK_COMMENT -> {
                    if (next == '*' && iterator.hasNext() && iterator.peek() == '/') {
                        iterator.next();
                        state = ParsingState.STATEMENT;
                    }
                }
                case VARCHAR_LITERAL -> {
                    result.appendCodePoint(next);
                    if (next == '\'') {
                        state = ParsingState.STATEMENT;
                    }
                }
                case DELIMITED_IDENTIFIER -> {
                    result.appendCodePoint(next);
                    if (next == '"') {
                        state = ParsingState.STATEMENT;
                    }
                }
            }
        }
        switch (state) {
            case LINE_COMMENT -> throw new RuntimeException("Line comment did not end properly");
            case BLOCK_COMMENT -> throw new RuntimeException("Block comment did not end properly");
            case VARCHAR_LITERAL -> throw new RuntimeException("Varchar literal did not end properly");
            case DELIMITED_IDENTIFIER -> throw new RuntimeException("Delimited identifier did not end properly");
            case STATEMENT -> {} // Expected end state
        }
        checkState(!literals.hasNext(), "Too many parameters");
        return result.toString();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
    {
        parameters.put(parameterIndex, Boolean.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x)
    {
        parameters.put(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x)
    {
        parameters.put(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x)
    {
        parameters.put(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x)
    {
        checkArgument(isFinite(x), "Salesforce does not support infinite numbers");
        parameters.put(parameterIndex, Double.toString(x));
    }

    @Override
    public void setString(int parameterIndex, String x)
    {
        parameters.put(parameterIndex, "'" + x.replace("'", "''") + "'");
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLFeatureNotSupportedException
    {
        if (x instanceof LocalDate localDate) {
            parameters.put(parameterIndex, "cast('%s' as date)".formatted(localDate.toString()));
            return;
        }
        if (x instanceof ZonedDateTime zonedDateTime) {
            parameters.put(parameterIndex, "cast('%s' as timestamp)".formatted(DATE_TIME_FORMATTER.format(zonedDateTime.withZoneSameInstant(UTC))));
            return;
        }
        if (x == null) {
            throw new SQLFeatureNotSupportedException("x is null");
        }
        throw new SQLFeatureNotSupportedException("%s class is not supported".formatted(x.getClass()));
    }

    @Override
    public void close() {}

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    @SuppressWarnings("deprecation") // Suppress warning is safe as this method is not supported anyway.
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void addBatch()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public ResultSet executeQuery(String sql)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int executeUpdate(String sql)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getMaxFieldSize()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setMaxFieldSize(int max)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getMaxRows()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setMaxRows(int max)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setEscapeProcessing(boolean enable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getQueryTimeout()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setQueryTimeout(int seconds)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void cancel()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setCursorName(String name)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean execute(String sql)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public ResultSet getResultSet()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getUpdateCount()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean getMoreResults()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getResultSetConcurrency()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getResultSetType()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void clearBatch()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int[] executeBatch()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean getMoreResults(int current)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public ResultSet getGeneratedKeys()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void setPoolable(boolean poolable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean isPoolable()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public void closeOnCompletion()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean isCloseOnCompletion()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Not supported");
    }
}
