/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A ResultSet that iterates through the first ResultSet and then the second.
 * All methods delegate to the current active ResultSet, except for
 * {@link #next()}, {@link #close()}, and {@link #isClosed()} which
 * handle the sequential transition.
 */
class SequentialResultSet
        implements ResultSet
{
    private final ResultSet first;
    private final ResultSet second;
    private boolean onSecond;

    SequentialResultSet(ResultSet first, ResultSet second)
    {
        this.first = requireNonNull(first, "first is null");
        this.second = requireNonNull(second, "second is null");
    }

    private ResultSet current()
    {
        return onSecond ? second : first;
    }

    @Override
    public boolean next()
            throws SQLException
    {
        if (!onSecond) {
            if (first.next()) {
                return true;
            }
            onSecond = true;
        }
        return second.next();
    }

    @Override
    public void close()
            throws SQLException
    {
        try {
            first.close();
        }
        finally {
            second.close();
        }
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return first.isClosed() && second.isClosed();
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return current().getMetaData();
    }

    @Override
    public Statement getStatement()
            throws SQLException
    {
        return current().getStatement();
    }

    @Override
    public boolean wasNull()
            throws SQLException
    {
        return current().wasNull();
    }

    @Override
    public String getString(int columnIndex)
            throws SQLException
    {
        return current().getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex)
            throws SQLException
    {
        return current().getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex)
            throws SQLException
    {
        return current().getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex)
            throws SQLException
    {
        return current().getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex)
            throws SQLException
    {
        return current().getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex)
            throws SQLException
    {
        return current().getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex)
            throws SQLException
    {
        return current().getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex)
            throws SQLException
    {
        return current().getDouble(columnIndex);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException
    {
        return current().getBigDecimal(columnIndex, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        return current().getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex)
            throws SQLException
    {
        return current().getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex)
            throws SQLException
    {
        return current().getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
            throws SQLException
    {
        return current().getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        return current().getAsciiStream(columnIndex);
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        return current().getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        return current().getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel)
            throws SQLException
    {
        return current().getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel)
            throws SQLException
    {
        return current().getBoolean(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel)
            throws SQLException
    {
        return current().getByte(columnLabel);
    }

    @Override
    public short getShort(String columnLabel)
            throws SQLException
    {
        return current().getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel)
            throws SQLException
    {
        return current().getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel)
            throws SQLException
    {
        return current().getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel)
            throws SQLException
    {
        return current().getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel)
            throws SQLException
    {
        return current().getDouble(columnLabel);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException
    {
        return current().getBigDecimal(columnLabel, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel)
            throws SQLException
    {
        return current().getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel)
            throws SQLException
    {
        return current().getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel)
            throws SQLException
    {
        return current().getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel)
            throws SQLException
    {
        return current().getTimestamp(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel)
            throws SQLException
    {
        return current().getAsciiStream(columnLabel);
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String columnLabel)
            throws SQLException
    {
        return current().getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel)
            throws SQLException
    {
        return current().getBinaryStream(columnLabel);
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        return current().getWarnings();
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        current().clearWarnings();
    }

    @Override
    public String getCursorName()
            throws SQLException
    {
        return current().getCursorName();
    }

    @Override
    public int findColumn(String columnLabel)
            throws SQLException
    {
        return current().findColumn(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        return current().getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel)
            throws SQLException
    {
        return current().getCharacterStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        return current().getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)
            throws SQLException
    {
        return current().getBigDecimal(columnLabel);
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException
    {
        return current().isBeforeFirst();
    }

    @Override
    public boolean isAfterLast()
            throws SQLException
    {
        return current().isAfterLast();
    }

    @Override
    public boolean isFirst()
            throws SQLException
    {
        return current().isFirst();
    }

    @Override
    public boolean isLast()
            throws SQLException
    {
        return current().isLast();
    }

    @Override
    public void beforeFirst()
            throws SQLException
    {
        current().beforeFirst();
    }

    @Override
    public void afterLast()
            throws SQLException
    {
        current().afterLast();
    }

    @Override
    public boolean first()
            throws SQLException
    {
        return current().first();
    }

    @Override
    public boolean last()
            throws SQLException
    {
        return current().last();
    }

    @Override
    public int getRow()
            throws SQLException
    {
        return current().getRow();
    }

    @Override
    public boolean absolute(int row)
            throws SQLException
    {
        return current().absolute(row);
    }

    @Override
    public boolean relative(int rows)
            throws SQLException
    {
        return current().relative(rows);
    }

    @Override
    public boolean previous()
            throws SQLException
    {
        return current().previous();
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        current().setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        return current().getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        current().setFetchSize(rows);
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        return current().getFetchSize();
    }

    @Override
    public int getType()
            throws SQLException
    {
        return current().getType();
    }

    @Override
    public int getConcurrency()
            throws SQLException
    {
        return current().getConcurrency();
    }

    @Override
    public boolean rowUpdated()
            throws SQLException
    {
        return current().rowUpdated();
    }

    @Override
    public boolean rowInserted()
            throws SQLException
    {
        return current().rowInserted();
    }

    @Override
    public boolean rowDeleted()
            throws SQLException
    {
        return current().rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex)
            throws SQLException
    {
        current().updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
        current().updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
        current().updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
        current().updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
        current().updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
        current().updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
        current().updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
        current().updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
        current().updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x)
            throws SQLException
    {
        current().updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
        current().updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
        current().updateDate(columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
        current().updateTime(columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
        current().updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        current().updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        current().updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
        current().updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException
    {
        current().updateObject(columnIndex, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
        current().updateObject(columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel)
            throws SQLException
    {
        current().updateNull(columnLabel);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException
    {
        current().updateBoolean(columnLabel, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x)
            throws SQLException
    {
        current().updateByte(columnLabel, x);
    }

    @Override
    public void updateShort(String columnLabel, short x)
            throws SQLException
    {
        current().updateShort(columnLabel, x);
    }

    @Override
    public void updateInt(String columnLabel, int x)
            throws SQLException
    {
        current().updateInt(columnLabel, x);
    }

    @Override
    public void updateLong(String columnLabel, long x)
            throws SQLException
    {
        current().updateLong(columnLabel, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x)
            throws SQLException
    {
        current().updateFloat(columnLabel, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x)
            throws SQLException
    {
        current().updateDouble(columnLabel, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException
    {
        current().updateBigDecimal(columnLabel, x);
    }

    @Override
    public void updateString(String columnLabel, String x)
            throws SQLException
    {
        current().updateString(columnLabel, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x)
            throws SQLException
    {
        current().updateBytes(columnLabel, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x)
            throws SQLException
    {
        current().updateDate(columnLabel, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x)
            throws SQLException
    {
        current().updateTime(columnLabel, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException
    {
        current().updateTimestamp(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        current().updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        current().updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader x, int length)
            throws SQLException
    {
        current().updateCharacterStream(columnLabel, x, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException
    {
        current().updateObject(columnLabel, x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x)
            throws SQLException
    {
        current().updateObject(columnLabel, x);
    }

    @Override
    public void insertRow()
            throws SQLException
    {
        current().insertRow();
    }

    @Override
    public void updateRow()
            throws SQLException
    {
        current().updateRow();
    }

    @Override
    public void deleteRow()
            throws SQLException
    {
        current().deleteRow();
    }

    @Override
    public void refreshRow()
            throws SQLException
    {
        current().refreshRow();
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException
    {
        current().cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow()
            throws SQLException
    {
        current().moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException
    {
        current().moveToCurrentRow();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException
    {
        return current().getObject(columnIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex)
            throws SQLException
    {
        return current().getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex)
            throws SQLException
    {
        return current().getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex)
            throws SQLException
    {
        return current().getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex)
            throws SQLException
    {
        return current().getArray(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException
    {
        return current().getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(String columnLabel)
            throws SQLException
    {
        return current().getRef(columnLabel);
    }

    @Override
    public Blob getBlob(String columnLabel)
            throws SQLException
    {
        return current().getBlob(columnLabel);
    }

    @Override
    public Clob getClob(String columnLabel)
            throws SQLException
    {
        return current().getClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel)
            throws SQLException
    {
        return current().getArray(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        return current().getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal)
            throws SQLException
    {
        return current().getDate(columnLabel, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        return current().getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal)
            throws SQLException
    {
        return current().getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        return current().getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException
    {
        return current().getTimestamp(columnLabel, cal);
    }

    @Override
    public URL getURL(int columnIndex)
            throws SQLException
    {
        return current().getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel)
            throws SQLException
    {
        return current().getURL(columnLabel);
    }

    @Override
    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
        current().updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x)
            throws SQLException
    {
        current().updateRef(columnLabel, x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
        current().updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x)
            throws SQLException
    {
        current().updateBlob(columnLabel, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
        current().updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x)
            throws SQLException
    {
        current().updateClob(columnLabel, x);
    }

    @Override
    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
        current().updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x)
            throws SQLException
    {
        current().updateArray(columnLabel, x);
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        return current().getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        return current().getRowId(columnLabel);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
        current().updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x)
            throws SQLException
    {
        current().updateRowId(columnLabel, x);
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        return current().getHoldability();
    }

    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException
    {
        current().updateNString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException
    {
        current().updateNString(columnLabel, nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
    {
        current().updateNClob(columnIndex, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException
    {
        current().updateNClob(columnLabel, nClob);
    }

    @Override
    public NClob getNClob(int columnIndex)
            throws SQLException
    {
        return current().getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        return current().getNClob(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        return current().getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        return current().getSQLXML(columnLabel);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
        current().updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
        current().updateSQLXML(columnLabel, xmlObject);
    }

    @Override
    public String getNString(int columnIndex)
            throws SQLException
    {
        return current().getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel)
            throws SQLException
    {
        return current().getNString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex)
            throws SQLException
    {
        return current().getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel)
            throws SQLException
    {
        return current().getNCharacterStream(columnLabel);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        current().updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        current().updateNCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        current().updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        current().updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        current().updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        current().updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        current().updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        current().updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        current().updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
        current().updateBlob(columnLabel, inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        current().updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        current().updateClob(columnLabel, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        current().updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        current().updateNClob(columnLabel, reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        current().updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        current().updateNCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
        current().updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
        current().updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        current().updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
        current().updateAsciiStream(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
        current().updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        current().updateCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
        current().updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
        current().updateBlob(columnLabel, inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
        current().updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
        current().updateClob(columnLabel, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
        current().updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
        current().updateNClob(columnLabel, reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException
    {
        return current().getObject(columnIndex, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException
    {
        return current().getObject(columnLabel, type);
    }

    @Override
    public Object getObject(int columnIndex)
            throws SQLException
    {
        return current().getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel)
            throws SQLException
    {
        return current().getObject(columnLabel);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException
    {
        current().updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException
    {
        current().updateObject(columnLabel, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType)
            throws SQLException
    {
        current().updateObject(columnIndex, x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
            throws SQLException
    {
        current().updateObject(columnLabel, x, targetSqlType);
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        return current().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return current().isWrapperFor(iface);
    }
}
