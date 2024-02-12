/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.BooleanReadFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DoubleReadFunction;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

public class StargateParallelRecordCursor
        implements RecordCursor
{
    private final List<JdbcColumnHandle> columnHandles;
    private final ReadFunction[] readFunctions;
    private final BooleanReadFunction[] booleanReadFunctions;
    private final DoubleReadFunction[] doubleReadFunctions;
    private final LongReadFunction[] longReadFunctions;
    private final SliceReadFunction[] sliceReadFunctions;
    private final ObjectReadFunction[] objectReadFunctions;
    private final AtomicLong readTimeNanos = new AtomicLong(0);
    private final ResultSet resultSet;
    private final long dataSizeBytes;
    private boolean closed;

    public StargateParallelRecordCursor(JdbcClient client, ResultSet resultSet, ConnectorSession session, List<JdbcColumnHandle> columnHandles, long dataSizeBytes)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.resultSet = requireNonNull(resultSet, "resultSet is null");
        this.dataSizeBytes = dataSizeBytes;

        readFunctions = new ReadFunction[columnHandles.size()];
        booleanReadFunctions = new BooleanReadFunction[columnHandles.size()];
        doubleReadFunctions = new DoubleReadFunction[columnHandles.size()];
        longReadFunctions = new LongReadFunction[columnHandles.size()];
        sliceReadFunctions = new SliceReadFunction[columnHandles.size()];
        objectReadFunctions = new ObjectReadFunction[columnHandles.size()];

        List<ColumnMapping> columnMappings = requireNonNull(client, "client is null")
                .toColumnMappings(session, columnHandles.stream()
                        .map(JdbcColumnHandle::getJdbcTypeHandle)
                        .collect(toImmutableList()));

        try {
            for (int i = 0; i < columnMappings.size(); i++) {
                ColumnMapping columnMapping = columnMappings.get(i);
                Class<?> javaType = columnMapping.getType().getJavaType();
                ReadFunction readFunction = columnMapping.getReadFunction();
                readFunctions[i] = readFunction;

                if (javaType == boolean.class) {
                    booleanReadFunctions[i] = (BooleanReadFunction) readFunction;
                }
                else if (javaType == double.class) {
                    doubleReadFunctions[i] = (DoubleReadFunction) readFunction;
                }
                else if (javaType == long.class) {
                    longReadFunctions[i] = (LongReadFunction) readFunction;
                }
                else if (javaType == Slice.class) {
                    sliceReadFunctions[i] = (SliceReadFunction) readFunction;
                }
                else {
                    objectReadFunctions[i] = (ObjectReadFunction) readFunction;
                }
            }
        }
        catch (RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }

        long start = System.nanoTime();
        try {
            if (!resultSet.next()) {
                closed = true;
                return false;
            }
            return true;
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
        finally {
            readTimeNanos.addAndGet(System.nanoTime() - start);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        requireNonNull(resultSet, "resultSet is null");
        try {
            return booleanReadFunctions[field].readBoolean(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        requireNonNull(resultSet, "resultSet is null");
        try {
            return longReadFunctions[field].readLong(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        requireNonNull(resultSet, "resultSet is null");
        try {
            return doubleReadFunctions[field].readDouble(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        requireNonNull(resultSet, "resultSet is null");
        try {
            return sliceReadFunctions[field].readSlice(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Object getObject(int field)
    {
        checkState(!closed, "cursor is closed");
        requireNonNull(resultSet, "resultSet is null");
        try {
            return objectReadFunctions[field].readObject(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");
        requireNonNull(resultSet, "resultSet is null");

        try {
            return readFunctions[field].isNull(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new TrinoException(JDBC_ERROR, e);
    }

    @Override
    public long getMemoryUsage()
    {
        return dataSizeBytes;
    }
}
