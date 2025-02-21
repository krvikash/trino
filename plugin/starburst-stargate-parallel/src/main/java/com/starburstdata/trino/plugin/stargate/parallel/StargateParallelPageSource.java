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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

public class StargateParallelPageSource
        implements ConnectorPageSource
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
    private final PageBuilder pageBuilder;
    private boolean closed;
    private long completedPositions;

    public StargateParallelPageSource(JdbcClient client, ResultSet resultSet, ConnectorSession session, List<JdbcColumnHandle> columnHandles, long dataSizeBytes)
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
            pageBuilder = new PageBuilder(columnHandles.stream()
                    .map(JdbcColumnHandle::getColumnType)
                    .collect(toImmutableList()));
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
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public Page getNextPage()
    {
        verify(pageBuilder.isEmpty(), "Expected pageBuilder to be empty");
        if (closed) {
            return null;
        }

        long start = System.nanoTime();
        try {
            while (!pageBuilder.isFull() && resultSet.next()) {
                pageBuilder.declarePosition();
                completedPositions++;
                for (int i = 0; i < columnHandles.size(); i++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(i);
                    Type type = columnHandles.get(i).getColumnType();
                    if (readFunctions[i].isNull(resultSet, i + 1)) {
                        output.appendNull();
                    }
                    else if (booleanReadFunctions[i] != null) {
                        type.writeBoolean(output, booleanReadFunctions[i].readBoolean(resultSet, i + 1));
                    }
                    else if (doubleReadFunctions[i] != null) {
                        type.writeDouble(output, doubleReadFunctions[i].readDouble(resultSet, i + 1));
                    }
                    else if (longReadFunctions[i] != null) {
                        type.writeLong(output, longReadFunctions[i].readLong(resultSet, i + 1));
                    }
                    else if (sliceReadFunctions[i] != null) {
                        type.writeSlice(output, sliceReadFunctions[i].readSlice(resultSet, i + 1));
                    }
                    else {
                        type.writeObject(output, objectReadFunctions[i].readObject(resultSet, i + 1));
                    }
                }
            }

            if (!pageBuilder.isFull()) {
                closed = true;
            }
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
        finally {
            readTimeNanos.addAndGet(System.nanoTime() - start);
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }

    @Override
    public long getMemoryUsage()
    {
        return dataSizeBytes + pageBuilder.getRetainedSizeInBytes();
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
}
