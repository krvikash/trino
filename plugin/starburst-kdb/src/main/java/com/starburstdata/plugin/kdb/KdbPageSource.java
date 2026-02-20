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

import com.google.common.collect.ImmutableList;
import com.starburstdata.plugin.kdb.KdbClient.KdbQueryResult;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class KdbPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(KdbPageSource.class);
    private static final int ROWS_PER_PAGE = 4096;

    private final KdbClient client;
    private final SchemaTableName schemaTableName;
    private final List<KdbColumnHandle> columns;

    private KdbQueryResult queryResult;
    private Map<String, Integer> columnIndexMap;
    private int currentRow;
    private long completedBytes;
    private long readTimeNanos;
    private boolean finished;

    public KdbPageSource(
            KdbClient client,
            SchemaTableName schemaTableName,
            List<KdbColumnHandle> columns)
    {
        this.client = requireNonNull(client, "client is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = ImmutableList.copyOf(columns);
    }

    private void loadData()
    {
        if (queryResult != null || finished) {
            return;
        }

        try {
            log.debug("Loading data for table: %s", schemaTableName);
            long start = System.nanoTime();
            queryResult = client.fetchData(schemaTableName, columns);
            readTimeNanos = System.nanoTime() - start;
            log.debug("Loaded %d rows in %d ms", queryResult.rowCount(), readTimeNanos / 1_000_000);

            String[] names = queryResult.columnNames();
            columnIndexMap = new HashMap<>(names.length);
            for (int i = 0; i < names.length; i++) {
                columnIndexMap.put(names[i], i);
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_USER_ERROR, "Failed to fetch data from KDB: %s".formatted(e.getMessage()), e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        loadData();

        if (queryResult == null) {
            finished = true;
            return null;
        }

        int totalRows = queryResult.rowCount();
        if (currentRow >= totalRows) {
            finished = true;
            return null;
        }

        int rowsToRead = Math.min(ROWS_PER_PAGE, totalRows - currentRow);
        int endRow = currentRow + rowsToRead;

        // Handle empty columns case (e.g., SELECT COUNT(*) FROM table)
        if (columns.isEmpty()) {
            currentRow = endRow;
            return SourcePage.create(rowsToRead);
        }

        BlockBuilder[] builders = new BlockBuilder[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).columnType();
            builders[i] = type.createBlockBuilder(null, rowsToRead);
        }

        for (int row = currentRow; row < endRow; row++) {
            for (int col = 0; col < columns.size(); col++) {
                KdbColumnHandle column = columns.get(col);
                Object value = getValue(column, row);
                writeValue(builders[col], column, value);
                completedBytes += estimateValueSize(column.kdbType());
            }
        }

        currentRow = endRow;

        Block[] blocks = new Block[builders.length];
        for (int i = 0; i < builders.length; i++) {
            blocks[i] = builders[i].build();
        }

        return SourcePage.create(new Page(blocks));
    }

    private Object getValue(KdbColumnHandle column, int row)
    {
        Integer columnIndex = columnIndexMap.get(column.columnName());
        if (columnIndex == null) {
            return null;
        }
        return queryResult.value(columnIndex, row);
    }

    private static void writeValue(BlockBuilder builder, KdbColumnHandle column, Object value)
    {
        ColumnMapping columnMapping = KdbTypeMapping.toColumnMapping(column.kdbType()).orElseThrow();
        ReadFunction readFunction = columnMapping.readFunction();
        if (value == null || readFunction.isNull(value)) {
            builder.appendNull();
            return;
        }

        Object trinoValue = readFunction.toTrinoValue(value);
        Type type = column.columnType();
        Class<?> javaType = type.getJavaType();

        if (javaType == boolean.class) {
            type.writeBoolean(builder, (Boolean) trinoValue);
        }
        else if (javaType == long.class) {
            type.writeLong(builder, ((Number) trinoValue).longValue());
        }
        else if (javaType == double.class) {
            type.writeDouble(builder, ((Number) trinoValue).doubleValue());
        }
        else if (javaType == Slice.class) {
            type.writeSlice(builder, (Slice) trinoValue);
        }
        else {
            type.writeObject(builder, trinoValue);
        }
    }

    private static long estimateValueSize(char kdbType)
    {
        return KdbTypeMapping.toColumnMapping(kdbType)
                .map(ColumnMapping::estimatedSize)
                .orElse((long) Long.BYTES);
    }

    @Override
    public void close()
    {
        finished = true;
        queryResult = null;
    }
}
