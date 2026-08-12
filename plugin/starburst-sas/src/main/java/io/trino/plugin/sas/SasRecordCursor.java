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
package io.trino.plugin.sas;

import com.epam.parso.impl.CustomSasFileParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class SasRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(SasRecordCursor.class);

    private final List<SasColumnHandle> columnHandles;
    private CustomSasFileParser reader;
    private final List<SasColumn> sasColumns;
    private Object[] currentRow;
    private final List<String> columnNames;
    private Map<String, Integer> columnNameToIndex;
    private InputStream inputStream;
    private final long start;
    private final long pageCount;
    private final TrinoFileSystem fileSystem;
    private final String file;
    private boolean started;

    public SasRecordCursor(List<SasColumnHandle> columnHandles, String file, long start, long pageCount, SasClient client, TrinoFileSystem fileSystem)
    {
        this.columnHandles = columnHandles;
        this.start = start;
        this.pageCount = pageCount;
        this.fileSystem = fileSystem;
        this.file = file;

        List<SasColumn> fileColumns = client.listColumnsFromFile(file, fileSystem);
        ImmutableList.Builder<SasColumn> sasColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        for (SasColumnHandle columnHandle : columnHandles) {
            SasColumn matched = null;
            for (SasColumn fileColumn : fileColumns) {
                if (fileColumn.name().equalsIgnoreCase(columnHandle.columnName())) {
                    matched = fileColumn;
                    break;
                }
            }
            if (matched == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Column '%s' not found in SAS file: %s".formatted(columnHandle.columnName(), file));
            }
            columnNamesBuilder.add(matched.name());
            sasColumnsBuilder.add(matched);
        }
        columnNames = columnNamesBuilder.build();
        sasColumns = sasColumnsBuilder.build();
    }

    private void advanceToStart()
    {
        try {
            long begin = System.currentTimeMillis();
            inputStream = fileSystem.newInputFile(Location.of(file)).newStream();

            reader = new CustomSasFileParser.Builder(inputStream).build();

            reader.setPageRange(start, start + pageCount);
            long end = System.currentTimeMillis();

            log.debug("Sas record cursor on: %s, start: %d end skip in %ds", file, start, (end - begin) / 1000);
        }
        catch (Exception e) {
            closeStream();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading SAS file: " + file, e);
        }
        started = true;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).columnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!started) {
            advanceToStart();
        }
        if (columnNameToIndex == null && columnNames != null) {
            ImmutableMap.Builder<String, Integer> mapBuilder = ImmutableMap.builder();
            int idx = 0;
            for (String column : columnNames) {
                mapBuilder.put(column, idx);
                idx++;
            }
            columnNameToIndex = mapBuilder.buildOrThrow();
        }

        try {
            currentRow = reader.readNext(columnNames, columnNameToIndex);
        }
        catch (Exception e) {
            close();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading SAS record", e);
        }
        return currentRow != null;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(currentRow[field] != null, "Unexpected null in non-null field %s", field);
        return Boolean.parseBoolean(currentRow[field].toString());
    }

    @Override
    public long getLong(int field)
    {
        checkState(currentRow[field] != null, "Unexpected null in non-null field %s", field);
        if (currentRow[field] instanceof Long) {
            return (Long) currentRow[field];
        }
        if (currentRow[field] instanceof Date && sasColumns.get(field).type() instanceof DateType) {
            return ((Date) currentRow[field]).toInstant()
                    .atZone(ZoneOffset.UTC)
                    .toLocalDate().toEpochDay();
        }
        if (currentRow[field] instanceof Date && sasColumns.get(field).type() instanceof TimestampType) {
            return Math.multiplyExact(((Date) currentRow[field]).toInstant().toEpochMilli(), 1000L);
        }

        return Long.parseLong(currentRow[field].toString());
    }

    @Override
    public double getDouble(int field)
    {
        checkState(currentRow[field] != null, "Unexpected null in non-null field %s", field);
        if (currentRow[field] instanceof Double) {
            return (Double) currentRow[field];
        }
        if (currentRow[field] instanceof Long) {
            return ((Long) currentRow[field]).doubleValue();
        }
        return Double.parseDouble(currentRow[field].toString());
    }

    @Override
    public Slice getSlice(int field)
    {
        return Slices.utf8Slice(currentRow[field].toString());
    }

    @Override
    public Object getObject(int field)
    {
        if (currentRow[field] == null) {
            return null;
        }

        return currentRow[field];
    }

    @Override
    public boolean isNull(int field)
    {
        return currentRow[field] == null || currentRow[field].toString().isEmpty();
    }

    @Override
    public void close()
    {
        closeStream();
    }

    private void closeStream()
    {
        if (inputStream != null) {
            try {
                inputStream.close();
            }
            catch (IOException e) {
                log.warn(e, "Error closing SAS record cursor stream for file: %s", file);
            }
        }
    }
}
