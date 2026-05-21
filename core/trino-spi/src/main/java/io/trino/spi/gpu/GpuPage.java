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
package io.trino.spi.gpu;

import io.trino.spi.Unstable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static io.trino.spi.gpu.Preconditions.checkArgument;
import static io.trino.spi.gpu.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * Data spanning CPU and GPU memory.
 * Owns its columns.
 */
public final class GpuPage
        implements RuntimeCloseable
{
    private final int positionCount;
    private final Column[] columns;
    private boolean closed;

    public GpuPage(int positionCount, @Borrow Column[] columns)
    {
        checkArgument(0 <= positionCount, "Invalid position count: %s", positionCount);
        for (Column column : columns) {
            checkArgument(column.positionCount() == positionCount, "Invalid column position count: %s != %s", column.positionCount(), positionCount);
        }
        this.positionCount = positionCount;

        @Own Column[] ownedColumns = new Column[columns.length];
        try {
            for (int i = 0; i < columns.length; i++) {
                ownedColumns[i] = columns[i].incRefCount();
            }
            this.columns = ownedColumns;
        }
        catch (Throwable e) {
            for (Column column : ownedColumns) {
                try {
                    if (column != null) {
                        column.close();
                    }
                }
                catch (Throwable closeException) {
                    if (e != closeException) {
                        e.addSuppressed(closeException);
                    }
                }
            }
            throw e;
        }
    }

    public int positionCount()
    {
        return positionCount;
    }

    public int columnCount()
    {
        return columns.length;
    }

    public List<@Borrow Column> columns()
    {
        checkState(!closed, "Already closed");
        return unmodifiableList(asList(columns));
    }

    public @Borrow Column column(int index)
    {
        checkState(!closed, "Already closed");
        return columns[index];
    }

    // TODO is this API right name?
    @Unstable
    @Move
    public GpuPage shallowCopy()
    {
        checkState(!closed, "Already closed");
        return new GpuPage(positionCount, columns);
    }

    @Override
    public void close()
    {
        closed = true;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                columns[i].close();
                columns[i] = null;
            }
        }
    }
}
