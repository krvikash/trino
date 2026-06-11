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
package io.trino.plugin.base.gpu;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Table;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import static java.lang.Math.toIntExact;

public final class GpuUtils
{
    private GpuUtils() {}

    /**
     * Closes all non-null columns in the array. Elements may be null if the array was partially populated.
     */
    public static void closeColumns(@Move Column[] columns)
    {
        try (var closer = UncheckedCloser.create()) {
            for (Column column : columns) {
                if (column != null) {
                    closer.register(column);
                }
            }
        }
    }

    public static @Move GpuPage toGpuPage(@Borrow Table table)
    {
        int rowCount = toIntExact(table.getRowCount());

        @Own Column[] columns = new Column[table.getNumberOfColumns()];
        try {
            for (int i = 0; i < columns.length; i++) {
                // TODO we could perhaps cheat here and skip incRefCount and then skip closeColumns too
                columns[i] = new Column.DeviceMemory(table.getColumn(i).incRefCount());
            }
            return new GpuPage(rowCount, columns);
        }
        finally {
            closeColumns(columns);
        }
    }

    public static @Move Table toTable(@Borrow GpuPage page)
    {
        @Borrow ColumnVector[] columns = new ColumnVector[page.columnCount()];
        for (int i = 0; i < page.columnCount(); i++) {
            columns[i] = ((Column.DeviceMemory) page.column(i)).columnVector();
        }
        return new Table(columns);
    }

    public static @Move Table toTable(@Borrow GpuPage page, int... selectedChannels)
    {
        ColumnVector[] selected = new ColumnVector[selectedChannels.length];
        for (int i = 0; i < selectedChannels.length; i++) {
            selected[i] = ((Column.DeviceMemory) page.column(selectedChannels[i])).columnVector();
        }
        return new Table(selected);
    }
}
