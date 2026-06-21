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
package io.trino.plugin.hive.parquet;

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public final class GpuColumnEvolution
{
    private GpuColumnEvolution() {}

    /**
     * Evolve a cuDF column to the expected DType, applying a cast when the Parquet physical type
     * differs from the Trino logical type.
     */
    public static @Move ColumnVector evolveColumn(String columnName, @Borrow ColumnVector cudfColumn, DType expectedDType, Type trinoType)
    {
        DType actualDType = cudfColumn.getType();
        if (actualDType.equals(expectedDType)) {
            return cudfColumn.incRefCount();
        }
        if (actualDType.isTimestampType() && expectedDType.isTimestampType()) {
            // TODO: Handle overflow https://starburstdata.atlassian.net/browse/ENG-18406
            return cudfColumn.castTo(expectedDType);
        }
        if (actualDType.isDecimalType() && expectedDType.isDecimalType()) {
            // TODO: Handle overflow https://starburstdata.atlassian.net/browse/ENG-18406
            return cudfColumn.castTo(expectedDType);
        }
        if (isIntegerType(actualDType) && (isIntegerType(expectedDType) || expectedDType.isDecimalType())
                && expectedDType.getSizeInBytes() >= actualDType.getSizeInBytes()) {
            // TODO: Handle overflow https://starburstdata.atlassian.net/browse/ENG-18406
            return cudfColumn.castTo(expectedDType);
        }
        if (trinoType instanceof VarbinaryType && actualDType.equals(DType.STRING) && expectedDType.equals(DType.LIST)) {
            // cuDF reads Parquet BINARY as STRING; reinterpret the byte payload as LIST<UINT8>.
            // The STRING data buffer becomes the child UINT8 column; offsets and validity carry
            // over unchanged.
            BaseDeviceMemoryBuffer dataBuffer = cudfColumn.getData();
            long childRowCount = dataBuffer == null ? 0 : dataBuffer.getLength();
            try (ColumnView childView = new ColumnView(DType.UINT8, childRowCount, Optional.of(0L), dataBuffer, null);
                    ColumnView listView = new ColumnView(
                            DType.LIST,
                            cudfColumn.getRowCount(),
                            Optional.of(cudfColumn.getNullCount()),
                            cudfColumn.getValid(),
                            cudfColumn.getOffsets(),
                            new ColumnView[] {childView})) {
                return listView.copyToColumnVector();
            }
        }
        throw new TrinoException(
                NOT_SUPPORTED,
                format("Column %s: cannot evolve cuDF type %s to expected type %s",
                        columnName,
                        actualDType,
                        expectedDType));
    }

    private static boolean isIntegerType(DType dtype)
    {
        return dtype == DType.INT8 || dtype == DType.INT16 || dtype == DType.INT32 || dtype == DType.INT64;
    }
}
