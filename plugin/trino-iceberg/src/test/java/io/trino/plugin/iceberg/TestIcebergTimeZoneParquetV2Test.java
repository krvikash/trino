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
package io.trino.plugin.iceberg;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.variants.Variant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.writeParquetDataToIcebergTable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergTimeZoneParquetV2Test
        extends BaseIcebergTimeZoneTest
{
    public TestIcebergTimeZoneParquetV2Test()
    {
        super(PARQUET);
    }

    @Override
    protected int formatVersion()
    {
        return 2;
    }

    @Override
    @ParameterizedTest
    @MethodSource("nanoSecondsTimestampPrecision")
    void testSelectNanoSecondsTimestampTz(int precision)
    {
        // with format-version = 2, the timestamp with time zone will always be in microseconds
        super.testSelectMicrosecondsTimestampTz(precision);
    }

    @Override
    @Test
    void testWriteDefaultValue()
    {
        assertThatThrownBy(super::testWriteDefaultValue)
                .hasMessageContaining("Default column values are not supported for Iceberg table format version < 3");
    }

    @Override
    @Test
    void testInitialDefaultValue()
    {
        assertThatThrownBy(super::testInitialDefaultValue)
                .hasMessageContaining("Invalid schema for v2");
    }

    @Override
    protected void writeVariantDataToIcebergTable(String outputFilePath, DataFiles.Builder fileBuilder, Table table, Variant... variantValues)
            throws IOException
    {
        writeParquetDataToIcebergTable(outputFilePath, fileBuilder, table, variantValues);
    }

    @Override
    @Test
    void testSelectTimestampTzVariantType()
    {
        assertThatThrownBy(super::testSelectTimestampTzVariantType)
                .hasMessageContaining("Invalid type for var: variant is not supported until v3");
    }
}
