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

import java.io.IOException;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.writeOrcDataToIcebergTable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergTimeZoneOrcTest
        extends BaseIcebergTimeZoneTest
{
    public TestIcebergTimeZoneOrcTest()
    {
        super(ORC);
    }

    @Override
    protected void writeVariantDataToIcebergTable(String outputFilePath, DataFiles.Builder fileBuilder, Table table, Variant... variantValues)
            throws IOException
    {
        writeOrcDataToIcebergTable(outputFilePath, variantValues[0], fileBuilder, table);
    }

    @Override
    @Test
    void testSelectTimestampTzVariantType()
            throws IOException
    {
        assertThatThrownBy(super::testSelectTimestampTzVariantType)
                .hasMessageContaining("Cannot read SQL type 'json' from ORC stream '.var' of type STRUCT with attributes");
    }
}
