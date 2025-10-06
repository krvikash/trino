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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.writeParquetDataToIcebergTable;
import static org.apache.iceberg.variants.VariantTestUtil.createMetadata;
import static org.apache.iceberg.variants.VariantTestUtil.createObject;
import static org.apache.iceberg.variants.Variants.metadata;
import static org.apache.iceberg.variants.Variants.ofIsoTimestamptz;
import static org.apache.iceberg.variants.Variants.value;

public class TestIcebergTimeZoneParquetV3Test
        extends BaseIcebergTimeZoneTest
{
    private static final ByteBuffer TEST_METADATA_BUFFER = createMetadata(ImmutableList.of("a", "b", "c", "d", "e"), true);
    private static final ByteBuffer TEST_OBJECT_BUFFER = createObject(
            TEST_METADATA_BUFFER,
            ImmutableMap.<String, VariantValue>builder()
                    .put("a", Variants.ofNull())
                    .put("d", ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00"))
                    .buildOrThrow());
    private static final ByteBuffer ARRAY_IN_OBJECT_BUFFER =
            VariantTestUtil.createObject(
                    TEST_METADATA_BUFFER,
                    ImmutableMap.of(
                            "a", Variants.ofNull(),
                            "d", array(Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00"), Variants.ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00"))));
    private static final ByteBuffer EMPTY_OBJECT_BUFFER = createObject(TEST_METADATA_BUFFER, ImmutableMap.of());
    private static final VariantMetadata TEST_METADATA = metadata(TEST_METADATA_BUFFER);
    private static final VariantObject TEST_OBJECT = (VariantObject) value(TEST_METADATA, TEST_OBJECT_BUFFER);
    private static final VariantObject EMPTY_OBJECT = (VariantObject) value(TEST_METADATA, EMPTY_OBJECT_BUFFER);
    private static final VariantObject ARRAY_IN_OBJECT = (VariantObject) Variants.value(TEST_METADATA, ARRAY_IN_OBJECT_BUFFER);

    public TestIcebergTimeZoneParquetV3Test()
    {
        super(PARQUET);
    }

    @Test
    @Override
    void testSelectTimestampTzVariantType()
            throws Exception
    {
        // Currently, the iceberg.time-zone is not used by variant type mappings and all output will be in UTC time zone.
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00")), "JSON '\"2024-11-07 12:33:54.123456+00:00\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00")), "JSON '\"1957-11-07 12:33:54.123456+00:00\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, ofIsoTimestamptz("1957-11-07T12:33:54.123456-05:00")), "JSON '\"1957-11-07 17:33:54.123456+00:00\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, ofIsoTimestamptz("1957-11-07T12:33:54.123456+09:00")), "JSON '\"1957-11-07 03:33:54.123456+00:00\"'");

        testVariantTypeMappings(Variant.of(EMPTY_METADATA, EMPTY_OBJECT), "JSON '{}'");
        testVariantTypeMappings(Variant.of(TEST_METADATA, TEST_OBJECT), "JSON '{\"a\":null, \"d\":\"2024-11-07 12:33:54.123456+00:00\"}'");

        testVariantTypeMappings(Variant.of(TEST_METADATA, ARRAY_IN_OBJECT), "JSON '{\"a\":null, \"d\":[\"2024-11-07 12:33:54.123456+00:00\", \"1957-11-07 12:33:54.123456+00:00\"]}'");

        // when the timestamp is written as text, it is not converted to the time zone
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of("1957-11-07T12:33:54.123456+05:30")), "JSON '\"1957-11-07T12:33:54.123456+05:30\"'");
    }

    @Override
    protected void writeVariantDataToIcebergTable(String outputFilePath, DataFiles.Builder fileBuilder, Table table, Variant... variantValues)
            throws IOException
    {
        writeParquetDataToIcebergTable(outputFilePath, fileBuilder, table, variantValues);
    }

    private static ValueArray array(VariantValue... values)
    {
        ValueArray arr = Variants.array();
        for (VariantValue value : values) {
            arr.add(value);
        }
        return arr;
    }
}
