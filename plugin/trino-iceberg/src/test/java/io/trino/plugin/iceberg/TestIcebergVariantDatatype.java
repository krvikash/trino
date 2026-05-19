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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.VARIANT;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.INT_COL_NAME;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.SCHEMA;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.VARIANT_COL_NAME;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.addVariantColumn;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.writeAvroDataToIcebergTable;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.writeOrcDataToIcebergTable;
import static io.trino.plugin.iceberg.IcebergVariantTypeUtil.writeParquetDataToIcebergTable;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.variants.VariantTestUtil.createMetadata;
import static org.apache.iceberg.variants.VariantTestUtil.createObject;
import static org.apache.iceberg.variants.Variants.metadata;
import static org.apache.iceberg.variants.Variants.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergVariantDatatype
        extends AbstractTestQueryFramework
{
    private static final ColumnIdentity INT_COLUMN_IDENTITY = new ColumnIdentity(1, INT_COL_NAME, PRIMITIVE, ImmutableList.of());
    private static final ColumnIdentity VARIANT_COLUMN_IDENTITY = new ColumnIdentity(2, VARIANT_COL_NAME, VARIANT, ImmutableList.of());

    private static final ByteBuffer TEST_METADATA_BUFFER = createMetadata(ImmutableList.of("a", "b", "c", "d", "e"), true);
    private static final ByteBuffer TEST_OBJECT_BUFFER = createObject(
            TEST_METADATA_BUFFER,
            ImmutableMap.<String, VariantValue>builder()
                    .put("a", Variants.ofNull())
                    .put("d", Variants.of("trino"))
                    .buildOrThrow());
    private static final ByteBuffer SIMILAR_OBJECT_BUFFER = createObject(
            TEST_METADATA_BUFFER,
            ImmutableMap.<String, VariantValue>builder()
                    .put("a", Variants.of(123456789))
                    .put("c", Variants.of("string"))
                    .buildOrThrow());
    private static final ByteBuffer EMPTY_OBJECT_BUFFER = createObject(TEST_METADATA_BUFFER, ImmutableMap.of());

    private static final VariantMetadata TEST_METADATA = metadata(TEST_METADATA_BUFFER);
    private static final VariantMetadata EMPTY_METADATA = metadata(VariantTestUtil.emptyMetadata());
    private static final VariantObject TEST_OBJECT = (VariantObject) value(TEST_METADATA, TEST_OBJECT_BUFFER);
    private static final VariantObject SIMILAR_OBJECT = (VariantObject) value(TEST_METADATA, SIMILAR_OBJECT_BUFFER);
    private static final VariantObject EMPTY_OBJECT = (VariantObject) value(TEST_METADATA, EMPTY_OBJECT_BUFFER);

    private HiveMetastore metastore;
    private File metastoreDir;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        metastoreDir = Files.createTempDirectory("test_iceberg_variant_").toFile();
        closeAfterClass(() -> deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE));
        metastore = createTestingFileHiveMetastore(HDFS_FILE_SYSTEM_FACTORY, Location.of(metastoreDir.getAbsolutePath()));
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.legacy-variant-type-mapping", "JSON")
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.format-version", "3")
                        .put("hive.metastore.catalog.dir", metastoreDir.getPath())
                        .buildOrThrow())
                .setMetastoreDirectory(metastoreDir)
                .build();
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
    }

    @Test
    void testVariantTypeMappings()
            throws Exception
    {
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofNull()), "JSON 'null'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(true)), "JSON 'true'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(false)), "JSON 'false'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of((byte) 34)), "CAST (34 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of((byte) -34)), "CAST (-34 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of((short) 1234)), "CAST (1234 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of((short) -1234)), "CAST (-1234 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(12345)), "CAST (12345 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(-12345)), "CAST (-12345 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(9876543210L)), "CAST (9876543210 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(-9876543210L)), "CAST (-9876543210 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(10.11f)), "CAST (10.11 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(-10.11f)), "CAST (-10.11 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(14.3d)), "CAST (14.3 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(-14.3d)), "CAST (-14.3 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, EMPTY_OBJECT), "JSON '{}'");
        testVariantTypeMappings(Variant.of(TEST_METADATA, TEST_OBJECT), "JSON '{\"a\":null,\"d\":\"trino\"}'");
        testVariantTypeMappings(Variant.of(TEST_METADATA, SIMILAR_OBJECT), "JSON '{\"a\":123456789,\"c\":\"string\"}'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofIsoDate("2024-11-07")), "JSON '\"2024-11-07\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofIsoDate("1957-11-07")), "JSON '\"1957-11-07\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00")), "JSON '\"2024-11-07 12:33:54.123456+00:00\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00")), "JSON '\"1957-11-07 12:33:54.123456+00:00\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456")), "JSON '\"2024-11-07 12:33:54.123456\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.ofIsoTimestampntz("1957-11-07T12:33:54.123456")), "JSON '\"1957-11-07 12:33:54.123456\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("123456.789"))), "CAST (123456.789 AS JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("-123456.789"))), "CAST (-123456.789 AS JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(ByteBuffer.wrap(new byte[] {0x0A, 0x0B, 0x0C, 0x0D}))), "JSON '\"CgsMDQ==\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of("trino")), "JSON '\"trino\"'");
    }

    private void testVariantTypeMappings(Variant variantData, @Language("SQL") String expectedVariant)
            throws Exception
    {
        // Iceberg writes and Trino reads
        String tableName = "test_variant_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");
        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table,
                variantData);
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, %s)".formatted(expectedVariant));
        assertThat(query("SELECT id FROM " + tableName + " WHERE var = " + expectedVariant)).matches("VALUES 1");
        assertUpdate("DROP TABLE " + tableName);

        // Trino writes and reads
        try (TestTable testTable = newTrinoTable("test_variant", "(id int, var json) WITH (format = 'PARQUET', format_version = 3)")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, " + expectedVariant + ")", 1);
            assertThat(query("SELECT count(*) FROM \"" + testTable.getName() + "$files\"")).matches("VALUES CAST(1 AS BIGINT)");
            assertThat(query("SELECT * FROM " + testTable.getName())).matches("VALUES (1, %s)".formatted(expectedVariant));
            assertThat(query("SELECT id FROM " + testTable.getName() + " WHERE var = " + expectedVariant)).matches("VALUES 1");
            assertThat(query("SELECT id FROM " + testTable.getName() + " WHERE var != " + expectedVariant)).returnsEmptyResult();
        }
    }

    @Test
    void testVariantArray()
    {
        try (TestTable table = newTrinoTable(
                "test_variant_array",
                "(id int, variant JSON)",
                List.of("1, JSON '[1, 2, 3]'", "2, JSON '[]'", "3, JSON '[\"a\", \"b\", \"c\"]'", "4, JSON '[null, 1, \"test\"]'"))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, JSON '[1,2,3]'), (2, JSON '[]'), (3, JSON '[\"a\",\"b\",\"c\"]'), (4, JSON '[null,1,\"test\"]')");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant = JSON '[1,2,3]'"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant = JSON '[]'"))
                    .matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant != JSON '[]'"))
                    .matches("VALUES 1, 3, 4");
        }
    }

    @Test
    void testVariantMap()
    {
        try (TestTable table = newTrinoTable("test_variant_map", "(id int, variant JSON)",
                List.of("1, JSON '{\"key1\": \"value1\", \"key2\": \"value2\"}'",
                        "2, JSON '{}'",
                        "3, JSON '{\"nested\": {\"a\": 1, \"b\": 2}}'",
                        "4, JSON '{\"mixed\": [1, 2, 3], \"value\": null}'",
                        "5, JSON 'null'"))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " +
                            "(1, JSON '{\"key1\":\"value1\",\"key2\":\"value2\"}'), " +
                            "(2, JSON '{}'), " +
                            "(3, JSON '{\"nested\":{\"a\":1,\"b\":2}}'), " +
                            "(4, JSON '{\"mixed\":[1,2,3],\"value\":null}')," +
                            "(5, JSON 'null')");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant = JSON '{}'"))
                    .matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant != JSON '{}'"))
                    .matches("VALUES 1, 3, 4, 5");
        }
    }

    @Test
    void testVariantRow()
    {
        try (TestTable table = newTrinoTable("test_variant_row", "(id int, variant JSON)",
                List.of("1, JSON '{\"field1\": 123, \"field2\": \"text\"}'",
                        "2, JSON '{\"field1\": null, \"field2\": null}'",
                        "3, JSON '{\"field1\": 456, \"field2\": \"another\", \"field3\": true}'",
                        "4, JSON '{\"nested_row\": {\"inner1\": 1, \"inner2\": \"value\"}}'"))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " +
                            "(1, JSON '{\"field1\":123,\"field2\":\"text\"}'), " +
                            "(2, JSON '{\"field1\":null,\"field2\":null}'), " +
                            "(3, JSON '{\"field1\":456,\"field2\":\"another\",\"field3\":true}'), " +
                            "(4, JSON '{\"nested_row\":{\"inner1\":1,\"inner2\":\"value\"}}')");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant = JSON '{\"field1\":123,\"field2\":\"text\"}'"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant IS NOT NULL"))
                    .matches("VALUES 1, 2, 3, 4");
        }
    }

    @Test
    void testVariantComplexNested()
    {
        try (TestTable table = newTrinoTable("test_variant_complex", "(id int, variant JSON)",
                List.of("1, JSON '{\"array_of_maps\": [{\"id\": 1}, {\"id\": 2}]}'",
                        "2, JSON '{\"map_of_arrays\": {\"list1\": [1, 2], \"list2\": [3, 4]}}'",
                        "3, JSON '{\"deeply_nested\": {\"level1\": {\"level2\": {\"value\": 42}}}}'",
                        "4, JSON '{\"mixed\": {\"arr\": [1, {\"key\": \"val\"}, null], \"num\": 123}}'"))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " +
                            "(1, JSON '{\"array_of_maps\":[{\"id\":1},{\"id\":2}]}'), " +
                            "(2, JSON '{\"map_of_arrays\":{\"list1\":[1,2],\"list2\":[3,4]}}'), " +
                            "(3, JSON '{\"deeply_nested\":{\"level1\":{\"level2\":{\"value\":42}}}}'), " +
                            "(4, JSON '{\"mixed\":{\"arr\":[1,{\"key\":\"val\"},null],\"num\":123}}')");
            assertThat(query("SELECT count(*) FROM " + table.getName() + " WHERE variant IS NOT NULL"))
                    .matches("VALUES CAST(4 AS BIGINT)");
        }
    }

    @Test
    void testVariantInRowType()
    {
        try (TestTable table = newTrinoTable("test_row_with_variant", "(id int, row_col ROW(name VARCHAR, data JSON))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES " +
                    "(1, ROW('test', JSON '{\"info\": \"value\"}')), " +
                    "(2, ROW('null_data', NULL))", 2);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " +
                            "(1, CAST(ROW('test', JSON '{\"info\":\"value\"}') AS ROW(name VARCHAR, data JSON))), " +
                            "(2, CAST(ROW(VARCHAR 'null_data', NULL) AS ROW(name VARCHAR, data JSON)))");

            assertThat(query("SELECT row_col.data FROM " + table.getName() + " WHERE row_col.name = 'test'"))
                    .matches("VALUES JSON '{\"info\":\"value\"}'");
        }
    }

    @Test
    void testVariantInNestedComplexType()
    {
        try (TestTable table = newTrinoTable("test_nested_complex", "(id int, nested ARRAY(ROW(id INT, variant JSON)))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES " +
                    "(1, ARRAY[ROW(1, JSON '{\"a\": 1}'), ROW(2, JSON 'null')]), " +
                    "(2, ARRAY[ROW(3, JSON '[]')])", 2);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " +
                            "(1, ARRAY[CAST(ROW(1, JSON '{\"a\":1}') AS ROW(id INT, variant JSON)), CAST(ROW(2, JSON 'null') AS ROW(id INT, variant JSON))]), " +
                            "(2, ARRAY[CAST(ROW(3, JSON '[]') AS ROW(id INT, variant JSON))])");
        }
    }

    @Test
    void testVariantInArrayType()
    {
        try (TestTable table = newTrinoTable("test_array_of_variant", "(id int, arr ARRAY(JSON))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES " +
                    "(1, ARRAY[JSON '{\"a\": 1}', JSON '123']),\n" +
                    "(2, ARRAY[JSON '[]', JSON '{}']),\n" +
                    "(3, ARRAY[JSON '[\"a\", \"b\", \"c\"]']),\n" +
                    "(4, ARRAY[JSON '[null, 1, \"test\"]'])", 4);

            assertThat(query("SELECT * FROM " + table.getName() + " WHERE id = 1"))
                    .matches("VALUES (1, ARRAY[JSON '{\"a\":1}', JSON '123'])");

            assertThat(query("SELECT * FROM " + table.getName() + " WHERE id = 2"))
                    .matches("VALUES (2, ARRAY[JSON '[]', JSON '{}'])");

            assertThat(query("SELECT * FROM " + table.getName() + " WHERE id = 3"))
                    .matches("VALUES (3, ARRAY[JSON '[\"a\",\"b\",\"c\"]'])");

            assertThat(query("SELECT * FROM " + table.getName() + " WHERE id = 4"))
                    .matches("VALUES (4, ARRAY[JSON '[null,1,\"test\"]'])");

            assertThat(query("SELECT id FROM " + table.getName() + " WHERE contains(arr, JSON '{\"a\":1}')"))
                    .matches("VALUES 1");
        }
    }

    @Test
    void testVariantInMapType()
    {
        try (TestTable table = newTrinoTable("test_map_of_variant", "(id int, map_col MAP(VARCHAR, JSON))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES " +
                    "(1, MAP(ARRAY['key1', 'key2'], ARRAY[JSON '{\"value\": 1}', JSON 'null'])), " +
                    "(2, MAP(ARRAY['a'], ARRAY[JSON '[]']))", 2);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " +
                            "(1, MAP(ARRAY[VARCHAR 'key1', VARCHAR 'key2'], ARRAY[JSON '{\"value\":1}', JSON 'null'])), " +
                            "(2, MAP(ARRAY[VARCHAR 'a'], ARRAY[JSON '[]']))");

            assertThat(query("SELECT map_col['key1'] FROM " + table.getName() + " WHERE id = 1"))
                    .matches("VALUES JSON '{\"value\":1}'");
        }
    }

    @Test
    void testVariantNull()
    {
        try (TestTable table = newTrinoTable("test_variant_null_", "(id int, variant JSON, var_map MAP(VARCHAR, JSON))", List.of("1, JSON 'null', NULL", "2, NULL, NULL", "3, JSON '{\"id\":3}', NULL", "4, NULL, NULL"))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (1, JSON 'null', NULL), (2, NULL, NULL),  (3, JSON '{\"id\":3}', NULL), (4, NULL, NULL)");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant = JSON 'null'"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant IS NOT NULL"))
                    .matches("VALUES 1, 3");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE variant IS NULL"))
                    .matches("VALUES 2, 4");
        }
    }

    @Test
    void testVariantShowStats()
    {
        try (TestTable table = newTrinoTable("test_variant", "(variant JSON)", List.of("JSON '{\"id\":1}'", "JSON '{\"id\":2}'"))) {
            assertThat(query("SHOW STATS FOR " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "('variant', null, 2e0, 0e0, NULL, NULL, NULL), " +
                            "(NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");

            assertThat(query("SHOW STATS FOR (SELECT * FROM " + table.getName() + " WHERE variant = JSON '{\"id\":1}')"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "('variant', null, 1e0, 0e0, NULL, NULL, NULL), " +
                            "(NULL, NULL, NULL, NULL, 1e0, NULL, NULL)");

            assertUpdate("ANALYZE " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (JSON '{\"id\":3}')", 1);
            assertThat(query("SHOW STATS FOR " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "('variant', null, 3e0, 0e0, NULL, NULL, NULL), " +
                            "(NULL, NULL, NULL, NULL, 3e0, NULL, NULL)");
        }
    }

    @Test
    void testVariantOptimize()
    {
        try (TestTable table = newTrinoTable("test_variant_optimize", "(id int, variant JSON)", List.of("1, JSON 'null'", "2, NULL"))) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, JSON '{\"id\":3}')", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize");

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, JSON 'null'), (2, NULL), (3, JSON '{\"id\":3}')");
        }
    }

    @Test
    void testRowTypeWithMetadataValueFields()
    {
        try (TestTable table = newTrinoTable("test_partition", "(x row(metadata varbinary, value varbinary))")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT row(x'12', x'34')", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(row(x'12', x'34') AS row(metadata varbinary, value varbinary))");
        }
    }

    @Test
    void testVariantTypePartitionFails()
            throws Exception
    {
        String tableName = "test_variant_partitioned_" + randomNameSuffix();

        assertQueryFails("CREATE TABLE " + tableName + "(x JSON) WITH (partitioning = ARRAY['x'])", ".*Cannot partition by non-primitive source field.*");
        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .identity(VARIANT_COLUMN_IDENTITY.getName())
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        PartitionSpec validPartitionSpec = PartitionSpec.builderFor(SCHEMA)
                .identity(INT_COLUMN_IDENTITY.getName())
                .build();

        assertUpdate(format("CREATE TABLE %s (%s int) WITH (format = 'PARQUET', format_version = 3, partitioning = ARRAY['%s'])", tableName, INT_COL_NAME, INT_COL_NAME));
        Table table = loadTable(tableName);
        addVariantColumn(table);

        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-part-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(validPartitionSpec)
                        .withPartition(PartitionData.fromJson("{\"partitionValues\":[\"1\"]}", new Type[] {Types.IntegerType.get()})),
                table,
                Variant.of(TEST_METADATA, TEST_OBJECT));
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, JSON '{\"a\":null,\"d\":\"trino\"}')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantTypePartitionTransformsFail()
            throws Exception
    {
        String tableName = "test_variant_partition_transforms_" + randomNameSuffix();

        // Test that all transforms fail with variant columns
        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .identity(VARIANT_COLUMN_IDENTITY.getName())
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .bucket(VARIANT_COLUMN_IDENTITY.getName(), 10)
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .truncate(VARIANT_COLUMN_IDENTITY.getName(), 10)
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .year(VARIANT_COLUMN_IDENTITY.getName())
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .month(VARIANT_COLUMN_IDENTITY.getName())
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .day(VARIANT_COLUMN_IDENTITY.getName())
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA)
                .hour(VARIANT_COLUMN_IDENTITY.getName())
                .build())
                .hasMessageContaining("Cannot partition by non-primitive source field: variant");

        PartitionSpec validPartitionSpec = PartitionSpec.builderFor(SCHEMA)
                .bucket(INT_COLUMN_IDENTITY.getName(), 2)
                .build();

        assertUpdate(format(
                "CREATE TABLE %s (%s int) WITH (format = 'PARQUET', format_version = 3, partitioning = ARRAY['bucket(%s, 2)'])",
                tableName,
                INT_COL_NAME,
                INT_COL_NAME));

        Table table = loadTable(tableName);
        addVariantColumn(table);

        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-bucket-part-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(validPartitionSpec)
                        .withPartition(PartitionData.fromJson("{\"partitionValues\":[\"1\"]}", new Type[] {Types.IntegerType.get()})),
                table,
                Variant.of(TEST_METADATA, TEST_OBJECT));
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, JSON '{\"a\":1,\"b\":2}'), (3, JSON 'null')", 2);
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, JSON '{\"a\":null,\"d\":\"trino\"}'), (2, JSON '{\"a\":1,\"b\":2}'), (3, JSON 'null')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSetVariantPartitionFails()
    {
        try (TestTable table = newTrinoTable("test_partition", "(id int, part json)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['part']",
                    "Unable to parse partitioning value: Cannot partition by non-primitive source field: variant");
        }
    }

    @Test
    void testAlterCommandFailsWhenVariantTypeIsAddedAsPartitionColumn()
            throws Exception
    {
        String tableName = "test_variant_alter_partition_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");

        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-alter-col-partition-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table,
                Variant.of(TEST_METADATA, TEST_OBJECT));
        assertThat(query("ALTER TABLE %s SET PROPERTIES partitioning = ARRAY['%s']".formatted(tableName, VARIANT_COL_NAME)))
                .failure().hasMessageContaining("Unable to parse partitioning value: Cannot partition by non-primitive source field: variant");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantTypeWithMultipleRows()
            throws Exception
    {
        String tableName = "test_variant_multiple_rows_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");

        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-multiple-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table,
                Variant.of(TEST_METADATA, TEST_OBJECT),
                Variant.of(TEST_METADATA, SIMILAR_OBJECT),
                Variant.of(TEST_METADATA, EMPTY_OBJECT));
        assertThat(query("SELECT count(*) FROM \"" + tableName + "$files\"")).matches("VALUES CAST(1 AS BIGINT)");
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES " +
                "(1, JSON '{\"a\":null,\"d\":\"trino\"}')," +
                "(1, JSON '{\"a\":123456789,\"c\":\"string\"}')," +
                "(1, JSON '{}')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantWrite()
            throws Exception
    {
        String tableName = "test_variant_write_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");

        // multiple files to kick the optimize procedure
        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-write-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table,
                Variant.of(TEST_METADATA, TEST_OBJECT));
        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-write-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table,
                Variant.of(TEST_METADATA, SIMILAR_OBJECT));
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES " +
                "(1, JSON '{\"a\":null,\"d\":\"trino\"}')," +
                "(1, JSON '{\"a\":123456789,\"c\":\"string\"}')");
        assertThat(query("SELECT count(*) FROM \"" + tableName + "$files\"")).matches("VALUES CAST(2 AS BIGINT)");

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES " +
                "(1, JSON '{\"a\":null,\"d\":\"trino\"}')," +
                "(1, JSON '{\"a\":123456789,\"c\":\"string\"}')");

        assertUpdate("INSERT INTO " + tableName + " VALUES (2, JSON '{\"a\":null,\"d\":\"trino\"}')", 1);
        assertThat(query("SELECT count(*) FROM \"" + tableName + "$files\"")).matches("VALUES CAST(3 AS BIGINT)");
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES " +
                "(1, JSON '{\"a\":null,\"d\":\"trino\"}')," +
                "(1, JSON '{\"a\":123456789,\"c\":\"string\"}')," +
                "(2, JSON '{\"a\":null,\"d\":\"trino\"}')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantTypeWithOrcFormatFails()
            throws Exception
    {
        String tableName = "test_variant_orc_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "ORC");

        writeOrcDataToIcebergTable(
                metastoreDir + "/variant-part-%s.orc".formatted(randomNameSuffix()),
                Variant.of(TEST_METADATA, TEST_OBJECT),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table);

        assertThat(query("SELECT * FROM " + tableName))
                .failure().hasMessageContaining("Cannot read SQL type 'json' from ORC stream '.var' of type STRUCT with attributes");

        assertThat(query(("INSERT INTO " + tableName + " VALUES (2, JSON '{\"a\":null,\"d\":\"trino\"}')")))
                .nonTrinoExceptionFailure().hasMessageContaining("io.trino.type.JsonType cannot be cast to class io.trino.spi.type.RowType");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantTypeWithAvroFormatFails()
            throws Exception
    {
        String tableName = "test_variant_avro_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "AVRO");

        writeAvroDataToIcebergTable(
                metastoreDir + "/variant-%s.avro".formatted(randomNameSuffix()),
                Variant.of(TEST_METADATA, TEST_OBJECT),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table);

        assertThat(query("SELECT * FROM " + tableName))
                .failure().hasMessage("unsupported type: json");
        assertThat(query(("INSERT INTO " + tableName + " VALUES (2, JSON '{\"a\":null,\"d\":\"trino\"}')")))
                .failure().hasMessageContaining("unsupported type: json");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnsupportedForFormatVersion()
    {
        assertThatThrownBy(
                () -> computeActual("CREATE TABLE test_unsupported_format_version(x int, variant JSON) WITH (format_version=2)"),
                "variant is not supported until v3");

        try (TestTable table = newTrinoTable("test_unsupported_format_version", "(x int)  WITH (format_version=2)")) {
            assertThatThrownBy(() -> computeActual("ALTER TABLE " + table.getName() + " ADD COLUMN variant JSON"), "variant is not supported until v3");
        }
    }

    @Test
    void testVariantRegisterTable()
            throws IOException
    {
        String tableName = "test_variant_register_" + randomNameSuffix();
        String registeredTableName = "registered_table_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");
        String location = table.operations().current().location();
        try {
            writeParquetDataToIcebergTable(
                    location + "/data/variant-reg-%s.parquet".formatted(randomNameSuffix()),
                    DataFiles.builder(PartitionSpec.unpartitioned()),
                    table,
                    Variant.of(TEST_METADATA, TEST_OBJECT));
            assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(registeredTableName, location));
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, JSON '{\"a\":null,\"d\":\"trino\"}')");
        }
        finally {
            deleteRecursively(Path.of(location), ALLOW_INSECURE);
            assertUpdate("DROP TABLE IF EXISTS " + registeredTableName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private BaseTable createTableWithVariantColumn(String tableName, String fileFormat)
    {
        assertUpdate(format("CREATE TABLE %s (%s int) WITH (format = '%s', format_version = 3)", tableName, INT_COL_NAME, fileFormat));
        BaseTable table = loadTable(tableName);
        addVariantColumn(table);
        return table;
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
