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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
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

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.VARIANT;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.variants.VariantTestUtil.createMetadata;
import static org.apache.iceberg.variants.VariantTestUtil.createObject;
import static org.apache.iceberg.variants.Variants.metadata;
import static org.apache.iceberg.variants.Variants.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergVariantDatatype
        extends AbstractTestQueryFramework
{
    private static final String INT_COL_NAME = "id";
    private static final String VARIANT_COL_NAME = "var";
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, INT_COL_NAME, Types.IntegerType.get()),
            Types.NestedField.required(2, VARIANT_COL_NAME, Types.VariantType.get()));
    private static final ColumnIdentity INT_COLUMN_IDENTITY = new ColumnIdentity(1, INT_COL_NAME, PRIMITIVE, ImmutableList.of());
    private static final ColumnIdentity VARIANT_COLUMN_IDENTITY = new ColumnIdentity(2, VARIANT_COL_NAME, VARIANT, ImmutableList.of());
    private static final GenericRecord RECORD = GenericRecord.create(SCHEMA);

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
                .setIcebergProperties(ImmutableMap.of("iceberg.register-table-procedure.enabled", "true"))
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
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(10.11F)), "CAST (10.11 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(-10.11F)), "CAST (-10.11 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(14.3D)), "CAST (14.3 as JSON)");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(-14.3D)), "CAST (-14.3 as JSON)");
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
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))), "JSON '\"CgsMDQ==\"'");
        testVariantTypeMappings(Variant.of(EMPTY_METADATA, Variants.of("trino")), "JSON '\"trino\"'");
        // TODO: Add tests for Array, Map and ROW https://starburstdata.atlassian.net/browse/CONNECT-588
    }

    private void testVariantTypeMappings(Variant variantData, @Language("SQL") String expectedVariant)
            throws Exception
    {
        String tableName = "test_variant_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");
        writeParquetDataToIcebergTable(
                metastoreDir + "/variant-%s.parquet".formatted(randomNameSuffix()),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table,
                variantData);

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, %s)".formatted(expectedVariant));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantTypePartitionFails()
            throws Exception
    {
        String tableName = "test_variant_partitioned_" + randomNameSuffix();
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
    void testAlterCommandFailsWhenVariantTypeIsAddedAsPartitionColumn()
            throws Exception
    {
        String tableName = "test_variant_alter_partition_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");

        writeParquetDataToIcebergTable(metastoreDir + "/variant-alter-col-partition-%s.parquet".formatted(randomNameSuffix()),
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

        writeParquetDataToIcebergTable(metastoreDir + "/variant-multiple-%s.parquet".formatted(randomNameSuffix()),
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
    void testVariantWriteFails()
            throws Exception
    {
        String tableName = "test_variant_write_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "PARQUET");

        // multiple files to kick the optimize procedure
        writeParquetDataToIcebergTable(metastoreDir + "/variant-write-%s.parquet".formatted(randomNameSuffix()),
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
        assertThatThrownBy(() -> computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE"))
                .hasMessageContaining("Trino type is null");
        assertThatThrownBy(() -> computeActual("INSERT INTO " + tableName + " VALUES (2, JSON '{\"a\":null,\"d\":\"trino\"}')"))
                .hasMessageContaining("Trino type is null");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantTypeWithOrcFormatFails()
            throws Exception
    {
        String tableName = "test_variant_orc_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "ORC");

        writeOrcDataToIcebergTable(metastoreDir + "/variant-part-%s.orc".formatted(randomNameSuffix()),
                Variant.of(TEST_METADATA, TEST_OBJECT),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table);

        assertThat(query("SELECT * FROM " + tableName))
                .failure().hasMessageContaining("Cannot read SQL type 'json' from ORC stream '.var' of type STRUCT with attributes");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testVariantTypeWithAvroFormatFails()
            throws Exception
    {
        String tableName = "test_variant_avro_" + randomNameSuffix();
        BaseTable table = createTableWithVariantColumn(tableName, "AVRO");

        writeAvroDataToIcebergTable(metastoreDir + "/variant-%s.avro".formatted(randomNameSuffix()),
                Variant.of(TEST_METADATA, TEST_OBJECT),
                DataFiles.builder(PartitionSpec.unpartitioned()),
                table);

        assertThat(query("SELECT * FROM " + tableName))
                .failure().hasMessage("unsupported type: json");
        assertUpdate("DROP TABLE " + tableName);
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

    private static void writeParquetDataToIcebergTable(String outputFilePath, DataFiles.Builder fileBuilder, Table table, Variant... variantValues)
            throws IOException
    {
        OutputFile outputFile = localOutput(outputFilePath);

        try (FileAppender<Record> writer = Parquet.write(outputFile)
                .schema(SCHEMA)
                .variantShreddingFunc((_, _) -> null)
                .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
                .build()) {
            for (Variant variantValue : variantValues) {
                Record record = RECORD.copy(INT_COL_NAME, 1, VARIANT_COL_NAME, variantValue);
                writer.add(record);
            }
            DataFile file = fileBuilder
                    .withRecordCount(1)
                    .withFileSizeInBytes(2000)
                    .withPath(outputFile.location())
                    .withFormat(FileFormat.PARQUET)
                    .build();

            table.newAppend().appendFile(file).commit();
        }
    }

    private static void writeOrcDataToIcebergTable(String outputFilePath, Variant variantValue, DataFiles.Builder fileBuilder, Table table)
            throws IOException
    {
        OutputFile outputFile = localOutput(outputFilePath);
        Record record = RECORD.copy(INT_COL_NAME, 1, VARIANT_COL_NAME, variantValue);

        try (FileAppender<Record> writer = ORC.write(outputFile)
                .schema(SCHEMA)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .build()) {
            writer.add(record);
            DataFile file = fileBuilder
                    .withRecordCount(1)
                    .withFileSizeInBytes(2000)
                    .withPath(outputFile.location())
                    .withFormat(FileFormat.ORC)
                    .build();

            table.newAppend().appendFile(file).commit();
        }
    }

    private static void writeAvroDataToIcebergTable(String outputFilePath, Variant variantValue, DataFiles.Builder fileBuilder, Table table)
            throws IOException
    {
        OutputFile outputFile = localOutput(outputFilePath);
        Record record = RECORD.copy(INT_COL_NAME, 1, VARIANT_COL_NAME, variantValue);

        try (FileAppender<Record> writer = Avro.write(outputFile)
                .schema(SCHEMA)
                .createWriterFunc(DataWriter::create)
                .build()) {
            writer.add(record);
            DataFile file = fileBuilder
                    .withRecordCount(1)
                    .withFileSizeInBytes(2000)
                    .withPath(outputFile.location())
                    .withFormat(FileFormat.AVRO)
                    .build();

            table.newAppend().appendFile(file).commit();
        }
    }

    private BaseTable createTableWithVariantColumn(String tableName, String fileFormat)
    {
        assertUpdate(format("CREATE TABLE %s (%s int) WITH (format = '" + fileFormat + "', format_version = 3)", tableName, INT_COL_NAME));
        BaseTable table = loadTable(tableName);
        addVariantColumn(table);
        return table;
    }

    private static void addVariantColumn(Table table)
    {
        UpdateSchema updateSchema = table.updateSchema();
        updateSchema.addColumn(VARIANT_COL_NAME, Types.VariantType.get());
        updateSchema.commit();
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
