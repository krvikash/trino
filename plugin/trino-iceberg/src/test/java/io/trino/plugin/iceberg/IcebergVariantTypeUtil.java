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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
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
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;

import java.io.IOException;

import static org.apache.iceberg.Files.localOutput;

final class IcebergVariantTypeUtil
{
    public static final String INT_COL_NAME = "id";
    public static final String VARIANT_COL_NAME = "var";
    public static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, INT_COL_NAME, Types.IntegerType.get()),
            Types.NestedField.required(2, VARIANT_COL_NAME, Types.VariantType.get()));
    private static final GenericRecord RECORD = GenericRecord.create(SCHEMA);

    private IcebergVariantTypeUtil() {}

    public static void addVariantColumn(Table table)
    {
        UpdateSchema updateSchema = table.updateSchema();
        updateSchema.addColumn(VARIANT_COL_NAME, Types.VariantType.get());
        updateSchema.commit();
    }

    public static void writeParquetDataToIcebergTable(String outputFilePath, DataFiles.Builder fileBuilder, Table table, Variant... variantValues)
            throws IOException
    {
        OutputFile outputFile = localOutput(outputFilePath);

        FileAppender<Record> writer = Parquet.write(outputFile)
                .schema(SCHEMA)
                .variantShreddingFunc((_, _) -> null)
                .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
                .build();
        try (writer) {
            for (Variant variantValue : variantValues) {
                Record record = RECORD.copy(INT_COL_NAME, 1, VARIANT_COL_NAME, variantValue);
                writer.add(record);
            }
        }
        DataFile file = fileBuilder
                .withRecordCount(1)
                // file size must be exact: reads trust it as the file length
                .withFileSizeInBytes(writer.length())
                .withPath(outputFile.location())
                .withFormat(FileFormat.PARQUET)
                .build();

        table.newAppend().appendFile(file).commit();
    }

    public static void writeOrcDataToIcebergTable(String outputFilePath, Variant variantValue, DataFiles.Builder fileBuilder, Table table)
            throws IOException
    {
        OutputFile outputFile = localOutput(outputFilePath);
        Record record = RECORD.copy(INT_COL_NAME, 1, VARIANT_COL_NAME, variantValue);

        FileAppender<Record> writer = ORC.write(outputFile)
                .schema(SCHEMA)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .build();
        try (writer) {
            writer.add(record);
        }
        DataFile file = fileBuilder
                .withRecordCount(1)
                // file size must be exact: reads trust it as the file length
                .withFileSizeInBytes(writer.length())
                .withPath(outputFile.location())
                .withFormat(FileFormat.ORC)
                .build();

        table.newAppend().appendFile(file).commit();
    }

    public static void writeAvroDataToIcebergTable(String outputFilePath, Variant variantValue, DataFiles.Builder fileBuilder, Table table)
            throws IOException
    {
        OutputFile outputFile = localOutput(outputFilePath);
        Record record = RECORD.copy(INT_COL_NAME, 1, VARIANT_COL_NAME, variantValue);

        FileAppender<Record> writer = Avro.write(outputFile)
                .schema(SCHEMA)
                .createWriterFunc(DataWriter::create)
                .build();
        try (writer) {
            writer.add(record);
        }
        DataFile file = fileBuilder
                .withRecordCount(1)
                // file size must be exact: reads trust it as the file length
                .withFileSizeInBytes(writer.length())
                .withPath(outputFile.location())
                .withFormat(FileFormat.AVRO)
                .build();

        table.newAppend().appendFile(file).commit();
    }
}
