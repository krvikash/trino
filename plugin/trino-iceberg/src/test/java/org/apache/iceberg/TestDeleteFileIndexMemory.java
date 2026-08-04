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
package org.apache.iceberg;

import io.trino.plugin.iceberg.IcebergConfig;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.iceberg.IcebergDeleteFilesMemoryEstimator.EQUALITY_DELETE_FILE_BASE_MEMORY;
import static io.trino.plugin.iceberg.IcebergDeleteFilesMemoryEstimator.EQUALITY_DELETE_FILE_MEMORY_PER_COLUMN;
import static io.trino.plugin.iceberg.IcebergDeleteFilesMemoryEstimator.equalityDeleteFileMemory;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestDeleteFileIndexMemory
{
    private static final int DELETE_FILE_COUNT = 10_000;
    private static final int PARTITION_COUNT = 100;
    private static final long MEMORY_PER_POSITIONAL_DELETE_FILE =
            new IcebergConfig().getRemoteSplitsGenerationMemoryPerPositionalDeleteFile().toBytes();
    private static final long MEMORY_PER_EQUALITY_DELETE_FILE =
            new IcebergConfig().getRemoteSplitsGenerationMemoryPerEqualityDeleteFile().toBytes();

    @Test
    public void testPartitionedSingleColumnDeleteFiles()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .build();
        measure("partitioned, 1 equality column", schema, spec, Set.of(), 1);
    }

    @Test
    public void testPartitionedDeleteFilesWithNullableColumn()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "region", Types.StringType.get()),
                Types.NestedField.optional(3, "nullable_key", Types.LongType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .build();
        measure("partitioned, 2 equality columns (1 nullable)",
                schema,
                spec,
                Set.of(3),
                1,
                3);
    }

    @Test
    public void testUnpartitionedDeleteFilesWithFiveColumns()
    {
        Schema schema = schemaWithLongColumns(5);
        PartitionSpec spec = PartitionSpec.builderFor(schema).build();
        measure("unpartitioned, 5 equality columns", schema, spec, Set.of(), equalityFieldIds(5));
    }

    @Test
    public void testUnpartitionedDeleteFilesWithTenColumns()
    {
        Schema schema = schemaWithLongColumns(10);
        PartitionSpec spec = PartitionSpec.builderFor(schema).build();
        measure("unpartitioned, 10 equality columns", schema, spec, Set.of(), equalityFieldIds(10));
    }

    @Test
    public void testUnpartitionedDeleteFilesWithTwentyColumns()
    {
        // past ~13 columns the flat configured default alone under-reserves; this scenario is
        // covered only by the per-column formula
        Schema schema = schemaWithLongColumns(20);
        PartitionSpec spec = PartitionSpec.builderFor(schema).build();
        measure("unpartitioned, 20 equality columns", schema, spec, Set.of(), equalityFieldIds(20));
    }

    @Test
    public void testEqualityDeleteFileMemoryFloorsAtConfiguredDefault()
    {
        // narrow identifier schemas stay on the configured floor; wide ones grow past it
        assertThat(equalityDeleteFileMemory(0, MEMORY_PER_EQUALITY_DELETE_FILE)).isEqualTo(MEMORY_PER_EQUALITY_DELETE_FILE);
        assertThat(equalityDeleteFileMemory(1, MEMORY_PER_EQUALITY_DELETE_FILE)).isEqualTo(MEMORY_PER_EQUALITY_DELETE_FILE);
        assertThat(equalityDeleteFileMemory(20, MEMORY_PER_EQUALITY_DELETE_FILE))
                .isEqualTo(EQUALITY_DELETE_FILE_BASE_MEMORY + 20 * EQUALITY_DELETE_FILE_MEMORY_PER_COLUMN)
                .isGreaterThan(MEMORY_PER_EQUALITY_DELETE_FILE);
    }

    @Test
    public void testPartitionedDeletionVectors()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .build();

        List<DeleteFile> deleteFiles = new ArrayList<>(DELETE_FILE_COUNT);
        for (int i = 0; i < DELETE_FILE_COUNT; i++) {
            int partition = i % PARTITION_COUNT;
            DeleteFile deleteFile = FileMetadata.deleteFileBuilder(spec)
                    .ofPositionDeletes()
                    .withPath(deletionVectorPath(i))
                    .withFormat(FileFormat.PUFFIN)
                    .withPartition(partition(spec, partition))
                    .withFileSizeInBytes(16L * 1024 * 1024)
                    .withReferencedDataFile(dataFilePath(spec, partition, i))
                    .withContentOffset((long) (i % PARTITION_COUNT) * 1_024)
                    .withContentSizeInBytes(1_024)
                    .withRecordCount(1_000)
                    .build();
            ((BaseFile<?>) deleteFile).setDataSequenceNumber((long) i + 1);
            deleteFiles.add(deleteFile);
        }

        DeleteFileIndex index = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(Map.of(spec.specId(), spec))
                .schemasById(Map.of(schema.schemaId(), schema))
                .build();

        measure("partitioned v3 deletion vectors", index, MEMORY_PER_POSITIONAL_DELETE_FILE);
    }

    private static void measure(
            String scenario,
            Schema schema,
            PartitionSpec spec,
            Set<Integer> nullableFields,
            int... equalityFieldIds)
    {
        int partitionCount = spec.isPartitioned() ? PARTITION_COUNT : 1;
        List<DeleteFile> deleteFiles = new ArrayList<>(DELETE_FILE_COUNT);
        for (int i = 0; i < DELETE_FILE_COUNT; i++) {
            long lowerBound = (long) i * 1_000;
            int partition = i % partitionCount;

            DeleteFile deleteFile = FileMetadata.deleteFileBuilder(spec)
                    .ofEqualityDeletes(equalityFieldIds)
                    .withPath("s3://production-style-bucket/warehouse/customer_events/" +
                            partitionPath(spec, partition) +
                            "/delete/00000-" + i + "-12345678-1234-1234-1234-123456789abc.parquet")
                    .withFormat(FileFormat.PARQUET)
                    .withPartition(partition(spec, partition))
                    .withFileSizeInBytes(64L * 1024 * equalityFieldIds.length)
                    .withMetrics(metrics(equalityFieldIds, nullableFields, 1_000, lowerBound, lowerBound + 999))
                    .build();
            ((BaseFile<?>) deleteFile).setDataSequenceNumber((long) i + 1);
            deleteFiles.add(deleteFile);
        }

        DeleteFileIndex index = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(Map.of(spec.specId(), spec))
                .schemasById(Map.of(schema.schemaId(), schema))
                .build();

        for (int partition = 0; partition < partitionCount; partition++) {
            index.forDataFile(0, representativeDataFile(spec, partition, equalityFieldIds, nullableFields));
        }

        // assert against what production actually reserves for this column width: the configured
        // default floored formula, with the identifier-field count driving the per-column part
        measure(scenario, index, equalityDeleteFileMemory(equalityFieldIds.length, MEMORY_PER_EQUALITY_DELETE_FILE));
    }

    private static int[] equalityFieldIds(int columnCount)
    {
        int[] fieldIds = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            fieldIds[i] = i + 1;
        }
        return fieldIds;
    }

    /**
     * Asserts the production default covers the measured footprint. Only an upper-bound check:
     * under-reserving is the failure that matters — a generous default merely reserves ahead of
     * time what the query would use anyway.
     */
    private static void measure(String scenario, DeleteFileIndex index, long configuredBytesPerFile)
    {
        System.setProperty("jol.magicFieldOffset", "true");
        GraphLayout layout = GraphLayout.parseInstance(index);
        long totalBytes = layout.totalSize();
        long bytesPerFile = (totalBytes + DELETE_FILE_COUNT - 1) / DELETE_FILE_COUNT;

        String result = "DeleteFileIndex (%s): %,d bytes total, %,d bytes/delete file"
                .formatted(scenario, totalBytes, bytesPerFile);
        System.out.println("%s%n%s".formatted(result, layout.toFootprint()));

        assertThat(bytesPerFile)
                .as(result)
                .isLessThanOrEqualTo(configuredBytesPerFile);
    }

    private static DataFile representativeDataFile(
            PartitionSpec spec,
            int partition,
            int[] equalityFieldIds,
            Set<Integer> nullableFields)
    {
        return DataFiles.builder(spec)
                .withPath("s3://production-style-bucket/warehouse/customer_events/" +
                        partitionPath(spec, partition) + "/data.parquet")
                .withFormat(FileFormat.PARQUET)
                .withPartition(partition(spec, partition))
                .withFileSizeInBytes(128L * 1024 * 1024)
                .withMetrics(metrics(equalityFieldIds, nullableFields, 100_000, 0, Long.MAX_VALUE))
                .build();
    }

    private static Metrics metrics(
            int[] fieldIds,
            Set<Integer> nullableFields,
            long rowCount,
            long lowerBound,
            long upperBound)
    {
        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
        for (int fieldId : fieldIds) {
            columnSizes.put(fieldId, rowCount * Long.BYTES);
            valueCounts.put(fieldId, rowCount);
            nullValueCounts.put(fieldId, nullableFields.contains(fieldId) ? rowCount / 10 : 0);
            lowerBounds.put(fieldId, Conversions.toByteBuffer(Types.LongType.get(), lowerBound));
            upperBounds.put(fieldId, Conversions.toByteBuffer(Types.LongType.get(), upperBound));
        }
        return new Metrics(
                rowCount,
                Map.copyOf(columnSizes),
                Map.copyOf(valueCounts),
                Map.copyOf(nullValueCounts),
                null,
                Map.copyOf(lowerBounds),
                Map.copyOf(upperBounds));
    }

    private static Schema schemaWithLongColumns(int columnCount)
    {
        List<Types.NestedField> columns = new ArrayList<>(columnCount);
        for (int fieldId = 1; fieldId <= columnCount; fieldId++) {
            columns.add(Types.NestedField.required(fieldId, "column_" + fieldId, Types.LongType.get()));
        }
        return new Schema(columns);
    }

    private static String partitionPath(PartitionSpec spec, int partition)
    {
        return spec.isPartitioned() ? "region=" + partition : "unpartitioned";
    }

    private static String dataFilePath(PartitionSpec spec, int partition, int fileNumber)
    {
        return "s3://production-style-bucket/warehouse/customer_events/" +
                partitionPath(spec, partition) +
                "/data/00000-" + fileNumber + "-12345678-1234-1234-1234-123456789abc.parquet";
    }

    private static String deletionVectorPath(int fileNumber)
    {
        return "s3://production-style-bucket/warehouse/customer_events/metadata/dv-" +
                (fileNumber / PARTITION_COUNT) + ".puffin";
    }

    private static PartitionData partition(PartitionSpec spec, int partitionNumber)
    {
        PartitionData partition = new PartitionData(spec.partitionType());
        if (spec.isPartitioned()) {
            partition.set(0, "region-" + partitionNumber);
        }
        return partition;
    }
}
